// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// Command gcpwritertest stress-tests the retry behaviour of
// backup-go's io/storage/gcp/storage.Writer.
//
// It builds a GCP storage client using the very same defaults as absctl
// (RetryAlways, initial backoff 60s, max backoff 90s, multiplier 2, 10
// attempts), wraps it with the ASB encoder and gcpStorage.Writer, and then
// writes the same ~100-120 byte record into several 1GB objects in parallel,
// using a 5MB chunk size. This forces multi-chunk resumable uploads, which is
// exactly the code path where transient 5xx/429 errors and the retry/deadline
// interaction show up. Any error returned by the writer chain is reported.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	gcpStorage "cloud.google.com/go/storage"
	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go"
	gcs "github.com/aerospike/backup-go/io/storage/gcp/storage"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/googleapis/gax-go/v2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// Writer parameters requested for the test.
const (
	defaultChunkSize = 5 * 1024 * 1024    // 5 MB.
	defaultFileSize  = 1024 * 1024 * 1024 // 1 GB per object.

	// Target size of a single encoded ASB record.
	minRecordSize = 100
	maxRecordSize = 120

	// GCP client defaults, copied from absctl (internal/models/default_values.go).
	defaultRetryMaxAttempts       = 1
	defaultRetryBackoffMaxMs      = 90000 // 90s.
	defaultRetryBackoffInitMs     = 60000 // 60s.
	defaultRetryBackoffMultiplier = 2.0
	defaultMaxConnsPerHost        = 0      // No limit.
	defaultRequestTimeoutMs       = 600000 // 600s.

	testNamespace = "test"
	testSet       = "testset"
	binName       = "b"

	// bytesPerMB is used to convert raw byte counters into megabytes for logs.
	bytesPerMB = 1024 * 1024
)

type config struct {
	bucket         string
	folder         string
	keyFile        string
	endpoint       string
	parallelism    int
	chunkSize      int
	fileSize       int64
	removeFiles    bool
	reportInterval time.Duration
}

func main() {
	cfg := parseFlags()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	if cfg.bucket == "" {
		logger.Error("bucket name is required, use -bucket")
		os.Exit(1)
	}

	// Build the GCP client exactly as absctl does. Use a plain background
	// context here so the auth token source is not tied to the interrupt.
	client, err := newGcpClient(context.Background(), cfg)
	if err != nil {
		logger.Error("failed to create GCP client", slog.Any("error", err))
		os.Exit(1)
	}

	// ASB encoder, same as a real backup.
	encoder := backup.NewEncoder[*models.Token](backup.EncoderTypeASB, testNamespace, false, false)

	// Prepare a single record whose encoded size lands in [100, 120] bytes.
	recordBytes, err := buildRecordBytes(encoder)
	if err != nil {
		logger.Error("failed to build record", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("record prepared", slog.Int("encoded_bytes", len(recordBytes)))

	// Pre-build one chunk-sized block of concatenated records. Writing big
	// blocks (instead of ~110-byte pieces) is what keeps the client streaming
	// at network speed - millions of tiny writes per 1GB file would otherwise
	// bottleneck on the storage.Writer's internal pipe and never generate
	// enough load to stress the upload/retry path.
	block := buildBlock(recordBytes, cfg.chunkSize)
	logger.Info("write block prepared",
		slog.Int("block_bytes", len(block)),
		slog.Int("records_per_block", len(block)/len(recordBytes)),
	)

	// Run context is cancelled on Ctrl+C (SIGINT) or SIGTERM. All in-flight
	// uploads use this context, so an interrupt stops them promptly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Build the writer under test.
	opts := []options.Opt{
		options.WithDir(cfg.folder),
		options.WithChunkSize(cfg.chunkSize),
		options.WithLogger(logger),
	}
	if cfg.removeFiles {
		opts = append(opts, options.WithRemoveFiles())
	} else {
		opts = append(opts, options.WithSkipDirCheck())
	}

	writer, err := gcs.NewWriter(ctx, client, cfg.bucket, opts...)
	if err != nil {
		logger.Error("failed to create writer", slog.Any("error", err))
		os.Exit(1)
	}

	logger.Info("starting write test, press Ctrl+C to stop",
		slog.Int("parallelism", cfg.parallelism),
		slog.Int("chunk_size", cfg.chunkSize),
		slog.Int64("file_size", cfg.fileSize),
		slog.String("bucket", cfg.bucket),
		slog.String("folder", cfg.folder),
	)

	start := time.Now()
	var (
		filesDone  atomic.Int64
		failed     atomic.Int64
		totalBytes atomic.Int64
	)

	// speedReporter logs upload throughput based on the shared byte counter
	// until ctx is cancelled. reporterDone lets main wait for its final flush.
	reporterDone := make(chan struct{})
	go speedReporter(ctx, &totalBytes, cfg.reportInterval, logger, reporterDone)

	var wg sync.WaitGroup
	for i := 0; i < cfg.parallelism; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			// Keep writing new files until interrupted.
			for ctx.Err() == nil {
				err := writeOneFile(ctx, writer, encoder, block, cfg.fileSize, &totalBytes, logger, worker)
				switch {
				case err == nil:
					filesDone.Add(1)
				case ctx.Err() != nil || errors.Is(err, context.Canceled):
					// Interrupted mid-write; clean shutdown, not a real failure.
					return
				default:
					// A genuine writer error - report it and keep stressing.
					failed.Add(1)
					logger.Error("worker failed",
						slog.Int("worker", worker),
						slog.Any("error", err),
					)
					os.Exit(1)
				}
			}
		}(i)
	}

	// Wait for the interrupt, then let in-flight workers unwind.
	<-ctx.Done()
	logger.Info("interrupt received, stopping workers...")
	wg.Wait()
	<-reporterDone

	elapsed := time.Since(start)
	var overallMBs float64
	if sec := elapsed.Seconds(); sec > 0 {
		overallMBs = float64(totalBytes.Load()) / bytesPerMB / sec
	}

	logger.Info("write test finished",
		slog.Duration("elapsed", elapsed),
		slog.Int("total_workers", cfg.parallelism),
		slog.Int64("files_written", filesDone.Load()),
		slog.Int64("errors", failed.Load()),
		slog.Int64("total_mb", totalBytes.Load()/bytesPerMB),
		slog.Float64("overall_mb_s", overallMBs),
	)

	if failed.Load() > 0 {
		os.Exit(1)
	}
}

// speedReporter periodically logs instantaneous and average upload throughput,
// computed from the delta of the shared byte counter between ticks. It runs
// until ctx is cancelled, then closes done so main can wait for it.
func speedReporter(
	ctx context.Context,
	totalBytes *atomic.Int64,
	interval time.Duration,
	logger *slog.Logger,
	done chan<- struct{},
) {
	defer close(done)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	lastTime := start
	lastBytes := int64(0)

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			curBytes := totalBytes.Load()

			intervalSec := now.Sub(lastTime).Seconds()
			totalSec := now.Sub(start).Seconds()

			var instMBs, avgMBs float64
			if intervalSec > 0 {
				instMBs = float64(curBytes-lastBytes) / bytesPerMB / intervalSec
			}
			if totalSec > 0 {
				avgMBs = float64(curBytes) / bytesPerMB / totalSec
			}

			logger.Info("upload speed",
				slog.Float64("current_mb_s", instMBs),
				slog.Float64("avg_mb_s", avgMBs),
				slog.Int64("total_mb", curBytes/bytesPerMB),
			)

			lastTime = now
			lastBytes = curBytes
		}
	}
}

// writeOneFile writes header + repeated record to a single object until fileSize
// bytes have been written, then closes it. Errors from Write/Close are returned.
// Every successful write also advances totalBytes for the speed reporter.
func writeOneFile(
	ctx context.Context,
	writer *gcs.Writer,
	encoder backup.Encoder[*models.Token],
	block []byte,
	fileSize int64,
	totalBytes *atomic.Int64,
	logger *slog.Logger,
	worker int,
) error {
	filename := encoder.GenerateFilename("", "")

	w, err := writer.NewWriter(ctx, filename)
	if err != nil {
		return fmt.Errorf("open writer for %s: %w", filename, err)
	}

	// Write the ASB header first, exactly like a real backup records file.
	header := encoder.GetHeader(0, true)

	var written int64
	n, err := w.Write(header)
	written += int64(n)
	totalBytes.Add(int64(n))
	if err != nil {
		// Best-effort close, but return the original write error.
		_ = w.Close()
		return fmt.Errorf("write header to %s: %w", filename, err)
	}

	// Write full blocks until the target size is reached. The last block may
	// overshoot fileSize by up to len(block); that is intentional to keep
	// records intact and is irrelevant for a write/retry stress test.
	for written < fileSize {
		if ctx.Err() != nil {
			_ = w.Close()
			return ctx.Err()
		}
		n, err = w.Write(block)
		written += int64(n)
		totalBytes.Add(int64(n))
		if err != nil {
			_ = w.Close()
			return fmt.Errorf("write block to %s (after %d bytes): %w", filename, written, err)
		}
	}

	logger.Info("closing file",
		slog.Int("worker", worker),
		slog.String("file", filename),
		slog.Int64("bytes", written),
	)

	if err = w.Close(); err != nil {
		return fmt.Errorf("close %s (after %d bytes): %w", filename, written, err)
	}
	return nil
}

// buildRecordBytes creates a record and encodes it, padding a single string bin
// until the encoded size is in the requested [minRecordSize, maxRecordSize] range.
func buildRecordBytes(encoder backup.Encoder[*models.Token]) ([]byte, error) {
	buf := new(bytes.Buffer)
	for valueLen := 1; valueLen <= 256; valueLen++ {
		buf.Reset()
		token, err := buildRecordToken(valueLen)
		if err != nil {
			return nil, err
		}
		if err = encoder.EncodeToken(token, buf); err != nil {
			return nil, fmt.Errorf("encode token: %w", err)
		}
		if size := buf.Len(); size >= minRecordSize && size <= maxRecordSize {
			out := make([]byte, size)
			copy(out, buf.Bytes())
			return out, nil
		}
	}
	return nil, errors.New("could not build a record within the requested size range")
}

func buildRecordToken(valueLen int) (*models.Token, error) {
	key, err := a.NewKey(testNamespace, testSet, 1)
	if err != nil {
		return nil, fmt.Errorf("create key: %w", err)
	}
	rec := &models.Record{
		Record: &a.Record{
			Key:        key,
			Bins:       a.BinMap{binName: strings.Repeat("x", valueLen)},
			Generation: 1,
		},
		VoidTime: models.VoidTimeNeverExpire,
	}
	return models.NewRecordToken(rec, 0, nil), nil
}

// buildBlock concatenates whole copies of record into a buffer close to
// blockSize bytes. Writing this block (instead of a single record) turns
// millions of tiny writes per file into a handful of chunk-sized writes.
func buildBlock(record []byte, blockSize int) []byte {
	count := blockSize / len(record)
	if count < 1 {
		count = 1
	}
	block := make([]byte, 0, count*len(record))
	for i := 0; i < count; i++ {
		block = append(block, record...)
	}
	return block
}

// newGcpClient builds a *storage.Client with the same options and retry policy
// that absctl configures by default (internal/storage/clients.go).
func newGcpClient(ctx context.Context, cfg config) (*gcpStorage.Client, error) {
	opts := make([]option.ClientOption, 0)

	if cfg.endpoint != "" {
		// Used with fake-gcs for tests only.
		opts = append(opts, option.WithEndpoint(cfg.endpoint), option.WithoutAuthentication())
	} else {
		transport, err := getGcpTransport(ctx, cfg.keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to get GCP transport: %w", err)
		}
		opts = append(opts, option.WithHTTPClient(newHTTPClient(transport, defaultRequestTimeoutMs)))
	}

	gcpClient, err := gcpStorage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCP client: %w", err)
	}

	backoff := gax.Backoff{
		Initial:    time.Duration(defaultRetryBackoffInitMs) * time.Millisecond,
		Max:        time.Duration(defaultRetryBackoffMaxMs) * time.Millisecond,
		Multiplier: defaultRetryBackoffMultiplier,
	}
	gcpClient.SetRetry(
		gcpStorage.WithPolicy(gcpStorage.RetryAlways),
		gcpStorage.WithBackoff(backoff),
		gcpStorage.WithMaxAttempts(defaultRetryMaxAttempts),
	)

	return gcpClient, nil
}

func getGcpTransport(ctx context.Context, keyFile string) (http.RoundTripper, error) {
	var (
		transport = newTransport(defaultMaxConnsPerHost)
		ts        oauth2.TokenSource
		err       error
	)

	if keyFile != "" {
		creds, err := getGcpAuth(ctx, keyFile)
		if err != nil {
			return nil, err
		}
		ts = creds.TokenSource
	} else {
		// ADC: uses attached VM service account or GOOGLE_APPLICATION_CREDENTIALS.
		ts, err = google.DefaultTokenSource(ctx, gcpStorage.ScopeReadWrite)
		if err != nil {
			return nil, fmt.Errorf("failed to get ADC token source: %w", err)
		}
	}

	return newAuthTransport(transport, ts), nil
}

func getGcpAuth(ctx context.Context, keyFile string) (*google.Credentials, error) {
	jsonKey, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file %s: %w", keyFile, err)
	}
	creds, err := google.CredentialsFromJSONWithType(
		ctx,
		jsonKey,
		google.ServiceAccount,
		gcpStorage.ScopeReadWrite,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON key file %s: %w", keyFile, err)
	}
	return creds, nil
}

func newTransport(maxConnsPerHost int) *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxConnsPerHost:     maxConnsPerHost,
		IdleConnTimeout:     120 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ReadBufferSize:      64 * 1024,
		ForceAttemptHTTP2:   true,
	}
}

func newAuthTransport(baseTransport http.RoundTripper, tokenSource oauth2.TokenSource) *oauth2.Transport {
	return &oauth2.Transport{
		Base:   baseTransport,
		Source: tokenSource,
	}
}

func newHTTPClient(transport http.RoundTripper, requestTimeoutMs int) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   time.Duration(requestTimeoutMs) * time.Millisecond,
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.bucket, "bucket", "", "GCS bucket name (required)")
	flag.StringVar(&cfg.folder, "folder", "writer-retry-test", "Destination folder (prefix) inside the bucket")
	flag.StringVar(&cfg.keyFile, "key-file", "", "Path to a service account JSON key; empty uses ADC")
	flag.StringVar(&cfg.endpoint, "endpoint", "", "Alternate endpoint (e.g. fake-gcs); empty uses real GCP")
	flag.IntVar(&cfg.parallelism, "parallelism", 80, "Number of concurrent files to write")
	flag.IntVar(&cfg.chunkSize, "chunk-size", defaultChunkSize, "Upload chunk size in bytes")
	flag.Int64Var(&cfg.fileSize, "file-size", defaultFileSize, "Size of each written object in bytes")
	flag.BoolVar(&cfg.removeFiles, "remove-files", true, "Remove existing files in the folder before writing")
	flag.DurationVar(&cfg.reportInterval, "report-interval", 5*time.Second, "How often to log upload speed")
	flag.Parse()
	return cfg
}
