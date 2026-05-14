package backup

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	a "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/backup-go/internal/processors"
	"github.com/aerospike/backup-go/io/aerospike"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe"
)

// PartitionTask represents a single partition backup task.
type PartitionTask struct {
	partID  int
	retries int
}

// PartitionWorkerPool manages a pool of workers that process Aerospike partitions individually.
// This is an alternative to the global M-to-N pipeline architecture.
type PartitionWorkerPool struct {
	bh          *BackupHandler
	concurrency int
	queue       chan PartitionTask
	wg          sync.WaitGroup
	taskWg      sync.WaitGroup
	errChan     chan error
	cancel      context.CancelFunc
	ctx         context.Context
}

// NewPartitionWorkerPool creates a new PartitionWorkerPool.
func NewPartitionWorkerPool(ctx context.Context, bh *BackupHandler, concurrency int) *PartitionWorkerPool {
	ctx, cancel := context.WithCancel(ctx)

	return &PartitionWorkerPool{
		bh:          bh,
		concurrency: concurrency,
		queue:       make(chan PartitionTask, 4096), // Buffer large enough for all 4096 partitions
		errChan:     make(chan error, 1),
		cancel:      cancel,
		ctx:         ctx,
	}
}

// Run starts the worker pool and blocks until all partitions are processed or an unrecoverable error occurs.
func (p *PartitionWorkerPool) Run() error {
	// Populate queue with all 4096 partitions
	for i := range 4096 {
		p.taskWg.Add(1)

		p.queue <- PartitionTask{partID: i, retries: 0}
	}

	// Start workers
	for i := range p.concurrency {
		p.wg.Add(1)

		worker := &PartitionWorker{
			id:   i,
			pool: p,
			bh:   p.bh,
		}

		go worker.start()
	}

	// Wait for all tasks to complete in a separate goroutine to close the queue
	go func() {
		p.taskWg.Wait()
		close(p.queue)
	}()

	// Wait for all workers to finish
	p.wg.Wait()
	close(p.errChan)

	return <-p.errChan
}

// reportError reports a fatal error and cancels the pool context.
func (p *PartitionWorkerPool) reportError(err error) {
	select {
	case p.errChan <- err:
		p.cancel() // cancel other workers on first fatal error
	default:
	}
}

// trackingWriter wraps a Writer to keep track of all created files so they can be deleted on failure.
type trackingWriter struct {
	writer Writer
	files  []string
	mu     sync.Mutex
}

func (t *trackingWriter) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	t.mu.Lock()
	t.files = append(t.files, filename)
	t.mu.Unlock()

	return t.writer.NewWriter(ctx, filename)
}

func (t *trackingWriter) GetType() string {
	return t.writer.GetType()
}

func (t *trackingWriter) RemoveFiles(ctx context.Context) error {
	return t.writer.RemoveFiles(ctx)
}

func (t *trackingWriter) Remove(ctx context.Context, path string) error {
	return t.writer.Remove(ctx, path)
}

func (t *trackingWriter) GetOptions() options.Options {
	return t.writer.GetOptions()
}

func (t *trackingWriter) deleteFiles(ctx context.Context, logger *slog.Logger) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, f := range t.files {
		err := t.writer.Remove(ctx, f)
		if err != nil {
			logger.Warn("failed to delete data for failed partition",
				slog.String("filename", f),
				slog.Any("error", err),
			)
		}
	}
}

type PartitionWorker struct {
	id   int
	pool *PartitionWorkerPool
	bh   *BackupHandler
}

// start begins the worker loop.
func (w *PartitionWorker) start() {
	defer w.pool.wg.Done()

	for task := range w.pool.queue {
		if w.pool.ctx.Err() != nil {
			// Context canceled, exit
			return
		}

		err := w.processPartition(w.pool.ctx, task.partID)
		if err != nil {
			w.bh.logger.Error("worker failed to process partition",
				slog.Int("workerId", w.id),
				slog.Int("partition", task.partID),
				slog.Any("error", err),
			)

			if task.retries < 3 {
				task.retries++
				w.pool.queue <- task // Put back to the end of the queue

				w.bh.logger.Info("retrying partition", slog.Int("partition", task.partID), slog.Int("retry", task.retries))
			} else {
				w.pool.reportError(fmt.Errorf("partition %d failed after %d retries: %w", task.partID, 3, err))
				w.pool.taskWg.Done()
			}

			// Worker dies and spawns a new one to replace itself
			w.pool.wg.Add(1)

			newWorker := &PartitionWorker{
				id:   w.id, // reuse ID
				pool: w.pool,
				bh:   w.bh,
			}

			go newWorker.start()

			return
		}

		// Success
		w.pool.taskWg.Done()
	}
}

// processPartition reads a single partition and writes it to its dedicated writer.
func (w *PartitionWorker) processPartition(ctx context.Context, partID int) (err error) {
	// 1. Create Reader for this specific partition
	scanPolicy := *w.bh.config.ScanPolicy
	scanPolicy.RawCDT = true
	partitionFilter := a.NewPartitionFilterById(partID)

	readerConfig := w.bh.readerProcessor.newRecordReaderConfig(partitionFilter, &scanPolicy)
	reader := aerospike.NewRecordReader(
		ctx,
		w.bh.aerospikeClient,
		readerConfig,
		w.bh.logger.With(slog.Int("partition", partID)),
		aerospike.NewRecordsetCloser(),
	)
	// reader is closed by the pipeline

	// 2. Create Writer for this specific partition
	tw := &trackingWriter{writer: w.bh.writerProcessor.writer}

	// Delete generated data on failure
	defer func() {
		if err != nil {
			tw.deleteFiles(context.Background(), w.bh.logger)
		}
	}()

	wp, err := newFileWriterProcessor[*models.Token](
		w.bh.config.OutputFilePrefix,
		w.bh.stateSuffixGenerator,
		tw,
		w.bh.encoder,
		w.bh.writerProcessor.encryptionKey,
		w.bh.config.CompressionPolicy,
		w.bh.state,
		w.bh.stats,
		w.bh.kbpsCollector,
		w.bh.config.FileLimit,
		1,
		w.bh.logger,
	)
	if err != nil {
		return fmt.Errorf("failed to create writer processor for partition %d: %w", partID, err)
	}

	rawWriter, err := wp.newWriter(ctx, partID)
	if err != nil {
		return fmt.Errorf("failed to create writer for partition %d: %w", partID, err)
	}

	dataWriter := wp.createDataWriter(rawWriter, partID)

	// 3. Setup data processors
	dataProcessors := newDataProcessor(
		processors.NewRecordCounter[*models.Token](&w.bh.stats.ReadRecords),
		processors.NewVoidTimeSetter[*models.Token](w.bh.logger),
		processors.NewTPSLimiter[*models.Token](ctx, w.bh.config.RecordsPerSecond),
	)

	// 4. Create and run pipeline for this single partition
	pl, err := pipe.NewPipe(
		dataProcessors,
		[]pipe.Reader[*models.Token]{reader},
		[]pipe.Writer[*models.Token]{dataWriter},
		w.bh.limiter,
		pipe.Fixed,
	)
	if err != nil {
		rawWriter.Close()
		return fmt.Errorf("failed to create pipe for partition %d: %w", partID, err)
	}

	// Run blocks until the partition is fully read and written
	err = pl.Run(ctx)
	if err != nil {
		return err
	}

	return nil
}
