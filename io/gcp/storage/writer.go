package storage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	fileType         = "application/octet-stream"
	defaultChunkSize = 5 * 1024 * 1024
)

// Writer represents a GCP storage writer.
type Writer struct {
	client *storage.Client
	config *Config
}

// NewWriter returns new GCP storage writer.
func NewWriter(ctx context.Context, client *storage.Client, config *Config, removeFiles bool) (*Writer, error) {
	// Check if backup dir is empty.
	isEmpty, err := isEmptyBucket(ctx, client, config.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket is empty: %w", err)
	}

	if !isEmpty && !removeFiles {
		return nil, fmt.Errorf("backup bucket must be empty or set removeFiles = true")
	}

	// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucket.
	if err = removeFilesFromBucket(ctx, client, config.Bucket); err != nil {
		return nil, fmt.Errorf("failed to remove files from bucket: %w", err)
	}

	return &Writer{
		client: client,
		config: config,
	}, nil
}

// NewWriter testing upload.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	sw := w.client.Bucket(w.config.Bucket).Object(filename).NewWriter(ctx)
	sw.ContentType = fileType
	sw.ChunkSize = defaultChunkSize

	return &gcpWriter{
		sw: sw,
	}, nil
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (w *Writer) GetType() string {
	return gcpStorageType
}

type gcpWriter struct {
	sw *storage.Writer
}

func (w *gcpWriter) Write(p []byte) (n int, err error) {
	return w.sw.Write(p)
}

func (w *gcpWriter) Close() error {
	return w.sw.Close()
}

func isEmptyBucket(ctx context.Context, client *storage.Client, bucketName string) (bool, error) {
	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid nil pointer error.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	it := bucket.Objects(ctx, nil)
	_, err = it.Next()
	// Success.
	if errors.Is(err, iterator.Done) {
		return true, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to list bucket objects: %v", err)
	}

	return false, nil
}

func removeFilesFromBucket(ctx context.Context, client *storage.Client, bucketName string) error {
	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid nil pointer error.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	it := bucket.Objects(ctx, nil)

	var objAttrs *storage.ObjectAttrs

	for {
		// Iterate over bucket until we're done.
		objAttrs, err = it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return fmt.Errorf("failed to read object attr from bucket %s: %w", bucketName, err)
		}

		if err = bucket.Object(objAttrs.Name).Delete(ctx); err != nil {
			return fmt.Errorf("failed to delete object %s: %w", objAttrs.Name, err)
		}
	}

	return nil
}
