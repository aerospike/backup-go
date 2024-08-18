package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

const (
	fileType         = "application/octet-stream"
	defaultChunkSize = 5 * 1024 * 1024
)

// Writer represents a GCP storage writer.
type Writer struct {
	// client contains configured gcp storage client.
	client *storage.Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
}

// NewWriter returns new GCP storage writer.
func NewWriter(
	ctx context.Context,
	client *storage.Client,
	bucketName string,
	folderName string,
	removeFiles bool,
) (*Writer, error) {
	prefix := folderName
	// Protection from incorrect input.
	if !strings.HasSuffix(folderName, "/") && folderName != "/" && folderName != "" {
		prefix = fmt.Sprintf("%s/", folderName)
	}

	// Check if backup dir is empty.
	isEmpty, err := isEmptyDirectory(ctx, client, bucketName, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to check if bucket is empty: %w", err)
	}

	if !isEmpty && !removeFiles {
		return nil, fmt.Errorf("backup bucket must be empty or set removeFiles = true")
	}

	// As we accept only empty dir or dir with files for removing. We can remove them even in an empty bucket.
	if err = removeFilesFromBucket(ctx, client, bucketName); err != nil {
		return nil, fmt.Errorf("failed to remove files from bucket: %w", err)
	}

	return &Writer{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
	}, nil
}

// NewWriter testing upload.
func (w *Writer) NewWriter(ctx context.Context, filename string) (io.WriteCloser, error) {
	sw := w.client.Bucket(w.bucketName).Object(filename).NewWriter(ctx)
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

func isEmptyDirectory(ctx context.Context, client *storage.Client, bucketName, prefix string) (bool, error) {
	bucket := client.Bucket(bucketName)
	// Check if bucket exists, to avoid nil pointer error.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to get bucket attr:%s:  %v", bucketName, err)
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	var filesCount uint

	for {
		var objAttrs *storage.ObjectAttrs
		// Iterate over bucket until we're done.
		objAttrs, err = it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return false, fmt.Errorf("failed to list bucket objects: %v", err)
		}

		// TODO: fix check of empty folder

		// Skip files in folders.
		if prefix == "" && strings.Contains(objAttrs.Name, "/") {
			fmt.Println("dir")
			continue
		}

		filesCount++
	}
	// Success.
	if filesCount == 0 {
		return true, nil
	}

	return false, nil
}

func removeFilesFromBucket(ctx context.Context, client *storage.Client, bucketName string) error {
	// TODO: remove only from current folder
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
