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

const gcpStorageType = "gcp-storage"

type validator interface {
	Run(fileName string) error
}

// StreamingReader describes GCP Storage streaming reader.
type StreamingReader struct {
	// client contains configured gcp storage client.
	client *storage.Client
	// bucketName contains name of the bucket to read from.
	bucketName string
	// prefix contains folder name if we have folders inside the bucket.
	prefix string
	// validator files validator.
	validator validator
}

// NewStreamingReader returns new GCP storage streaming reader.
func NewStreamingReader(
	client *storage.Client,
	bucketName string,
	folderName string,
	validator validator,
) (*StreamingReader, error) {
	if validator == nil {
		return nil, fmt.Errorf("validator cannot be nil")
	}

	prefix := folderName
	// Protection from incorrect input.
	if !strings.HasSuffix(folderName, "/") && folderName != "/" && folderName != "" {
		prefix = fmt.Sprintf("%s/", folderName)
	}

	return &StreamingReader{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
		validator:  validator,
	}, nil
}

// StreamFiles streams files form GCP cloud storage to `readersCh`.
// If error occurs, it will be sent to `errorsCh.`
func (r *StreamingReader) StreamFiles(
	ctx context.Context, readersCh chan<- io.ReadCloser, errorsCh chan<- error,
) {
	bucket := r.client.Bucket(r.bucketName)
	// Check if bucket exists, to avoid nil pointer error.
	_, err := bucket.Attrs(ctx)
	if err != nil {
		errorsCh <- fmt.Errorf("failed to get bucket attr:%s:  %v", r.bucketName, err)
	}

	it := bucket.Objects(ctx, &storage.Query{
		Prefix: r.prefix,
	})
	
	for {
		var objAttrs *storage.ObjectAttrs
		// Iterate over bucket until we're done.
		objAttrs, err = it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			errorsCh <- fmt.Errorf("failed to read object attr from bucket %s: %w", r.bucketName, err)
		}

		// Skip files in folders.
		if r.prefix == "" && strings.Contains(objAttrs.Name, "/") {
			fmt.Println("dir")
			continue
		}

		// Skip not valid files.
		if err = r.validator.Run(objAttrs.Name); err != nil {
			continue
		}

		// Create readers for files.
		var reader *storage.Reader

		reader, err = bucket.Object(objAttrs.Name).NewReader(ctx)
		if err != nil {
			errorsCh <- fmt.Errorf("failerd to create reader from file %s: %w", objAttrs.Name, err)
		}

		readersCh <- reader
	}

	close(readersCh)
}

// GetType return `gcpStorageType` type of storage. Used in logging.
func (r *StreamingReader) GetType() string {
	return gcpStorageType
}
