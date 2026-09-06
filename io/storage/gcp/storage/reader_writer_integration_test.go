// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build integration

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/googleapis/gax-go/v2"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/option"
)

const (
	testServiceAddress            = "http://127.0.0.1:4443/storage/v1/b"
	testProjectID                 = "test-project"
	testBucketName                = "test-bucket"
	testReadFolderEmpty           = "folder_read_empty/"
	testReadFolderWithData        = "folder_read_with_data/"
	testReadFolderMixedData       = "folder_read_mixed_data/"
	testReadFolderOneFile         = "folder_read_one_file/"
	testReadFolderWithStartOffset = "folder_read_with_start_offset/"
	testReadFolderPathList        = "folder_path_list/"
	testReadFolderFileList        = "folder_file_list/"
	testReadFolderSorted          = "folder_sorted/"
	testReadFolderSkipped         = "folder_read_skipped/"

	testWriteFolderEmpty         = "folder_write_empty/"
	testWriteFolderWithData      = "folder_write_with_data/"
	testWriteFolderWithDataError = "folder_write_with_data_error/"
	testWriteFolderMixedData     = "folder_write_mixed_data/"
	testWriteFolderOneFile       = "folder_write_one_file/"
	testFolderMixedBackups       = "folder_mixed_backup/"
	testFolderTypeCheck          = "folder_type_check/"

	testFolderNameTemplate    = "folder_%d/"
	testFileNameTemplate      = "backup_%d.asb"
	testFileNameTemplateWrong = "file_%d.zip"
	testFileNameOneFile       = "one_file.any"
	testMetadataPrefix        = "metadata_"

	testFileContent        = "content"
	testFileContentSorted1 = "sorted1"
	testFileContentSorted2 = "sorted2"
	testFileContentSorted3 = "sorted3"

	testFileContentLength = 7
	testFilesNumber       = 5
)

type GCPSuite struct {
	suite.Suite
	client  *storage.Client
	suiteWg sync.WaitGroup
}

func (s *GCPSuite) SetupSuite() {
	defer s.suiteWg.Done() // Signal that setup is complete
	ctx := s.T().Context()
	client, err := storage.NewClient(ctx, option.WithEndpoint(testServiceAddress), option.WithoutAuthentication())
	s.Require().NoError(err)

	err = fillTestData(ctx, client)
	s.Require().NoError(err)
	s.client = client
}

func (s *GCPSuite) TearDownSuite() {
	err := s.client.Close()
	s.Require().NoError(err)
}

func TestGCPSuite(t *testing.T) {
	t.Parallel()
	// Add 1 to the WaitGroup - will be "Done" when SetupSuite completes
	s := new(GCPSuite)
	s.suiteWg.Add(1)
	suite.Run(t, s)
}

func fillTestData(ctx context.Context, client *storage.Client) error {
	bucket := client.Bucket(testBucketName)

	_ = bucket.Create(ctx, testProjectID, nil)

	// empty folders.
	sw := client.Bucket(testBucketName).Object(testReadFolderEmpty).NewWriter(ctx)
	if err := writeContent(sw, ""); err != nil {
		return err
	}

	sw = client.Bucket(testBucketName).Object(testWriteFolderEmpty).NewWriter(ctx)
	if err := writeContent(sw, ""); err != nil {
		return err
	}

	folderName := fmt.Sprintf("%s%s%s", testReadFolderMixedData, testWriteFolderEmpty, testFileNameTemplate)
	sw = client.Bucket(testBucketName).Object(folderName).NewWriter(ctx)
	if err := writeContent(sw, testFileContent); err != nil {
		return err
	}

	// one file
	folderName = fmt.Sprintf("%s%s", testReadFolderOneFile, testFileNameOneFile)
	sw = client.Bucket(testBucketName).Object(folderName).NewWriter(ctx)
	if err := writeContent(sw, testFileContent); err != nil {
		return err
	}

	folderName = fmt.Sprintf("%s%s", testFolderTypeCheck, testFileNameOneFile)
	sw = client.Bucket(testBucketName).Object(folderName).NewWriter(ctx)
	if err := writeContent(sw, testFileContent); err != nil {
		return err
	}

	// not an empty folders.
	for i := range testFilesNumber {
		// for reading tests.
		fileName := fmt.Sprintf("%s%s", testReadFolderWithData, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testReadFolderWithStartOffset, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}
		// for writing tests.
		fileName = fmt.Sprintf("%s%s", testWriteFolderWithData, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testWriteFolderWithDataError, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		// mixed content
		fileName = fmt.Sprintf("%s%s", testReadFolderMixedData, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testReadFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		// Skipped
		fileName = fmt.Sprintf("%s%s", testReadFolderSkipped, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testReadFolderSkipped,
				fmt.Sprintf("%s%s", testMetadataPrefix, fmt.Sprintf(testFileNameTemplate, i)))
		}
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		// Path list.
		fileName = fmt.Sprintf("%s%s%s",
			testReadFolderPathList,
			fmt.Sprintf(testFolderNameTemplate, i),
			fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		// File list
		fileName = fmt.Sprintf("%s%s", testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}

		// Mixed backup: asb and asbx.
		fileName = fmt.Sprintf("%s%s", testFolderMixedBackups, fmt.Sprintf(testFileNameTemplate, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContent); err != nil {
			return err
		}
	}

	return nil
}

func writeContent(sw *storage.Writer, content string) error {
	if _, err := sw.Write([]byte(content)); err != nil {
		return err
	}

	if err := sw.Close(); err != nil {
		return err
	}

	return nil
}

type validatorMock struct{}

func (mock validatorMock) Run(fileName string) error {
	if !strings.HasSuffix(fileName, asb.Extension) {
		return fmt.Errorf("file name must end with .asb")
	}
	return nil
}

func (s *GCPSuite) TestReader_StreamFilesOk() {
	s.suiteWg.Wait()

	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderWithData),
		options.WithValidator(validatorMock{}),
		options.WithCalculateTotalSize(),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(testFilesNumber, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamFilesEmpty() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	_, err = NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderEmpty),
		options.WithValidator(validatorMock{}),
		options.WithNestedDir(),
	)
	s.Require().ErrorContains(err, "is empty")
}

func (s *GCPSuite) TestReader_StreamFilesMixed() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderMixedData),
		options.WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_GetType() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderMixedData),
		options.WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	result := reader.GetType()
	s.Require().Equal(TypeGcp, result)
}

func (s *GCPSuite) TestWriter_WriteEmptyDir() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithDir(testWriteFolderEmpty),
		options.WithChunkSize(defaultChunkSize),
		options.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	for i := range testFilesNumber {
		fileName := fmt.Sprintf(testFileNameTemplate, i)
		w, err := writer.NewWriter(ctx, fileName)
		s.Require().NoError(err)
		n, err := w.Write([]byte(testFileContent))
		s.Require().NoError(err)
		s.Equal(testFileContentLength, n)
		err = w.Close()
		s.Require().NoError(err)
	}
}

func (s *GCPSuite) TestWriter_WriteNotEmptyDirError() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	_, err = NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithDir(testWriteFolderWithDataError),
		options.WithChunkSize(defaultChunkSize),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")
}

func (s *GCPSuite) TestWriter_WriteNotEmptyDir() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithDir(testWriteFolderWithData),
		options.WithRemoveFiles(),
		options.WithChunkSize(defaultChunkSize),
	)
	s.Require().NoError(err)

	for i := range testFilesNumber {
		fileName := fmt.Sprintf(testFileNameTemplate, i)
		w, err := writer.NewWriter(ctx, fileName)
		s.Require().NoError(err)
		n, err := w.Write([]byte(testFileContent))
		s.Require().NoError(err)
		s.Equal(testFileContentLength, n)
		err = w.Close()
		s.Require().NoError(err)
	}
}

func (s *GCPSuite) TestWriter_WriteMixedDir() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithDir(testWriteFolderMixedData),
		options.WithRemoveFiles(),
		options.WithChunkSize(defaultChunkSize),
	)
	s.Require().NoError(err)

	for i := range testFilesNumber {
		fileName := fmt.Sprintf(testFileNameTemplate, i)
		w, err := writer.NewWriter(ctx, fileName)
		s.Require().NoError(err)
		n, err := w.Write([]byte(testFileContent))
		s.Require().NoError(err)
		s.Equal(testFileContentLength, n)
		err = w.Close()
		s.Require().NoError(err)
	}
}

func (s *GCPSuite) TestWriter_GetType() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithDir(testFolderTypeCheck),
		options.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	result := writer.GetType()
	s.Require().Equal(TypeGcp, result)
}

func (s *GCPSuite) TestReader_OpenFileOk() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithFile(fmt.Sprintf("%s%s", testReadFolderOneFile, testFileNameOneFile)),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err = <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(1, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_OpenFileErr() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithFile(fmt.Sprintf("%s%s", testReadFolderOneFile, "file_error")),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	for err = range eCH {
		s.Require().ErrorContains(err, "object doesn't exist")
		return
	}
}

func (s *GCPSuite) TestWriter_WriteSingleFile() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucketName,
		options.WithFile(fmt.Sprintf("%s%s", testWriteFolderOneFile, testFileNameOneFile)),
	)
	s.Require().NoError(err)

	w, err := writer.NewWriter(ctx, testFileNameOneFile)
	s.Require().NoError(err)
	n, err := w.Write([]byte(testFileContent))
	s.Require().NoError(err)
	s.Equal(testFileContentLength, n)
	err = w.Close()
	s.Require().NoError(err)
}

func (s *GCPSuite) TestReader_WithStartOffset() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	startOffset := fmt.Sprintf("%s%s", testReadFolderWithStartOffset, fmt.Sprintf(testFileNameTemplate, 2))

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderWithStartOffset),
		options.WithStartAfter(startOffset),
		options.WithSkipDirCheck(),
		options.WithNestedDir(),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(3, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamPathList() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	pathList := []string{
		path.Join(testReadFolderPathList, fmt.Sprintf(testFolderNameTemplate, 0)),
		path.Join(testReadFolderPathList, fmt.Sprintf(testFolderNameTemplate, 2)),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDirList(pathList),
		options.WithValidator(validatorMock{}),
		options.WithSkipDirCheck(),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamFilesList() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	pathList := []string{
		path.Join(testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, 0)),
		path.Join(testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, 2)),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithFileList(pathList),
		options.WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamFiles_Skipped() {
	s.suiteWg.Wait()

	ctx := s.T().Context()
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(testServiceAddress),
		option.WithoutAuthentication(),
	)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucketName,
		options.WithDir(testReadFolderSkipped),
		options.WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, []string{testMetadataPrefix})

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				s.Require().Equal(2, filesCounter)
				goto Done
			}
			filesCounter++
		}
	}

Done:
	skipped := reader.GetSkipped()
	s.Require().Len(skipped, 3)
}

func TestWriter_GetOptions(t *testing.T) {
	t.Parallel()

	o1 := options.Options{
		PathList: []string{testPath},
		IsDir:    false,
	}

	w := &Writer{
		Options: o1,
	}

	o2 := w.GetOptions()
	require.Equal(t, o1, o2)
}

const (
	testFolderChunkRetry   = "folder_chunk_retry/"
	testChunkRetryFileName = "chunk_retry_%d.asb"

	// testRetryChunkSize is the minimum chunk size accepted by the GCS SDK (256KiB),
	// so the object is uploaded as a multi-chunk resumable upload.
	testRetryChunkSize = 256 * 1024
	// testRetryChunkCount is the number of chunk-sized blocks written per object.
	testRetryChunkCount = 4
	// testRetryFillByte is used to build the payload.
	testRetryFillByte = 'a'

	// testSDKChunkRetryDeadline is the SDK default per-chunk retry deadline.
	// An outage longer than this one is survivable only if the Writer sets
	// its own ChunkRetryDeadline.
	testSDKChunkRetryDeadline = 32 * time.Second
	// testOutageMargin is added to the SDK deadline so the "long outage" case
	// clearly crosses it, and to the short case so retries can land.
	testOutageMargin = 8 * time.Second

	// Retry backoff for the test client: small enough for retries to land
	// inside the injected outage window.
	testRetryBackoffInitial    = 500 * time.Millisecond
	testRetryBackoffMax        = 2 * time.Second
	testRetryBackoffMultiplier = 2.0

	// uploadIDParam is present only on resumable chunk-transfer requests.
	uploadIDParam = "upload_id"
	// contentRangeHeader names the byte range of a chunk. It is stable across
	// retries of the same chunk, so upload_id + range identify a chunk.
	contentRangeHeader = "Content-Range"
)

// testFaultCodes are retriable HTTP statuses injected on chunk requests.
var testFaultCodes = []int{
	http.StatusInternalServerError, // 500.
	http.StatusServiceUnavailable,  // 503.
	http.StatusTooManyRequests,     // 429.
}

// TestWriter_ChunkRetryDeadline verifies that Writer applies
// defaultChunkRetryDeadline to the underlying storage.Writer, so a multi-chunk
// resumable upload survives a transient 5xx/429 outage that lasts longer than
// the SDK default per-chunk deadline (32s). Without the deadline being set,
// the "outage longer than SDK default" case fails on Close.
func (s *GCPSuite) TestWriter_ChunkRetryDeadline() {
	s.suiteWg.Wait()

	tests := []struct {
		name        string
		faultWindow time.Duration
		isLong      bool
	}{
		{
			name:        "outage shorter than SDK default deadline",
			faultWindow: testOutageMargin,
		},
		{
			name:        "outage longer than SDK default deadline",
			faultWindow: testSDKChunkRetryDeadline + testOutageMargin,
			isLong:      true,
		},
	}

	for i, tt := range tests {
		s.Run(tt.name, func() {
			if tt.isLong && testing.Short() {
				s.T().Skip("skipping long chunk retry test in short mode")
			}

			ctx := s.T().Context()

			injector := newFaultTransport(http.DefaultTransport, tt.faultWindow)

			client, err := storage.NewClient(
				ctx,
				option.WithEndpoint(testServiceAddress),
				option.WithoutAuthentication(),
				option.WithHTTPClient(&http.Client{Transport: injector}),
			)
			s.Require().NoError(err)
			defer func() {
				s.Require().NoError(client.Close())
			}()

			// Retry on every retriable error, with a short backoff so retries
			// keep landing inside the injected outage window.
			client.SetRetry(
				storage.WithPolicy(storage.RetryAlways),
				storage.WithBackoff(gax.Backoff{
					Initial:    testRetryBackoffInitial,
					Max:        testRetryBackoffMax,
					Multiplier: testRetryBackoffMultiplier,
				}),
			)

			writer, err := NewWriter(
				ctx,
				client,
				testBucketName,
				options.WithDir(testFolderChunkRetry),
				options.WithChunkSize(testRetryChunkSize),
				options.WithSkipDirCheck(),
			)
			s.Require().NoError(err)

			w, err := writer.NewWriter(ctx, fmt.Sprintf(testChunkRetryFileName, i))
			s.Require().NoError(err)

			// The deadline must be propagated to the underlying storage.Writer.
			sw, ok := w.(*storage.Writer)
			s.Require().True(ok)
			s.Require().Equal(defaultChunkRetryDeadline, sw.ChunkRetryDeadline)

			block := bytes.Repeat([]byte{testRetryFillByte}, testRetryChunkSize)
			for range testRetryChunkCount {
				n, err := w.Write(block)
				s.Require().NoError(err)
				s.Require().Equal(len(block), n)
			}

			// Errors of the chunked upload surface here.
			s.Require().NoError(w.Close())

			// Guard against a false green: if no fault was injected, the retry
			// path was never exercised.
			s.Require().Positive(injector.Injected())
		})
	}
}

// chunkID identifies a single resumable-upload chunk: the session upload_id
// plus its byte range, which stays stable across retries of that chunk.
type chunkID struct {
	uploadID  string
	byteRange string
}

// faultTransport injects retriable HTTP errors (500/503/429) into the first
// resumable chunk upload it sees, for the configured window. It emulates a
// transient GCS outage: only a retry that lands after the window succeeds,
// which is exactly what ChunkRetryDeadline controls.
type faultTransport struct {
	base   http.RoundTripper
	window time.Duration

	mu       sync.Mutex
	poisoned chunkID
	deadline time.Time

	injected atomic.Int64
}

func newFaultTransport(base http.RoundTripper, window time.Duration) *faultTransport {
	return &faultTransport{
		base:   base,
		window: window,
	}
}

// Injected returns the number of injected faults.
func (t *faultTransport) Injected() int64 {
	return t.injected.Load()
}

// RoundTrip faults the poisoned chunk inside its window and passes everything
// else (auth, session creation, released chunks) to the base transport.
func (t *faultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	id, ok := chunkIdentity(req)
	if !ok || !t.decide(id) {
		return t.base.RoundTrip(req)
	}

	n := t.injected.Add(1)
	code := testFaultCodes[int(n-1)%len(testFaultCodes)]

	// The client owns the request body; drain and close it so the connection
	// state stays consistent even though the request is never sent.
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}

	return newFaultResponse(req, code), nil
}

// decide reports whether the request must be faulted. The first chunk seen is
// poisoned and keeps failing until its window elapses; other chunks pass through.
func (t *faultTransport) decide(id chunkID) bool {
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.deadline.IsZero() {
		t.poisoned = id
		t.deadline = now.Add(t.window)

		return true
	}

	if id != t.poisoned {
		return false
	}

	return now.Before(t.deadline)
}

// chunkIdentity returns the chunk identity if req is a resumable chunk transfer.
// Session-creation requests carry no upload_id and no Content-Range, so they are
// never faulted: they are retried by the outer retryer and do not exercise the
// per-chunk deadline.
func chunkIdentity(req *http.Request) (chunkID, bool) {
	uploadID := req.URL.Query().Get(uploadIDParam)
	byteRange := req.Header.Get(contentRangeHeader)

	if uploadID == "" || byteRange == "" {
		return chunkID{}, false
	}

	return chunkID{uploadID: uploadID, byteRange: byteRange}, true
}

// newFaultResponse builds a synthetic error response the GCS client treats as
// retriable under RetryAlways.
func newFaultResponse(req *http.Request, code int) *http.Response {
	body := fmt.Sprintf("injected fault: HTTP %d %s", code, http.StatusText(code))

	return &http.Response{
		StatusCode:    code,
		Status:        fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        http.Header{"Content-Type": []string{"text/plain; charset=utf-8"}},
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}
