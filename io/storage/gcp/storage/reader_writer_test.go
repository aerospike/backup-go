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

package storage

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/aerospike/backup-go/internal/util"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
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
	testFileNameTemplateAsbx  = "%d_backup_%d.asbx"
	testFileNameTemplateWrong = "file_%d.zip"
	testFileNameOneFile       = "one_file.any"
	testMetadataPrefix        = "metadata_"

	testFileContent        = "content"
	testFileContentAsbx    = "content-asbx"
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
	ctx := context.Background()
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

//nolint:gocyclo //it is a test function for filling data. No need to split it.
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
	for i := 0; i < testFilesNumber; i++ {
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

		fileName = fmt.Sprintf("%s%s", testFolderMixedBackups, fmt.Sprintf(testFileNameTemplateAsbx, 0, i))
		sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
		sw.ContentType = fileType
		if err := writeContent(sw, testFileContentAsbx); err != nil {
			return err
		}
	}

	// unsorted files.
	fileName := fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 3))
	sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
	sw.ContentType = fileType
	if err := writeContent(sw, testFileContentSorted3); err != nil {
		return err
	}

	fileName = fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 1))
	sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
	sw.ContentType = fileType
	if err := writeContent(sw, testFileContentSorted1); err != nil {
		return err
	}

	fileName = fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 2))
	sw = client.Bucket(testBucketName).Object(fileName).NewWriter(ctx)
	sw.ContentType = fileType
	if err := writeContent(sw, testFileContentSorted2); err != nil {
		return err
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
	if !strings.HasSuffix(fileName, util.FileExtAsb) {
		return fmt.Errorf("file name must end with .asb")
	}
	return nil
}

func (s *GCPSuite) TestReader_StreamFilesOk() {
	s.T().Parallel()
	s.suiteWg.Wait()

	ctx := context.Background()
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
				require.Equal(s.T(), testFilesNumber, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_WithSorting() {
	s.T().Parallel()
	s.suiteWg.Wait()

	ctx := context.Background()
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
		options.WithDir(testReadFolderSorted),
		options.WithSorting(),
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
		case f, ok := <-rCH:
			if !ok {
				require.Equal(s.T(), 3, filesCounter)
				return
			}
			filesCounter++

			result, err := readAll(f.Reader)
			expecting := fmt.Sprintf("%s%d", "sorted", filesCounter)

			s.Require().NoError(err)
			s.Require().Equal(expecting, result)
		}
	}
}

func (s *GCPSuite) TestReader_StreamFilesEmpty() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_GetType() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	require.Equal(s.T(), gcpStorageType, result)
}

func (s *GCPSuite) TestWriter_WriteEmptyDir() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderEmpty, fmt.Sprintf(testFileNameTemplate, i))
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderWithData, fmt.Sprintf(testFileNameTemplate, i))
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplate, i))
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	require.Equal(s.T(), gcpStorageType, result)
}

func (s *GCPSuite) TestReader_OpenFileOk() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
				require.Equal(s.T(), 1, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_OpenFileErr() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
				require.Equal(s.T(), 3, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamPathList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamFilesList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *GCPSuite) TestReader_StreamFilesPreloaded() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
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
		options.WithDir(testFolderMixedBackups),
	)
	s.Require().NoError(err)

	list, err := reader.ListObjects(ctx, testFolderMixedBackups)
	s.Require().NoError(err)

	_, asbxList := filterList(list)
	reader.SetObjectsToStream(asbxList)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case f, ok := <-rCH:
			if !ok {
				require.Equal(s.T(), 5, filesCounter)
				return
			}
			filesCounter++

			result, err := readAll(f.Reader)
			s.Require().NoError(err)
			s.Require().Equal(testFileContentAsbx, result)
		}
	}
}

func filterList(list []string) (asbList, asbxList []string) {
	for i := range list {
		switch filepath.Ext(list[i]) {
		case util.FileExtAsb:
			asbList = append(asbList, list[i])
		case util.FileExtAsbx:
			asbxList = append(asbxList, list[i])
		}
	}
	return asbList, asbxList
}

func readAll(r io.ReadCloser) (string, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("failed to read data: %w", err)
	}
	if err := r.Close(); err != nil {
		return "", fmt.Errorf("failed to close reader: %w", err)
	}

	return string(data), nil
}

func (s *GCPSuite) TestReader_StreamFiles_Skipped() {
	s.T().Parallel()
	s.suiteWg.Wait()

	ctx := context.Background()
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
				require.Equal(s.T(), 2, filesCounter)
				goto Done
			}
			filesCounter++
		}
	}

Done:
	skipped := reader.GetSkipped()
	require.Equal(s.T(), 3, len(skipped))
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
