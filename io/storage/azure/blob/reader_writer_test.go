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

package blob

import (
	"context"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aerospike/backup-go/internal/util/files"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	azuritAccountName = "devstoreaccount1"
	azuritAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	testServiceAddress = "http://127.0.0.1:10000/devstoreaccount1"
	testContainerName  = "test-container"

	testReadFolderEmpty      = "folder_read_empty/"
	testReadFolderWithData   = "folder_read_with_data/"
	testReadFolderMixedData  = "folder_read_mixed_data/"
	testReadFolderOneFile    = "folder_read_one_file/"
	testReadFolderWithMarker = "folder_read_with_marker/"
	testReadFolderPathList   = "folder_path_list/"
	testReadFolderFileList   = "folder_file_list/"
	testReadFolderSorted     = "folder_sorted/"
	testReadFolderSkipped    = "folder_read_skipped/"

	testWriteFolderEmpty         = "folder_write_empty/"
	testWriteFolderWithData      = "folder_write_with_data/"
	testWriteFolderWithDataError = "folder_write_with_data_error/"
	testWriteFolderMixedData     = "folder_write_mixed_data/"
	testWriteFolderOneFile       = "folder_write_one_file/"
	testFolderMixedBackups       = "folder_mixed_backup/"

	testFolderNameTemplate    = "folder_%d/"
	testFileNameTemplate      = "backup_%d.asb"
	testFileNameTemplateAsbx  = "%d_backup_%d.asbx"
	testFileNameTemplateWrong = "file_%d.zip"
	testFileNameOneFile       = "one_file.any"
	testMetadataPrefix        = "metadata_"

	testFileContent       = "content"
	testFileContentAsbx   = "content-asbx"
	testFileContentLength = 7

	testFileContentSorted1 = "sorted1"
	testFileContentSorted2 = "sorted2"
	testFileContentSorted3 = "sorted3"

	testFilesNumber = 5
)

type AzureSuite struct {
	suite.Suite
	client  *azblob.Client
	suiteWg sync.WaitGroup
}

func (s *AzureSuite) SetupSuite() {
	defer s.suiteWg.Done() // Signal that setup is complete
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	err = fillTestData(ctx, client)
	s.Require().NoError(err)
	s.client = client
}

func (s *AzureSuite) TearDownSuite() {

}

func TestAzureSuite(t *testing.T) {
	t.Parallel()
	// Add 1 to the WaitGroup - will be "Done" when SetupSuite completes
	s := new(AzureSuite)
	s.suiteWg.Add(1)
	suite.Run(t, s)
}

//nolint:gocyclo // Long function to generate all kinds of data.
func fillTestData(ctx context.Context, client *azblob.Client) error {
	_, _ = client.CreateContainer(ctx, testContainerName, nil)

	containerClient := client.ServiceClient().NewContainerClient(testContainerName)

	blockBlobClient := containerClient.NewBlockBlobClient(testReadFolderEmpty)
	if _, err := blockBlobClient.Upload(ctx, nil, nil); err != nil {
		return err
	}

	blockBlobClient = containerClient.NewBlockBlobClient(testWriteFolderEmpty)
	if _, err := blockBlobClient.Upload(ctx, nil, nil); err != nil {
		return err
	}

	folderName := fmt.Sprintf("%s%s%s", testReadFolderMixedData, testWriteFolderEmpty, testFileNameTemplate)
	if _, err := client.UploadStream(ctx, testContainerName, folderName, strings.NewReader(testFileContent), nil); err != nil {
		return err
	}

	folderName = fmt.Sprintf("%s%s", testReadFolderOneFile, testFileNameOneFile)
	if _, err := client.UploadStream(ctx, testContainerName, folderName, strings.NewReader(testFileContent), nil); err != nil {
		return err
	}

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testReadFolderWithData, fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testReadFolderWithMarker, fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testWriteFolderWithDataError, fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testReadFolderMixedData, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testReadFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		// Skipped
		fileName = fmt.Sprintf("%s%s", testReadFolderSkipped, fmt.Sprintf(testFileNameTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testReadFolderSkipped,
				fmt.Sprintf("%s%s", testMetadataPrefix, fmt.Sprintf(testFileNameTemplate, i)))
		}
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		// Path list.
		fileName = fmt.Sprintf("%s%s%s",
			testReadFolderPathList,
			fmt.Sprintf(testFolderNameTemplate, i),
			fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		// File list
		fileName = fmt.Sprintf("%s%s", testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		// Mixed backup: asb and asbx.
		fileName = fmt.Sprintf("%s%s", testFolderMixedBackups, fmt.Sprintf(testFileNameTemplate, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContent), nil); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s%s", testFolderMixedBackups, fmt.Sprintf(testFileNameTemplateAsbx, 0, i))
		if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContentAsbx), nil); err != nil {
			return err
		}
	}

	// unsorted files.
	fileName := fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 3))
	if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContentSorted3), nil); err != nil {
		return err
	}
	fileName = fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 1))
	if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContentSorted1), nil); err != nil {
		return err
	}
	fileName = fmt.Sprintf("%s%s", testReadFolderSorted, fmt.Sprintf(testFileNameTemplateAsbx, 0, 2))
	if _, err := client.UploadStream(ctx, testContainerName, fileName, strings.NewReader(testFileContentSorted2), nil); err != nil {
		return err
	}

	return nil
}

type validatorMock struct{}

func (mock validatorMock) Run(fileName string) error {
	if !strings.HasSuffix(fileName, files.ExtensionASB) {
		return fmt.Errorf("file name must end with .asb")
	}
	return nil
}

func (s *AzureSuite) TestReader_StreamFilesOk() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderWithData),
		options.WithValidator(validatorMock{}),
		options.WithCalculateTotalSize(),
		options.WithAccessTier("Hot"),
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
				require.Equal(s.T(), int64(testFilesNumber), reader.GetNumber())
				require.Equal(s.T(), int64(testFilesNumber*testFileContentLength), reader.GetSize())
				return
			}
			filesCounter++
		}
	}
}

func (s *AzureSuite) TestReader_WithSorting() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderSorted),
		options.WithCalculateTotalSize(),
		options.WithSorting(),
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

func (s *AzureSuite) TestReader_StreamFilesEmpty() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	_, err = NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderEmpty),
		options.WithValidator(validatorMock{}),
	)
	s.Require().ErrorContains(err, "is empty")
}

func (s *AzureSuite) TestReader_StreamFilesMixed() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderMixedData),
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AzureSuite) TestReader_GetType() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderMixedData),
		options.WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	result := reader.GetType()
	require.Equal(s.T(), azureBlobType, result)
}

func (s *AzureSuite) TestReader_OpenFileOk() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
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

func (s *AzureSuite) TestReader_OpenFileErr() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithFile(fmt.Sprintf("%s%s", testReadFolderOneFile, "file_error")),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	for err = range eCH {
		s.Require().ErrorContains(err, "blob does not exist")
		return
	}
}

func (s *AzureSuite) TestWriter_WriteEmptyDir() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithDir(testWriteFolderEmpty),
		options.WithChunkSize(uploadStreamBlockSize),
		options.WithAccessTier("Hot"),
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

func (s *AzureSuite) TestWriter_WriteNotEmptyDirError() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	_, err = NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithDir(testWriteFolderWithDataError),
		options.WithChunkSize(uploadStreamBlockSize),
		options.WithAccessTier("Hot"),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")
}

func (s *AzureSuite) TestWriter_WriteNotEmptyDir() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithDir(testWriteFolderWithData),
		options.WithRemoveFiles(),
		options.WithChunkSize(uploadStreamBlockSize),
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

func (s *AzureSuite) TestWriter_WriteMixedDir() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithDir(testWriteFolderMixedData),
		options.WithRemoveFiles(),
		options.WithChunkSize(uploadStreamBlockSize),
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

func (s *AzureSuite) TestWriter_WriteSingleFile() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithFile(fmt.Sprintf("%s%s", testWriteFolderOneFile, testFileNameOneFile)),
		options.WithChunkSize(uploadStreamBlockSize),
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

func (s *AzureSuite) TestWriter_GetType() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		options.WithDir(testWriteFolderWithData),
		options.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	result := writer.GetType()
	require.Equal(s.T(), azureBlobType, result)
}

func (s *AzureSuite) TestReader_WithMarker() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	marker := fmt.Sprintf("%s%s", testReadFolderWithMarker, fmt.Sprintf(testFileNameTemplate, 2))

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		options.WithDir(testReadFolderWithMarker),
		options.WithStartAfter(marker),
		options.WithUploadConcurrency(5),
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AzureSuite) TestReader_StreamPathList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	pathList := []string{
		path.Join(testReadFolderPathList, fmt.Sprintf(testFolderNameTemplate, 0)),
		path.Join(testReadFolderPathList, fmt.Sprintf(testFolderNameTemplate, 2)),
	}

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
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

func (s *AzureSuite) TestReader_StreamFilesList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	pathList := []string{
		path.Join(testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, 0)),
		path.Join(testReadFolderFileList, fmt.Sprintf(testFileNameTemplate, 2)),
	}

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
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

func (s *AzureSuite) TestReader_StreamFilesPreloaded() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
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

func (s *AzureSuite) TestIsSkippedByStartAfter() {
	s.T().Parallel()
	s.suiteWg.Wait()
	tests := []struct {
		name       string
		startAfter string
		fileName   string
		expected   bool
	}{
		{
			name:       "Empty startAfter - do not skip",
			startAfter: "",
			fileName:   "file1",
			expected:   false,
		},
		{
			name:       "File name is before startAfter - skip",
			startAfter: "file2",
			fileName:   "file1",
			expected:   true,
		},
		{
			name:       "File name equals startAfter - skip",
			startAfter: "file1",
			fileName:   "file1",
			expected:   true,
		},
		{
			name:       "File name is after startAfter - do not skip",
			startAfter: "file1",
			fileName:   "file2",
			expected:   false,
		},
	}

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isSkippedByStartAfter(tt.startAfter, tt.fileName)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestParseAccessTier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		tier          string
		expectedTier  blob.AccessTier
		expectedError error
	}{
		{
			name:          "valid hot tier",
			tier:          string(blob.AccessTierHot),
			expectedTier:  blob.AccessTierHot,
			expectedError: nil,
		},
		{
			name:          "valid cool tier",
			tier:          string(blob.AccessTierCool),
			expectedTier:  blob.AccessTierCool,
			expectedError: nil,
		},
		{
			name:          "invalid tier",
			tier:          "invalid",
			expectedTier:  "",
			expectedError: fmt.Errorf("invalid access tier Invalid"),
		},
		{
			name:          "archive tier not allowed",
			tier:          string(blob.AccessTierArchive),
			expectedTier:  "",
			expectedError: fmt.Errorf("archive tier is not allowed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tier, err := parseAccessTier(tt.tier)

			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedTier, tier)
			}
		})
	}
}

func filterList(list []string) (asbList, asbxList []string) {
	for i := range list {
		switch filepath.Ext(list[i]) {
		case files.ExtensionASB:
			asbList = append(asbList, list[i])
		case files.ExtensionASBX:
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

func (s *AzureSuite) TestReader_StreamFiles_Skipped() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
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
