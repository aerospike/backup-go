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
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	azuritAccountName = "devstoreaccount1"
	azuritAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

	testServiceAddress = "http://127.0.0.1:5000/devstoreaccount1"
	testContainerName  = "test-container"

	testReadFolderEmpty          = "folder_read_empty/"
	testReadFolderWithData       = "folder_read_with_data/"
	testReadFolderMixedData      = "folder_read_mixed_data/"
	testReadFolderOneFile        = "folder_read_one_file/"
	testReadFolderWithMarker     = "folder_read_with_marker/"
	testWriteFolderEmpty         = "folder_write_empty/"
	testWriteFolderWithData      = "folder_write_with_data/"
	testWriteFolderWithDataError = "folder_write_with_data_error/"
	testWriteFolderMixedData     = "folder_write_mixed_data/"
	testWriteFolderOneFile       = "folder_write_one_file/"
	testFileNameTemplate         = "backup_%d.asb"
	testFileNameTemplateWrong    = "file_%d.zip"
	testFileNameOneFile          = "one_file.any"

	testFileContent       = "content"
	testFileContentLength = 7

	testFilesNumber = 5
)

type AzureSuite struct {
	suite.Suite
	client *azblob.Client
}

func (s *AzureSuite) SetupSuite() {
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
	ctx := context.Background()
	err := removeTestData(ctx, s.client)
	s.Require().NoError(err)
}

func TestGCPSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AzureSuite))
}

func fillTestData(ctx context.Context, client *azblob.Client) error {
	if _, err := client.CreateContainer(ctx, testContainerName, nil); err != nil {
		return err
	}

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
	}

	return nil
}

func removeTestData(ctx context.Context, client *azblob.Client) error {
	if _, err := client.DeleteContainer(ctx, testContainerName, nil); err != nil {
		return err
	}

	return nil
}

type validatorMock struct{}

func (mock validatorMock) Run(fileName string) error {
	if !strings.HasSuffix(fileName, ".asb") {
		return fmt.Errorf("file name must end with .asb")
	}
	return nil
}

func (s *AzureSuite) TestReader_StreamFilesOk() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithDir(testReadFolderWithData),
		WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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

func (s *AzureSuite) TestReader_StreamFilesEmpty() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithDir(testReadFolderEmpty),
		WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

	var filesCounter int

	for {
		select {
		case err := <-eCH:
			s.Require().NoError(err)
		case _, ok := <-rCH:
			if !ok {
				require.Equal(s.T(), 0, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AzureSuite) TestReader_StreamFilesMixed() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithDir(testReadFolderMixedData),
		WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithDir(testReadFolderMixedData),
		WithValidator(validatorMock{}),
	)
	s.Require().NoError(err)

	result := reader.GetType()
	require.Equal(s.T(), azureBlobType, result)
}

func (s *AzureSuite) TestReader_isDirectory() {
	prefix := "/"
	fileNames := []string{
		"test/innerfldr/",
		"test/innerfldr/test_inner.asb",
		"test/test.asb",
		"test/test2.asb",
		"test3.asb",
	}
	var dirCounter int
	for i := range fileNames {
		if isDirectory(prefix, fileNames[i]) {
			dirCounter++
		}
	}
	require.Equal(s.T(), 4, dirCounter)
}

func (s *AzureSuite) TestReader_OpenFileOk() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithFile(fmt.Sprintf("%s%s", testReadFolderOneFile, testFileNameOneFile)),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testContainerName,
		WithFile(fmt.Sprintf("%s%s", testReadFolderOneFile, "file_error")),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

	for err = range eCH {
		s.Require().ErrorContains(err, "blob does not exist")
		return
	}
}

func (s *AzureSuite) TestWriter_WriteEmptyDir() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		WithDir(testWriteFolderEmpty),
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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	_, err = NewWriter(
		ctx,
		client,
		testContainerName,
		WithDir(testWriteFolderWithDataError),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")
}

func (s *AzureSuite) TestWriter_WriteNotEmptyDir() {
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		WithDir(testWriteFolderWithData),
		WithRemoveFiles(),
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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		WithDir(testWriteFolderMixedData),
		WithRemoveFiles(),
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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		WithFile(fmt.Sprintf("%s%s", testWriteFolderOneFile, testFileNameOneFile)),
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
	ctx := context.Background()
	cred, err := azblob.NewSharedKeyCredential(azuritAccountName, azuritAccountKey)
	s.Require().NoError(err)
	client, err := azblob.NewClientWithSharedKeyCredential(testServiceAddress, cred, nil)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testContainerName,
		WithDir(testWriteFolderWithData),
		WithRemoveFiles(),
	)
	s.Require().NoError(err)

	result := writer.GetType()
	require.Equal(s.T(), azureBlobType, result)
}

func (s *AzureSuite) TestReader_WithMarker() {
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
		WithDir(testReadFolderWithMarker),
		WithMarker(marker),
	)
	s.Require().NoError(err)

	rCH := make(chan io.ReadCloser)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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
