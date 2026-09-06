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

package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aerospike/backup-go/io/storage/options"
	optMocks "github.com/aerospike/backup-go/io/storage/options/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

const (
	testS3Endpoint = "http://localhost:9000"
	testS3Region   = "eu"
	testS3Profile  = "minio"

	testMinioAccessKeyID     = "minioadmin"
	testMinioSecretAccessKey = "minioadminpassword"

	testFolderStartAfter     = "folder_start_after"
	testFolderPathList       = "folder_path_list"
	testFolderFileList       = "folder_file_list"
	testFolderSorted         = "folder_sorted"
	testFolderMixed          = "folder_mixed"
	testFolderEmpty          = "folder_empty"
	testFolderWithData       = "folder_with_data"
	testFolderMixedData      = "folder_mixed_data"
	testFolderOneFile        = "folder_one_file"
	testFileNameMetadata     = "metadata.yaml"
	testFileNameAsbTemplate  = "backup_%d.asb"
	testFileNameAsbxTemplate = "%d_backup_%d.asbx"
	testFileContentAsb       = "content-asb"
	testFileContentAsbx      = "content-asbx"

	testReadFolderSkipped = "folder_read_skipped"
	testMetadataPrefix    = "metadata_"

	testFileContentSorted1 = "sorted1"
	testFileContentSorted2 = "sorted2"
	testFileContentSorted3 = "sorted3"
)

var testFoldersTimestamps = []string{"1732519290025", "1732519390025", "1732519490025", "1732519590025", "1732519790025"}

type AwsSuite struct {
	suite.Suite
	client  *s3.Client
	suiteWg sync.WaitGroup
}

// minioCredentialsFile writes the MinIO profile into a file under the test's own
// temporary directory and returns its path. The developer's ~/.aws/credentials
// is never read or written.
func minioCredentialsFile(t *testing.T) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), "credentials")

	credentials := []byte(`[` + testS3Profile + `]
aws_access_key_id = ` + testMinioAccessKeyID + `
aws_secret_access_key = ` + testMinioSecretAccessKey)

	if err := os.WriteFile(filePath, credentials, 0o600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	return filePath
}

func testClient(ctx context.Context, credentialsFile string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(testS3Profile),
		config.WithSharedCredentialsFiles([]string{credentialsFile}),
		config.WithRegion(testS3Region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(testS3Endpoint)
		o.UsePathStyle = true
	})

	return client, nil
}

func (s *AwsSuite) SetupSuite() {
	defer s.suiteWg.Done() // Signal that setup is complete
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)
	err = fillTestData(ctx, client)
	s.Require().NoError(err)
	s.client = client
}

func (s *AwsSuite) TearDownSuite() {

}

func TestAWSSuite(t *testing.T) {
	t.Parallel()
	// Add 1 to the WaitGroup - will be "Done" when SetupSuite completes
	s := new(AwsSuite)
	s.suiteWg.Add(1)
	suite.Run(t, s)
}

func fillTestData(ctx context.Context, client *s3.Client) error {
	// Create files for start after test.
	for i := range testFoldersTimestamps {
		fileName := fmt.Sprintf("%s/%s/%s", testFolderStartAfter, testFoldersTimestamps[i], testFileNameMetadata)
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s/%s/%s", testFolderPathList, testFoldersTimestamps[i],
			fmt.Sprintf(testFileNameAsbTemplate, i))
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}
	}

	for i := range testFilesNumber {
		fileName := fmt.Sprintf("%s/%s", testFolderFileList, fmt.Sprintf(testFileNameAsbTemplate, i))
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s/%s", testFolderMixed, fmt.Sprintf(testFileNameAsbTemplate, i))
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}

		fileName = fmt.Sprintf("%s/%s", testFolderMixed, fmt.Sprintf(testFileNameAsbxTemplate, 0, i))
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsbx)),
		}); err != nil {
			return err
		}

		// For StreamFilesOk test
		fileName = fmt.Sprintf("%s/%s", testFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, i))
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}

		// For StreamFilesMixed test
		fileName = fmt.Sprintf("%s/%s", testFolderMixedData, fmt.Sprintf(testFileNameAsbTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s/%s", testFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}

		// Skipped.
		fileName = fmt.Sprintf("%s/%s", testReadFolderSkipped, fmt.Sprintf(testFileNameAsbTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s/%s", testReadFolderSkipped,
				fmt.Sprintf("%s%s", testMetadataPrefix, fmt.Sprintf(testFileNameAsbTemplate, i)))
		}
		if _, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		}); err != nil {
			return err
		}
	}

	// Create empty folder
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFolderEmpty),
		Body:   nil,
	})
	if err != nil {
		return err
	}

	// Create one file for OpenFileOk test
	fileName := fmt.Sprintf("%s/%s", testFolderOneFile, testFileNameOneFile)
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader([]byte(testFileContentAsb)),
	}); err != nil {
		return err
	}

	// Unsorted files.
	fileName = fmt.Sprintf("%s/%s", testFolderSorted, fmt.Sprintf(testFileNameAsbxTemplate, 0, 3))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader([]byte(testFileContentSorted3)),
	}); err != nil {
		return err
	}

	fileName = fmt.Sprintf("%s/%s", testFolderSorted, fmt.Sprintf(testFileNameAsbxTemplate, 0, 1))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader([]byte(testFileContentSorted1)),
	}); err != nil {
		return err
	}

	fileName = fmt.Sprintf("%s/%s", testFolderSorted, fmt.Sprintf(testFileNameAsbxTemplate, 0, 2))
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(fileName),
		Body:   bytes.NewReader([]byte(testFileContentSorted2)),
	}); err != nil {
		return err
	}

	return nil
}

func (s *AwsSuite) TestReader_WithStartAfter() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	startAfter := fmt.Sprintf("%s/%s", testFolderStartAfter, testFoldersTimestamps[3])

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderStartAfter),
		options.WithStartAfter(startAfter),
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
				s.Require().Equal(2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamPathList() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		path.Join(testFolderPathList, "1732519390025"),
		path.Join(testFolderPathList, "1732519590025"),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDirList(pathList),
		options.WithValidator(mockValidator),
		options.WithSkipDirCheck(),
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
				s.Require().Equal(2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamFilesList() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		path.Join(testFolderFileList, "backup_1.asb"),
		path.Join(testFolderFileList, "backup_2.asb"),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithFileList(pathList),
		options.WithValidator(mockValidator),
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

func (s *AwsSuite) TestReader_StreamFilesOk() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderWithData),
		options.WithValidator(mockValidator),
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

func (s *AwsSuite) TestReader_StreamFilesEmpty() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	_, err = NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderEmpty),
		options.WithValidator(mockValidator),
	)
	s.Require().ErrorContains(err, "is empty")
}

func (s *AwsSuite) TestReader_StreamFilesMixed() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderMixedData),
		options.WithValidator(mockValidator),
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
				s.Require().Equal(testFilesNumber/2, filesCounter) // Only half of the files have .asb extension
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_GetType() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderWithData),
	)
	s.Require().NoError(err)

	result := reader.GetType()
	s.Require().Equal(TypeAwsS3, result)
}

func (s *AwsSuite) TestReader_OpenFileOk() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithFile(fmt.Sprintf("%s/%s", testFolderOneFile, testFileNameOneFile)),
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

func (s *AwsSuite) TestReader_OpenFileErr() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithFile(fmt.Sprintf("%s/%s", testFolderOneFile, "file_error")),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH, nil)

	for err = range eCH {
		s.Require().Error(err)
		return
	}
}

func (s *AwsSuite) TestReader_ListObjects() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	// Create a reader for a directory with known files
	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderWithData),
		options.WithSkipDirCheck(),
	)
	s.Require().NoError(err)

	// List objects in the directory
	objects, err := reader.ListObjects(ctx, testFolderWithData)
	s.Require().NoError(err)

	// Check that the correct number of objects is returned
	s.Require().Len(objects, testFilesNumber, "Expected number of objects to be equal to testFilesNumber")

	// Check that all objects have the correct prefix
	for _, obj := range objects {
		s.Require().True(strings.HasPrefix(obj, testFolderWithData),
			"Expected object %s to have prefix %s", obj, testFolderWithData)
	}
}

func (s *AwsSuite) TestReader_StreamFiles_Skipped() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == asb.Extension {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testReadFolderSkipped),
		options.WithValidator(mockValidator),
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

func (s *AwsSuite) TestReaderWriter_RoundTripLargeFilesParallel() {
	s.suiteWg.Wait()
	ctx := s.T().Context()
	client, err := testClient(ctx, minioCredentialsFile(s.T()))
	s.Require().NoError(err)

	uploadPrefix := fmt.Sprintf("transfermanager_roundtrip_%d", time.Now().UnixNano())

	writer, err := NewWriter(
		ctx,
		client,
		testBucket,
		options.WithDir(uploadPrefix),
		options.WithUploadConcurrency(4),
	)
	s.Require().NoError(err)

	makePayload := func(size int, seed byte) []byte {
		payload := make([]byte, size)
		for i := range payload {
			payload[i] = seed + byte(i%251)
		}
		return payload
	}

	filesToUpload := []struct {
		name    string
		payload []byte
	}{
		{name: "parallel_large_1.asb", payload: makePayload(int(s3DefaultChunkSize)+12345, 11)},
		{name: "parallel_large_2.asb", payload: makePayload(int(s3DefaultChunkSize*2)+54321, 27)},
		{name: "parallel_large_3.asb", payload: makePayload(int(s3DefaultChunkSize*3)+11111, 39)},
	}

	expected := make(map[string][]byte, len(filesToUpload))
	for _, file := range filesToUpload {
		expected[file.name] = file.payload
	}

	var uploadWG sync.WaitGroup
	uploadErrCh := make(chan error, len(filesToUpload))

	for _, file := range filesToUpload {
		uploadWG.Go(func() {
			w, err := writer.NewWriter(ctx, file.name)
			if err != nil {
				uploadErrCh <- err
				return
			}

			if _, err = io.Copy(w, bytes.NewReader(file.payload)); err != nil {
				uploadErrCh <- err
				_ = w.Close()
				return
			}

			if err = w.Close(); err != nil {
				uploadErrCh <- err
			}
		})
	}

	uploadWG.Wait()
	close(uploadErrCh)

	for err := range uploadErrCh {
		s.Require().NoError(err)
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(uploadPrefix),
		options.WithChunkSize(s3DefaultChunkSize),
		options.WithUploadConcurrency(4),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)
	go reader.StreamFiles(ctx, rCH, eCH, nil)

	received := make(map[string][]byte, len(filesToUpload))

	for {
		select {
		case err = <-eCH:
			s.Require().NoError(err)
		case file, ok := <-rCH:
			if !ok {
				s.Require().Len(received, len(filesToUpload))
				for fileName, expectedData := range expected {
					actualData, ok := received[fileName]
					s.Require().True(ok, "downloaded file %s was not found", fileName)
					s.Require().Equal(expectedData, actualData)
				}
				return
			}

			data, readErr := io.ReadAll(file.Reader)
			s.Require().NoError(readErr)
			s.Require().NoError(file.Reader.Close())
			received[file.Name] = data
		}
	}
}
