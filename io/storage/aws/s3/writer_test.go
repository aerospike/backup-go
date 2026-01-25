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

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// Additional constants for writer tests
const (
	testFileNameOneFile       = "one_file.any"
	testFileNameTemplateWrong = "file_%d.zip"
	testFilesNumber           = 5

	// Define the chunk size to match s3DefaultChunkSize in writer.go
	testChunkSize = 5 * 1024 * 1024 // 5MB, minimum size of a part

	testWriteFolderEmpty         = "folder_write_empty/"
	testWriteFolderWithData      = "folder_write_with_data/"
	testWriteFolderWithDataError = "folder_write_with_data_error/"
	testWriteFolderMixedData     = "folder_write_mixed_data/"
	testWriteFolderOneFile       = "folder_write_one_file/"
	testFolderTypeCheck          = "folder_type_check/"

	testDir      = "test-dir/"
	testFile     = "test-file.txt"
	testUploadID = "test-upload-id"
	testEtag     = "test-etag"
	testChecksum = "test-checksum"
)

var testData = []byte("test data")

type WriterSuite struct {
	suite.Suite
	client  *s3.Client
	suiteWg sync.WaitGroup
}

func (s *WriterSuite) SetupSuite() {
	defer s.suiteWg.Done() // Signal that setup is complete

	err := createAwsCredentials()
	s.Require().NoError(err)

	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	// Create test data for writer tests
	err = fillWriterTestData(ctx, client)
	s.Require().NoError(err)
	s.client = client
}

func (s *WriterSuite) TearDownSuite() {
	// Clean up if needed
}

func TestWriterSuite(t *testing.T) {
	// Add 1 to the WaitGroup - will be "Done" when SetupSuite completes
	s := new(WriterSuite)
	s.suiteWg.Add(1)
	suite.Run(t, s)
}

func createAwsCredentials() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %w", err)
	}

	awsDir := path.Join(home, ".aws")
	err = os.MkdirAll(awsDir, 0o700)
	if err != nil {
		return fmt.Errorf("error creating .aws directory: %w", err)
	}

	filePath := path.Join(awsDir, "credentials")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		credentialsFileBytes := []byte(`[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %w", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}

func fillWriterTestData(ctx context.Context, client *s3.Client) error {
	// Create empty folder
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testWriteFolderEmpty),
		Body:   nil,
	})
	if err != nil {
		return err
	}

	// Create folder with data for error test
	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderWithDataError, fmt.Sprintf(testFileNameAsbTemplate, i))
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		})
		if err != nil {
			return err
		}
	}

	// Create folder with data for removal test
	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, i))
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		})
		if err != nil {
			return err
		}
	}

	// Create folder with mixed data
	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameAsbTemplate, i))
		if i%2 == 0 {
			fileName = fmt.Sprintf("%s%s", testWriteFolderMixedData, fmt.Sprintf(testFileNameTemplateWrong, i))
		}
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
			Body:   bytes.NewReader([]byte(testFileContentAsb)),
		})
		if err != nil {
			return err
		}
	}

	// Create folder for type check
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(testFolderTypeCheck),
		Body:   nil,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *WriterSuite) TestWriter_WriteEmptyDir() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucket,
		options.WithDir(testWriteFolderEmpty),
		options.WithRemoveFiles(),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().NoError(err)

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf(testFileNameAsbTemplate, i)
		w, err := writer.NewWriter(ctx, fileName)
		s.Require().NoError(err)
		n, err := w.Write([]byte(testFileContentAsb))
		s.Require().NoError(err)
		s.Equal(len(testFileContentAsb), n)
		err = w.Close()
		s.Require().NoError(err)
	}

	// Verify files were written
	for i := 0; i < testFilesNumber; i++ {
		fileName := path.Join(testWriteFolderEmpty, fmt.Sprintf(testFileNameAsbTemplate, i))
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
		})
		s.Require().NoError(err)
	}
}

func (s *WriterSuite) TestWriter_WriteNotEmptyDirError() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	_, err = NewWriter(
		ctx,
		client,
		testBucket,
		options.WithDir(testWriteFolderWithDataError),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")
}

func (s *WriterSuite) TestWriter_WriteNotEmptyDir() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucket,
		options.WithDir(testWriteFolderWithData),
		options.WithRemoveFiles(),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().NoError(err)

	for i := 0; i < testFilesNumber; i++ {
		fileName := fmt.Sprintf(testFileNameAsbTemplate, i)
		w, err := writer.NewWriter(ctx, fileName)
		s.Require().NoError(err)
		n, err := w.Write([]byte(testFileContentAsb))
		s.Require().NoError(err)
		s.Equal(len(testFileContentAsb), n)
		err = w.Close()
		s.Require().NoError(err)
	}

	// Verify files were written
	for i := 0; i < testFilesNumber; i++ {
		fileName := path.Join(testWriteFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, i))
		_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
		})
		s.Require().NoError(err)
	}
}

func (s *WriterSuite) TestWriter_WriteSingleFile() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	filePath := fmt.Sprintf("%s%s", testWriteFolderOneFile, testFileNameOneFile)
	writer, err := NewWriter(
		ctx,
		client,
		testBucket,
		options.WithFile(filePath),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().NoError(err)

	w, err := writer.NewWriter(ctx, testFileNameOneFile)
	s.Require().NoError(err)
	n, err := w.Write([]byte(testFileContentAsb))
	s.Require().NoError(err)
	s.Equal(len(testFileContentAsb), n)
	err = w.Close()
	s.Require().NoError(err)

	// Verify file was written
	_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(filePath),
	})
	s.Require().NoError(err)
}

func (s *WriterSuite) TestWriter_GetType() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	writer, err := NewWriter(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderTypeCheck),
		options.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	result := writer.GetType()
	require.Equal(s.T(), s3type, result)
}

// TestNewWriter_Success tests successful Writer creation
func TestNewWriter_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)

	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.Equal(t, testBucket, writer.bucketName)
	assert.Equal(t, testDir, writer.prefix)
}

// TestNewWriter_NoPath tests error when no path provided
func TestNewWriter_NoPath(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	_, err := NewWriter(ctx, mockClient, testBucket)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "one path is required")
}

// TestNewWriter_MultiplePaths tests error when multiple paths provided
func TestNewWriter_MultiplePaths(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	opts := func(o *options.Options) {
		o.PathList = []string{"path1", "path2"}
	}

	_, err := NewWriter(ctx, mockClient, testBucket, opts)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "one path is required")
}

// TestNewWriter_NegativeChunkSize tests error for negative chunk size
func TestNewWriter_NegativeChunkSize(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	_, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(-1),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "chunk size must be positive")
}

// TestNewWriter_DefaultChunkSize tests that default chunk size is set
func TestNewWriter_DefaultChunkSize(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)

	assert.NoError(t, err)
	assert.Equal(t, s3DefaultChunkSize, writer.ChunkSize)
}

// TestNewWriter_BucketNotExists tests error when bucket doesn't exist
func TestNewWriter_BucketNotExists(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(nil, errors.New("bucket not found")).
		Once()

	_, err := NewWriter(
		ctx,
		mockClient,
		"non-existent-bucket",
		options.WithDir(testDir),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist or you don't have access")
}

// TestNewWriter_DirectoryNotEmpty tests error when directory is not empty and RemoveFiles not set
func TestNewWriter_DirectoryNotEmpty(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	_, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backup folder must be empty or set RemoveFiles = true")
}

// TestNewWriter_DirectoryNotEmptyWithRemoveFiles tests successful cleanup when RemoveFiles is set
func TestNewWriter_DirectoryNotEmptyWithRemoveFiles(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Times(2) // Once for check, once for removal

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithRemoveFiles(),
	)

	assert.NoError(t, err)
	assert.NotNil(t, writer)
}

// TestNewWriter_InvalidStorageClass tests error for invalid storage class
func TestNewWriter_InvalidStorageClass(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.StorageClass = "INVALID_CLASS"
	}

	_, err := NewWriter(ctx, mockClient, testBucket, opts)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse storage class")
}

// TestNewWriter_ValidStorageClass tests that valid storage classes work
func TestNewWriter_ValidStorageClass(t *testing.T) {
	testCases := []struct {
		name         string
		storageClass string
		expected     types.StorageClass
	}{
		{
			name:         "STANDARD lowercase",
			storageClass: "standard",
			expected:     types.StorageClassStandard,
		},
		{
			name:         "STANDARD_IA uppercase",
			storageClass: "STANDARD_IA",
			expected:     types.StorageClassStandardIa,
		},
		{
			name:         "GLACIER mixed case",
			storageClass: "GlAcIeR",
			expected:     types.StorageClassGlacier,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := mocks.NewMockClient(t)
			ctx := context.Background()

			mockClient.EXPECT().
				HeadBucket(ctx, mock.Anything).
				Return(&s3.HeadBucketOutput{}, nil).
				Once()

			mockClient.EXPECT().
				ListObjectsV2(ctx, mock.Anything).
				Return(&s3.ListObjectsV2Output{
					Contents: []types.Object{},
				}, nil).
				Once()

			opts := func(o *options.Options) {
				o.PathList = []string{testDir}
				o.IsDir = true
				o.StorageClass = tc.storageClass
			}

			writer, err := NewWriter(ctx, mockClient, testBucket, opts)

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, writer.storageClass)
		})
	}
}

// TestNewWriter_FileMode tests successful Writer creation for single file
func TestNewWriter_FileMode(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)

	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.False(t, writer.IsDir)
}

// TestNewWriter_SkipDirCheck tests that directory check is skipped when SkipDirCheck is true
func TestNewWriter_SkipDirCheck(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.SkipDirCheck = true
	}

	writer, err := NewWriter(ctx, mockClient, testBucket, opts)

	assert.NoError(t, err)
	assert.NotNil(t, writer)
}

// TestWriter_GetType tests GetType method
func TestWriter_GetType(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)

	assert.NoError(t, err)
	assert.Equal(t, s3type, writer.GetType())
}

// TestWriter_NewWriter_Success tests successful creation of s3Writer
func TestWriter_NewWriter_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)

	assert.NoError(t, err)
	assert.NotNil(t, w)
}

// TestWriter_NewWriter_CreateMultipartUploadError tests error in CreateMultipartUpload
func TestWriter_NewWriter_CreateMultipartUploadError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(nil, errors.New("upload creation failed")).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	_, err = writer.NewWriter(ctx, testFile)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create multipart upload")
}

// TestS3Writer_Write_Success tests successful Write operation
func TestS3Writer_Write_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(100), // Small chunk for testing
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	n, err := w.Write(testData)

	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
}

// TestS3Writer_Write_TriggerMultipart tests that Write triggers multipart upload when buffer exceeds chunk size
func TestS3Writer_Write_TriggerMultipart(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(10), // Very small chunk for testing
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write data larger than chunk size to trigger multipart upload
	data := make([]byte, 50)
	for i := range data {
		data[i] = byte(i)
	}

	n, err := w.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	err = w.Close()
	assert.NoError(t, err)
}

// TestS3Writer_Write_AfterClose tests that Write after Close returns error
func TestS3Writer_Write_AfterClose(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	// Try to write after close
	_, err = w.Write([]byte("test"))

	assert.Error(t, err)
	assert.ErrorIs(t, err, os.ErrClosed)
}

// TestS3Writer_Write_WithUploadError tests that Write returns error when upload fails
func TestS3Writer_Write_WithUploadError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(nil, errors.New("upload failed")).
		Maybe()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(10),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write data to trigger upload
	data := make([]byte, 50)
	n, err := w.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	time.Sleep(100 * time.Millisecond)

	_, _ = w.Write(data)
	err = w.Close()
	assert.Error(t, err)
}

// TestS3Writer_Close_Success tests successful Close operation
func TestS3Writer_Close_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write some data
	_, err = w.Write(testData)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)
}

// TestS3Writer_Close_AfterClose tests that Close after Close returns error
func TestS3Writer_Close_AfterClose(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write some data
	_, err = w.Write(testData)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	// Try to close again
	err = w.Close()
	assert.Error(t, err)
	assert.ErrorIs(t, err, os.ErrClosed)
}

// TestS3Writer_Close_CompleteMultipartUploadError tests error handling when CompleteMultipartUpload fails
func TestS3Writer_Close_CompleteMultipartUploadError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(nil, errors.New("complete failed")).
		Once()

	mockClient.EXPECT().
		AbortMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.AbortMultipartUploadOutput{}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write some data
	_, err = w.Write(testData)
	assert.NoError(t, err)

	err = w.Close()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to complete multipart upload")
}

// TestS3Writer_Close_AbortMultipartUploadError tests error handling when both Complete and Abort fail
func TestS3Writer_Close_AbortMultipartUploadError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(nil, errors.New("complete failed")).
		Once()

	mockClient.EXPECT().
		AbortMultipartUpload(mock.Anything, mock.Anything).
		Return(nil, errors.New("abort failed")).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.Logger = logger
	}

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		opts,
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	_, err = w.Write(testData)
	assert.NoError(t, err)

	err = w.Close()
	assert.Error(t, err)

	assert.Contains(t, err.Error(), "complete failed")
}

// TestS3Writer_UploadPart_WithRetryPolicy tests uploadPart with retry policy
func TestS3Writer_UploadPart_WithRetryPolicy(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum

	// Fail first 2 times, succeed on 3rd attempt
	callCount := 0
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			callCount++
			if callCount < 3 {
				return nil, errors.New("temporary upload error")
			}
			return &s3.UploadPartOutput{
				ETag:          &etag,
				ChecksumCRC32: &checksum,
			}, nil
		}).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	retryPolicy := &models.RetryPolicy{
		BaseTimeout: 1 * time.Millisecond,
		Multiplier:  2,
		MaxRetries:  3,
	}

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.ChunkSize = 10
		o.RetryPolicy = retryPolicy
	}

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		opts,
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write data to trigger upload
	data := make([]byte, 50)
	_, err = w.Write(data)
	assert.NoError(t, err)

	err = w.Close()
	assert.NoError(t, err)

	// Verify UploadPart was called 3 times (2 retries + 1 success)
	assert.GreaterOrEqual(t, callCount, 3)
}

// TestS3Writer_ContextCancellation tests that context cancellation stops uploads
func TestS3Writer_ContextCancellation(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx, cancel := context.WithCancel(context.Background())

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	// Cancel context before upload completes
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, _ *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
			cancel()
			return nil, context.Canceled
		}).
		Maybe()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(10),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	data := make([]byte, 50)
	n, err := w.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)

	_, _ = w.Write(data)
	err = w.Close()
	assert.Error(t, err)
}

// TestS3Writer_ConcurrentWrites tests concurrent writes to different files
func TestS3Writer_ConcurrentWrites(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Times(5)

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Times(5)

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(10),
	)
	assert.NoError(t, err)

	numWriters := 5
	var wg sync.WaitGroup
	wg.Add(numWriters)

	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()

			fileName := fmt.Sprintf("file-%d.txt", id)
			w, err := writer.NewWriter(ctx, fileName)
			assert.NoError(t, err)

			data := []byte(fmt.Sprintf("test data %d", id))
			_, err = w.Write(data)
			assert.NoError(t, err)

			err = w.Close()
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
}

// TestS3Writer_EmptyBuffer tests Close with empty buffer (no writes)
func TestS3Writer_EmptyBuffer(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Close without any writes
	err = w.Close()
	assert.NoError(t, err)
}

func TestWriter_RemoveFiles_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
				{Key: aws.String("test-dir/file2.txt")},
			},
		}, nil).
		Times(2) // Once for check, once for removal

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Times(2)

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithRemoveFiles(),
	)

	assert.NoError(t, err)
	assert.NotNil(t, writer)
}

// TestWriter_RemoveFiles_WithPagination tests RemoveFiles with pagination
func TestWriter_RemoveFiles_WithPagination(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	token := "next-token"
	// First page for directory check
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.MatchedBy(func(input *s3.ListObjectsV2Input) bool {
			return input.ContinuationToken == nil && *input.MaxKeys == 1
		})).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	// First page for removal
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.MatchedBy(func(input *s3.ListObjectsV2Input) bool {
			return input.ContinuationToken == nil && input.MaxKeys == nil
		})).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
			NextContinuationToken: &token,
		}, nil).
		Once()

	// Second page for removal
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.MatchedBy(func(input *s3.ListObjectsV2Input) bool {
			return input.ContinuationToken != nil && *input.ContinuationToken == token
		})).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file2.txt")},
			},
		}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Times(2)

	_, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithRemoveFiles(),
	)

	assert.NoError(t, err)
}

// TestWriter_Remove_SingleFile tests removing a single file
func TestWriter_Remove_SingleFile(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	assert.NoError(t, err)

	err = writer.Remove(ctx, testFile)
	assert.NoError(t, err)
}

// TestWriter_Remove_Directory tests removing a directory
func TestWriter_Remove_Directory(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Once()

	err = writer.Remove(ctx, testDir)
	assert.NoError(t, err)
}

// TestWriter_Remove_DirectoryWithNestedDir tests removing directory with nested directories
func TestWriter_Remove_DirectoryWithNestedDir(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.WithNestedDir = true
	}

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		opts,
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/nested/")},
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Times(2)

	err = writer.Remove(ctx, testDir)
	assert.NoError(t, err)
}

// TestWriter_Remove_SkipDirectory tests that directories are skipped during removal
func TestWriter_Remove_SkipDirectory(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/nested/")}, // Directory
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	// Only file should be deleted, directory should be skipped
	mockClient.EXPECT().
		DeleteObject(ctx, mock.MatchedBy(func(input *s3.DeleteObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.DeleteObjectOutput{}, nil).
		Once()

	err = writer.Remove(ctx, testDir)
	assert.NoError(t, err)
}

// TestWriter_Remove_NullKey tests handling of null keys in ListObjectsV2 response
func TestWriter_Remove_NullKey(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: nil}, // Null key should be skipped
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(&s3.DeleteObjectOutput{}, nil).
		Once()

	err = writer.Remove(ctx, testDir)
	assert.NoError(t, err)
}

// TestWriter_Remove_ListObjectsError tests error handling when ListObjects fails
func TestWriter_Remove_ListObjectsError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(nil, errors.New("list failed")).
		Once()

	err = writer.Remove(ctx, testDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list objects")
}

// TestWriter_Remove_DeleteObjectError tests error handling when DeleteObject fails
func TestWriter_Remove_DeleteObjectError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt")},
			},
		}, nil).
		Once()

	mockClient.EXPECT().
		DeleteObject(ctx, mock.Anything).
		Return(nil, errors.New("delete failed")).
		Once()

	err = writer.Remove(ctx, testDir)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete object")
}

// TestIsEmptyDirectory tests isEmptyDirectory function
func TestIsEmptyDirectory(t *testing.T) {
	tests := []struct {
		name     string
		contents []types.Object
		prefix   string
		expected bool
	}{
		{
			name:     "empty directory",
			contents: []types.Object{},
			prefix:   testDir,
			expected: true,
		},
		{
			name: "non-empty directory",
			contents: []types.Object{
				{Key: aws.String("test-dir/file.txt")},
			},
			prefix:   testDir,
			expected: false,
		},
		{
			name: "directory with single object matching prefix",
			contents: []types.Object{
				{Key: aws.String(testDir)},
			},
			prefix:   testDir,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := mocks.NewMockClient(t)
			ctx := context.Background()

			mockClient.EXPECT().
				ListObjectsV2(ctx, mock.Anything).
				Return(&s3.ListObjectsV2Output{
					Contents: tt.contents,
				}, nil).
				Once()

			result, err := isEmptyDirectory(ctx, mockClient, testBucket, tt.prefix)

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestIsEmptyDirectory_Error tests isEmptyDirectory error handling
func TestIsEmptyDirectory_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(nil, errors.New("list failed")).
		Once()

	_, err := isEmptyDirectory(ctx, mockClient, testBucket, testDir)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list bucket objects")
}

// TestNewWriter_CleanPath tests that paths are cleaned correctly
func TestNewWriter_CleanPath(t *testing.T) {
	tests := []struct {
		name         string
		inputPath    string
		expectedPath string
	}{
		{
			name:         "path with trailing slash",
			inputPath:    testDir,
			expectedPath: testDir,
		},
		{
			name:         "path without trailing slash",
			inputPath:    "test-dir",
			expectedPath: testDir,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := mocks.NewMockClient(t)
			ctx := context.Background()

			mockClient.EXPECT().
				HeadBucket(ctx, mock.Anything).
				Return(&s3.HeadBucketOutput{}, nil).
				Once()

			mockClient.EXPECT().
				ListObjectsV2(ctx, mock.Anything).
				Return(&s3.ListObjectsV2Output{
					Contents: []types.Object{},
				}, nil).
				Once()

			writer, err := NewWriter(
				ctx,
				mockClient,
				testBucket,
				options.WithDir(tt.inputPath),
			)

			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPath, writer.prefix)
		})
	}
}

// TestS3Writer_WriteZeroBytes tests writing zero bytes
func TestS3Writer_WriteZeroBytes(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write zero bytes
	n, err := w.Write([]byte{})
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	err = w.Close()
	assert.NoError(t, err)
}

// TestWriter_NewWriter_WithStorageClass tests that storage class is passed to CreateMultipartUpload
func TestWriter_NewWriter_WithStorageClass(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID

	// Verify storage class is passed correctly
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.MatchedBy(func(input *s3.CreateMultipartUploadInput) bool {
			return input.StorageClass == types.StorageClassGlacier
		})).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.StorageClass = "GLACIER"
	}

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		opts,
	)
	assert.NoError(t, err)

	_, err = writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)
}

// TestS3Writer_WriteIncrementally tests writing data incrementally
func TestS3Writer_WriteIncrementally(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(100),
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write data in small increments
	totalBytes := 0
	for i := 0; i < 200; i++ {
		data := []byte{byte(i)}
		n, err := w.Write(data)
		assert.NoError(t, err)
		totalBytes += n
	}

	err = w.Close()
	assert.NoError(t, err)

	assert.Equal(t, 200, totalBytes)
}

// TestS3Writer_LargeWrite tests single large write operation
func TestS3Writer_LargeWrite(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := context.Background()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{},
		}, nil).
		Once()

	uploadID := testUploadID
	mockClient.EXPECT().
		CreateMultipartUpload(ctx, mock.Anything).
		Return(&s3.CreateMultipartUploadOutput{
			UploadId: &uploadID,
		}, nil).
		Once()

	etag := testEtag
	checksum := testChecksum
	mockClient.EXPECT().
		UploadPart(mock.Anything, mock.Anything).
		Return(&s3.UploadPartOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Maybe()

	mockClient.EXPECT().
		CompleteMultipartUpload(mock.Anything, mock.Anything).
		Return(&s3.CompleteMultipartUploadOutput{
			ETag:          &etag,
			ChecksumCRC32: &checksum,
		}, nil).
		Once()

	writer, err := NewWriter(
		ctx,
		mockClient,
		testBucket,
		options.WithDir(testDir),
		options.WithChunkSize(1024), // 1KB chunks
	)
	assert.NoError(t, err)

	w, err := writer.NewWriter(ctx, testFile)
	assert.NoError(t, err)

	// Write 10KB in one call
	largeData := make([]byte, 10*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	n, err := w.Write(largeData)
	assert.NoError(t, err)
	assert.Equal(t, len(largeData), n)

	err = w.Close()
	assert.NoError(t, err)
}

func TestWriter_GetOptions(t *testing.T) {
	t.Parallel()

	o1 := options.Options{
		PathList: []string{testFile},
		IsDir:    false,
	}

	w := &Writer{
		Options: o1,
	}

	o2 := w.GetOptions()
	require.Equal(t, o1, o2)
}
