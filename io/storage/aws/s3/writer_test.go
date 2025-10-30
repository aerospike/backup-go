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
	"fmt"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
)

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
		w, err := writer.NewWriter(ctx, fileName, true)
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
		w, err := writer.NewWriter(ctx, fileName, true)
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

	w, err := writer.NewWriter(ctx, testFileNameOneFile, true)
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
