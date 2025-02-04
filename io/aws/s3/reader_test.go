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
	"io"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/io/local/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testBucket     = "asbackup"
	testS3Endpoint = "http://localhost:9000"
	testS3Region   = "eu"
	testS3Profile  = "minio"

	testFolderStartAfter     = "folder_start_after"
	testFolderPathList       = "folder_path_list"
	testFolderFileList       = "folder_file_list"
	testFolderSorted         = "folder_sorted"
	testFolderMixed          = "folder_mixed"
	testFileNameMetadata     = "metadata.yaml"
	testFileNameAsbTemplate  = "backup_%d.asb"
	testFileNameAsbxTemplate = "%d_backup_%d.asbx"
	testFileContentAsb       = "content-asb"
	testFileContentAsbx      = "content-asbx"

	testFileContentSorted1 = "sorted1"
	testFileContentSorted2 = "sorted2"
	testFileContentSorted3 = "sorted3"

	testFoldersNumber = 5
)

var testFoldersTimestamps = []string{"1732519290025", "1732519390025", "1732519490025", "1732519590025", "1732519790025"}

type AwsSuite struct {
	suite.Suite
	client *s3.Client
}

func testClient(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(testS3Profile),
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)
	err = fillTestData(ctx, client)
	s.Require().NoError(err)
	s.client = client
}

func (s *AwsSuite) TearDownSuite() {
	ctx := context.Background()
	err := removeTestData(ctx, s.client)
	s.Require().NoError(err)
}

func TestAWSSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(AwsSuite))
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

	for i := range testFoldersNumber {
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
	}

	// Unsorted files.
	fileName := fmt.Sprintf("%s/%s", testFolderSorted, fmt.Sprintf(testFileNameAsbxTemplate, 0, 3))
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

func removeTestData(ctx context.Context, client *s3.Client) error {
	for i := range testFoldersTimestamps {
		fileName := fmt.Sprintf("%s/%s/%s", testFolderStartAfter, testFoldersTimestamps[i], testFileNameMetadata)
		_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(testBucket),
			Key:    aws.String(fileName),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *AwsSuite) TestReader_WithStartAfter() {
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	startAfter := fmt.Sprintf("%s/%s", testFolderStartAfter, testFoldersTimestamps[3])

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		WithDir(testFolderStartAfter),
		WithStartAfter(startAfter),
		WithSkipDirCheck(),
		WithNestedDir(),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
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

func (s *AwsSuite) TestReader_StreamPathList() {
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(testFolderPathList, "1732519390025"),
		filepath.Join(testFolderPathList, "1732519590025"),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		WithDirList(pathList),
		WithValidator(mockValidator),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
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

func (s *AwsSuite) TestReader_StreamFilesList() {
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asb" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	pathList := []string{
		filepath.Join(testFolderFileList, "backup_1.asb"),
		filepath.Join(testFolderFileList, "backup_2.asb"),
	}

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		WithFileList(pathList),
		WithValidator(mockValidator),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
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

func (s *AwsSuite) TestReader_WithSorting() {
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(mocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == ".asbx" {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		WithDir(testFolderSorted),
		WithValidator(mockValidator),
		WithSorting(),
	)
	s.Require().NoError(err)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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

func (s *AwsSuite) TestReader_StreamFilesPreloaded() {
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		WithDir(testFolderMixed),
	)
	s.Require().NoError(err)

	list, err := reader.ListObjects(ctx, testFolderMixed)
	s.Require().NoError(err)

	_, asbxList := filterList(list)

	reader.SetObjectsToStream(asbxList)

	rCH := make(chan models.File)
	eCH := make(chan error)

	go reader.StreamFiles(ctx, rCH, eCH)

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
		case ".asb":
			asbList = append(asbList, list[i])
		case ".asbx":
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
	defer r.Close()

	return string(data), nil
}
