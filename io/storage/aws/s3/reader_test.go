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
	"io"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/internal/util/files"
	"github.com/aerospike/backup-go/io/storage/aws/s3/mocks"
	"github.com/aerospike/backup-go/io/storage/options"
	optMocks "github.com/aerospike/backup-go/io/storage/options/mocks"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
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
	defer s.suiteWg.Done() // Signal that setup is complete
	ctx := context.Background()
	client, err := testClient(ctx)
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
	ctx := context.Background()
	client, err := testClient(ctx)
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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

func (s *AwsSuite) TestReader_WithSorting() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASBX {
			return nil
		}
		return fmt.Errorf("invalid file extension")
	})

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderSorted),
		options.WithValidator(mockValidator),
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
				s.Require().Equal(3, filesCounter)
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
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderMixed),
	)
	s.Require().NoError(err)

	list, err := reader.ListObjects(ctx, testFolderMixed)
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
				s.Require().Equal(5, filesCounter)
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

func (s *AwsSuite) TestReader_StreamFilesOk() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderWithData),
	)
	s.Require().NoError(err)

	result := reader.GetType()
	s.Require().Equal(s3type, result)
}

func (s *AwsSuite) TestReader_OpenFileOk() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
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
	ctx := context.Background()
	client, err := testClient(ctx)
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

func TestParseStorageClass(t *testing.T) {
	tests := []struct {
		name          string
		class         string
		expected      types.StorageClass
		expectedError error
	}{
		{
			name:          "standard storage class",
			class:         "STANDARD",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "reduced redundancy storage class",
			class:         "REDUCED_REDUNDANCY",
			expected:      types.StorageClassReducedRedundancy,
			expectedError: nil,
		},
		{
			name:          "glacier storage class",
			class:         "GLACIER",
			expected:      types.StorageClassGlacier,
			expectedError: nil,
		},
		{
			name:          "standard ia storage class",
			class:         "STANDARD_IA",
			expected:      types.StorageClassStandardIa,
			expectedError: nil,
		},
		{
			name:          "onezone ia storage class",
			class:         "ONEZONE_IA",
			expected:      types.StorageClassOnezoneIa,
			expectedError: nil,
		},
		{
			name:          "intelligent tiering storage class",
			class:         "INTELLIGENT_TIERING",
			expected:      types.StorageClassIntelligentTiering,
			expectedError: nil,
		},
		{
			name:          "deep archive storage class",
			class:         "DEEP_ARCHIVE",
			expected:      types.StorageClassDeepArchive,
			expectedError: nil,
		},
		{
			name:          "outposts storage class",
			class:         "OUTPOSTS",
			expected:      types.StorageClassOutposts,
			expectedError: nil,
		},
		{
			name:          "glacier ir storage class",
			class:         "GLACIER_IR",
			expected:      types.StorageClassGlacierIr,
			expectedError: nil,
		},
		{
			name:          "lower case input",
			class:         "standard",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "mixed case input",
			class:         "StAnDaRd",
			expected:      types.StorageClassStandard,
			expectedError: nil,
		},
		{
			name:          "empty input",
			class:         "",
			expected:      "",
			expectedError: fmt.Errorf("invalid storage class "),
		},
		{
			name:          "invalid storage class",
			class:         "INVALID_STORAGE_CLASS",
			expected:      "",
			expectedError: fmt.Errorf("invalid storage class INVALID_STORAGE_CLASS"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseStorageClass(tc.class)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestParseAccessTier(t *testing.T) {
	tests := []struct {
		name          string
		tier          string
		expected      types.Tier
		expectedError error
	}{
		{
			name:          "expedited tier",
			tier:          "Expedited",
			expected:      types.TierExpedited,
			expectedError: nil,
		},
		{
			name:          "standard tier",
			tier:          "Standard",
			expected:      types.TierStandard,
			expectedError: nil,
		},
		{
			name:          "bulk tier",
			tier:          "Bulk",
			expected:      types.TierBulk,
			expectedError: nil,
		},
		{
			name:          "lower case input",
			tier:          "expedited",
			expected:      types.TierExpedited,
			expectedError: nil,
		},
		{
			name:          "mixed case input",
			tier:          "sTaNdArD",
			expected:      types.TierStandard,
			expectedError: nil,
		},
		{
			name:          "empty input",
			tier:          "",
			expected:      "",
			expectedError: fmt.Errorf("invalid access tier "),
		},
		{
			name:          "invalid tier",
			tier:          "INVALID_TIER",
			expected:      "",
			expectedError: fmt.Errorf("invalid access tier Invalid_tier"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseAccessTier(tc.tier)

			if tc.expectedError != nil {
				require.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func (s *AwsSuite) TestReader_ListObjects() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
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

func TestReader_ShouldSkip(t *testing.T) {
	testSize := int64(1000)
	testEmptySize := int64(0)

	tests := []struct {
		name          string
		path          string
		fileName      *string
		fileSize      *int64
		withNestedDir bool
		expected      bool
	}{
		{
			name:          "nil filename",
			path:          "test/path",
			fileName:      nil,
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      true,
		},
		{
			name:          "directory with nested dir enabled",
			path:          "test/path/",
			fileName:      aws.String("test/path/subdir/"),
			fileSize:      &testSize,
			withNestedDir: true,
			expected:      false,
		},
		{
			name:          "directory with nested dir disabled",
			path:          "test/path/",
			fileName:      aws.String("test/path/subdir/"),
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      true,
		},
		{
			name:          "regular file",
			path:          "test/path/",
			fileName:      aws.String("test/path/file.txt"),
			fileSize:      &testSize,
			withNestedDir: false,
			expected:      false,
		},
		{
			name:          "nil filename",
			path:          "test/path",
			fileName:      aws.String("file.txt"),
			fileSize:      &testEmptySize,
			withNestedDir: false,
			expected:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &Reader{
				Options: options.Options{
					WithNestedDir: tc.withNestedDir,
				},
			}
			result := reader.shouldSkip(tc.path, tc.fileName, tc.fileSize)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func (s *AwsSuite) TestReader_SetObjectsToStream() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	// Create a reader
	reader, err := NewReader(
		ctx,
		client,
		testBucket,
		options.WithDir(testFolderWithData),
		options.WithSkipDirCheck(),
	)
	s.Require().NoError(err)

	// Define a list of objects to stream
	objectsToStream := []string{
		path.Join(testFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, 0)),
		path.Join(testFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, 1)),
	}

	// Set the objects to stream
	reader.SetObjectsToStream(objectsToStream)

	// Verify that the objects were set correctly
	s.Require().Equal(objectsToStream, reader.objectsToStream)

	// Test streaming the objects
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
				s.Require().Equal(len(objectsToStream), filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamFiles_Skipped() {
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == files.ExtensionASB {
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

// TestNewReader_WithAccessTier_Standard tests warming with Standard tier.
func TestNewReader_WithAccessTier_Standard(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For warmDirectory - ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - object already available
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.AccessTier = "Standard"
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)

	require.NoError(t, err)
	assert.NotNil(t, reader)
}

// TestNewReader_WithAccessTier_Expedited tests warming with Expedited tier.
func TestNewReader_WithAccessTier_Expedited(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For warmDirectory.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	// For pollWarmDirStatus - now available.
	restoreStatus := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreStatus,
		}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testDir}
		o.IsDir = true
		o.AccessTier = "Expedited"
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)

	time.Sleep(15 * time.Millisecond)

	require.NoError(t, err)
	assert.NotNil(t, reader)
}

// TestReader_RestoreObject_Success tests successful object restoration.
func TestReader_RestoreObject_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	mockClient.EXPECT().
		RestoreObject(ctx, mock.MatchedBy(func(input *s3.RestoreObjectInput) bool {
			return *input.Bucket == testBucket &&
				*input.Key == testFile &&
				input.RestoreRequest.GlacierJobParameters.Tier == types.TierStandard
		})).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	err = reader.restoreObject(ctx, testFile, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_RestoreObject_Error tests error handling in restoreObject.
func TestReader_RestoreObject_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(nil, errors.New("restore failed")).
		Once()

	err = reader.restoreObject(ctx, testFile, types.TierStandard)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to restore object")
}

// TestReader_WarmStorage_Success tests successful storage warming.
func TestReader_WarmStorage_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects in warmDirectory.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - available.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_WarmDirectory_ArchivedObject tests warming archived object.
func TestReader_WarmDirectory_ArchivedObject(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	require.NoError(t, err)
	assert.Len(t, reader.objectsToWarm, 1)
	assert.Equal(t, "test-dir/file1.txt", reader.objectsToWarm[0])
}

// TestReader_WarmDirectory_RestoringObject tests warming already restoring object.
func TestReader_WarmDirectory_RestoringObject(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - restoring.
	restoreStatus := restoreValueOngoing
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreStatus,
		}, nil).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	require.NoError(t, err)
	assert.Len(t, reader.objectsToWarm, 1)
}

// TestReader_WarmDirectory_Error tests error in warmDirectory.
func TestReader_WarmDirectory_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)

	// For ListObjects - error.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(nil, errors.New("list failed")).
		Once()

	err = reader.warmDirectory(ctx, testDir, types.TierStandard)

	assert.Error(t, err)
}

// TestReader_CheckWarm_EmptyQueue tests checkWarm with empty queue.
func TestReader_CheckWarm_EmptyQueue(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.objectsToWarm = []string{}

	err = reader.checkWarm(ctx)

	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_Success tests successful polling.
func TestReader_PollWarmDirStatus_Success(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// First poll - still restoring.
	restoreOngoing := restoreValueOngoing
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreOngoing,
		}, nil).
		Once()

	// Second poll - now available.
	restoreFinished := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreFinished,
		}, nil).
		Once()

	err = reader.pollWarmDirStatus(ctx, testFile)

	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_ContextCancelled tests context cancellation.
func TestReader_PollWarmDirStatus_ContextCancelled(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx, cancel := context.WithCancel(t.Context())

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// Cancel context immediately.
	cancel()

	err = reader.pollWarmDirStatus(ctx, testFile)

	// Should return nil on context cancellation.
	require.NoError(t, err)
}

// TestReader_PollWarmDirStatus_Error tests error in polling.
func TestReader_PollWarmDirStatus_Error(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)

	// Polling returns error.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(nil, errors.New("head object failed")).
		Once()

	err = reader.pollWarmDirStatus(ctx, testFile)

	assert.Error(t, err)
}

// TestReader_WarmStorage_RestoreFailed tests error in restore during warming.
func TestReader_WarmStorage_RestoreFailed(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)

	// For ListObjects.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
			},
		}, nil).
		Once()

	// For checkObjectAvailability - archived.
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	// For restoreObject - fails.
	mockClient.EXPECT().
		RestoreObject(ctx, mock.Anything).
		Return(nil, errors.New("restore failed")).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to warm directory")
}

// TestReader_WarmStorage_MultipleFiles tests warming multiple files.
func TestReader_WarmStorage_MultipleFiles(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	reader, err := NewReader(
		ctx,
		mockClient,
		testBucket,
		options.WithFile(testFile),
	)
	require.NoError(t, err)
	reader.PathList = []string{testDir}
	reader.objectsToWarm = make([]string, 0)
	reader.PollWarmDuration = 10 * time.Millisecond

	// For ListObjects - return multiple files.
	mockClient.EXPECT().
		ListObjectsV2(ctx, mock.Anything).
		Return(&s3.ListObjectsV2Output{
			Contents: []types.Object{
				{Key: aws.String("test-dir/file1.txt"), Size: aws.Int64(100)},
				{Key: aws.String("test-dir/file2.txt"), Size: aws.Int64(200)},
			},
		}, nil).
		Once()

	// First file - archived, needs restore.
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassGlacier,
		}, nil).
		Once()

	mockClient.EXPECT().
		RestoreObject(ctx, mock.MatchedBy(func(input *s3.RestoreObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.RestoreObjectOutput{}, nil).
		Once()

	// Second file - already available.
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file2.txt"
		})).
		Return(&s3.HeadObjectOutput{
			StorageClass: types.StorageClassStandard,
		}, nil).
		Once()

	// Polling for file1 - now available.
	restoreFinished := restoreValueFinished
	mockClient.EXPECT().
		HeadObject(ctx, mock.MatchedBy(func(input *s3.HeadObjectInput) bool {
			return *input.Key == "test-dir/file1.txt"
		})).
		Return(&s3.HeadObjectOutput{
			Restore: &restoreFinished,
		}, nil).
		Once()

	err = reader.warmStorage(ctx, types.TierStandard)

	require.NoError(t, err)
}

// TestReader_CheckWarm_PollError tests error in checkWarm polling.
func TestReader_CheckWarm_PollError(t *testing.T) {
	mockClient := mocks.NewMockClient(t)
	ctx := t.Context()

	mockClient.EXPECT().
		HeadBucket(ctx, mock.Anything).
		Return(&s3.HeadBucketOutput{}, nil).
		Once()

	opts := func(o *options.Options) {
		o.PathList = []string{testFile}
		o.PollWarmDuration = 10 * time.Millisecond
	}

	reader, err := NewReader(ctx, mockClient, testBucket, opts)
	require.NoError(t, err)
	reader.objectsToWarm = []string{testFile}

	// Polling returns error
	mockClient.EXPECT().
		HeadObject(ctx, mock.Anything).
		Return(nil, errors.New("head object failed")).
		Once()

	err = reader.checkWarm(ctx)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to poll dir status")
}
