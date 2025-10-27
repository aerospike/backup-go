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
	"strings"
	"sync"
	"testing"

	"github.com/aerospike/backup-go/internal/util"
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
	s.T().Parallel()
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamPathList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamFilesList() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
				require.Equal(s.T(), 2, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_WithSorting() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsbx {
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
	s.T().Parallel()
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

func (s *AwsSuite) TestReader_StreamFilesOk() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
				require.Equal(s.T(), testFilesNumber, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamFilesEmpty() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
				require.Equal(s.T(), testFilesNumber/2, filesCounter) // Only half of the files have .asb extension
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_GetType() {
	s.T().Parallel()
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
	require.Equal(s.T(), s3type, result)
}

func (s *AwsSuite) TestReader_OpenFileOk() {
	s.T().Parallel()
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
				require.Equal(s.T(), 1, filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_OpenFileErr() {
	s.T().Parallel()
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
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
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
				assert.Error(t, err)
				assert.Equal(t, tc.expectedError.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expected, result)
		})
	}
}

func (s *AwsSuite) TestReader_ListObjects() {
	s.T().Parallel()
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
	s.Require().Equal(testFilesNumber, len(objects), "Expected number of objects to be equal to testFilesNumber")

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
	s.T().Parallel()
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
		filepath.Join(testFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, 0)),
		filepath.Join(testFolderWithData, fmt.Sprintf(testFileNameAsbTemplate, 1)),
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
				require.Equal(s.T(), len(objectsToStream), filesCounter)
				return
			}
			filesCounter++
		}
	}
}

func (s *AwsSuite) TestReader_StreamFiles_Skipped() {
	s.T().Parallel()
	s.suiteWg.Wait()
	ctx := context.Background()
	client, err := testClient(ctx)
	s.Require().NoError(err)

	mockValidator := new(optMocks.Mockvalidator)
	mockValidator.On("Run", mock.AnythingOfType("string")).Return(func(fileName string) error {
		if filepath.Ext(fileName) == util.FileExtAsb {
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
