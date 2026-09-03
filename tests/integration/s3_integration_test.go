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

package integration

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aerospike/backup-go/io/encoding/asb"
	s3Storasge "github.com/aerospike/backup-go/io/storage/aws/s3"
	"github.com/aerospike/backup-go/io/storage/options"
	"github.com/aerospike/backup-go/models"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/ptr"
	"github.com/stretchr/testify/suite"
)

const (
	testBackupDir  = "/"
	testBackupFile = "backup_folder/backup_file.txt"
	testChunkSize  = 5242880

	profile  = "minio"
	region   = "eu"
	endpoint = "http://localhost:9000"

	minioAccessKeyID     = "minioadmin"
	minioSecretAccessKey = "minioadminpassword"

	// awsCredentialsFileEnv points the AWS SDK at a specific credentials file.
	awsCredentialsFileEnv = "AWS_SHARED_CREDENTIALS_FILE"
)

type writeReadTestSuite struct {
	suite.Suite
}

func TestReadWrite(t *testing.T) {
	testSuite := writeReadTestSuite{}

	suite.Run(t, &testSuite)
}

func (s *writeReadTestSuite) SetupSuite() {
	s.T().Setenv(awsCredentialsFileEnv, writeMinioCredentialsFile(s.T()))
}

// writeMinioCredentialsFile writes the MinIO profile into a file under the
// test's own temporary directory and returns its path. The developer's
// ~/.aws/credentials is never read or written.
func writeMinioCredentialsFile(t *testing.T) string {
	t.Helper()

	filePath := filepath.Join(t.TempDir(), "credentials")

	credentials := []byte(`[` + profile + `]
aws_access_key_id = ` + minioAccessKeyID + `
aws_secret_access_key = ` + minioSecretAccessKey)

	if err := os.WriteFile(filePath, credentials, 0o600); err != nil {
		t.Fatalf("failed to write credentials file: %v", err)
	}

	return filePath
}

func (s *writeReadTestSuite) TearDownSuite() {}

func (s *writeReadTestSuite) TestWriteRead() {
	s3Client, err := getS3Client(s.T().Context())
	s.Require().NoError(err)

	size := 500_000
	times := 100
	written := s.write("ns1.asb", size, times, s3Client)
	read := s.read(s3Client)

	s.Len(read, size*times)
	s.Equal(written, read)
}

func (s *writeReadTestSuite) TestWriteReadSingleFile() {
	s3Client, err := getS3Client(s.T().Context())
	s.Require().NoError(err)

	size := 500_000
	times := 100
	written := s.writeSingleFile(size, times, s3Client)
	read := s.readSingleFile(s3Client)

	s.Len(read, size*times)
	s.Equal(written, read)
}

func randomBytes(n int) []byte {
	data := make([]byte, n)

	_, _ = io.ReadFull(&io.LimitedReader{
		R: rand.Reader,
		N: int64(n),
	}, data)

	return data
}

func (s *writeReadTestSuite) write(filename string, bytes, times int, client *s3.Client) []byte {
	ctx := s.T().Context()
	writers, err := s3Storasge.NewWriter(
		ctx,
		client,
		"backup",
		options.WithDir(testBackupDir),
		options.WithRemoveFiles(),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().NoError(err)

	writer, err := writers.NewWriter(ctx, filename)
	if err != nil {
		s.FailNow("failed to create writer", err)
	}

	var allBytesWritten []byte
	for range times {
		bytes := randomBytes(bytes)
		n, err := writer.Write(bytes)
		if err != nil {
			s.FailNow("failed to write", err)
		}

		s.Equal(len(bytes), n)
		allBytesWritten = append(allBytesWritten, bytes...)
	}

	err = writer.Close()
	if err != nil {
		s.FailNow("failed to close writer", err)
	}

	// cannot create new streamingReader because folder is not empty
	_, err = s3Storasge.NewWriter(
		ctx,
		client,
		"backup",
		options.WithDir(testBackupDir),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")

	return allBytesWritten
}

func (s *writeReadTestSuite) read(client *s3.Client) []byte {
	reader, err := s3Storasge.NewReader(
		s.T().Context(),
		client,
		"backup",
		options.WithDir(testBackupDir),
		options.WithValidator(asb.NewValidator()),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go reader.StreamFiles(s.T().Context(), readerChan, errorChan, nil)

	select {
	case r := <-readerChan:
		buffer, err := io.ReadAll(r.Reader)
		if err != nil {
			s.FailNow("failed to read", err)
		}
		_ = r.Reader.Close()
		return buffer
	case err = <-errorChan:
		s.Require().NoError(err)
	}
	return nil
}

func (s *writeReadTestSuite) writeSingleFile(bytes, times int, client *s3.Client) []byte {
	ctx := s.T().Context()
	writers, err := s3Storasge.NewWriter(
		ctx,
		client,
		"backup",
		options.WithFile(testBackupFile),
		options.WithRemoveFiles(),
		options.WithChunkSize(testChunkSize),
	)
	s.Require().NoError(err)

	writer, err := writers.NewWriter(ctx, "")
	if err != nil {
		s.FailNow("failed to create writer", err)
	}

	var allBytesWritten []byte
	for range times {
		bytes := randomBytes(bytes)
		n, err := writer.Write(bytes)
		if err != nil {
			s.FailNow("failed to write", err)
		}

		s.Equal(len(bytes), n)
		allBytesWritten = append(allBytesWritten, bytes...)
	}

	err = writer.Close()
	if err != nil {
		s.FailNow("failed to close writer", err)
	}

	return allBytesWritten
}

func (s *writeReadTestSuite) readSingleFile(client *s3.Client) []byte {
	reader, err := s3Storasge.NewReader(
		s.T().Context(),
		client,
		"backup",
		options.WithFile(testBackupFile),
		options.WithValidator(asb.NewValidator()),
	)
	s.Require().NoError(err)

	readerChan := make(chan models.File)
	errorChan := make(chan error)
	go reader.StreamFiles(s.T().Context(), readerChan, errorChan, nil)

	select {
	case r := <-readerChan:
		buffer, err := io.ReadAll(r.Reader)
		if err != nil {
			s.FailNow("failed to read", err)
		}
		_ = r.Reader.Close()
		return buffer
	case err = <-errorChan:
		s.Require().NoError(err)
	}
	return nil
}

func getS3Client(ctx context.Context) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = ptr.String(endpoint)
		o.UsePathStyle = true
	})

	return client, nil
}
