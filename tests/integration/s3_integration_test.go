package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	s3Storasge "github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	backupDir  = "/"
	backupFile = "/backup_folder/backup_file.txt"
)

type writeReadTestSuite struct {
	suite.Suite
}

func TestReadWrite(t *testing.T) {
	testSuite := writeReadTestSuite{}

	suite.Run(t, &testSuite)
}

func (s *writeReadTestSuite) SetupSuite() {
	if err := createMinioCredentialsFile(); err != nil {
		s.FailNow("could not create credentials file", err)
	}
}

func createMinioCredentialsFile() error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("error getting home directory: %v", err)
	}

	awsDir := filepath.Join(home, ".aws")
	err = os.MkdirAll(awsDir, 0o700)
	if err != nil {
		return fmt.Errorf("error creating .aws directory: %v", err)
	}

	filePath := filepath.Join(awsDir, "credentials")

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		credentialsFileBytes := []byte(`[minio]
aws_access_key_id = minioadmin
aws_secret_access_key = minioadminpassword`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %v", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}

func (s *writeReadTestSuite) TearDownSuite() {}

func (s *writeReadTestSuite) TestWriteRead() {
	s3Client, err := getS3Client(
		context.Background(),
		"minio",
		"eu",
		"http://localhost:9000",
	)
	s.Require().NoError(err)

	size := 500_000
	times := 100
	written := s.write("ns1.asb", size, times, s3Client)
	read := s.read(s3Client)

	s.Equal(size*times, len(read))
	s.Equal(written, read)
}

func (s *writeReadTestSuite) TestWriteReadSingleFile() {
	s3Client, err := getS3Client(
		context.Background(),
		"minio",
		"eu",
		"http://localhost:9000",
	)
	s.Require().NoError(err)

	size := 500_000
	times := 100
	written := s.writeSingleFile("ns1.asb", size, times, s3Client)
	read := s.readSingleFile(s3Client)

	s.Equal(size*times, len(read))
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
	ctx := context.Background()
	writers, err := s3Storasge.NewWriter(
		ctx,
		client,
		"backup",
		s3Storasge.WithDir(backupDir),
		s3Storasge.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	writer, err := writers.NewWriter(ctx, filename)
	if err != nil {
		s.FailNow("failed to create writer", err)
	}

	var allBytesWritten []byte
	for i := 0; i < times; i++ {
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
		s3Storasge.WithDir(backupDir),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set RemoveFiles = true")

	return allBytesWritten
}

func (s *writeReadTestSuite) read(client *s3.Client) []byte {
	reader, err := s3Storasge.NewReader(
		context.Background(),
		client,
		"backup",
		s3Storasge.WithDir(backupDir),
		s3Storasge.WithValidator(asb.NewValidator()),
	)
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
	errorChan := make(chan error)
	go reader.StreamFiles(context.Background(), readerChan, errorChan)

	select {
	case r := <-readerChan:
		buffer, err := io.ReadAll(r)
		if err != nil {
			s.FailNow("failed to read", err)
		}
		_ = r.Close()
		return buffer
	case err = <-errorChan:
		require.NoError(s.T(), err)
	}
	return nil
}

func (s *writeReadTestSuite) writeSingleFile(filename string, bytes, times int, client *s3.Client) []byte {
	ctx := context.Background()
	writers, err := s3Storasge.NewWriter(
		ctx,
		client,
		"backup",
		s3Storasge.WithFile(backupFile),
		s3Storasge.WithRemoveFiles(),
	)
	s.Require().NoError(err)

	writer, err := writers.NewWriter(ctx, filename)
	if err != nil {
		s.FailNow("failed to create writer", err)
	}

	var allBytesWritten []byte
	for i := 0; i < times; i++ {
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
		context.Background(),
		client,
		"backup",
		s3Storasge.WithFile(backupFile),
		s3Storasge.WithValidator(asb.NewValidator()),
	)
	s.Require().NoError(err)

	readerChan := make(chan io.ReadCloser)
	errorChan := make(chan error)
	go reader.StreamFiles(context.Background(), readerChan, errorChan)

	select {
	case r := <-readerChan:
		buffer, err := io.ReadAll(r)
		if err != nil {
			s.FailNow("failed to read", err)
		}
		_ = r.Close()
		return buffer
	case err = <-errorChan:
		require.NoError(s.T(), err)
	}
	return nil
}

func getS3Client(ctx context.Context, profile, region, endpoint string) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(profile),
		config.WithRegion(region),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
