package integration

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	s3Storasge "github.com/aerospike/backup-go/io/aws/s3"
	"github.com/aerospike/backup-go/io/encoding/asb"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/go-connections/nat"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	backupDir  = "/"
	backupFile = "/backup_folder/backup_file.txt"
)

type writeReadTestSuite struct {
	suite.Suite
	docker  *client.Client
	minioID string
}

func TestReadWrite(t *testing.T) {
	testSuite := writeReadTestSuite{}

	suite.Run(t, &testSuite)
}

func (s *writeReadTestSuite) SetupSuite() {
	var err error
	s.docker, err = client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		s.FailNow("Failed to create Docker client", err)
	}

	ctx := context.Background()
	responseBody, err := s.docker.ImagePull(ctx, "minio/minio", image.PullOptions{})
	if err != nil {
		s.FailNow("could not pull minio image", err)
	}
	defer func(responseBody io.ReadCloser) {
		_ = responseBody.Close()
	}(responseBody)

	dec := json.NewDecoder(responseBody)
	for {
		var jm jsonmessage.JSONMessage
		if err := dec.Decode(&jm); err != nil {
			if err == io.EOF {
				break
			}
			s.FailNow("could not decode pull image response", err)
		}
		slog.Info("Image pull", "status", jm)
	}

	minioResponse, err := s.docker.ContainerCreate(ctx, &container.Config{
		Image: "minio/minio",
		Cmd:   []string{"server", "/data", "--console-address", ":9001"},
		Env: []string{
			"MINIO_ROOT_USER=minioadmin",
			"MINIO_ROOT_PASSWORD=minioadmin",
		},
		ExposedPorts: nat.PortSet{
			"9001/tcp": struct{}{},
		},
	}, &container.HostConfig{
		PortBindings: nat.PortMap{
			"9000/tcp": []nat.PortBinding{
				{HostIP: "0.0.0.0", HostPort: "9000"},
			},
			"9001/tcp": []nat.PortBinding{
				{HostIP: "0.0.0.0", HostPort: "9001"},
			},
		},
	}, nil, nil, "minio_test")
	if err != nil {
		s.FailNow("could not create minio container", err)
	}

	s.minioID = minioResponse.ID
	if err := s.docker.ContainerStart(ctx, s.minioID, container.StartOptions{}); err != nil {
		s.FailNow("could not start minio container", err)
	}

	minioClient, err := minio.New("localhost:9000", &minio.Options{
		Creds: credentials.NewStaticV4("minioadmin", "minioadmin", ""),
	})
	if err != nil {
		s.FailNow("could not create minio client", err)
	}

	err = minioClient.MakeBucket(ctx, "backup", minio.MakeBucketOptions{})
	if err != nil {
		s.FailNow("could not create bucket", err)
	}

	err = createMinioCredentialsFile()
	if err != nil {
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
aws_secret_access_key = minioadmin`)

		err = os.WriteFile(filePath, credentialsFileBytes, 0o600)
		if err != nil {
			return fmt.Errorf("error writing ~/.aws/credentials file: %v", err)
		}

		fmt.Println("Credentials file created successfully!")
	}

	return nil
}

func (s *writeReadTestSuite) TearDownSuite() {
	ctx := context.Background()
	_ = s.docker.ContainerRemove(ctx, s.minioID, container.RemoveOptions{Force: true})
}

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

	s.Assertions.Equal(size*times, len(read))
	s.Assertions.Equal(written, read)
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

	s.Assertions.Equal(size*times, len(read))
	s.Assertions.Equal(written, read)
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

		s.Assertions.Equal(len(bytes), n)
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
	s.Require().ErrorContains(err, "backup folder must be empty or set removeFiles = true")

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

		s.Assertions.Equal(len(bytes), n)
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
		s3Storasge.WithFile(backupFile),
	)
	s.Require().ErrorContains(err, "backup folder must be empty or set removeFiles = true")

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
