package backup_test

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

	"github.com/aerospike/backup-go"
	"github.com/aerospike/backup-go/encoding"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/go-connections/nat"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/suite"
)

type writeReadTestSuite struct {
	suite.Suite
	docker  *client.Client
	minioID string
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
	config := &backup.S3Config{
		Bucket:   "backup",
		Region:   "eu",
		Endpoint: "http://localhost:9000",
		Profile:  "minio",
	}

	size := 500_000
	times := 100
	written := s.write("ns1", size, times, config)
	read := s.read(config)

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

func (s *writeReadTestSuite) write(namespace string, bytes, times int, config *backup.S3Config) []byte {
	factory, _ := backup.NewS3WriterFactory(config, encoding.NewASBEncoderFactory())

	writer, err := factory.NewWriter(namespace, func(_ io.WriteCloser) error {
		return nil
	})
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

		s.Assertions.Equal(len(bytes), n)
		allBytesWritten = append(allBytesWritten, bytes...)
	}

	err = writer.Close()
	if err != nil {
		s.FailNow("failed to close writer", err)
	}
	return allBytesWritten
}

func (s *writeReadTestSuite) read(config *backup.S3Config) []byte {
	factory, _ := backup.NewS3ReaderFactory(config, encoding.NewASBDecoderFactory())

	readers, err := factory.Readers()
	if err != nil {
		s.FailNow("failed to create readers", err)
	}

	s.Assertions.Equal(1, len(readers))

	buffer, err := io.ReadAll(readers[0])
	if err != nil {
		s.FailNow("failed to read", err)
	}

	_ = readers[0].Close()
	return buffer
}

func TestReadWrite(t *testing.T) {
	testSuite := writeReadTestSuite{}

	suite.Run(t, &testSuite)
}