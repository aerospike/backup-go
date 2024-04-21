package backup_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
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
	defer responseBody.Close()

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

	bytes := []byte{1, 2, 3, 4, 5}
	namespace := "ns1"
	s.write(namespace, bytes, config)

	buffer, n := s.read(config)
	s.Assertions.Equal(len(bytes), n)
	s.Assertions.Equal(bytes, buffer[:n])
}

func (s *writeReadTestSuite) write(namespace string, bytes []byte, config *backup.S3Config) {
	factory, _ := backup.NewS3WriterFactory(config, encoding.NewASBEncoderFactory())

	writer, err := factory.NewWriter(namespace, func(_ io.WriteCloser) error {
		return nil
	})
	if err != nil {
		s.FailNow("failed to create writer", err)
	}

	n, err := writer.Write(bytes)
	if err != nil {
		s.FailNow("failed to write", err)
	}

	s.Assertions.Equal(len(bytes), n)

	err = writer.Close()
	if err != nil {
		s.FailNow("failed to close writer", err)
	}
}

func (s *writeReadTestSuite) read(config *backup.S3Config) (buffer []byte, n int) {
	factory, _ := backup.NewS3ReaderFactory(config, encoding.NewASBDecoderFactory())

	readers, err := factory.Readers()
	if err != nil {
		s.FailNow("failed to create readers", err)
	}

	s.Assertions.Equal(1, len(readers))

	buffer = make([]byte, 10)
	n, err = readers[0].Read(buffer)
	if err != nil {
		s.FailNow("failed to read", err)
	}

	return buffer, n
}

func TestReadWrite(t *testing.T) {
	testSuite := writeReadTestSuite{}

	suite.Run(t, &testSuite)
}
