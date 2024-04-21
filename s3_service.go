package backup

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}

const s3DefaultChunkSize = 5 * 1024 * 1024       // 5MB, minimum size of a part
const maxS3File = s3DefaultChunkSize * 1_000_000 // 5 TB
const s3type = "s3"

func newS3Client(s3Config *S3Config) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithSharedConfigProfile(s3Config.Profile),
		config.WithRegion(s3Config.Region),
	)

	if err != nil {
		return nil, fmt.Errorf("unable to load SDK s3Config, %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if s3Config.Endpoint != "" {
			o.BaseEndpoint = &s3Config.Endpoint
		}

		o.UsePathStyle = true
	})

	return client, nil
}
