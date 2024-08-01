package backup

import "github.com/aerospike/backup-go/io/aws/s3"

// S3Config represents the AWS S3 configuration.
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}

// NewS3Config returns new AWS S3 configuration.
func NewS3Config(
	bucket string,
	region string,
	endpoint string,
	profile string,
	prefix string,
	chunkSize int,
) *S3Config {
	return &S3Config{
		Bucket:    bucket,
		Region:    region,
		Endpoint:  endpoint,
		Profile:   profile,
		Prefix:    prefix,
		ChunkSize: chunkSize,
	}
}

// mapS3Config maps config from package backup to internal s3 config.
func mapS3Config(cfg *S3Config) *s3.Config {
	return &s3.Config{
		Bucket:    cfg.Bucket,
		Region:    cfg.Region,
		Endpoint:  cfg.Endpoint,
		Profile:   cfg.Profile,
		Prefix:    cfg.Prefix,
		ChunkSize: cfg.ChunkSize,
	}
}
