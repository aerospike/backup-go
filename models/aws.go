package models

// S3Config represents the AWS S3 configuration.
// TODO: rename to AWS Config
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
