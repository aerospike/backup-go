package s3

// Config represents the AWS S3 configuration.
type Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}

// NewConfig returns new AWS S3 configuration.
func NewConfig(
	bucket string,
	region string,
	endpoint string,
	profile string,
	prefix string,
	chunkSize int,
) *Config {
	return &Config{
		Bucket:    bucket,
		Region:    region,
		Endpoint:  endpoint,
		Profile:   profile,
		Prefix:    prefix,
		ChunkSize: chunkSize,
	}
}
