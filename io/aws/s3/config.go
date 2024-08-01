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
