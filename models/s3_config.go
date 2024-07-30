package models

// S3Config represents the AWS S3 configuration.
type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}
