package models

type S3Config struct {
	Bucket    string
	Region    string
	Endpoint  string
	Profile   string
	Prefix    string
	ChunkSize int
}
