package models

type AwsS3 struct {
	Region      string
	Profile     string
	Endpoint    string
	MinPartSize int
}
