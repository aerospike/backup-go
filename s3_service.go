package backup

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Config struct {
	Bucket   string
	Region   string
	Endpoint string
	Profile  string
	Prefix   string
}

func NewSession(config *S3Config) (*session.Session, error) {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String(config.Region),
			Endpoint:         aws.String(config.Endpoint),
			S3ForcePathStyle: aws.Bool(true),
		},
		Profile: config.Profile,
	})

	if err != nil {
		return nil, err
	}

	svc := s3.New(sess)
	_, err = svc.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(config.Bucket),
	})

	return sess, err
}
