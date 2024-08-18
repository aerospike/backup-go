package storage

// Config represents GCP Storage configuration.
type Config struct {
	// Bucket name of the bucket at GCP Storage.
	Bucket string
	// Folder name where backup files are placed.
	Folder string
}
