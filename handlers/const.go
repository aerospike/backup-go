package handlers

type WorkMode int

const (
	Invalid WorkMode = iota
	SingleFile
	Directory
	// s3File
	// s3Dir
)
