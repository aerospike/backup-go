package models

import "io"

// File represents a file with name and reader.
type File struct {
	Name   string
	Reader io.ReadCloser
}
