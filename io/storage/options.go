// Copyright 2024 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"log/slog"
	"time"
)

type validator interface {
	Run(fileName string) error
}

type Options struct {
	// PathList contains a list of files or directories.
	PathList []string
	// IsDir flag determines whether the path is a directory.
	IsDir bool
	// IsRemovingFiles flag specifies whether to clear the backup folder.
	IsRemovingFiles bool
	// Validator contains files Validator that is applied to files if IsDir is true.
	Validator validator
	// WithNestedDir determines whether the read/write operations should treat objects
	// identified as directories like regular files.
	// When we stream files or delete files in folder, we skip directories. Setting
	// WithNestedDir to true disables this directory skipping behavior.
	// Default: false
	WithNestedDir bool
	// StartAfter is an artificial parameter. Used to skip objects in the storage.
	// The Result will not include an object specified in startAfter.
	// If it is set, then we compare the names received from the storage lexicographically,
	// and if the name is less than the specified parameter, we skip this object.
	StartAfter string
	// SkipDirCheck, if true, the backup directory won't be checked.
	SkipDirCheck bool
	// SortFiles determines whether we need to sort files before reading.
	SortFiles bool

	// UploadConcurrency defines the max number of concurrent uploads to be performed to
	// upload the file. Each concurrent upload will create a buffer of size BlockSize.
	UploadConcurrency int

	// StorageClass specifies the type of storage class.
	// Supported values depend on the cloud provider being used.
	StorageClass string

	// The access tier that will be applied to back up files.
	AccessTier string

	// Logger contains logger.
	Logger *slog.Logger

	// How often restore status will be polled from cloud provider.
	PollWarmDuration time.Duration

	// Size of chunk to upload.
	ChunkSize int
}

type Opt func(*Options)

// WithDir adds the directory to reading/writing files from/to.
func WithDir(path string) Opt {
	return func(r *Options) {
		r.PathList = append(r.PathList, path)
		r.IsDir = true
	}
}

// WithDirList adds the directory list to read files from.
// Is used only for Reader.
func WithDirList(pathList []string) Opt {
	return func(r *Options) {
		r.PathList = pathList
		r.IsDir = true
	}
}

// WithFile adds the file path to reading/writing from/to.
func WithFile(path string) Opt {
	return func(r *Options) {
		r.PathList = append(r.PathList, path)
		r.IsDir = false
	}
}

// WithFileList adds the file list to read from.
// Is used only for Reader.
func WithFileList(pathList []string) Opt {
	return func(r *Options) {
		r.PathList = pathList
		r.IsDir = false
	}
}

// WithValidator adds the Validator to Reader, so files will be validated before reading.
// Is used only for Reader.
func WithValidator(v validator) Opt {
	return func(r *Options) {
		r.Validator = v
	}
}

// WithNestedDir adds WithNestedDir = true parameter. That means that we won't skip nested folders.
func WithNestedDir() Opt {
	return func(r *Options) {
		r.WithNestedDir = true
	}
}

// WithRemoveFiles adds remove files flag, so all files will be removed from backup folder before backup.
// Is used only for Writer.
func WithRemoveFiles() Opt {
	return func(r *Options) {
		r.IsRemovingFiles = true
	}
}

// WithStartAfter adds start after parameter to list request.
// Is used only for Reader.
func WithStartAfter(v string) Opt {
	return func(r *Options) {
		r.StartAfter = v
	}
}

// WithSkipDirCheck adds skip dir check flags.
// Which means that backup directory won't be checked for emptiness.
func WithSkipDirCheck() Opt {
	return func(r *Options) {
		r.SkipDirCheck = true
	}
}

// WithSorting adds a sorting flag.
// Which means that files will be read from directory in the sorted order.
// Is used only for Reader.
func WithSorting() Opt {
	return func(r *Options) {
		r.SortFiles = true
	}
}

// WithUploadConcurrency defines max number of concurrent uploads to be performed to upload the file.
// Is used only for Azure Writer.
func WithUploadConcurrency(v int) Opt {
	return func(r *Options) {
		r.UploadConcurrency = v
	}
}

// WithStorageClass sets the storage class.
// For writers, files are saved to the specified class (in case of Azure it will be tier).
func WithStorageClass(class string) Opt {
	return func(r *Options) {
		r.StorageClass = class
	}
}

// WithAccessTier sets the access tier for archived files.
// For readers, archived files are restored to the specified tier.
func WithAccessTier(tier string) Opt {
	return func(r *Options) {
		r.AccessTier = tier
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Opt {
	return func(r *Options) {
		r.Logger = logger
	}
}

// WithWarmPollDuration sets the restore status polling interval.
func WithWarmPollDuration(duration time.Duration) Opt {
	return func(r *Options) {
		r.PollWarmDuration = duration
	}
}

// WithChunkSize sets the chunk size for uploading files.
// Is used only for Writer.
func WithChunkSize(bytes int) Opt {
	return func(r *Options) {
		r.ChunkSize = bytes
	}
}
