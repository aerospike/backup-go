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

package blob

type options struct {
	// pathList contains list of files or directories.
	pathList []string
	// isDir flag describes what we have in path, file or directory.
	isDir bool
	// isRemovingFiles flag describes should we remove everything from backup folder or not.
	isRemovingFiles bool
	// validator contains files validator that is applied to files if isDir = true.
	validator validator
	// Concurrency defines the max number of concurrent uploads to be performed to upload the file.
	// Each concurrent upload will create a buffer of size BlockSize.
	uploadConcurrency int
	// withNestedDir describes if we should check for if an object is a directory for read/write operations.
	// When we stream files or delete files in folder, we skip directories. This flag will avoid skipping.
	// Default: false
	withNestedDir bool
	// startAfter is an artificial parameter. Used to skip objects in the storage.
	// The Result will not include an object specified in startAfter.
	// If it is set, then we compare the names received from the storage lexicographically,
	// and if the name is less than the specified parameter, we skip this object.
	startAfter string
	// skipDirCheck if true, backup directory won't be checked.
	skipDirCheck bool
	// sortFiles shows if we need to sort files before read.
	sortFiles bool
}

type Opt func(*options)

// WithDir adds directory to reading/writing files from/to.
func WithDir(path string) Opt {
	return func(r *options) {
		r.pathList = append(r.pathList, path)
		r.isDir = true
	}
}

// WithDirList adds a directory list to read files from.
// Is used only for Reader.
func WithDirList(pathList []string) Opt {
	return func(r *options) {
		r.pathList = pathList
		r.isDir = true
	}
}

// WithFile adds a file path to reading/writing from/to.
func WithFile(path string) Opt {
	return func(r *options) {
		r.pathList = append(r.pathList, path)
		r.isDir = false
	}
}

// WithFileList adds a file list to read from.
// Is used only for Reader.
func WithFileList(pathList []string) Opt {
	return func(r *options) {
		r.pathList = pathList
		r.isDir = false
	}
}

// WithValidator adds validator to Reader, so files will be validated before reading.
// Is used only for Reader.
func WithValidator(v validator) Opt {
	return func(r *options) {
		r.validator = v
	}
}

// WithNestedDir adds withNestedDir = true parameter. That means that we won't skip nested folders.
func WithNestedDir() Opt {
	return func(r *options) {
		r.withNestedDir = true
	}
}

// WithRemoveFiles adds remove files flag, so all files will be removed from backup folder before backup.
// Is used only for Writer.
func WithRemoveFiles() Opt {
	return func(r *options) {
		r.isRemovingFiles = true
	}
}

// WithUploadConcurrency define max number of concurrent uploads to be performed to upload the file.
// Is used only for Writer.
func WithUploadConcurrency(v int) Opt {
	return func(r *options) {
		r.uploadConcurrency = v
	}
}

// WithStartAfter adds start after parameter to list request.
// The Value of start after will be not included in a result.
// You will receive objects after this parameter.
// Is used only for Reader.
func WithStartAfter(v string) Opt {
	return func(r *options) {
		r.startAfter = v
	}
}

// WithSkipDirCheck adds skip dir check flags.
// Which means that backup directory won't be checked for emptiness.
func WithSkipDirCheck() Opt {
	return func(r *options) {
		r.skipDirCheck = true
	}
}

// WithSorting adds a sorting flag.
// Which means that files will be read from directory in the sorted order.
// Is used only for Reader.
func WithSorting() Opt {
	return func(r *options) {
		r.sortFiles = true
	}
}
