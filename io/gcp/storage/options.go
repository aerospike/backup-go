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

type options struct {
	// path contains path to file or directory.
	path string
	// isDir flag describes what we have in path, file or directory.
	isDir bool
	// isRemovingFiles flag describes should we remove everything from backup folder or not.
	isRemovingFiles bool
	// validator contains files validator that is applied to files if isDir = true.
	validator validator
	// withNestedDir describes if we should check for if an object is a directory for read/write operations.
	// When we stream files or delete files in folder, we skip directories. This flag will avoid skipping.
	// Default: false
	withNestedDir bool
	// startOffset is used to filter results to objects whose names are
	// lexicographically equal to or after startOffset.
	startOffset string
	// skipDirCheck if true, backup directory won't be checked.
	skipDirCheck bool
}

type Opt func(*options)

// WithDir adds directory to reading/writing files from/to.
func WithDir(path string) Opt {
	return func(r *options) {
		r.path = path
		r.isDir = true
	}
}

// WithFile adds a file path to reading/writing from/to.
func WithFile(path string) Opt {
	return func(r *options) {
		r.path = path
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

// WithStartOffset adds start offset parameter to list request.
// The Value of start offset will be included in a result.
// You will receive objects including start offset.
// Is used only for Reader.
func WithStartOffset(v string) Opt {
	return func(r *options) {
		r.startOffset = v
	}
}

// WithSkipDirCheck adds skip dir check flags.
// Which means that backup directory won't be checked for emptiness.
func WithSkipDirCheck() Opt {
	return func(r *options) {
		r.skipDirCheck = true
	}
}
