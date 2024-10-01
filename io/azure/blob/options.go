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
	// path contains path to file or directory.
	path string
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
	// marker is a string value that identifies the portion of the list of containers to be
	// returned with the next listing operation.
	// The operation returns the NextMarker value within the response body if the listing
	// operation did not return all containers remaining to be listed with the current page.
	// The NextMarker value can be used
	// as the value for the marker parameter in a subsequent call to request the next
	// page of list items. The marker value is opaque to the client.
	marker string
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

// WithUploadConcurrency define max number of concurrent uploads to be performed to upload the file.
// Is used only for Writer.
func WithUploadConcurrency(v int) Opt {
	return func(r *options) {
		r.uploadConcurrency = v
	}
}

// WithMarker adds marker parameter to list request.
// The Value of marker will be not included in a result.
// You will receive objects after marker.
// Is used only for Reader.
func WithMarker(v string) Opt {
	return func(r *options) {
		r.marker = v
	}
}
