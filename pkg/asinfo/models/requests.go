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

package models

// RequestBackup represents a request to start a backup job on the server.
type RequestBackup struct {
	RequestCommon
	ModifiedBefore     string
	ModifiedAfter      string
	SetList            string
	NoIndexes          bool
	NoUDFs             bool
	EnableChangeStream bool
}

// RequestRestore represents a request to start a restore job on the server.
type RequestRestore struct {
	RequestCommon
	JobID        string
	Path         string
	FuzzyRestore bool
}

// RequestCommon represents common fields for backup and restore requests.
type RequestCommon struct {
	Namespace string
	Storage   string
	Bucket    string
	Region    string
	Profile   string
	AccessKey string
	SecretKey string
	Endpoint  string
}
