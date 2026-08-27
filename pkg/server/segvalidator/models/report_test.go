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

import "testing"

func TestValidationReport_Failed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		report ValidationReport
		want   bool
	}{
		{
			name:   "nothing wrong",
			report: ValidationReport{CheckedSegments: 10, ValidSegments: 10},
			want:   false,
		},
		{
			name:   "a segment did not parse",
			report: ValidationReport{CheckedSegments: 10, ValidSegments: 9, InvalidSegments: 1},
			want:   true,
		},
		{
			// Every segment read back and the backup is still broken: a
			// manifest names something the storage does not hold.
			name:   "only the manifests are wrong",
			report: ValidationReport{CheckedSegments: 10, ValidSegments: 10, Manifests: ManifestReport{Problems: 2}},
			want:   true,
		},
		{
			// Segments nothing recorded are reported, not counted as failures.
			name:   "unrecorded segments alone",
			report: ValidationReport{CheckedSegments: 10, ValidSegments: 10, Manifests: ManifestReport{Unrecorded: 3}},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.report.Failed(); got != tt.want {
				t.Fatalf("Failed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidationReport_Truncated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		report ValidationReport
		want   bool
	}{
		{
			name:   "no failures",
			report: ValidationReport{},
			want:   false,
		},
		{
			name: "every failure is described",
			report: ValidationReport{
				InvalidSegments: 2,
				Issues:          make([]ValidationIssue, 2),
			},
			want: false,
		},
		{
			name: "more failures than issues",
			report: ValidationReport{
				InvalidSegments: 5000,
				Issues:          make([]ValidationIssue, 1000),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.report.Truncated(); got != tt.want {
				t.Fatalf("Truncated() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManifestReport_Truncated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		report ManifestReport
		want   bool
	}{
		{
			name:   "no problems",
			report: ManifestReport{},
			want:   false,
		},
		{
			name:   "every problem is described",
			report: ManifestReport{Problems: 3, Issues: make([]ManifestIssue, 3)},
			want:   false,
		},
		{
			name:   "more problems than issues",
			report: ManifestReport{Problems: 4000, Issues: make([]ManifestIssue, 1000)},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.report.Truncated(); got != tt.want {
				t.Fatalf("Truncated() = %v, want %v", got, tt.want)
			}
		})
	}
}
