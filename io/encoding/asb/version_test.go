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

package asb

import (
	"strings"
	"testing"
)

func TestParseVersion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		input       string
		expected    version
		shouldError bool
		errorMsg    string
	}{
		{
			name:     "valid version 3.1",
			input:    "3.1",
			expected: version{Major: 3, Minor: 1},
		},
		{
			name:     "valid version 0.0",
			input:    "0.0",
			expected: version{Major: 0, Minor: 0},
		},
		{
			name:     "valid version 10.15",
			input:    "10.15",
			expected: version{Major: 10, Minor: 15},
		},
		{
			name:     "valid version 1.999",
			input:    "1.999",
			expected: version{Major: 1, Minor: 999},
		},
		{
			name:        "empty string",
			input:       "",
			shouldError: true,
			errorMsg:    "invalid version format",
		},
		{
			name:        "single number",
			input:       "3",
			shouldError: true,
			errorMsg:    "invalid version format",
		},
		{
			name:        "too many parts",
			input:       "3.1.2",
			shouldError: true,
			errorMsg:    "invalid version format",
		},
		{
			name:        "invalid major - letters",
			input:       "a.1",
			shouldError: true,
			errorMsg:    "invalid major version",
		},
		{
			name:        "invalid minor - letters",
			input:       "3.b",
			shouldError: true,
			errorMsg:    "invalid minor version",
		},
		{
			name:        "float major",
			input:       "3.5.1",
			shouldError: true,
			errorMsg:    "invalid version format",
		},
		{
			name:        "spaces in version",
			input:       " 3.1 ",
			shouldError: true,
			errorMsg:    "invalid major version",
		},
		{
			name:        "dot only",
			input:       ".",
			shouldError: true,
			errorMsg:    "invalid major version",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := parseVersion(tt.input)

			if tt.shouldError {
				if err == nil {
					t.Errorf("parseVersion(%q) expected error but got nil", tt.input)
					return
				}
				if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("parseVersion(%q) error = %v, want error containing %q",
						tt.input, err, tt.errorMsg)
				}
				return
			}

			if err != nil {
				t.Errorf("parseVersion(%q) unexpected error: %v", tt.input, err)
				return
			}

			if result.Major != tt.expected.Major || result.Minor != tt.expected.Minor {
				t.Errorf("parseVersion(%q) = %+v, want %+v",
					tt.input, result, tt.expected)
			}
		})
	}
}

func TestVersionCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		version1 *version
		version2 *version
		expected int
	}{
		{
			name:     "equal versions",
			version1: &version{Major: 3, Minor: 1},
			version2: &version{Major: 3, Minor: 1},
			expected: 0,
		},
		{
			name:     "version1 greater by major",
			version1: &version{Major: 4, Minor: 0},
			version2: &version{Major: 3, Minor: 1},
			expected: 1,
		},
		{
			name:     "version1 less by major",
			version1: &version{Major: 2, Minor: 9},
			version2: &version{Major: 3, Minor: 1},
			expected: -1,
		},
		{
			name:     "version1 greater by minor",
			version1: &version{Major: 3, Minor: 2},
			version2: &version{Major: 3, Minor: 1},
			expected: 1,
		},
		{
			name:     "version1 less by minor",
			version1: &version{Major: 3, Minor: 0},
			version2: &version{Major: 3, Minor: 1},
			expected: -1,
		},
		{
			name:     "major difference overrides minor",
			version1: &version{Major: 4, Minor: 0},
			version2: &version{Major: 3, Minor: 999},
			expected: 1,
		},
		{
			name:     "zero versions",
			version1: &version{Major: 0, Minor: 0},
			version2: &version{Major: 0, Minor: 0},
			expected: 0,
		},
		{
			name:     "large version numbers",
			version1: &version{Major: 100, Minor: 50},
			version2: &version{Major: 100, Minor: 49},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.version1.compare(tt.version2)
			if result != tt.expected {
				t.Errorf("version{%d, %d}.compare(version{%d, %d}) = %d, want %d",
					tt.version1.Major, tt.version1.Minor,
					tt.version2.Major, tt.version2.Minor,
					result, tt.expected)
			}
		})
	}
}

func TestVersionGreaterOrEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		version1 *version
		version2 *version
		expected bool
	}{
		{
			name:     "equal versions should return true",
			version1: &version{Major: 3, Minor: 1},
			version2: &version{Major: 3, Minor: 1},
			expected: true,
		},
		{
			name:     "greater major version should return true",
			version1: &version{Major: 4, Minor: 0},
			version2: &version{Major: 3, Minor: 1},
			expected: true,
		},
		{
			name:     "greater minor version should return true",
			version1: &version{Major: 3, Minor: 2},
			version2: &version{Major: 3, Minor: 1},
			expected: true,
		},
		{
			name:     "less major version should return false",
			version1: &version{Major: 2, Minor: 9},
			version2: &version{Major: 3, Minor: 1},
			expected: false,
		},
		{
			name:     "less minor version should return false",
			version1: &version{Major: 3, Minor: 0},
			version2: &version{Major: 3, Minor: 1},
			expected: false,
		},
		{
			name:     "zero version comparison",
			version1: &version{Major: 0, Minor: 1},
			version2: &version{Major: 0, Minor: 0},
			expected: true,
		},
		{
			name:     "real world example - supported version check",
			version1: &version{Major: 3, Minor: 5},
			version2: &version{Major: 3, Minor: 1}, // minimum required
			expected: true,
		},
		{
			name:     "real world example - unsupported version",
			version1: &version{Major: 2, Minor: 9},
			version2: &version{Major: 3, Minor: 1}, // minimum required
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.version1.greaterOrEqual(tt.version2)
			if result != tt.expected {
				t.Errorf("version{%d, %d}.greaterOrEqual(version{%d, %d}) = %t, want %t",
					tt.version1.Major, tt.version1.Minor,
					tt.version2.Major, tt.version2.Minor,
					result, tt.expected)
			}
		})
	}
}

func TestVersionValidationFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		headerVersion    string
		supportedVersion string
		shouldBeValid    bool
	}{
		{
			name:             "valid newer version",
			headerVersion:    "3.5",
			supportedVersion: "3.1",
			shouldBeValid:    true,
		},
		{
			name:             "valid equal version",
			headerVersion:    "3.1",
			supportedVersion: "3.1",
			shouldBeValid:    true,
		},
		{
			name:             "invalid older version",
			headerVersion:    "2.9",
			supportedVersion: "3.1",
			shouldBeValid:    false,
		},
		{
			name:             "invalid version format",
			headerVersion:    "invalid",
			supportedVersion: "3.1",
			shouldBeValid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			headerVer, err := parseVersion(tt.headerVersion)
			if err != nil {
				if tt.shouldBeValid {
					t.Errorf("unexpected error parsing header version: %v", err)
				}
				return
			}

			supportedVer, err := parseVersion(tt.supportedVersion)
			if err != nil {
				t.Errorf("unexpected error parsing supported version: %v", err)
				return
			}

			isValid := headerVer.greaterOrEqual(supportedVer)
			if isValid != tt.shouldBeValid {
				t.Errorf("version validation failed: header=%s, supported=%s, got=%t, want=%t",
					tt.headerVersion, tt.supportedVersion, isValid, tt.shouldBeValid)
			}
		})
	}
}
