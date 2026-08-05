package common

import (
	"testing"
)

func TestValidateFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{"empty", "", false},
		{"valid", "metadata.asb", false},
		{"valid_numbered", "001.asb", false},
		{"dot", ".", true},
		{"dotdot", "..", true},
		{"slash", "dir/file", true},
		{"backslash", "dir\\file", true},
		{"null", "file\x00.asb", true},
		{"absolute", "/etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFilename(tt.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateFilename() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
