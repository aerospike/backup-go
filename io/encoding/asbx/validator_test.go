package asbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidator_Run(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
		wantErr  string
	}{
		{
			name:     "valid extension",
			fileName: "backup.asbx",
		},
		{
			name:     "valid extension with path",
			fileName: "/path/to/backup.asbx",
		},
		{
			name:     "valid extension with multiple dots",
			fileName: "backup.2024.01.asbx",
		},
		{
			name:     "invalid extension",
			fileName: "backup.txt",
			wantErr:  "restore file backup.txt is in an invalid format, expected extension: .asbx, got: .txt",
		},
		{
			name:     "no extension",
			fileName: "backup",
			wantErr:  "restore file backup is in an invalid format, expected extension: .asbx, got: ",
		},
		{
			name:     "wrong case extension",
			fileName: "backup.ASBX",
			wantErr:  "restore file backup.ASBX is in an invalid format, expected extension: .asbx, got: .ASBX",
		},
		{
			name:     "dot file without extension",
			fileName: ".backup",
			wantErr:  "restore file .backup is in an invalid format, expected extension: .asbx, got: .backup",
		},
		{
			name:     "empty filename",
			fileName: "",
			wantErr:  "restore file  is in an invalid format, expected extension: .asbx, got: ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewValidator()
			err := v.Run(tt.fileName)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantErr, err.Error())
				return
			}

			require.NoError(t, err)
		})
	}
}
