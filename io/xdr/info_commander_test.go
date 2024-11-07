package xdr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testASDC            = "dc1"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testASRewind        = "all"
)

func TestInfoCommander_EnableDisableXDR(t *testing.T) {
	t.Parallel()

	c := NewInfoCommander(testASHost, testASPort, testASLoginPassword, testASLoginPassword)

	err := c.EnableXDR(testASDC, testASNamespace, testASRewind)
	require.NoError(t, err)

	err = c.DisableXDR(testASDC, testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_BlockUnblockMRTWrites(t *testing.T) {
	t.Parallel()

	c := NewInfoCommander(testASHost, testASPort, testASLoginPassword, testASLoginPassword)

	err := c.BlockMRTWrites(testASDC, testASNamespace)
	require.NoError(t, err)

	err = c.UnBlockMRTWrites(testASDC, testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_parseResultResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cmd      string
		input    map[string]string
		expected error
	}{
		{
			name:     "Command exists with OK response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "ok"},
			expected: nil,
		},
		{
			name:     "Command exists with failure response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "error"},
			expected: fmt.Errorf("command testCommand failed: error"),
		},
		{
			name:     "Command not found in response map",
			cmd:      "missingCommand",
			input:    map[string]string{"testCommand": "ok"},
			expected: fmt.Errorf("no response for command missingCommand"),
		},
		{
			name:     "Empty response map",
			cmd:      "testCommand",
			input:    map[string]string{},
			expected: fmt.Errorf("no response for command testCommand"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseResultResponse(tt.cmd, tt.input)
			if (err == nil && tt.expected != nil) || (err != nil && tt.expected == nil) {
				t.Errorf("expected %v, got %v", tt.expected, err)
			} else if err != nil && tt.expected != nil && err.Error() != tt.expected.Error() {
				t.Errorf("expected error message %v, got %v", tt.expected, err)
			}
		})
	}
}
