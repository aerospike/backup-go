package aerospike

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/stretchr/testify/require"
)

const (
	testASLoginPassword = "admin"
	testASNamespace     = "test"
	testASDC            = "DC1"
	testASHost          = "127.0.0.1"
	testASPort          = 3000
	testASRewind        = "all"
	testXRdHostPort     = "127.0.0.1:3003"
)

func TestInfoCommander_EnableDisableXDR(t *testing.T) {
	t.Parallel()

	asHost := aerospike.NewHost(testASHost, testASPort)
	asPolicy := aerospike.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword

	c := NewInfoCommander(asHost, asPolicy)

	err := c.StartXDR(testASDC, testXRdHostPort, testASNamespace, testASRewind)
	require.NoError(t, err)

	err = c.StopXDR(testASDC, testXRdHostPort, testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_BlockUnblockMRTWrites(t *testing.T) {
	t.Parallel()

	asHost := aerospike.NewHost(testASHost, testASPort)
	asPolicy := aerospike.NewClientPolicy()
	asPolicy.User = testASLoginPassword
	asPolicy.Password = testASLoginPassword

	c := NewInfoCommander(asHost, asPolicy)
	err := c.BlockMRTWrites(testASDC, testASNamespace)
	require.NoError(t, err)

	err = c.UnBlockMRTWrites(testASDC, testASNamespace)
	require.NoError(t, err)
}

func TestInfoCommander_parseResultResponse(t *testing.T) {
	tests := []struct {
		name     string
		cmd      string
		input    map[string]string
		expected string
		errMsg   string
	}{
		{
			name:     "Command exists with successful response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "success"},
			expected: "success",
			errMsg:   "",
		},
		{
			name:     "Command exists with failure response",
			cmd:      "testCommand",
			input:    map[string]string{"testCommand": "ERROR: command failed"},
			expected: "",
			errMsg:   "command testCommand failed: ERROR: command failed",
		},
		{
			name:     "Command not found in map",
			cmd:      "missingCommand",
			input:    map[string]string{"testCommand": "success"},
			expected: "",
			errMsg:   "no response for command missingCommand",
		},
		{
			name:     "Empty response map",
			cmd:      "testCommand",
			input:    map[string]string{},
			expected: "",
			errMsg:   "no response for command testCommand",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseResultResponse(tt.cmd, tt.input)
			if result != tt.expected {
				t.Errorf("expected result %v, got %v", tt.expected, result)
			}
			if err != nil {
				if err.Error() != tt.errMsg {
					t.Errorf("expected error message %v, got %v", tt.errMsg, err)
				}
			} else if tt.errMsg != "" {
				t.Errorf("expected error message %v, got nil", tt.errMsg)
			}
		})
	}
}
