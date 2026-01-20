package collections

import "testing"

func TestListToMap(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    []string
		expected map[string]bool
	}{
		{
			name:     "empty slice",
			input:    []string{},
			expected: map[string]bool{},
		},
		{
			name:     "single element",
			input:    []string{"one"},
			expected: map[string]bool{"one": true},
		},
		{
			name:     "multiple unique elements",
			input:    []string{"one", "two", "three"},
			expected: map[string]bool{"one": true, "two": true, "three": true},
		},
		{
			name:     "duplicate elements",
			input:    []string{"one", "one", "two"},
			expected: map[string]bool{"one": true, "two": true},
		},
		{
			name:     "empty string element",
			input:    []string{""},
			expected: map[string]bool{"": true},
		},
		{
			name:     "mixed elements",
			input:    []string{"", "one", "two", ""},
			expected: map[string]bool{"": true, "one": true, "two": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ListToMap(tt.input)

			// Check if maps have the same length
			if len(result) != len(tt.expected) {
				t.Errorf("map length mismatch: got %d, want %d", len(result), len(tt.expected))
			}

			// Check if all expected keys exist with correct values
			for key := range tt.expected {
				if value, exists := result[key]; !exists || !value {
					t.Errorf("key %q: got %v, want true", key, value)
				}
			}

			// Check if there are no extra keys
			for key := range result {
				if _, exists := tt.expected[key]; !exists {
					t.Errorf("unexpected key in result: %q", key)
				}
			}
		})
	}
}
