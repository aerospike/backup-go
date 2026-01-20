package collections

// ListToMap converts a slice of strings to a map of strings with boolean true values.
func ListToMap(slice []string) map[string]bool {
	result := make(map[string]bool, len(slice))

	for _, value := range slice {
		result[value] = true
	}

	return result
}
