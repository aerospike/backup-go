package util

func ListToMap(slice []string) map[string]bool {
	result := make(map[string]bool, len(slice))

	for _, value := range slice {
		result[value] = true
	}

	return result
}
