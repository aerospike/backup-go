package internal

func ListToMap(slice []string) map[string]bool {
	result := make(map[string]bool)

	for _, value := range slice {
		result[value] = true
	}

	return result
}
