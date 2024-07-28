package asb

import (
	"fmt"
	"path/filepath"
)

// Validate validates file name to match current encoder.
func Validate(fileName string) error {
	if filepath.Ext(fileName) != ".asb" {
		return fmt.Errorf("restore file %s is in an invalid format, expected extension: .asb, got: %s",
			fileName, filepath.Ext(fileName))
	}

	return nil
}
