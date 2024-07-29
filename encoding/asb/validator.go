package asb

import (
	"fmt"
	"path/filepath"
)

type Validator struct {
}

func NewValidator() *Validator {
	return &Validator{}
}

// Run validates file name to match current encoder.
func (v *Validator) Run(fileName string) error {
	if filepath.Ext(fileName) != ".asb" {
		return fmt.Errorf("restore file %s is in an invalid format, expected extension: .asb, got: %s",
			fileName, filepath.Ext(fileName))
	}

	return nil
}
