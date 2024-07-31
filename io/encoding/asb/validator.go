package asb

import (
	"fmt"
	"path/filepath"
)

// Validator represents backup files validator.
type Validator struct {
}

// NewValidator returns new validator instance for files validation.
func NewValidator() *Validator {
	return &Validator{}
}

// Run performs backup files validation.
func (v *Validator) Run(fileName string) error {
	if filepath.Ext(fileName) != ".asb" {
		return fmt.Errorf("restore file %s is in an invalid format, expected extension: .asb, got: %s",
			fileName, filepath.Ext(fileName))
	}

	return nil
}
