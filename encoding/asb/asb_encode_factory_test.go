package asb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test(t *testing.T) {
	factory := NewASBEncoderFactory()
	filename := factory.GenerateFilename("test", 1)
	assert.Equal(t, "test_1.asb", filename)
}
