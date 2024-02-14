package datahandlers

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type proccessorTestSuite struct {
	suite.Suite
}

func (suite *proccessorTestSuite) TestNOOPProcessor() {
	noop := NewNOOPProcessor()
	suite.NotNil(noop)

	data := "test"
	processed, err := noop.Process(data)
	suite.Nil(err)
	suite.Equal(data, processed)
}

func TestProcessors(t *testing.T) {
	suite.Run(t, new(proccessorTestSuite))
}
