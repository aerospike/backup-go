package pipe

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func defaultToken() *models.Token {
	return &models.Token{
		Type:   models.TokenTypeRecord,
		Record: &models.Record{},
		Size:   10,
		Filter: nil,
	}
}

func TestChains_ReaderBackupChain(t *testing.T) {
	t.Parallel()

	readerMock := mocks.NewMockreader[*models.Token](t)

	var (
		result *models.Token
		mockCounter int
	)
	readerMock.On("Read").Run(func(args mock.Arguments) {
		if mockCounter < 5 {
			return
		}
		time.Sleep(1 * time.Second)
	}).Return(defaultToken(), nil)
	}).Return(nil, io.EOF)
	readerMock.On("Close")

	processorMock := mocks.NewMockprocessor[*models.Token](t)
	processorMock.On("Process", defaultToken()).Return(defaultToken(), nil)

	readChain, output := NewReaderBackupChain[*models.Token](readerMock, processorMock)

	go func() {
		err := readChain.Run(context.Background())
		require.NoError(t, err)
	}()

	for msg := range output {
		fmt.Println(msg)
	}
}
