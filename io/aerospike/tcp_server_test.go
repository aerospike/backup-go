package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/stretchr/testify/require"
)

const (
	testAddress = "localhost:1000"
	testTimeout = 1 * time.Second
	testPayload = "payload"
)

func Test_TCPServer(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	resultChan := make(chan *models.XDRToken)
	errChan := make(chan error, 1)

	tcpSrv := NewTCPServer(testAddress, testTimeout, nil, resultChan, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := tcpSrv.Start(ctx); err != nil {
			errChan <- err
		}
	}()

	// Wait for the server to start.
	time.Sleep(1 * time.Second)

	conn, err := net.Dial("tcp", testAddress)
	require.NoError(t, err)
	defer conn.Close()

	data := []byte(testPayload)
	length := len(data)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(length))
	_, err = conn.Write(append(header, data...))
	require.NoError(t, err)

	result := <-resultChan
	fmt.Println("result:", result)
	require.NotNil(t, result)
}
