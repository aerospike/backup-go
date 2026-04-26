package pipe

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/backup-go/models"
	"github.com/aerospike/backup-go/pipe/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestPipe_DynamicScaling(t *testing.T) {
	t.Parallel()

	// 1. Setup mocks
	newProcessor := func() Processor[*models.Token] {
		m := mocks.NewMockProcessor[*models.Token](t)
		m.EXPECT().Process(mock.Anything).Return(testToken(), nil).Maybe()
		return m
	}

	// First reader provides some data then stops.
	reader1 := mocks.NewMockReader[*models.Token](t)
	var r1Count int
	var r1Mu sync.Mutex
	reader1.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		time.Sleep(10 * time.Millisecond) // Slow it down
		r1Mu.Lock()
		defer r1Mu.Unlock()
		if r1Count < 10 {
			r1Count++
			return testToken(), nil
		}
		return nil, io.EOF
	})
	reader1.EXPECT().Close().Return()

	// Second reader to be added mid-flight.
	reader2 := mocks.NewMockReader[*models.Token](t)
	var r2Count int
	var r2Mu sync.Mutex
	reader2.EXPECT().Read(mock.Anything).RunAndReturn(func(context.Context) (*models.Token, error) {
		time.Sleep(10 * time.Millisecond) // Slow it down
		r2Mu.Lock()
		defer r2Mu.Unlock()
		if r2Count < 10 {
			r2Count++
			return testToken(), nil
		}
		return nil, io.EOF
	})
	reader2.EXPECT().Close().Return()

	// Writer
	writer1 := mocks.NewMockWriter[*models.Token](t)
	var wCount int
	var wMu sync.Mutex
	writer1.EXPECT().Write(mock.Anything).RunAndReturn(func(*models.Token) (int, error) {
		wMu.Lock()
		defer wMu.Unlock()
		wCount++
		return testSize, nil
	}).Maybe()
	writer1.EXPECT().Close().Return(nil).Maybe()

	// Second writer to be added mid-flight.
	writer2 := mocks.NewMockWriter[*models.Token](t)
	var w2Count int
	var w2Mu sync.Mutex
	writer2.EXPECT().Write(mock.Anything).RunAndReturn(func(*models.Token) (int, error) {
		w2Mu.Lock()
		defer w2Mu.Unlock()
		w2Count++
		return testSize, nil
	}).Maybe()
	writer2.EXPECT().Close().Return(nil).Maybe()

	// 2. Create Pipe
	p, err := NewPipe(
		newProcessor,
		[]Reader[*models.Token]{reader1},
		[]Writer[*models.Token]{writer1},
		nil,
		RoundRobin,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// 3. Run Pipe
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		done <- p.Run(ctx)
	}()

	<-started
	// Give it a tiny bit of time to actually enter Run and initialize errgroups
	time.Sleep(10 * time.Millisecond)

	// 4. Scale mid-flight
	// NO sleep here, we want to add them while others might be still starting or running.
	err = p.AddReader(ctx, reader2)
	require.NoError(t, err)
	err = p.AddWriter(ctx, writer2)
	require.NoError(t, err)

	// 5. Wait for completion
	err = <-done
	require.NoError(t, err)

	// 6. Verify results
	require.Equal(t, 10, r1Count)
	require.Equal(t, 10, r2Count)
	require.Equal(t, 20, wCount+w2Count)
	require.Positive(t, w2Count, "Writer 2 should have processed some records")
}
