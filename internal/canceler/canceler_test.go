package canceler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestContext(t *testing.T) {
	tests := []struct {
		name       string
		setup      func() (*Context, *int64) // returns context and call counter
		operations func(*Context, *int64)    // operations to perform
		wantCalls  int64                     // expected number of cancel calls
		wantPanic  bool                      // should panic
		concurrent bool                      // test concurrency
	}{
		{
			name: "new_context_empty",
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				ctx.Cancel() // should not panic on empty
			},
			wantCalls: 0,
			wantPanic: false,
		},
		{
			name: "add_single_function",
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				ctx.AddCancelFunc(func() { atomic.AddInt64(counter, 1) })
				ctx.Cancel()
			},
			wantCalls: 1,
			wantPanic: false,
		},
		{
			name: "add_multiple_functions",
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				for i := 0; i < 5; i++ {
					ctx.AddCancelFunc(func() { atomic.AddInt64(counter, 1) })
				}
				ctx.Cancel()
			},
			wantCalls: 5,
			wantPanic: false,
		},
		{
			name: "multiple_cancels",
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				ctx.AddCancelFunc(func() { atomic.AddInt64(counter, 1) })
				ctx.Cancel()
				ctx.Cancel() // second cancel should call again
			},
			wantCalls: 2,
			wantPanic: false,
		},
		{
			name:       "concurrent_add_and_cancel",
			concurrent: true,
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				var wg sync.WaitGroup

				// Add functions concurrently
				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for j := 0; j < 10; j++ {
							ctx.AddCancelFunc(func() { atomic.AddInt64(counter, 1) })
						}
					}()
				}

				// Cancel concurrently
				wg.Add(1)
				go func() {
					defer wg.Done()
					time.Sleep(5 * time.Millisecond)
					ctx.Cancel()
				}()

				wg.Wait()
			},
			wantCalls: -1, // variable, just check no panic
			wantPanic: false,
		},
		{
			name: "integration_with_real_context",
			setup: func() (*Context, *int64) {
				ctx := NewContext()
				counter := new(int64)
				return ctx, counter
			},
			operations: func(ctx *Context, counter *int64) {
				realCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()

				ctx.AddCancelFunc(cancel)
				ctx.AddCancelFunc(func() { atomic.AddInt64(counter, 1) })

				ctx.Cancel()

				// Verify context was cancelled
				select {
				case <-realCtx.Done():
					if realCtx.Err() != context.Canceled {
						t.Errorf("Expected context.Canceled, got %v", realCtx.Err())
					}
				case <-time.After(100 * time.Millisecond):
					t.Error("Context was not cancelled")
				}
			},
			wantCalls: 1,
			wantPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.concurrent {
				// Run with race detector for concurrent tests
				t.Parallel()
			}

			ctx, counter := tt.setup()

			// Verify NewContext initialization
			if ctx == nil {
				t.Fatal("NewContext() returned nil")
			}
			if ctx.cancelFunc == nil {
				t.Fatal("NewContext() cancelFunc slice is nil")
			}
			if len(ctx.cancelFunc) != 0 {
				t.Errorf("NewContext() initial length = %d, want 0", len(ctx.cancelFunc))
			}

			var panicOccurred bool
			func() {
				defer func() {
					if r := recover(); r != nil {
						panicOccurred = true
						if !tt.wantPanic {
							t.Errorf("Unexpected panic: %v", r)
						}
					}
				}()

				tt.operations(ctx, counter)
			}()

			if tt.wantPanic && !panicOccurred {
				t.Error("Expected panic but none occurred")
			}

			if tt.wantCalls >= 0 {
				if got := atomic.LoadInt64(counter); got != tt.wantCalls {
					t.Errorf("Cancel functions called %d times, want %d", got, tt.wantCalls)
				}
			} else {
				// For concurrent tests, just verify some calls happened and no panic
				if got := atomic.LoadInt64(counter); got == 0 && !tt.wantPanic {
					t.Error("Expected some cancel functions to be called in concurrent test")
				}
			}
		})
	}
}
