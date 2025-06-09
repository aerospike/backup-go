package pprof

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	DefaultProfilingDuration = 5 * time.Second
	DefaultProfilingDir      = "pprof"
)

type PeriodicProfiler struct {
	interval    time.Duration
	outputDir   string
	cpuDuration time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	counter int64
	mu      sync.RWMutex
}

// NewDefaultProfiler new creates a new PeriodicProfiler instance.
func NewDefaultProfiler() *PeriodicProfiler {
	return NewPeriodicProfilerWithCPUDuration(DefaultProfilingDuration, DefaultProfilingDir, DefaultProfilingDuration)
}

// NewPeriodicProfilerWithCPUDuration creates a new PeriodicProfiler instance with custom CPU duration.
func NewPeriodicProfilerWithCPUDuration(interval time.Duration, outputDir string, cpuDuration time.Duration,
) *PeriodicProfiler {
	ctx, cancel := context.WithCancel(context.Background())

	return &PeriodicProfiler{
		interval:    interval,
		outputDir:   outputDir,
		cpuDuration: cpuDuration,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts periodic profiling.
func (p *PeriodicProfiler) Start() error {
	if err := os.MkdirAll(p.outputDir, 0o755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	p.wg.Add(1)
	go p.run()

	log.Printf("Periodic profiler started (interval: %v, output: %s)", p.interval, p.outputDir)

	return nil
}

// Stop stops profiling.
func (p *PeriodicProfiler) Stop() {
	log.Println("Stopping periodic profiler...")
	p.cancel()
	p.wg.Wait()
	log.Println("Periodic profiler stopped")
}

// GetProfileCount returns the number of profiles taken so far.
func (p *PeriodicProfiler) GetProfileCount() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.counter
}

func (p *PeriodicProfiler) run() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.takeProfiles()
		}
	}
}

func (p *PeriodicProfiler) takeProfiles() {
	p.mu.Lock()
	p.counter++
	currentCount := p.counter
	p.mu.Unlock()

	timestamp := time.Now().Format("20060102_150405")

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := p.takeCPUProfile(timestamp, currentCount); err != nil {
			log.Printf("Error taking CPU profile: %v", err)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := p.takeMemoryProfile(timestamp, currentCount); err != nil {
			log.Printf("Error taking memory profile: %v", err)
		}
	}()

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := p.takeGoroutineProfile(timestamp, currentCount); err != nil {
			log.Printf("Error taking goroutine profile: %v", err)
		}
	}()

	wg.Wait()
	log.Printf("Profiles #%d saved (timestamp: %s)", currentCount, timestamp)
}

func (p *PeriodicProfiler) takeCPUProfile(timestamp string, count int64) error {
	filename := filepath.Join(p.outputDir, fmt.Sprintf("cpu_%s_%03d.prof", timestamp, count))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CPU profile file: %w", err)
	}
	defer file.Close()

	if err := pprof.StartCPUProfile(file); err != nil {
		return fmt.Errorf("failed to start CPU profile: %w", err)
	}

	time.Sleep(p.cpuDuration)

	pprof.StopCPUProfile()

	log.Printf("CPU profile saved: %s", filename)

	return nil
}

func (p *PeriodicProfiler) takeMemoryProfile(timestamp string, count int64) error {
	filename := filepath.Join(p.outputDir, fmt.Sprintf("mem_%s_%03d.prof", timestamp, count))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create memory profile file: %w", err)
	}
	defer file.Close()

	runtime.GC()

	if err := pprof.WriteHeapProfile(file); err != nil {
		return fmt.Errorf("failed to write heap profile: %w", err)
	}

	log.Printf("Memory profile saved: %s", filename)

	return nil
}

func (p *PeriodicProfiler) takeGoroutineProfile(timestamp string, count int64) error {
	filename := filepath.Join(p.outputDir, fmt.Sprintf("goroutine_%s_%03d.prof", timestamp, count))

	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create goroutine profile file: %w", err)
	}
	defer file.Close()

	if err := pprof.Lookup("goroutine").WriteTo(file, 0); err != nil {
		return fmt.Errorf("failed to write goroutine profile: %w", err)
	}

	log.Printf("Goroutine profile saved: %s (count: %d)", filename, runtime.NumGoroutine())

	return nil
}
