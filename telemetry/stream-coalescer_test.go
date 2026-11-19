package main

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestCoalesceStream_SizeAndTimerTriggers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan TelemetryEvent, 16)

	var mu sync.Mutex
	var flushedBatches [][]TelemetryEvent

	flushFn := func(batch []TelemetryEvent) {
		mu.Lock()
		defer mu.Unlock()
		// Crucial: clone the slice because CoalesceStream reuses the underlying backing array via batch[:0]
		flushedBatches = append(flushedBatches, slices.Clone(batch))
	}

	done := make(chan struct{})
	go func() {
		CoalesceStream(ctx, in, 3, 60*time.Millisecond, flushFn)
		close(done)
	}()

	// 1. Send 3 events rapidly -> should trigger an immediate size-based flush of length 3
	for i := 1; i <= 3; i++ {
		in <- TelemetryEvent{SwitchID: "sv1-leaf-01", Interface: "Eth1", State: "UP"}
	}
	time.Sleep(15 * time.Millisecond)

	mu.Lock()
	if len(flushedBatches) != 1 || len(flushedBatches[0]) != 3 {
		mu.Unlock()
		t.Fatalf("expected 1 size-triggered batch of 3 events, got %+v", flushedBatches)
	}
	mu.Unlock()

	// 2. Send 2 events and wait past 60ms -> should trigger a timer-based flush of length 2
	in <- TelemetryEvent{SwitchID: "sv1-leaf-01", Interface: "Eth2", State: "DOWN"}
	in <- TelemetryEvent{SwitchID: "sv1-leaf-01", Interface: "Eth3", State: "DOWN"}
	time.Sleep(85 * time.Millisecond)

	mu.Lock()
	if len(flushedBatches) != 2 || len(flushedBatches[1]) != 2 {
		mu.Unlock()
		t.Fatalf("expected 2nd timer-triggered batch of 2 events, got %+v", flushedBatches)
	}
	mu.Unlock()

	// 3. Send 1 final event and immediately close channel -> should drain on exit
	in <- TelemetryEvent{SwitchID: "sv1-leaf-01", Interface: "Eth4", State: "UP"}
	close(in)
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(flushedBatches) != 3 || len(flushedBatches[2]) != 1 {
		t.Fatalf("expected 3rd shutdown-drain batch of 1 event, got %+v", flushedBatches)
	}
}
