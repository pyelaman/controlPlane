// Drill 3: Telemetry Stream Coalescer (Fan-In + Time/Size Batching)

// Scenario: Thousands of switches stream ON_CHANGE gNMI telemetry events
// into your Go service. Writing each event individually to a database or
// message bus will melt downstream storage.
// Build a coalescing pipeline that batches events and flushes whenever
// either maxBatchSize is reached or flushInterval expires.

package main

import (
	"context"
	"fmt"
	"time"
)

type TelemetryEvent struct {
	SwitchID  string
	Interface string
	State     string
}

func CoalesceStream(ctx context.Context, in <-chan TelemetryEvent, maxBatch int,
	flushInterval time.Duration, flushFn func([]TelemetryEvent)) {

	batch := make([]TelemetryEvent, 0, maxBatch)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) > 0 {
			flushFn(batch)
			// Resize to length 0 while keeping allocated capacity (no GC churn)
			batch = batch[:0]
		}
	}

	for {
		select {
		case <-ctx.Done():
			flush() // Drain remaining buffered items before exit
			return

		case event, ok := <-in:
			if !ok {
				flush()
				return
			}
			batch = append(batch, event)
			if len(batch) >= maxBatch {
				flush()
				// Reset timer so that double flush isn't done right away
				ticker.Reset(flushInterval)
			}

		case <-ticker.C:
			flush()
		}
	}
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(),
		350*time.Millisecond)

	defer cancel()

	events := make(chan TelemetryEvent, 32)
	go func() {
		for i := 1; i <= 20; i++ {
			events <- TelemetryEvent{
				SwitchID:  "ny5-leaf-01",
				Interface: fmt.Sprintf("Ethernet %d", i),
				State:     "UP",
			}
			time.Sleep(30 * time.Millisecond)
		}
		close(events)
	}()

	CoalesceStream(ctx, events, 3, 100*time.Millisecond,
		func(b []TelemetryEvent) {
			fmt.Printf("[Flushed Batch] Count: %d | First: %s | Last: %s\n",
				len(b), b[0].Interface, b[len(b)-1].Interface)
		})
}
