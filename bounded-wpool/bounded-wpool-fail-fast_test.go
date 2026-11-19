package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessBatchConcurrent_SuccessAndBounds(t *testing.T) {
	const (
		totalTasks = 50
		maxWorkers = 5
	)

	tasks := make([]DeviceTask, totalTasks)
	for i := 0; i < totalTasks; i++ {
		tasks[i] = DeviceTask{DeviceID: fmt.Sprintf("leaf-%02d", i)}
	}

	var activeWorkers atomic.Int32
	var peakWorkers atomic.Int32
	var completedTasks atomic.Int32

	fn := func(ctx context.Context, task DeviceTask) error {
		curr := activeWorkers.Add(1)
		// Track peak concurrency safely using a CAS loop
		for {
			peak := peakWorkers.Load()
			if curr <= peak || peakWorkers.CompareAndSwap(peak, curr) {
				break
			}
		}

		time.Sleep(5 * time.Millisecond)
		activeWorkers.Add(-1)
		completedTasks.Add(1)
		return nil
	}

	err := ProcessBatchConcurrent(context.Background(), tasks, maxWorkers, fn)
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}

	if completedTasks.Load() != totalTasks {
		t.Errorf("expected %d completed tasks, got %d", totalTasks, completedTasks.Load())
	}

	if peakWorkers.Load() > maxWorkers {
		t.Errorf("exceeded maxWorkers bound: peak was %d, limit was %d", peakWorkers.Load(), maxWorkers)
	}
}

func TestProcessBatchConcurrent_FailFastCancellation(t *testing.T) {
	tasks := make([]DeviceTask, 100)
	for i := range tasks {
		tasks[i] = DeviceTask{DeviceID: fmt.Sprintf("spine-%02d", i)}
	}

	var processedCount atomic.Int32
	expectedSentinel := errors.New("gNMI dial timeout")

	fn := func(ctx context.Context, task DeviceTask) error {
		if task.DeviceID == "spine-05" {
			return expectedSentinel
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
			processedCount.Add(1)
			return nil
		}
	}

	err := ProcessBatchConcurrent(context.Background(), tasks, 4, fn)
	if !errors.Is(err, expectedSentinel) {
		t.Fatalf("expected error wrapping %v, got: %v", expectedSentinel, err)
	}

	// Verify that fail-fast aborted the batch well before all 100 tasks finished
	if processedCount.Load() >= 20 {
		t.Errorf("expected early abort on error, but %d tasks still completed", processedCount.Load())
	}
}
