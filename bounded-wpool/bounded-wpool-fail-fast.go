// Bounded worker pool with context cancellation and First-Error Fail-Fast
// You need to push configuration updates to 10,000 edge switches concurrently,
// capping in-flight gRPC calls at 50 workers to prevent overwhelming the network
// or exhausting file descriptors.
// If any critical device fails, cancel all remaining in-flight operations
// immediately.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type DeviceTask struct {
	DeviceID string
	Payload  string
}

// ProcessBatchConcurrent executes tasks across a bounded worker pool.
// Cancels all remaining work immediately if any task returns an error.
func ProcessBatchConcurrent(parentCtx context.Context, tasks []DeviceTask,
	maxWorkers int, fn func(context.Context, DeviceTask) error) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// taskCh is unbuffered to pause the producer when all workers busy
	taskCh := make(chan DeviceTask)
	var wg sync.WaitGroup

	// Capture only the first error safely across concurrent workers
	// Avoid race condition on firstErr
	var errOnce sync.Once
	var firstErr error

	// 1. Spawn bounded worker pool
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskCh:
					if !ok {
						return
					}
					if err := fn(ctx, task); err != nil {
						errOnce.Do(func() {
							firstErr = fmt.Errorf("device %s failed: %w",
								task.DeviceID, err)
							cancel() // Signal all peer workers to abort
						})
						return
					}
				}
			}
		}()
	}

	// 2. Feed tasks into channel while respecting context cancellation
FeedLoop:
	for _, t := range tasks {
		select {
		case <-ctx.Done(): // Prevents deadlock if workers exit early
			break FeedLoop
		case taskCh <- t:
		}
	}
	close(taskCh)

	// 3. Wait for all workers to drain cleanly efore return
	wg.Wait()
	return firstErr
}

func main() {
	tasks := make([]DeviceTask, 20)
	for i := range tasks {
		tasks[i] = DeviceTask{DeviceID: fmt.Sprintf("sv1-leaf-%02d", i)}
	}

	mockPush := func(ctx context.Context, t DeviceTask) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Millisecond):
			if t.DeviceID == "sv1-leaf-07" {
				return errors.New("gNMI connection refused")
			}
			return nil
		}
	}

	err := ProcessBatchConcurrent(context.Background(), tasks, 4, mockPush)
	fmt.Printf("Batch Result: %v\n", err)
}
