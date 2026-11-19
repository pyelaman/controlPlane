package main

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// TestMPMCQueue_RaceStress verifies zero lost, duplicated, or corrupted items
// under heavy multi-producer, multi-consumer contention with -race enabled.
func TestMPMCQueue_RaceStress(t *testing.T) {
	const (
		queueCap         = 1024
		numProducers     = 8
		numConsumers     = 8
		itemsPerProducer = 25000
		totalItems       = numProducers * itemsPerProducer
	)

	q := NewMPMCQueue[uint64](queueCap)

	// Bitmap/atomic counter array to verify every unique sequence ID is received exactly once
	seen := make([]atomic.Int32, totalItems)

	var prodWG sync.WaitGroup
	var consWG sync.WaitGroup
	var consumedCount atomic.Uint64

	// 1. Launch Consumers
	consWG.Add(numConsumers)
	for c := 0; c < numConsumers; c++ {
		go func() {
			defer consWG.Done()
			for {
				if consumedCount.Load() >= totalItems {
					return
				}
				val, ok := q.Dequeue()
				if !ok {
					runtime.Gosched()
					continue
				}
				if val >= totalItems {
					t.Errorf("corrupted value dequeued: %d", val)
					return
				}
				seen[val].Add(1)
				consumedCount.Add(1)
			}
		}()
	}

	// 2. Launch Producers
	prodWG.Add(numProducers)
	for p := 0; p < numProducers; p++ {
		go func(producerID uint64) {
			defer prodWG.Done()
			base := producerID * itemsPerProducer
			for i := uint64(0); i < itemsPerProducer; i++ {
				item := base + i
				for !q.Enqueue(item) {
					runtime.Gosched()
				}
			}
		}(uint64(p))
	}

	prodWG.Wait()
	consWG.Wait()

	if consumedCount.Load() != totalItems {
		t.Fatalf("expected %d total items consumed, got %d", totalItems, consumedCount.Load())
	}

	// Verify exact-once delivery across all 200,000 items
	for i := 0; i < totalItems; i++ {
		if count := seen[i].Load(); count != 1 {
			t.Fatalf("item %d was consumed %d times (expected exact-once = 1)", i, count)
		}
	}
}

// BenchmarkMPMCQueue_Parallel measures lock-free MPMC throughput under high core contention.
func BenchmarkMPMCQueue_Parallel(b *testing.B) {
	q := NewMPMCQueue[uint64](4096)

	// Pre-fill half the queue so producers and consumers collide continuously
	for i := uint64(0); i < 2048; i++ {
		q.Enqueue(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var localCounter uint64
		for pb.Next() {
			localCounter++
			if localCounter&1 == 0 {
				for !q.Enqueue(localCounter) {
					runtime.Gosched()
				}
			} else {
				for {
					if _, ok := q.Dequeue(); ok {
						break
					}
					runtime.Gosched()
				}
			}
		}
	})
}

// BenchmarkGoChannel_Parallel measures standard mutex-backed buffered channel throughput
// under the exact same parallel workload for direct comparison.
func BenchmarkGoChannel_Parallel(b *testing.B) {
	ch := make(chan uint64, 4096)

	for i := uint64(0); i < 2048; i++ {
		ch <- i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var localCounter uint64
		for pb.Next() {
			localCounter++
			if localCounter&1 == 0 {
				ch <- localCounter
			} else {
				<-ch
			}
		}
	})
}
