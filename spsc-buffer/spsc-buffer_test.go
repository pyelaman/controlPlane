package main

import (
	"runtime"
	"sync"
	"testing"
)

// TestSPSCRingBuffer_RaceAndFIFOStress verifies strict FIFO ordering, zero data loss,
// and race-free execution across 500,000 items between a dedicated producer and consumer.
func TestSPSCRingBuffer_RaceAndFIFOStress(t *testing.T) {
	const (
		queueCap   = 1024
		totalItems = 500_000
	)

	q := NewSPSCRingBuffer[uint64](queueCap)

	var wg sync.WaitGroup
	wg.Add(2)

	// 1. Single Producer Goroutine
	go func() {
		defer wg.Done()
		for i := uint64(0); i < totalItems; i++ {
			for !q.Push(i) {
				runtime.Gosched()
			}
		}
	}()

	// 2. Single Consumer Goroutine
	go func() {
		defer wg.Done()
		for expected := uint64(0); expected < totalItems; expected++ {
			for {
				val, ok := q.Pop()
				if !ok {
					runtime.Gosched()
					continue
				}
				if val != expected {
					t.Errorf("FIFO order violation: expected %d, got %d", expected, val)
					return
				}
				break
			}
		}
	}()

	wg.Wait()
}

// BenchmarkSPSCRingBuffer_Throughput measures dedicated 1-producer -> 1-consumer throughput.
func BenchmarkSPSCRingBuffer_Throughput(b *testing.B) {
	q := NewSPSCRingBuffer[uint64](4096)
	done := make(chan struct{})

	// Background consumer draining items as fast as the benchmark producer pushes them
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				if _, ok := q.Pop(); !ok {
					runtime.Gosched()
				}
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for !q.Push(uint64(i)) {
			runtime.Gosched()
		}
	}
	b.StopTimer()
	close(done)
}

// BenchmarkGoChannel_SPSCThroughput measures a standard buffered Go channel under the exact same 1:1 workload.
func BenchmarkGoChannel_SPSCThroughput(b *testing.B) {
	ch := make(chan uint64, 4096)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ch:
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- uint64(i)
	}
	b.StopTimer()
	close(done)
}
