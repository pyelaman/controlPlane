package main

import (
	"fmt"
	"sync/atomic"
)

// SPSCRingBuffer is a lock-free, cache-line-padded ring buffer
// for 1 writer and 1 reader
type SPSCRingBuffer[T any] struct {
	_    [64]byte // Cache-line padding
	head atomic.Uint64
	_    [56]byte // Prevent false sharing between head and tail
	tail atomic.Uint64
	_    [56]byte
	mask uint64
	buf  []T
}

func NewSPSCRingBuffer[T any](powerOfTwoSize uint64) *SPSCRingBuffer[T] {
	if powerOfTwoSize == 0 || (powerOfTwoSize&(powerOfTwoSize-1)) != 0 {
		panic("capacity must be a power of 2")
	}
	return &SPSCRingBuffer[T]{
		mask: powerOfTwoSize - 1,
		buf:  make([]T, powerOfTwoSize),
	}
}

// Push is called exclusively by the single producer goroutine
func (q *SPSCRingBuffer[T]) Push(val T) bool {
	tail := q.tail.Load()
	head := q.head.Load()

	if tail-head > q.mask {
		return false // Buffer is full
	}

	q.buf[tail&q.mask] = val
	// Store acts as a release barrier ensuring write is complete before tail moves
	q.tail.Store(tail + 1)
	return true
}

// Pop is called exclusively by the single consumer goroutine
func (q *SPSCRingBuffer[T]) Pop() (T, bool) {
	var zero T
	head := q.head.Load()
	tail := q.tail.Load()

	if head == tail {
		return zero, false // Buffer is empty
	}

	val := q.buf[head&q.mask]
	q.buf[head&q.mask] = zero // Clear reference to avoid memory leaks for pointer types
	q.head.Store(head + 1)
	return val, true
}

func main() {
	// 1. SPSC lock-free ring buffer
	spsc := NewSPSCRingBuffer[int](1024)

	spsc.Push(42)
	spsc.Push(99)

	v1, _ := spsc.Pop()
	v2, _ := spsc.Pop()
	fmt.Printf("[SPSC] Popped values in strict FIFO order: %d, %d\n", v1, v2)
}
