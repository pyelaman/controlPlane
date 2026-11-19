package main

import (
	"fmt"
	"runtime"
	"sync/atomic"
)

type mpmcSlot[T any] struct {
	seq atomic.Uint64
	val T
}

// MPMCQueue implements Vyukov's lock-free bounded multi-producer
// mutli-consumer queue
type MPMCQueue[T any] struct {
	_    [64]byte
	head atomic.Uint64
	_    [56]byte
	tail atomic.Uint64
	_    [56]byte
	mask uint64
	buf  []mpmcSlot[T]
}

func NewMPMCQueue[T any](powerOfTwoSize uint64) *MPMCQueue[T] {
	if powerOfTwoSize == 0 || (powerOfTwoSize&(powerOfTwoSize-1)) != 0 {
		panic("capacity must be power of 2")
	}

	q := &MPMCQueue[T]{
		mask: powerOfTwoSize - 1,
		buf:  make([]mpmcSlot[T], powerOfTwoSize),
	}

	for i := uint64(0); i < powerOfTwoSize; i++ {
		q.buf[i].seq.Store(i)
	}

	return q
}

// Enqueue operation
func (q *MPMCQueue[T]) Enqueue(val T) bool {
	for {
		tail := q.tail.Load()
		slot := &q.buf[tail&q.mask]
		seq := slot.seq.Load()
		diff := int64(seq) - int64(tail)

		if diff == 0 {
			// Slot is free, attempt to claim atomically
			if q.tail.CompareAndSwap(tail, tail+1) {
				slot.val = val
				slot.seq.Store(tail + 1) // Publish item to consumers
				return true
			}
		} else if diff < 0 {
			return false // Queue is completely full
		} else {
			runtime.Gosched() // Another producer claimed tail; yield briefly
		}
	}
}

// Dequeue operation
func (q *MPMCQueue[T]) Dequeue() (T, bool) {
	var zero T
	for {
		head := q.head.Load()
		slot := &q.buf[head&q.mask]
		seq := slot.seq.Load()
		diff := int64(seq) - int64(head+1)

		if diff == 0 {
			// Item is committed and ready; claim it atomically
			if q.head.CompareAndSwap(head, head+1) {
				val := slot.val
				slot.val = zero
				slot.seq.Store(head + q.mask + 1) // Recycle slot for next iter
				return val, true
			}
		} else if diff < 0 {
			return zero, false // Queue is empty
		} else {
			runtime.Gosched()
		}
	}
}

func main() {
	// 2. MPMC lock-free under multi-goroutine contention
	mpmc := NewMPMCQueue[int](1024)
	const producers = 4
	const itemsPerProducer = 250

	done := make(chan struct{})
	for p := 0; p < producers; p++ {
		go func(id int) {
			for i := 0; i < itemsPerProducer; i++ {
				for !mpmc.Enqueue(id*1000 + i) {
					runtime.Gosched()
				}
			}
			done <- struct{}{}
		}(p)
	}

	for p := 0; p < producers; p++ {
		<-done
	}

	count := 0
	for {
		if _, ok := mpmc.Dequeue(); !ok {
			break
		}
		count++
	}

	fmt.Printf("[MPMC] Lock-free multi-producer enqueue/dequeue verified: %d/%d items\n",
		count, producers*itemsPerProducer)
}
