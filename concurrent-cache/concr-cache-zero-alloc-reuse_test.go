package main

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestShardedARPTable_SetGetAndTTL(t *testing.T) {
	table := NewShardedARPTable()

	table.Set(1001, "10.0.0.1", "aa:bb:cc:dd:ee:01", 60*time.Millisecond)

	// Immediate lookup should hit
	mac, ok := table.Get(1001, "10.0.0.1")
	if !ok || mac != "aa:bb:cc:dd:ee:01" {
		t.Fatalf("expected cache hit with aa:bb:cc:dd:ee:01, got ok=%v mac=%s", ok, mac)
	}

	// Different VNI with identical IP must remain isolated
	if _, ok := table.Get(2002, "10.0.0.1"); ok {
		t.Fatal("expected cache miss for overlapping IP in isolated VNI 2002")
	}

	// Wait past TTL expiration
	time.Sleep(80 * time.Millisecond)
	if _, ok := table.Get(1001, "10.0.0.1"); ok {
		t.Fatal("expected cache miss after TTL expiration")
	}
}

func TestShardedARPTable_HighConcurrencyRace(t *testing.T) {
	table := NewShardedARPTable()
	const goroutines = 64
	const opsPerWorker = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(workerID int) {
			defer wg.Done()
			vni := uint32(5000 + (workerID % 8))
			for i := 0; i < opsPerWorker; i++ {
				ip := fmt.Sprintf("192.168.%d.%d", workerID%255, i%255)
				mac := fmt.Sprintf("02:00:00:00:%02x:%02x", workerID%255, i%255)

				table.Set(vni, ip, mac, time.Minute)
				_, _ = table.Get(vni, ip)
			}
		}(g)
	}

	wg.Wait()
}

func BenchmarkShardedARPTable_Parallel(b *testing.B) {
	table := NewShardedARPTable()
	table.Set(500100, "10.10.1.5", "00:1B:44:11:3A:B7", 10*time.Minute)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = table.Get(500100, "10.10.1.5")
		}
	})
}
