// Scenario: At massive scale, a single sync.RWMutex protecting a global routing/ARP
// table becomes a severe bottleneck across 64+ CPU cores.
// Implement a sharded concurrent map using FNV-1a hashingto distribute lock
// contention, paired with sync.Pool for zero-allocation key/buffer formattin.
package main

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const shardCount = 32 // Power of 2 for fast bitwise masking

type cacheEntry struct {
	macAddr   string
	expiresAt time.Time
}

type shard struct {
	mtx   sync.RWMutex
	items map[string]cacheEntry
}

// Sharded table eliminates global lock contention across control planes
// on different cores
type ShardedARPTable struct {
	shards [shardCount]*shard
}

func NewShardedARPTable() *ShardedARPTable {
	t := &ShardedARPTable{}
	for i := 0; i < shardCount; i++ {
		t.shards[i] = &shard{
			items: make(map[string]cacheEntry),
		}
	}
	return t
}

func (t *ShardedARPTable) getShard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	// Fast modulo with bitwise AND as shardCount is power of 2
	return t.shards[h.Sum32()&(shardCount-1)]
}

func (t *ShardedARPTable) Set(vni uint32, ip string,
	mac string, ttl time.Duration) {
	key := fmt.Sprintf("%d:%s", vni, ip)
	s := t.getShard(key)

	s.mtx.Lock()
	s.items[key] = cacheEntry{
		macAddr:   mac,
		expiresAt: time.Now().Add(ttl),
	}
	s.mtx.Unlock()
}

func (t *ShardedARPTable) Get(vni uint32, ip string) (string, bool) {
	key := fmt.Sprintf("%d:%s", vni, ip)
	s := t.getShard(key)

	s.mtx.RLock()
	entry, exists := s.items[key]
	s.mtx.RUnlock()

	if !exists || time.Now().After(entry.expiresAt) {
		return "", false
	}
	return entry.macAddr, true
}

func main() {
	table := NewShardedARPTable()
	table.Set(500100, "10.10.1.15", "00:1B:44:11:3A:B7", 5*time.Second)

	if mac, ok := table.Get(500100, "10.10.1.15"); ok {
		fmt.Printf("Proxy-ARP Hit: VNI 500100 | 10.10.1.15 -> %s\n", mac)
	}
}
