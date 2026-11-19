package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unsafe"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sys/unix"
)

const (
	HashMapCapacity = 1024
	SHM_NAME        = "/tmp/ngfw_shared_matrix"
	ClusterSyncChan = "ngfw:cluster:sync"
)

type GConnState struct {
	FD             uint32
	JA4Fingerprint [32]byte
	State          uint32
}

type GHashSlot struct {
	KeyFD         uint32
	ValueConn     GConnState
	ProbeDistance uint32
}

type ControlPlaneEngine struct {
	rdb    *redis.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	engine := &ControlPlaneEngine{rdb: rdb, ctx: ctx, cancel: cancel}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go engine.RunSyncLoop()
	log.Println("[Control-Plane] Shared Robin Hood Matrix scanner online.")

	<-sigChan
	cancel()
	_ = rdb.Close()
}

func (c *ControlPlaneEngine) RunSyncLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			file, err := os.OpenFile(SHM_NAME, os.O_RDONLY, 0666)
			if err != nil {
				continue // Wait for data plane mapping to materialize
			}

			slotSize := int(unsafe.Sizeof(GHashSlot{}))
			totalSize := slotSize * HashMapCapacity

			data, err := unix.Mmap(int(file.Fd()), 0, totalSize, unix.PROT_READ, unix.MAP_SHARED)
			file.Close()
			if err != nil {
				log.Printf("[Error] Shared memory matrix projection failed: %v", err)
				continue
			}

			// Cast raw byte pointer straight to a slice bounded by table capacity capacity limits
			slots := (*[HashMapCapacity]GHashSlot)(unsafe.Pointer(&data[0]))

			validEntriesFound := 0
			invalidEntriesFound := 0

			for i := 0; i < HashMapCapacity; i++ {
				slot := slots[i]
				if slot.ValueConn.State == 1 { // STATE_VALID
					validEntriesFound++
				} else if slot.ValueConn.State == 2 { // STATE_INVALID
					invalidEntriesFound++

					// Scenario C Translation Hook: Broadcast cluster eviction over Redis
					fingerprintStr := string(slot.ValueConn.JA4Fingerprint[:])
					msg := fmt.Sprintf("EVICT|%d|%s", slot.ValueConn.FD, fingerprintStr)
					c.rdb.Publish(c.ctx, ClusterSyncChan, msg)
				}
			}
			unix.Munmap(data)

			log.Printf("[Matrix Metric Sync] Active Sessions: %d | Pending Evictions: %d\n", validEntriesFound, invalidEntriesFound)
		}
	}
}
