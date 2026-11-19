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
	SyncInterval     = 2 * time.Second
	RedisIdentityKey = "ngfw:identity:active_cluster"
	MockFingerprint  = "ja4_7a8b9c_d1e2f3"
)

// Sliding Window Parameters: Max 100 packets per 10 seconds
const (
	RateLimitThreshold = 100
	WindowSizeMS       = 10000
)

// Redis Lua script execution source block
// Keys[1]: Rate limit key tracker
// Argv[1]: Current high-resolution timestamp (ms)
// Argv[2]: Moving window size bound (ms)
// Argv[3]: Maximum capacity threshold
// Argv[4]: Count increment value to apply

const SlidingWindowLuaScript = `
local window_key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local increment = tonumber(ARGV[4])

local clear_before = now - window

-- 1. Remove outdated elements outside our active sliding horizon
redis.call('ZREMRANGEBYSCORE', window_key, 0, clear_before)

-- 2. Check aggregate size of current transactions
local current_requests = redis.call('ZCARD', window_key)

if current_requests + increment > limit then
    return 0 -- Rate limit breached. Alert control plane!
else
    -- 3. Log each packet batch entry transaction atomicaly inside the sorted set
    for i = 1, increment do
        redis.call('ZADD', window_key, now, now .. '_' .. i)
    end
    -- Set TTL on the sorted set to clean up idle allocations automatically
    redis.call('PEXPIRE', window_key, window)
    return 1 -- Allowed
end
`

type ControlPlaneSyncEngine struct {
	redisClient *redis.Client
	ctx         context.Context
	cancel      context.CancelFunc
	luaSHA      string
}

func NewSyncEngine(redisAddr string) *ControlPlaneSyncEngine {
	ctx, cancel := context.WithCancel(context.Background())
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "", // No password for mock testing
		DB:       0,  // Standard logic DB
	})

	// Pre-load Lua script to maximize parsing speeds via SHA calls
	sha, err := rdb.ScriptLoad(ctx, SlidingWindowLuaScript).Result()
	if err != nil {
		log.Fatalf("Failed to initialize atomic Lua micro-engine script: %v", err)
	}

	return &ControlPlaneSyncEngine{
		redisClient: rdb,
		ctx:         ctx,
		cancel:      cancel,
		luaSHA:      sha,
	}
}

// Define structural parity matching the C packed attribute exactly
type CDeviceSlot struct {
	JA4Fingerprint    [32]byte
	LastSeenTimestamp uint64
	PacketCount       uint32
}

func (engine *ControlPlaneSyncEngine) fetchLocalDataPlaneMetrics() (int64, string) {
	// macOS mounts POSIX shared memory spaces under /dev/shm or directly inside named paths.
	// Opening the descriptor file natively bypasses the missing unix.ShmOpen mapping.
	shmPath := "/dev/shm/ngfw_shared_metrics"

	// Fallback check for systems utilizing standard path structures
	if _, err := os.Stat(shmPath); os.IsNotExist(err) {
		// If compiled on older macOS targets, it mounts relative to standard sandbox or tmp lines
		shmPath = "/tmp/ngfw_shared_metrics"
	}

	file, err := os.OpenFile(shmPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("[Control-Plane Warning] Shared memory segment not readable yet: %v\n", err)
		return 0, "awaiting_dataplane_init"
	}
	defer file.Close()

	// Memory map (mmap) the native file descriptor directly into Go runtime space
	size := int(unsafe.Sizeof(CDeviceSlot{}))
	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		log.Printf("[Control-Plane Error] Mmap reflection failed: %v\n", err)
		return 0, "mmap_failed"
	}
	defer unix.Munmap(data)

	// Cast the mapped byte slice directly into our clean Go structure layout
	slot := (*CDeviceSlot)(unsafe.Pointer(&data[0]))

	// Read snapshot values safely
	currentPackets := slot.PacketCount

	// Strip null bytes for clean string representation
	endIdx := 0
	for endIdx < len(slot.JA4Fingerprint) && slot.JA4Fingerprint[endIdx] != 0 {
		endIdx++
	}
	fingerprintStr := string(slot.JA4Fingerprint[:endIdx])

	// Clean up byte block representation to string
	//fingerprintStr := string(slot.JA4Fingerprint[:])

	return int64(currentPackets), fingerprintStr
}

// Emulates interacting with the local shared-memory layer of the C Data Plane
func (engine *ControlPlaneSyncEngine) fetchLocalDataPlaneMetricsMock() (int64, string) {
	// Mock counter read out of shared memory
	packetCount := time.Now().Unix() % 1000
	return packetCount, MockFingerprint
}

func (engine *ControlPlaneSyncEngine) RunX() {
	ticker := time.NewTicker(SyncInterval)
	defer ticker.Stop()

	log.Println("[Control-Plane] State Sync Engine started successfully.")

	for {
		select {
		case <-engine.ctx.Done():
			log.Println("[Control-Plane] Stopping Sync Pipeline gracefully...")
			return
		case <-ticker.C:
			packets, fingerprint := engine.fetchLocalDataPlaneMetrics()

			// Execute atomic transaction map write over HSET inside Redis
			pipe := engine.redisClient.Pipeline()
			pipe.HSet(engine.ctx, RedisIdentityKey, "last_updated", time.Now().UTC().Format(time.RFC3339))
			pipe.HSet(engine.ctx, RedisIdentityKey, "fingerprint_signature", fingerprint)
			pipe.HIncrBy(engine.ctx, RedisIdentityKey, "total_accumulated_packets", packets)

			_, err := pipe.Exec(engine.ctx)
			if err != nil {
				log.Printf("[Control-Plane Error] Direct sync to core Redis matrix failed: %v\n", err)
			} else {
				log.Printf("[Control-Plane Sync] Pushed metrics to Redis: JA4=%s | AddedPackets=%d\n", fingerprint, packets)
			}
		}
	}
}

func (engine *ControlPlaneSyncEngine) Run() {
	ticker := time.NewTicker(SyncInterval)
	defer ticker.Stop()

	log.Println("[Control-Plane] State Sync Engine started successfully.")
	var lastSeenPackets int64 = 0

	for {
		select {
		case <-engine.ctx.Done():
			log.Println("[Control-Plane] Stopping Sync Pipeline gracefully...")
			return
		case <-ticker.C:
			totalPackets, fingerprint := engine.fetchLocalDataPlaneMetrics()
			if fingerprint == "awaiting_dataplane_init" || totalPackets == 0 {
				continue
			}

			// Calculate the differential delta since the last sync run window
			packetDelta := totalPackets - lastSeenPackets
			if packetDelta <= 0 {
				continue // No active traffic shifts to process
			}
			lastSeenPackets = totalPackets

			nowMS := time.Now().UnixNano() / int64(time.Millisecond)
			rateLimitKey := fmt.Sprintf("ngfw:ratelimit:%s", fingerprint)

			// Execute the atomic lua transaction frame
			res, err := engine.redisClient.EvalSha(engine.ctx, engine.luaSHA, []string{rateLimitKey}, nowMS, WindowSizeMS, RateLimitThreshold, packetDelta).Result()
			if err != nil {
				log.Printf("[Control-Plane Error] Lua execution fault: %v\n", err)
				continue
			}

			if res.(int64) == 0 {
				log.Printf("[SECURITY ALERT] Device '%s' breached sliding-window rate limits! Packet Delta: %d\n", fingerprint, packetDelta)
			} else {
				log.Printf("[Control-Plane Sync] Device '%s' synced safely. Added Batch Delta: %d | Total: %d\n", fingerprint, packetDelta, totalPackets)
			}
		}
	}
}

func main() {
	// Expects localized or clustered local redis container instance
	redisAddress := "localhost:6379"
	engine := NewSyncEngine(redisAddress)

	// Intercept execution traps to guarantee connection closures
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go engine.Run()

	<-sigChan
	engine.cancel()
	_ = engine.redisClient.Close()
	fmt.Println("[Control-Plane] Clean Shutdown complete.")
}
