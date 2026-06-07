package turbolock

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not available: %v", err)
	}
	return client
}

func TestAutoRenew_ExtendsLockTTL(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	locker := NewTurboLocker(client,
		WithExpiry(2*time.Second),
		WithAutoRenew(), // 0 = use Expiry/3 = ~666ms
	)
	defer locker.Close()

	ctx := context.Background()
	key := "test_renew_basic"

	unlock, err := locker.Lock(ctx, key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// 等待远超 TTL 的时间（2×TTL），验证锁仍存在
	time.Sleep(5 * time.Second)

	// 检查 Redis 中 key 是否仍存在
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists != 1 {
		t.Fatal("lock should still exist with auto-renew enabled")
	}

	// 释放
	if err := unlock(ctx); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}
}

func TestAutoRenew_UnlockRemovesKey(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	locker := NewTurboLocker(client,
		WithExpiry(2*time.Second),
		WithAutoRenew(),
	)
	defer locker.Close()

	ctx := context.Background()
	key := "test_renew_unlock"

	unlock, err := locker.Lock(ctx, key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// 等一个续期周期
	time.Sleep(1 * time.Second)

	// 释放
	if err := unlock(ctx); err != nil {
		t.Fatalf("unlock failed: %v", err)
	}

	// 立即检查：key 应该已删除
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists != 0 {
		t.Fatal("lock should be deleted after unlock")
	}
}

func TestAutoRenew_DisabledExpires(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	locker := NewTurboLocker(client,
		WithExpiry(1*time.Second),
		// AutoRenew 默认关闭
	)
	defer locker.Close()

	ctx := context.Background()
	key := "test_no_renew"

	unlock, err := locker.Lock(ctx, key)
	if err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// 等待超过 TTL
	time.Sleep(3 * time.Second)

	// key 应该已过期
	exists, err := client.Exists(ctx, key).Result()
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists != 0 {
		t.Fatal("lock should have expired (auto-renew disabled)")
	}

	// unlock 应该无害（锁已过期）
	_ = unlock(ctx)
}

func TestAutoRenew_NoGoroutineLeak(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	goroutinesBefore := runtime.NumGoroutine()

	locker := NewTurboLocker(client,
		WithExpiry(2*time.Second),
		WithAutoRenew(),
	)

	// 抢 50 个不同 key 的锁
	var unlocks []UnlockFunc
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		unlock, err := locker.Lock(ctx, "test_leak_"+string(rune('A'+i)))
		if err != nil {
			t.Fatalf("Lock %d failed: %v", i, err)
		}
		unlocks = append(unlocks, unlock)
	}

	// 等续期稳定
	time.Sleep(1 * time.Second)

	goroutinesAfter := runtime.NumGoroutine()

	// 释放所有锁
	for _, u := range unlocks {
		_ = u(ctx)
	}
	locker.Close()

	// 时间轮只有 1 个 goroutine，每个续期回调另起 1 个（异步）
	// 50 个锁 × 续期 ≈ 50 个 callback goroutine 短暂存在
	delta := goroutinesAfter - goroutinesBefore
	t.Logf("goroutine delta: %d", delta)
	if delta > 100 {
		t.Fatalf("too many goroutines: %d", delta)
	}
}

// ================================================

func BenchmarkTurboLock_WithAutoRenew(b *testing.B) {
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer client.Close()

	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Skipf("redis not available: %v", err)
	}

	locker := NewTurboLocker(client,
		WithExpiry(2*time.Second),
		WithAutoRenew(),
	)
	defer locker.Close()

	ctx := context.Background()
	lockKey := "bench_autorenew_key"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			unlock, err := locker.Lock(ctx, lockKey)
			if err == nil {
				time.Sleep(1 * time.Millisecond)
				_ = unlock(ctx)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	})
}
