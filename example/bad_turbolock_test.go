package example

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// BenchmarkBadTurboLock_TimeAfter 演示 time.After 在高并发下的灾难表现
func BenchmarkBadTurboLock_TimeAfter(b *testing.B) {
	// 1. 初始化本地 Redis 客户端 (确保本地 6379 端口有 Redis 在运行)
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// 测试连通性
	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("Redis connection failed: %v", err)
	}

	// 2. 初始化带毒版本的 Locker
	locker := NewBadTurboLocker(client)
	ctx := context.Background()
	lockKey := "blog_test_deadly_key"

	// ================= 核心改动 =================
	// 在并发测试开始前，由主线程充当“流氓业务”，把坑位死死占住 60 秒！
	// 这样就能逼迫所有参与压测的协程，100% 掉进重试 100 次的 time.After 深渊
	client.Set(ctx, lockKey, "occupy_by_main", 60*time.Second)
	// 测试结束后清理战场
	defer client.Del(ctx, lockKey)
	// ============================================

	b.ResetTimer() // 重置计时器，排除初始化的干扰

	// 3. 模拟海量 Goroutine 并发抢同一把锁
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 所有人都在这里撞墙，硬生生吃满 100 次 time.After 的内存逃逸
			_ = locker.Lock(ctx, lockKey)
		}
	})
}
