package turbolock

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func BenchmarkTurboLock_HighConcurrency(b *testing.B) {

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	// 确保网络通畅
	if err := client.Ping(context.Background()).Err(); err != nil {
		b.Fatalf("redis connection failed: %v", err)
	}

	// 初始化锁组件，配置高频重试
	locker := NewTurboLocker(client,
		WithExpiry(5*time.Second),
		WithTries(100),
		WithRetryDelay(10*time.Millisecond),
	)
	ctx := context.Background()
	lockKey := "bench_exclusive_resource_key"

	// 充当资源临界区计数器
	var sharedCounter int

	b.ResetTimer() // 重置定时器，排除初始化代码干扰

	// 开始并发压力测试
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 每个 goroutine 疯狂抢锁
			unlock, err := locker.Lock(ctx, lockKey)
			if err == nil {
				// 成功进入临界区
				sharedCounter++

				// 模拟业务耗时（让锁产生实质性竞争）
				time.Sleep(1 * time.Millisecond)

				// 释放锁
				err := unlock(ctx)
				if err != nil {
					b.Fatal("unlock err: ", err)
				}
			} else {
				// 模拟真实业务：加锁失败后等待重试
				time.Sleep(10 * time.Millisecond)
			}
		}
	})

}
