package example

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type BadTurboLocker struct {
	client     *redis.Client
	optsTries  int
	optsExpiry time.Duration
	optsDelay  time.Duration
}

func NewBadTurboLocker(client *redis.Client) *BadTurboLocker {
	return &BadTurboLocker{
		client:     client,
		optsTries:  100, // 模拟高频重试：100次
		optsExpiry: 5 * time.Second,
		optsDelay:  50 * time.Millisecond, // 每次重试间隔 50ms
	}
}

func (t *BadTurboLocker) Lock222(ctx context.Context, key string) error {
	// 模拟生成防伪随机值
	value := "mock_random_value"

	for i := 0; i < t.optsTries; i++ {
		ok, err := t.client.SetNX(ctx, key, value, t.optsExpiry).Result()
		if err == nil && ok {
			return nil // 抢锁成功
		}
		//没抢到，进入剧毒退避等待
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(t.optsDelay):
			// 阻塞等待结束，进入下一次 抢锁
		}
	}
	return errors.New("failed to acquire lock")
}

func (t *BadTurboLocker) Lock(ctx context.Context, key string) error {
	// 模拟生成防伪随机值
	value := "mock_random_value"

	timer := time.NewTimer(t.optsDelay)
	defer timer.Stop() // 函数退出时，手动释放，是一个好习惯

	for i := 0; i < t.optsTries; i++ {
		ok, err := t.client.SetNX(ctx, key, value, t.optsExpiry).Result()
		if err == nil && ok {
			return nil // 抢锁成功
		}
		// 循环内部，复用这同一个 Timer 对象！
		timer.Reset(t.optsDelay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			// 阻塞等待结束，进入下一次 抢锁
		}
	}
	return errors.New("failed to acquire lock")
}
