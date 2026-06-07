package turbolock

import "context"

// 接口层

// UnlockFunc 是释放锁的闭包函数，调用者拿到后直接 defer customUnlock 即可
/*
	设计让它接收一个 ctx，这是为了以后释放锁时调用 Redis 的 Lua 脚本也能享受超时控制。
*/
type UnlockFunc func(ctx context.Context) error

// Turbolocker 核心接口定义
type Turbolocker interface {
	// Lock 阻塞式抢锁，直到成功 或 ctx 超时/取消
	Lock(ctx context.Context, key string) (UnlockFunc, error)
	Close() error // 停止时间轮，优雅关闭
}
