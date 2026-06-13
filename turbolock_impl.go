package turbolock

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockFailed = errors.New("turbolock: failed to acquire lock")
)

const maxDelay = 2 * time.Second // 延迟上限

// 释放锁的经典 Lua 脚本：先比较 Value 是否一致（防误删），一致才执行删除
const delLuaScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("DEL", KEYS[1])
	else 
		return 0
	end
`

// 续期专用 Lua 脚本（与释放脚本同模式：先比对 Value 再操作）
const renewLuaScript = `
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		return redis.call("EXPIRE", KEYS[1], ARGV[2])
	else 
		return 0
	end
`

// 定义本地锁槽，每一个独立的 Key 都会对应一个锁槽
type localSlot struct {
	mu        sync.Mutex // 保护当前槽位内状态的局部锁
	cond      *sync.Cond // 用于挂起和唤醒当前 key 的追随者
	active    bool       // 是否已经有代表（Leader）去远程 Redis 抢锁了
	isSuccess bool       // 【新增】标记 Leader 最终有没有把锁抢成功
	lastUsed  time.Time  // 最后一次被 getSlot 返回的时间
}

type defaultTurboLocker struct {
	client *redis.Client
	opts   *Options

	slots sync.Map // key: string -> value: *localSlot

	wheel *timingWheel // 全局时间轮看门狗
}

// NewTurboLocker 构造器
func NewTurboLocker(client *redis.Client, opts ...Option) Turbolocker {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		opt(baseOpts)
	}
	t := &defaultTurboLocker{
		client: client,
		opts:   baseOpts,
		wheel:  newTimingWheel(),
	}

	go t.wheel.run() // 启动单协程心跳

	return t
}

func (t *defaultTurboLocker) getSlot(key string) *localSlot {
	// 快速路径：无锁读取（覆盖 99.99% 的调用）
	if v, ok := t.slots.Load(key); ok {
		slot := v.(*localSlot)
		slot.lastUsed = time.Now() // 无锁写入，近似值可接受
		return slot
	}
	// 慢速路径：创建新 slot（仅在首次遇到新 Key 时触发）
	slot := &localSlot{lastUsed: time.Now()}
	slot.cond = sync.NewCond(&slot.mu)
	actual, loaded := t.slots.LoadOrStore(key, slot)
	if loaded {
		// 两个协程同时创建一个新 key， 只保留一个，另一个被GC回收
		/*
			为什么碰撞丢弃无伤大雅？

			碰撞触发条件：两个协程同时首次访问 Key="new_lock"
				→ G1 创建 slot_A，G2 创建 slot_B
				→ LoadOrStore("new_lock", slot_A) 返回 (slot_A, false)  ← G1 赢了
				→ LoadOrStore("new_lock", slot_B) 返回 (slot_A, true)   ← G2 发现已存在
				→ slot_B 无任何引用，立即被 GC 回收

				代价：1 个 `*localSlot` 分配 + 1 个 `sync.Cond`（~80 bytes），纳秒级 GC
				频率：仅在新 Key 首次并发访问时，百万分之一的概率
		*/

		return actual.(*localSlot)
	}
	return slot
}

// Lock 核心抢锁逻辑
func (t *defaultTurboLocker) Lock(ctx context.Context, key string) (UnlockFunc, error) {

	// 获取该 key 对应的本地闸门
	slot := t.getSlot(key)
	slot.mu.Lock()
	// 【核心拦截点】：如果已经有代表出发了，后来的协程全部原地卧倒
	for slot.active {
		slot.cond.Wait() // // 自动释放锁并挂起，被唤醒后重新获取锁
		// 惊醒（被唤醒）后，由于是排他分布式锁，第一版我们直接让追随者在被唤醒后重新参与竞争或返回失败
		// 为了让第一版最稳，被唤醒后跳出循环，重新往下走去博弈
		// 【拦截器生效】
	}

	// 【关键改动】：当被唤醒时，Leader 已经把战报带回来了
	if slot.isSuccess {
		slot.mu.Unlock()
		// Leader 成功了，意味着锁被 Leader 占了，作为排他锁，Follower 直接宣告失败，拒绝卷入 Redis！
		return nil, ErrLockFailed
	}

	slot.active = true
	slot.isSuccess = false
	slot.mu.Unlock()

	// 只要成为了 Leader，无论结果如何，退出时必须重置本地状态并唤醒所有卧倒的协程
	var success bool
	defer func() {
		slot.mu.Lock()
		slot.active = false
		slot.isSuccess = success
		slot.cond.Broadcast() // 广播，唤醒等待队列中的所有 goroutine
		slot.mu.Unlock()
	}()

	// ------------ 离开本地，走向网络 ------------
	// 1. 生成这把锁唯一的 “防伪标识别” DNA
	value, err := t.getValue()
	if err != nil {
		return nil, err
	}

	// 循环前检查一次 ctx 有效期，避免产生一次无效 redis SetNx
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		// ctx 仍有效，继续
	}

	timer := time.NewTimer(t.opts.RetryDelay)
	defer timer.Stop()

	// 2. 经典的重试退避循环
	for i := 0; i < t.opts.Tries; i++ {
		// 尝试通过 SetNX 占位置
		ok, err := t.client.SetNX(ctx, key, value, t.opts.Expiry).Result()
		if err == nil && ok {
			success = true // 标记成功

			// 抢锁成功，挂载自动续期任务到时间轮系统
			var renewTask *timerTask
			if t.opts.AutoRenew {

				acquiredAt := time.Now() // 记录单次持锁开始时间点（为的是防止服务崩溃导致锁被无限期持有）

				// TTL/3 是最佳实践——Kafka、etcd、Redisson 等主流分布式锁都默认续期为 TTL/3，没有例外场景需要自定义
				renewInterval := t.opts.Expiry / 3
				renewTask = taskPool.Get().(*timerTask)
				renewTask.key = key
				renewTask.ticks = int(renewInterval / tickMs)
				renewTask.callback = func(ctx context.Context) error {

					// 每次续期前检查：check 锁是否被持有太久了？
					if t.opts.MaxHoldDuration > 0 && time.Since(acquiredAt) > t.opts.MaxHoldDuration {
						renewTask.cancelled = true // 标记停止，下次 fireSlot 会惰性清理
						return nil                 // 不再续期，让 TTL 自然过期
					}

					return t.client.Eval(ctx, renewLuaScript,
						[]string{key}, value, int(t.opts.Expiry.Seconds())).Err()
				}
				renewTask.cancelled = false
				t.wheel.addTaskDirect(renewTask) // ← 不再 new，直接挂入时间轮

			}

			// 抢锁成功，构建并返回释放锁的闭包
			return func(unCtx context.Context) error {
				// 从时间轮摘除续期任务（在 DEL 之前，防止续期与 DEL 竞态）
				if renewTask != nil {
					t.wheel.removeTask(renewTask)
				}

				// 执行 Lua 脚本原子释放
				err := t.client.Eval(unCtx, delLuaScript, []string{key}, value).Err()
				// 无论释放成功与否，都重置 isSuccess。
				// 如果释放失败（网络超时等），Redis 锁最终会 TTL 过期；
				// 此时重置 isSuccess 允许下一轮抢锁，SETNX 才是最终仲裁者。
				slot.mu.Lock()
				slot.isSuccess = false
				slot.mu.Unlock()
				return err
			}, nil
		}

		delay := t.opts.RetryDelay * time.Duration(1<<i) // // 50ms, 100ms, 200ms, 400ms...
		if delay > maxDelay {
			delay = maxDelay // 上限控制（2s）
		}

		// 循环内部，复用这同一个 Timer 对象！（timer.Reset(delay) 天然支持动态重置）
		timer.Reset(delay)

		// 检查上下文是否已经到期或被取消，防止死循环
		select {
		case <-ctx.Done():
			// 上层 ctx 已结束，直接返回，避免无效cpu消耗
			return nil, ctx.Err()
		case <-timer.C:
			// 等待重试间隔后进入下一轮循环
		}
	}
	return nil, ErrLockFailed
}

// genValue 生成 32 字节的强随机数并转为 base64 字符串
func (t *defaultTurboLocker) getValue() (string, error) {
	b := randPool.Get().([]byte)
	if _, err := rand.Read(b); err != nil {
		randPool.Put(b) // 出错也要放回
		return "", err
	}
	// 先编码再放回——EncodeToString 会复制数据到新 string，b 不再被引用
	// 使用 crypto/rand（真随机，读取系统熵池）而不是 math/rand（伪随机），确保分布式环境下多台机器生成的 Value 绝对不会碰撞。
	s := base64.StdEncoding.EncodeToString(b)
	randPool.Put(b)
	return s, nil
}

// isIdle 判断 slot 是否处于空闲且长时间未使用状态（调用方需持有 mu）
func (s *localSlot) isIdle(now time.Time, maxAge time.Duration) bool {
	return !s.active && now.Sub(s.lastUsed) > maxAge
}

// CleanupSlots 清理超过 maxAge 未被访问且非活跃的 slot。
// 建议通过外部定时器周期性调用（如每 5 分钟一次）。
// 返回本次清理的 slot 数量。
func (t *defaultTurboLocker) CleanupSlots(maxAge time.Duration) int {
	now := time.Now()
	removed := 0
	t.slots.Range(func(key, value any) bool {
		slot := value.(*localSlot)
		slot.mu.Lock()
		idel := slot.isIdle(now, maxAge)
		slot.mu.Unlock()
		if idel {
			t.slots.Delete(key)
			removed++
		}
		return true
	})
	return removed
}

// Close 优雅关闭 TurboLocker，停止时间轮并释放资源。
func (t *defaultTurboLocker) Close() error {
	t.wheel.Stop()
	return nil
}

// ─── sync.Pool 对象池（阶段四：零内存分配） ───

// randPool 复用 crypto/rand 的 32 字节缓冲区
var randPool = sync.Pool{
	New: func() any { return make([]byte, 32) },
}

// taskPool 复用时间轮任务节点，在 fireSlot 惰性清理时放回
var taskPool = sync.Pool{
	New: func() any { return &timerTask{} },
}
