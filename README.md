# TurboLock

[![Go Version](https://img.shields.io/badge/Go-1.25.x-blue)](https://go.dev/)
![License](https://img.shields.io/badge/license-MIT-green)

**TurboLock 是一个面向 Go 服务热点锁场景的 Redis 单节点工程锁库。**

它的核心目标不是重新发明 `SETNX`，而是在大量 goroutine 争抢同一个 lock key 时，通过本地 **Leader-Follower 合流** 减少无效 Redis 请求，并通过 **层级时间轮** 统一管理自动续期任务。

> 简单来说：  
> 当大量 goroutine 同时抢同一把 Redis 锁时，TurboLock 尽量只让一个 leader 请求 Redis；其余 goroutine 在本地等待 leader 返回。  
> 如果 leader 成功拿到锁，followers 会直接在本地返回抢锁失败，避免继续打到 Redis。

---

## Why TurboLock?

很多 Redis 锁实现会关注这些问题：

- `SET NX PX` 是否正确
- 释放锁时是否使用 Lua 校验 value
- 是否支持自动续期
- 锁过期后是否会误删别人的锁

这些当然重要。

但在真实业务里，还有一个容易被忽略的问题：

> 当大量 goroutine 同时争抢同一个热点 key 时，即使最后只有一个 goroutine 能成功拿到锁，Redis 也可能先被成千上万次无效抢锁请求打穿。

TurboLock 主要解决两个工程问题：

### 1. 热点锁惊群

同一进程内大量 goroutine 争抢同一个 lock key 时，TurboLock 会在本地进行 Leader-Follower 合流。

同一时刻、同一个 key，只放行一个 leader 请求 Redis；其他 goroutine 作为 follower 在本地等待 leader 返回。

如果 leader 成功拿到锁，followers 不会共享这把锁的 `UnlockFunc`，而是直接返回抢锁失败。这样可以保持锁的排他语义，并避免 follower 在同一轮竞争中继续打到 Redis。

这可以显著减少热点 key 场景下的 Redis 请求尖峰。

### 2. 自动续期调度膨胀

常见自动续期模型是：

> 一把锁 = 一个 goroutine + 一个 ticker

锁数量一多，goroutine 和 ticker 的调度成本会快速膨胀。

TurboLock 使用全局层级时间轮统一管理续期任务：

> N 把锁的续期任务，由全局 1 个 goroutine + 1 个 ticker 统一调度；实际续期回调会短暂异步执行，避免阻塞时间轮推进。

---

## Features

- **Leader-Follower 本地合流**  
  同一时刻、同一 key 只允许一个 leader 请求 Redis，其他 goroutine 在本地等待，降低热点锁惊群带来的 Redis 压力。

- **层级时间轮自动续期**  
  使用全局时间轮统一管理自动续期任务，避免“一锁一 goroutine + 一锁一 ticker”的调度模型。

- **Lua 原子释放与续期**  
  释放锁和续期前都会校验锁 value，避免误删、误续其他持有者的锁。

- **MaxHoldDuration 最大持锁时间**  
  防止业务逻辑异常导致锁被无限续期。达到最大持锁时间后，TurboLock 会停止续期，让 Redis TTL 自然释放锁。

  需要注意：当前实现会在调用 `unlock` 时重置本地成功状态；如果持锁方长期不调用 `unlock`，同一个 `locker` 实例内的同 key 新请求可能仍会被本地状态拒绝，直到旧持有者释放或实例重建。

- **低分配实现**  
  对随机 buffer、timer task 等热点对象进行复用，降低高并发场景下的内存分配压力。

- **面向 Go 服务的工程锁场景**  
  适合订单防重复处理、用户维度任务互斥、定时任务单实例执行、热点资源短时间保护等业务场景。

---

## Scope and Limitations

TurboLock 面向的是 **单 Redis 节点** 或 **业务可接受单 Redis 锁语义** 的工程场景。

它不试图解决强一致分布式共识问题，也不承诺在以下场景下提供 CP 级别的锁语义：

- Redis 主从切换
- 网络分区
- 跨机房部署
- 极端 GC pause
- Redis failover 期间的锁语义保持
- 旧锁持有者恢复后继续写入资源的问题

如果你的业务需要严格一致性协调，建议优先考虑：

- etcd
- ZooKeeper
- Consul
- 带 fencing token 的资源写入方案

TurboLock 的定位更接近：

> 一个为 Go 服务热点锁争抢场景设计的 Redis 工程锁库。

而不是：

> 一个解决所有分布式一致性问题的锁系统。

---

## When to Use

TurboLock 适合用于：

- 高并发热点 key 抢锁
- 订单防重复处理
- 活动、任务、用户维度的短时间互斥
- 定时任务单实例执行
- 单 Redis 节点锁语义可以接受的业务系统
- 同一服务实例内大量 goroutine 争抢同一个 Redis lock key 的场景
- 希望降低自动续期 goroutine / ticker 成本的场景

例如：

```text
order:pay:12345
user:task:10001
cron:daily-report
inventory:sku:8888
```

---

## When NOT to Use

TurboLock 不适合用于：

- 金融级强一致事务锁
- 跨机房强一致协调
- Redis failover 期间不能容忍任何锁语义异常的系统
- 长时间持锁任务
- 必须依赖 fencing token 防止旧持有者写入的资源保护场景
- 需要 CP 级别一致性保证的分布式协调场景

如果你要保护的是强一致资源写入，尤其是“旧持有者恢复后不能继续写”的场景，请优先考虑 fencing token 或 CP 协调系统。

---

## Install

```bash
go get github.com/ThanksGiveMeCourage/turbolock
```

---

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "time"

    "github.com/ThanksGiveMeCourage/turbolock"
    "github.com/redis/go-redis/v9"
)

func main() {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer client.Close()

    // Simple mode without auto renewal.
    locker := turbolock.NewTurboLocker(client,
        turbolock.WithExpiry(8*time.Second),
    )
    defer locker.Close()

    unlock, err := locker.Lock(context.Background(), "order:12345")
    if err != nil {
        // The lock is already held by others.
        return
    }
    defer unlock(context.Background())

    // Critical section:
    // safely execute business logic here.
}
```

---

## Auto Renew

When the business logic may run longer than the Redis lock TTL, enable auto renewal:

```go
locker := turbolock.NewTurboLocker(client,
    turbolock.WithExpiry(8*time.Second),             // Redis lock TTL
    turbolock.WithAutoRenew(),                       // renew at about TTL/3
    turbolock.WithMaxHoldDuration(30*time.Second),   // stop renewal after 30s
)
defer locker.Close()

unlock, err := locker.Lock(context.Background(), "my_resource")
if err != nil {
    return
}
defer unlock(context.Background())

// Even if the business logic takes longer than 8s,
// TurboLock will renew the lock before it expires.
time.Sleep(20 * time.Second)
```

> `WithMaxHoldDuration` is important.  
> It prevents abnormal business logic from keeping the lock alive forever.

---

## Options

| Option | Default | Description |
|---|---:|---|
| `WithExpiry(d)` | `8s` | Redis TTL of the lock |
| `WithTries(n)` | `32` | Max retry attempts for acquiring the lock |
| `WithRetryDelay(d)` | `50ms` | Retry interval with exponential backoff, capped at 2s |
| `WithAutoRenew()` | disabled | Enable automatic renewal |
| `WithMaxHoldDuration(d)` | `30s` | Max duration for holding a lock before renewal stops |

> Renewal currently uses Redis `EXPIRE`, so renewal TTL is rounded to whole seconds. Prefer `WithExpiry` values of at least 1 second.

---

## Benchmark

Current benchmark evidence in this repository:

- Go 1.25.x
- Intel i7-7700HQ
- Local single-node Redis
- `BenchmarkTurboLock_HighConcurrency-8`
- Hot-key contention through Go's parallel benchmark runner

| Case | ns/op | allocs/op | B/op | Description |
|---|---:|---:|---:|---|
| TurboLock + Coalescing | 812,395 | 10 | 460 | Leader-Follower coalescing |
| TurboLock + Coalescing | 809,275 | 11 | 464 | repeated run |
| TurboLock + Coalescing | 793,669 | 11 | 474 | repeated run |

In this hot-key contention benchmark, TurboLock reduces redundant Redis lock attempts by coalescing local goroutines before hitting Redis.

```bash
go test -bench=. -benchmem ./...
```

> Note:  
> The performance improvement comes from request coalescing under hot-key contention.  
> It does **not** mean a single Redis `SETNX` command becomes faster.  
> Raw Redis and auto-renew comparison numbers should be treated as historical analysis unless the matching benchmark cases are present in the current tree.

---

## Architecture

![](https://github.com/ThanksGiveMeCourage/ThanksGiveNotes/blob/main/images/turbolock.png)

---

## How It Works

### Lock Acquire

```text
goroutine A ─┐
goroutine B ─┼── same key ──> local coalescing ──> leader ──> Redis SETNX
goroutine C ─┘                                      followers wait locally
```

For the same lock key, TurboLock allows only one leader to access Redis at a time.

Followers wait locally while the leader tries Redis. If the leader succeeds, followers return lock failure locally; if the leader fails, the next caller can become a new leader and retry Redis.

### Lock Release

TurboLock uses Lua to compare the lock value before deleting the key:

```text
if redis.get(key) == value then
    redis.del(key)
else
    return 0
end
```

This prevents a client from accidentally deleting a lock held by another client.

### Auto Renewal

When auto renewal is enabled, TurboLock schedules renewal tasks into the global time wheel.

```text
lock acquired
    ↓
schedule renewal at TTL/3
    ↓
Lua compare-and-renew
    ↓
reschedule next renewal
    ↓
stop when unlocked or MaxHoldDuration reached
```

When `MaxHoldDuration` is reached, renewal stops and Redis TTL is allowed to expire naturally. In the current implementation, calling `unlock` is still the normal path that clears the local success marker for the same `locker` instance.

---

## Comparison

| Solution | Best For | Limitation |
|---|---|---|
| `sync.Mutex` | In-process mutual exclusion | Cannot coordinate across processes |
| Raw Redis `SETNX` + Lua | Simple Redis lock usage | Hot-key contention may cause Redis request spikes |
| One goroutine per lock renewal | Simple auto-renew implementation | Goroutine and ticker overhead grows with lock count |
| RedSync | Multi-node Redis lock algorithm | More complex deployment and semantics |
| etcd Lock | Strong consistency coordination | Higher operational and performance cost |
| TurboLock | Hot-key Redis lock contention in Go services | Not a CP consensus lock |

---

## Documents

| Document | Description |
|---|---|
| [功能定位](doc/turbolock%20功能定位.md) | Use cases and comparison with `sync.Mutex` / RedSync / etcd |
| [时间轮代码解析](doc/blog/go_层级时间轮看门狗_完整代码解析.md) | Code walkthrough of the hierarchical time-wheel watchdog |
| [为什么续期是 2.6 秒](doc/blog/go_分布式锁续期_为什么是2.6秒.md) | Why renewal happens at TTL/3 |
| [第二阶段压测分析](doc/test_doc/第二阶段压测数据分析.md) | Benchmark analysis and bug fixes |
| [第三阶段任务书](doc/阶段三-时间轮看门狗-开发任务书.md) | Time-wheel design and implementation plan |
| [第四阶段任务书](doc/阶段四-零内存分配-开发任务书.md) | `sync.Pool` and escape analysis |

---

## License

MIT. The current tree does not include a standalone `LICENSE` file yet.
