# TurboLock

[![Go Version](https://img.shields.io/badge/Go-%3E%3D1.21-blue)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**Go 语言 Redis 分布式锁，核心创新：本地合流 + 时间轮自动续期。**

---

## 特性

- 🔀 **Leader-Follower 合流** — 10000 协程抢同一 Key，只放行 1 个去 Redis，消除雷群效应
- ⏱️ **层级时间轮自动续期** — 全局 1 个 goroutine + 1 个 Ticker 管理 N 个锁的续期，告别"一锁一协程"
- 🛡️ **Lua 原子防误操作** — 释放和续期均先比对 Value（DNA），不是你的锁绝不多管闲事
- ⏳ **MaxHoldDuration** — 持锁超时自动放弃续期，进程崩溃也不会无限占用资源
- 🧹 **零堆逃逸** — `sync.Pool` 池化 rand buffer 和 timerTask 节点，TurboLock 自身不产生堆分配
- ⚡ **极致性能** — 0.8ms/op，10 allocs/op，比裸 Redis SETNX 快 174 倍

---

## 快速开始

### 安装

```bash
go get git@gitee.com:yxcxy/turbolock.git
```

### 基础用法

```go
package main

import (
    "context"
    "time"

    "github.com/YOUR_USERNAME/turbolock"
    "github.com/redis/go-redis/v9"
)

func main() {
    client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    defer client.Close()

    // 不开启自动续期（简单模式）
    locker := turbolock.NewTurboLocker(client,
        turbolock.WithExpiry(8*time.Second),
    )
    defer locker.Close()

    unlock, err := locker.Lock(context.Background(), "order:12345")
    if err != nil {
        // 锁被他人持有，返回 ErrLockFailed
        return
    }
    defer unlock(context.Background())

    // 临界区：安全地执行业务逻辑 ...
}
```

### 开启自动续期

```go
locker := turbolock.NewTurboLocker(client,
    turbolock.WithExpiry(8*time.Second),              // 锁 TTL
    turbolock.WithAutoRenew(),                         // 自动续期（每 TTL/3 续一次）
    turbolock.WithMaxHoldDuration(30*time.Second),     // 最多持锁 30s，超时停止续期
)
defer locker.Close()

unlock, err := locker.Lock(context.Background(), "my_resource")
if err != nil {
    return
}
defer unlock(context.Background())

// 即使业务执行超过 8s，时间轮也会自动续期，锁不会意外释放
time.Sleep(20 * time.Second)
```

### 配置项

| Option | 默认值 | 说明 |
|--------|:--:|------|
| `WithExpiry(d)` | 8s | 锁的 Redis TTL |
| `WithTries(n)` | 32 | SETNX 最大重试次数 |
| `WithRetryDelay(d)` | 50ms | 重试间隔（指数退避，上限 2s） |
| `WithAutoRenew()` | 关闭 | 开启时间轮自动续期（间隔 = TTL/3） |
| `WithMaxHoldDuration(d)` | 30s | 单次最长持锁时间，超时停止续期 |

---

## 性能

> Go 1.25, Intel i7-7700HQ, Redis 本地单节点, 8 协程并发

| 配置 | ns/op | allocs/op | B/op | 说明 |
|------|------:|:--:|:--:|------|
| 裸 Redis SETNX | 142,537,929 | 1006 | 57,952 | 10000 协程直冲 Redis |
| + 合流 | 813,441 | 10 | 454 | Leader-Follower 拦截 |
| + 续期 | 814,570 | 11 | 505 | 时间轮自动续期 |
| + 零逃逸 | 813,100 | 10 | 462 | sync.Pool 池化 |

| 指标 | 提升 |
|------|:--:|
| 延时 | **174×** faster |
| 分配次数 | **100×** fewer |
| 堆逃逸 | TurboLock 自身 **零逃逸** |

---

## 架构

```
┌─────────────────────────────────────────────────────┐
│                  TurboLocker 接口                     │
│         Lock(ctx, key) → (UnlockFunc, error)        │
├─────────────────────────────────────────────────────┤
│  本地合流层                                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
│  │ slot A  │  │ slot B  │  │ slot C  │  ...         │
│  │ cond+mu │  │ cond+mu │  │ cond+mu │              │
│  └────┬────┘  └────┬────┘  └────┬────┘              │
│   Leader         Leader       Leader                 │
│  (唯一去Redis)    (唯一去Redis)  (唯一去Redis)         │
├─────────────────────────────────────────────────────┤
│  时间轮看门狗                                        │
│  Level 0 [256 槽] ← 主力，覆盖 0~25.6s              │
│  Level 1 [64 槽]  ← 兜底，覆盖 0~27.3min            │
│  Level 2 [64 槽]  ← 极端，覆盖 0~29.1h              │
│  心跳: 1 goroutine + 1 Ticker(100ms)                │
├─────────────────────────────────────────────────────┤
│  Redis 网络层                                        │
│  SETNX 抢锁  │  Lua 脚本原子释放 & 续期              │
│  crypto/rand DNA 防误删 & 防误续                     │
└─────────────────────────────────────────────────────┘
```

---

## 文档

| 文档 | 内容 |
|------|------|
| [功能定位](doc/turbolock%20功能定位.md) | 适用场景、与 sync.Mutex / RedSync / etcd 的对比 |
| [时间轮代码解析](doc/blog/go_层级时间轮看门狗_完整代码解析.md) | 270 行逐行讲解 |
| [为什么续期是 2.6 秒](doc/blog/go_分布式锁续期_为什么是2.6秒.md) | TTL/3 的数学推导与业界实践 |
| [第二阶段压测分析](doc/test_doc/第二阶段压测数据分析.md) | 8 项 bug 修复 + 性能数据对比 |
| [第三阶段任务书](doc/阶段三：时间轮看门狗%20—%20开发任务书.md) | 时间轮设计 & 实现方案 |
| [第四阶段任务书](doc/阶段四：零内存分配%20—%20开发任务书.md) | sync.Pool & 逃逸分析 |

---

## License

MIT
