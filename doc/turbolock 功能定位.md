# TurboLock 功能定位

## 一句话定义

TurboLock 是一个面向 **多实例 + 高并发** 场景的 **Redis 单节点分布式锁**，核心创新是在本地进程内通过 **Leader-Follower 合流（Coalescing）** 机制，将同 Key 的海量协程请求合并为 1 次 Redis 调用，从而消除"雷群效应"（Thundering Herd）。

---

## 它是什么？

| 维度 | 说明 |
|------|------|
| **本质** | Redis SETNX 分布式锁 + 本地 `sync.Cond` 协程合流层 |
| **接口** | `Lock(ctx, key) → (UnlockFunc, error)` — 阻塞式抢锁，返回释放闭包 |
| **释放安全** | Lua 脚本原子比对 Value（DNA），只允许持有者删除 |
| **随机值** | `crypto/rand` 真随机 + base64，杜绝多机 Value 碰撞 |
| **配置模式** | Functional Options（`WithExpiry`, `WithTries`, `WithRetryDelay`） |

---

## 它解决什么问题？

### 核心痛点：雷群效应（Thundering Herd）

```
❌ 不用合流：
   50 个 Pod × 每 Pod 1000 协程 = 50,000 次 Redis SETNX 同时涌入
   → Redis CPU 飙升、网络拥堵、大量协程在 time.After 中空转

✅ 用 TurboLock：
   50 个 Pod × 每 Pod 1 个 Leader = 50 次 Redis SETNX
   → 其余 49,950 个协程在本地 cond.Wait() 挂起，零网络开销
```

### 合流原理（Leader-Follower）

```
同一 Key 的并发请求进入 Lock()：

  1. 第一个协程 → 成为 Leader → active=true → 出发去 Redis 抢锁
  2. 后续协程 → 发现 active=true → cond.Wait() 原地挂起
  3. Leader 归来 → active=false, isSuccess=true/false → Broadcast 唤醒全部
  4. 被唤醒的 Follower：
     - isSuccess=true  → 直接返回 ErrLockFailed（Leader 已抢到，排他锁）
     - isSuccess=false → 下一个 Follower 晋升为新 Leader，循环
```

---

## 适用前提（缺一不可）

TurboLock 只有在 **三个条件同时满足** 时才体现价值：

| # | 条件 | 不满足则 |
|---|------|---------|
| ① | **多个服务实例**（跨进程 / 跨机器） | 直接用 `sync.Mutex`，Redis 都不需要 |
| ② | **共享同一资源需要互斥** | 不需要分布式锁 |
| ③ | **单实例内部有高并发同 Key 请求** | 合流层无意义，原生 Redis SETNX 足够 |

### 典型适用场景

- 秒杀库存扣减（50 个 Pod，每 Pod 数千协程抢同一商品）
- 限量优惠券发放
- 分布式定时任务（多实例抢同一任务执行权）
- 缓存击穿防护（多实例同时重建同一热点缓存）

### 不适用的场景

| 场景 | 为什么不该用 TurboLock |
|------|------------------------|
| 单进程多协程 | `sync.Mutex` 零网络开销 |
| 多 Key 低并发 | 合流层几乎不触发，反而多了 `globalMu` 和 slot 管理开销 |
| 需要强一致性的锁 | TurboLock 基于单 Redis，无故障转移，不保证 RedLock 级别的安全性 |
| 跨数据中心 / 高网络延迟 | 应使用 etcd / Consul / Zookeeper 这类基于共识协议的锁 |

---

## 与其他方案的对比

| 方案 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| `sync.Mutex` | 单进程 | 零网络开销，纳秒级 | 无法跨进程 |
| Redis SETNX（裸用） | 低并发多实例 | 简单 | 高并发下雷群效应严重 |
| **TurboLock** | **高并发多实例** | 合流降维打击，Redis 压力降 90%+ | 只支持单 Redis，代码尚在迭代 |
| RedSync（redsync） | 需要高可靠性的多实例 | 多 Redis 节点容错 | 重（RedLock 争议），合流需自行实现 |
| etcd / Zookeeper | 强一致性要求 | 基于 Raft/ZAB，可靠性高 | 重依赖，运维成本高 |

---

## 当前开发阶段与已知局限

TurboLock 目前处于 **阶段二（单机合流）** 开发中，已完成 Leader-Follower 核心逻辑并通过压测验证（50ms/op, 46 allocs/op），但存在以下待解决问题：

| 阶段 | 状态 | 说明 |
|------|:--:|------|
| 阶段一：骨架 + 基准压测 | ✅ | SETNX + Lua 释放 + Benchmark |
| 阶段二：单机合流 | 🟡 | 核心逻辑完成，但 globalMu 竞争、slot 泄漏、time.After 等问题待修 |
| 阶段三：时间轮看门狗 | ⬜ | 锁自动续期，尚未开始 |
| 阶段四：零内存分配 | ⬜ | 逃逸分析 + sync.Pool |
| 阶段五：开源发布 | ⬜ | README + 博客 + 社区 |

> 详见 `turbolock 开发阶段总规划.md`

---

## 架构概览

```
┌─────────────────────────────────────────────────────┐
│                    TurboLocker 接口                   │
│          Lock(ctx, key) → (UnlockFunc, error)        │
├─────────────────────────────────────────────────────┤
│                  本地合流层（核心）                     │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
│  │ slot A  │  │ slot B  │  │ slot C  │  ...         │
│  │ (key=a) │  │ (key=b) │  │ (key=c) │              │
│  │ cond+mu │  │ cond+mu │  │ cond+mu │              │
│  └────┬────┘  └────┬────┘  └────┬────┘              │
│       │            │            │                    │
│   Leader         Leader       Leader                 │
│   (唯一去Redis)   (唯一去Redis) (唯一去Redis)          │
├─────────────────────────────────────────────────────┤
│                   Redis 网络层                        │
│         SETNX 抢锁  │  Lua 脚本原子释放               │
│         crypto/rand DNA 防误删                       │
└─────────────────────────────────────────────────────┘
```

---

## FAQ

### Q: 单 Redis 节点，直接用 Redis 单线程特性不就行了？为什么要合流？

A: Redis 单线程保证的是 **"命令执行的原子性"**，不保证 **"网络 IO 的承受能力"**。10000 个客户端同时发 SETNX，Redis 能正确处理，但网络带宽、连接数、客户端侧的协程调度和 time.After 内存分配会爆炸。合流层把 10000 次网络调用降为 1 次，保护的是 **客户端侧的资源和 Redis 的网络承载上限**。

### Q: 既然合流了，为什么不用 channel 而是用 sync.Cond？

A: `sync.Cond` 基于 Go runtime 的 goroutine parking 机制，挂起的协程不占 CPU 时间片。channel 也可以实现类似效果，但 `sync.Cond` + `Broadcast` 的"一唤醒全部"语义更契合 Follower 全部同时返回失败的场景。

### Q: 锁释放后 isSuccess 为什么没重置？是 bug 吗？

A: 是 bug。当前设计下，Leader 成功抢锁后 `isSuccess` 被置为 `true` 且永不重置，导致该 Key 的 slot 永久"中毒"——此后所有 Lock() 调用都会立即返回 `ErrLockFailed`。需要在 UnlockFunc 或 Lock 入口处增加重置逻辑。详见 `test_doc/第二阶段压测数据分析.md` 问题 3。

### Q: 为什么不直接用 RedLock（多 Redis 节点）？

A: TurboLock 的定位是单 Redis 场景下的性能优化工具，不是高可靠性分布式锁。RedLock 解决的是"Redis 挂了怎么办"，TurboLock 解决的是"Redis 没挂但流量太大怎么办"，两者是正交的。如果需要高可靠性，应在 TurboLock 上层叠加 RedLock 或 etcd。
