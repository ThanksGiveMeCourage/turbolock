# 层级时间轮看门狗 — 完整代码解析

> 对应源码：`timing_wheel.go`（~270 行）  
> 前置知识：Go sync.Mutex、双向链表、time.Ticker  
> 学习目标：掌握层级时间轮的核心原理与 Go 实现技巧

---

## 一、这个模块解决了什么问题？

### 1.1 场景

分布式锁有一个致命漏洞：业务执行时间可能超过锁的 TTL，导致锁自动过期，另一个协程趁虚而入。

```
Leader 持锁 → TTL=8s → 业务执行了 9s → TTL 到期 → 另一个 Leader 也拿到了锁
```

解法是**自动续期**：在 TTL 到期前，由系统自动向 Redis 发送 `EXPIRE` 刷新过期时间。

### 1.2 为什么不能「一锁一协程」？

最直观的方案是为每个锁起一个 goroutine，循环 sleep 然后续期：

```go
go func() {
    for {
        time.Sleep(interval)
        redis.EXPIRE(key, ttl)
    }
}()
```

| 锁数量 | goroutine 数 | Timer 数 | 内存 |
|--------|:--:|:--:|------|
| 100 | 100 | 100 | ~800KB |
| 10,000 | 10,000 | 10,000 | **~80MB** |
| 100,000 | 100,000 | 100,000 | **爆炸** |

每个 goroutine 最少占用 2KB 栈空间 + 一个 `runtime.Timer` 对象。当锁数量达到万级时，内存和调度开销将不可接受。

### 1.3 时间轮的答案

**整个系统只用一个 goroutine + 一个 `time.Ticker`，管理所有锁的续期。**

用数据结构（数组 + 链表）取代 goroutine，把 O(N) 的 goroutine 开销降低到 O(1) 的链表操作。

---

## 二、数据结构全景图

```
timingWheel（全局单例）
│
├── tick: 100ms（心跳间隔）
├── 单 goroutine 驱动
│
├── Level 0 [256 槽] ← 主力，覆盖 0~25.6s
│   ├── cursor0: 当前指针位置
│   └── slots0[0..255]: 每个槽位是一个双向链表头
│
├── Level 1 [64 槽]  ← 兜底，覆盖 0~27.3min
│   ├── cursor1
│   └── slots1[0..63]
│
└── Level 2 [64 槽]  ← 极端兜底，覆盖 0~29.1h
    ├── cursor2
    └── slots2[0..63]
```

### 2.1 槽位内的链表结构

```
slots0[17]
  │
  ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│ task A   │───→│ task B   │───→│ task C   │
│ key="x"  │←───│ key="y"  │←───│ key="z"  │
│ ticks=26 │    │ ticks=26 │    │ ticks=26 │
└──────────┘    └──────────┘    └──────────┘
```

每个 `timerTask` 既是"待续期的锁描述"，也是双向链表的节点。

### 2.2 为什么是三层层级？

```
单层 256 槽 × 100ms = 只能覆盖 25.6 秒

如果续期间隔 = 60s，需要覆盖 60 秒 → 单层不够
  → 加一层：Level 1，每槽 = 25.6 秒，64 槽 = 27.3 分钟

如果续期间隔 = 1 小时 → 双层不够
  → 再加一层：Level 2，每槽 = 27.3 分钟，64 槽 = 29.1 小时
```

**实际使用中**，续期间隔 = TTL/3 ≈ 2.6s，远小于 25.6s，所以 99.99% 的任务只在 Level 0。Level 1/2 是为极端配置兜底的"安全气囊"。

---

## 三、核心数据结构逐字段解读

### 3.1 `timerTask`

```go
type timerTask struct {
    key        string                          // Redis 锁的键名
    callback   func(ctx context.Context) error // 续期函数（Lua EXPIRE）
    ticks      int                             // 续期间隔 ÷ 100ms，如 2.6s → 26
    cancelled  bool                            // 惰性删除标记
    prev, next *timerTask                      // 双向链表指针
}
```

| 字段 | 为什么需要 | 使用时机 |
|------|-----------|---------|
| `key` | 日志中标识是哪个锁 | `fireSlot` 中回调失败时打日志 |
| `callback` | 续期逻辑（Redis Lua 脚本） | 被 `fireSlot` 触发时调用 |
| `ticks` | 决定放入哪个槽位，级联时重算位置 | `insertTask` + `cascade` |
| `cancelled` | 物理摘除会导致槽位指针悬空，改为标记+惰性清理 | `removeTask` 标记，`fireSlot` 检查 |
| `prev/next` | 双向链表操作 O(1) | `insertHead` / `unlink` |

### 3.2 `timingWheel`

```go
type timingWheel struct {
    slots0  [256]*timerTask  // Level 0: 秒针层
    cursor0 int               // Level 0 当前指针

    slots1  [64]*timerTask   // Level 1: 分针层
    cursor1 int

    slots2  [64]*timerTask   // Level 2: 时针层
    cursor2 int

    mu      sync.Mutex       // 保护所有槽位和游标
    stopCh  chan struct{}    // 停止信号
    ctx     context.Context  // 传递给回调的上下文
    cancel  context.CancelFunc
}
```

**为什么用数组而不是 slice？**

`[256]*timerTask` 是固定长度的数组，编译期确定大小，分配在结构体内存中，零额外堆分配。且槽位数是时间轮设计的核心常量，永远不需要动态调整。

---

## 四、核心算法逐行解析

### 4.1 添加任务：`addTask` + `insertTask`

```go
func (tw *timingWheel) addTask(key string, interval time.Duration, 
    callback func(context.Context) error) *timerTask {
    
    task := &timerTask{
        key:      key,
        callback: callback,
        ticks:    int(interval / tickMs),  // ← 关键：interval → tick 数
    }
    tw.mu.Lock()
    tw.insertTask(task)
    tw.mu.Unlock()
    return task
}
```

`ticks` 的计算是整个时间轮的基石。例如：

```
续期间隔 2.6s → ticks = 2600ms / 100ms = 26
含义：这个任务应该在 26 个 tick（2.6 秒）后触发
```

**插入算法**：

```go
// insertTask 根据 task.ticks 决定放入哪一层哪个槽位。调用前需持有 tw.mu。
func (tw *timingWheel) insertTask(task *timerTask) {
	ticks := task.ticks

	switch {
	case ticks < slotsL0:
		// Level 0: 直接计算槽位
		/*
			举例说明，假如当前 ticks = 26

			假设当前指针在 100 号格：
			slot = (100 + 26) % 256 = 126

			贴在 126 号格 ✓

			时间流逝...
			0.1s 后指针到 101
			0.2s 后指针到 102
			...
			2.6s 后指针到 126 → 碰到纸条！
		*/
		slot := (tw.cursor0 + ticks) % slotsL0
		insertHead(&tw.slots0[slot], task)
	case ticks < slotsL0*slotsL1:
		// Level 1: ticks 整除 L0 槽数，余数在级联时处理
		/*
			假设要 30 秒后提醒 → ticks = 300

				300 < 256?  不成立
				300 < 256×64?  300 < 16384?  成立 → 进入 Level 1

			这就是 Level 1 存在的意义——当时间太长，一盘转不过来，就用两盘接力：

				大盘 (Level 0): 256 格，每格 0.1 秒，转一圈 = 25.6 秒
				小盘 (Level 1):  64 格，每格 25.6 秒，转一圈 ≈ 27 分钟

				"30 秒后" = 300 个 tick = 1 个小盘格 + 44 个大盘格

				300 ÷ 256 = 1（小盘走 1 格）
				300 % 256 = 44（在 Level 0 上的剩余格子）
		*/
		l1Ticks := ticks / slotsL0
		slot := (tw.cursor1 + l1Ticks) % slotsL1
		insertHead(&tw.slots1[slot], task)
	default:
		// Level 2: 兜底
		/*
						Level 2 同理：超长时间的三级接力

						Level 2: 64 格，每格 27.3 分钟

						"1 小时后" →
						小时盘转 2 格 → 分针盘再转 → 秒针盘触发

					类比于时钟的（时、分、秒 来进行理解）

						秒针(Level 0): 一圈 256 格,  每格 = 0.1 秒
						分针(Level 1): 一圈 64 格,   每格 = 25.6 秒
						时针(Level 2): 一圈 64 格,   每格 = 27.3 分钟

						1 个时针格 = 分针走满一圈
			           			= 分针 64 格

						1 个分针格 = 秒针走满一圈
								= 秒针 256 格

						所以 1 个时针格 = 64 × 256 = 16384 个秒针 tick

						时针的格数 = 总 tick / 16384
								= ticks / (256 × 64)
								= ticks / (slotsL0 × slotsL1)    ← 就是这行！

		*/
		l2Ticks := ticks / (slotsL0 * slotsL1)
		slot := (tw.cursor2 + l2Ticks) % slotsL2
		insertHead(&tw.slots2[slot], task)
	}
}
```

**为什么是 `cursor + ticks` 而不是绝对位置？**

```
假设 cursor0 当前在槽位 100

添加一个 ticks=26 的任务：
  槽位 = (100 + 26) % 256 = 126

100ms 后 cursor0 推进到 101
200ms 后 cursor0 推进到 102
...
2.6s 后 cursor0 推进到 126 → 任务触发！

cursor 是"现在"，ticks 是"多久以后" → cursor + ticks = "未来的位置"
```

### 4.2 删除任务：`removeTask`（惰性删除）

```go
func (tw *timingWheel) removeTask(task *timerTask) {
    tw.mu.Lock()
    task.cancelled = true  // 只标记，不物理摘除
    tw.mu.Unlock()
}
```

**为什么不直接 `unlink`？**

```
物理摘除的问题：

  slots0[17] → A → B → C
  
  unlink(B)  → B.prev.next = B.next  ✓ B 从链表脱离
             → 但 slots0[17] 仍指向 A（不受影响）✓
             
  但如果 unlink(A)：
             → A 是链表头，slots0[17] 仍指向 A！
             → A 已被移走，但指针残留
             → 256 tick 后光标绕回，残留指针导致重复触发
```

惰性删除将物理摘除推迟到 `fireSlot` 遍历该槽位时——此时槽位头指针被保存并清空，不存在悬空指针问题。

### 4.3 指针推进：`advance` + `fireSlot`

```go
func (tw *timingWheel) advance() {
    tw.mu.Lock()
    defer tw.mu.Unlock()

    // 步骤 1：推进光标
    tw.cursor0 = (tw.cursor0 + 1) % 256

    // 步骤 2：保存并清空当前槽位（防止光标绕回时二次触发）
    head := tw.slots0[tw.cursor0]
    tw.slots0[tw.cursor0] = nil
    tw.fireSlot(head)

    // 步骤 3：Level 0 走满一圈 → 级联 Level 1
    if tw.cursor0 == 0 {
        tw.cascadeLevel1()
    }
}
```

**为什么 `head := ...; slots0[cursor0] = nil` 是必须的？**

```
不保存清空时：
  T=0: fireSlot(slots0[17]) → 把所有任务移到新槽位
       但 slots0[17] 仍指向已被移走的旧任务 A
       
  T=256: 光标绕回 17 → fireSlot(slots0[17])
        → 传入旧任务 A → 被重复触发！

保存清空后：
  T=0: head = slots0[17]; slots0[17] = nil; fireSlot(head)
  T=256: slots0[17] = nil → fireSlot(nil) → 空操作 ✓
```

**`fireSlot` 的处理流程**：

```go
func (tw *timingWheel) fireSlot(head *timerTask) {
    task := head
    for task != nil {
        next := task.next          // ① 先保存后继（链表即将被修改）
        tw.unlink(task)            // ② 从当前槽位链表摘除

        if task.cancelled {        // ③ 惰性删除：已取消的任务直接丢弃
            task = next
            continue
        }

        go func(t *timerTask) {   // ④ 异步执行回调（不阻塞时间轮）
            defer func() {
                if r := recover(); r != nil {
                    log.Printf("panic: %v", r)
                }
            }()
            t.callback(tw.ctx)
        }(task)

        tw.insertTask(task)        // ⑤ 重新调度到未来的槽位
        task = next                // ⑥ 处理下一个任务
    }
}
```

**为什么是异步回调？**

回调是 Redis 网络操作（EXPIRE），可能耗时数毫秒。如果在持锁状态下同步执行，会阻塞整个时间轮的所有其他任务。异步执行保证了 100ms 的 tick 精度不受单个慢回调影响。

**为什么先 unlink 再 insertTask？**

`unlink` 将任务从当前槽位摘除，`insertTask` 将其放入未来槽位。同一个任务不能同时出现在两个槽位中——先摘后插保证了这个不变式。

---

## 五、级联降级机制详解

### 5.1 为什么需要级联？

```
任务 ticks=300（30 秒后续期）

insertTask:
  300 >= 256 → 放入 Level 1
  l1Ticks = 300/256 = 1
  slot = (cursor1 + 1) % 64   ← 放在 cursor1 前方 1 格

时光流逝... Level 0 每 100ms 推进一次

Level 0 推进 256 次（25.6 秒）→ cursor0 绕回 0
  → 触发 cascadeLevel1()
```

### 5.2 `cascadeLevel1` 逐行解析

```go
func (tw *timingWheel) cascadeLevel1() {
    tw.cursor1 = (tw.cursor1 + 1) % 64  // ① Level 1 的"时钟"走了一格

    // ② 取出当前槽位的所有任务，清空槽位
    task := tw.slots1[tw.cursor1]
    tw.slots1[tw.cursor1] = nil

    for task != nil {
        next := task.next
        task.prev = nil
        task.next = nil

        // ③ 计算在 Level 0 的剩余 tick 数
        remaining := task.ticks % 256
        // 例: ticks=300 → 已在 L1 等了 256 tick → 剩余 44 tick → 4.4 秒后触发

        // ④ 放入 Level 0 的对应槽位
        slot := (tw.cursor0 + remaining) % 256
        insertHead(&tw.slots0[slot], task)

        task = next
    }

    // ⑤ Level 1 也走满一圈 → 级联 Level 2
    if tw.cursor1 == 0 {
        tw.cascadeLevel2()
    }
}
```

**级联数学验证**（以 ticks=300 为例）：

```
初始: cursor0=X, cursor1=Y
插入: slots1[(Y + 1) % 64] = task

Level 0 推进 256 次 → cursor0 回到 X（但不读取 X，而是 X 前方的 256 个槽都处理过了）
                    → cascadeLevel1: cursor1 = (Y + 1) % 64

取出 slots1[(Y + 1) % 64] ← 正是我们的 task！
remaining = 300 % 256 = 44
放入 slots0[(X + 44) % 256] ← 44 tick (4.4s) 后触发

总等待: 256 + 44 = 300 tick = 30s ✓ 准确！
```

### 5.3 级联示意图

```
Time ─────────────────────────────────────────────►

┌─ Level 0 循环 ───────────────────────────────────┐
│ [X] [X+1] ... [X+255]                            │
│  0   100ms         25.5s                          │
│                                                   │
│ cursor0 每 100ms 推进 1 格                         │
│ 推进 256 次 → 走满一圈 → 触发 cascadeLevel1()      │
└───────────────────────────────────────────────────┘
                            │
                            ▼
┌─ Level 1 ─────────────────┐
│ cursor1 += 1               │
│ 将 slots1[cursor1] 中      │
│ 的所有任务级联到 Level 0    │
└────────────────────────────┘
```

---

## 六、并发模型

### 6.1 锁策略

```
timingWheel 只有一把锁：tw.mu (sync.Mutex)

所有公开 API（addTask、removeTask）和内部方法（advance、fireSlot、
cascadeLevel1/2）都通过持有 tw.mu 来保护共享状态。

持锁范围：
  addTask:    Lock → insertTask → Unlock          (~100ns)
  removeTask: Lock → cancelled=true → Unlock       (~50ns)
  advance:    Lock → fireSlot + cascade → Unlock   (~μs 级，不含回调)
```

### 6.2 回调不持锁

```go
go func(t *timerTask) {     // ← 新 goroutine
    t.callback(tw.ctx)      // ← 这里没有 tw.mu
}(task)
```

回调在独立的 goroutine 中执行，不持有 `tw.mu`。这意味着：

- 续期期间，时间轮可以继续推进其他槽位
- `addTask` 和 `removeTask` 不会被续期的网络 I/O 阻塞
- 100ms 的 tick 精度不受影响

### 6.3 goroutine 生命周期

```
timingWheel.run()           1 个 goroutine，永不退出直到 Stop()
fireSlot → go func()       每次触发最多 N 个瞬时 goroutine（N = 槽位中任务数）
                            每个 goroutine 执行一次回调后自动退出
```

---

## 七、边界场景与设计决策

### 7.1 续期失败

```go
if err := t.callback(tw.ctx); err != nil {
    log.Printf("turbolock: renew key=%s failed: %v", t.key, err)
}
```

**不重试、不断开、不打标记。** 因为任务已经通过 `insertTask` 重新调度了——下一次 tick 会自然再次触发续期。如果 Redis 持续不可达，TTL 最终会过期，锁自动释放（最终安全）。

### 7.2 回调 panic

```go
defer func() {
    if r := recover(); r != nil {
        log.Printf("turbolock: renew key=%s panic: %v", t.key, r)
    }
}()
```

回调 panic 不会导致时间轮进程崩溃。任务已经通过 `insertTask` 重新调度，下次 tick 继续尝试（除非 panic 是持久性的——此时与续期失败的处理一致）。

### 7.3 任务被 remove 后仍然可能触发一次回调

```
竞态场景：
  T=99ms: fireSlot 将 task 从槽位摘除、cancelled=false、go func() 已启动
  T=100ms: removeTask 标记 cancelled=true
  T=101ms: goroutine 中的 callback 执行（此时 cancelled 已为 true，但不检查）

结果：回调仍然执行了一次。
影响：回调是 Lua 脚本（GET + EXPIRE），会先检查 value 是否匹配。
     如果锁已被释放（value 不匹配），Lua 返回 0，无实际效果。
     → 无害，只是多一次无效果的 Redis 调用。
```

### 7.4 `ticks` 整数截断

```go
ticks := int(interval / tickMs)
```

`interval / 100ms` 的整数除法会截断小数部分。例如 150ms 间隔 → 1 tick → 实际触发在 ~100ms 后，而不是精确的 150ms。**对于续期场景，±50ms 的抖动完全可接受**——只要在 TTL 到期前续上即可。

### 7.5 槽位数量是 2 的幂

```
256 = 2^8
64  = 2^6
```

这使得 `cursor = (cursor + 1) % 256` 可以被编译器优化为 `cursor = (cursor + 1) & 255`（位运算），比真正的取模快一个数量级。

---

## 八、性能特征总结

| 操作 | 时间复杂度 | 实际耗时 | 锁持有时间 |
|------|:--:|------|:--:|
| `addTask` | O(1) | ~100ns | 持锁 |
| `removeTask` | O(1) | ~50ns | 持锁 |
| `advance` (空槽) | O(1) | ~100ns | 持锁 |
| `advance` (N 个任务) | O(N) | ~1μs × N | 持锁 |
| 回调执行 | O(1) | ~1ms (Redis 网络) | **不持锁** |
| 级联 | O(N_cascade) | ~1μs × N | 持锁 |

**关键设计原则**：持锁路径都是纯内存操作（纳秒-微秒级），网络 I/O 全部异步化（不持锁）。

---

## 九、与主流实现的对比

| 特性 | 本实现 | Kafka TimingWheel | Netty HashedWheelTimer |
|------|--------|:--:|:--:|
| 层级数 | 3 | 多级（按需） | 1 |
| 槽位数 | 256 + 64 + 64 | 动态 | 512 |
| tick 精度 | 100ms | 1ms | 可配置 |
| 任务类型 | 固定间隔重复 | 一次性 | 一次性 |
| 惰性删除 | ✅ cancelled 标记 | ✅ | ✅ |
| 适用场景 | 锁续期（低频高可靠） | 网络超时（高频） | 通用定时器 |

本实现的 3 层级 + 100ms tick + 数组槽位，是为"锁续期"这个特定场景定制的——牺牲了毫秒级精度（不需要），换来了极简的代码和零外部依赖。

---

## 十、完整生命周期时序

```
  调用方               TurboLocker            TimingWheel             Redis
   │                      │                      │                     │
   │── Lock("order_1") ──►│                      │                     │
   │                      │── SETNX ──────────────────────────────────►│
   │                      │◄─────── OK ──────────────────────────────│
   │                      │                      │                     │
   │                      │── addTask(2.6s) ────►│                     │
   │                      │   ticks=26           │                     │
   │                      │   放入 slots0[(c+26)%256]                  │
   │                      │◄── renewTask ────────│                     │
   │                      │                      │                     │
   │◄── UnlockFunc ───────│                      │                     │
   │                      │                      │                     │
   │ [业务执行...]         │                      │                     │
   │                      │              ┌───────┤ 每 100ms            │
   │                      │              │ tick  │ advance()           │
   │                      │              │       │ cursor0 += 1        │
   │                      │              │ ...   │ fireSlot(槽位)       │
   │                      │              │       │                     │
   │                      │              │ 2.6s  │ cursor0 到达目标槽   │
   │                      │              │ 后    │                     │
   │                      │              │       │ unlink(task)        │
   │                      │              │       │ go callback():       │
   │                      │              │       │   Lua EXPIRE ──────►│
   │                      │              │       │   ◄──── OK ────────│
   │                      │              │       │ insertTask(task)    │
   │                      │              └───────┤ 重新调度            │
   │                      │                      │                     │
   │ [业务完成]            │                      │                     │
   │── unlock() ──────────►│                      │                     │
   │                      │── removeTask ────────►│                     │
   │                      │   task.cancelled=true │                     │
   │                      │── Lua DEL ────────────────────────────────►│
   │                      │◄─────── OK ───────────────────────────────│
   │                      │                      │                     │
   │                      │                      │ 下次 fireSlot 到达   │
   │                      │                      │ → cancelled=true     │
   │                      │                      │ → 惰性丢弃, 不再调度  │
```

---

## 十一、关键面试点

如果面试中被问到"你实现的时间轮"，以下是必须能回答的核心问题：

| 问题 | 答案要点 |
|------|---------|
| 为什么用时间轮而不是堆？ | 堆 O(log N)，时间轮 O(1)；场景是固定间隔重复任务，时间轮更优 |
| 为什么是三层？ | Level 0 主力（256×100ms=25.6s），L1/L2 兜底极端间隔 |
| 删除为什么是惰性的？ | 物理摘除会导致槽位指针悬空；`cancelled` 标记 + `fireSlot` 遍历时清理 |
| 回调为什么异步？ | Redis 网络 I/O 不持锁，保证 tick 精度 |
| 级联的数学原理？ | `remaining = ticks % 256`，还原 L1 等待后剩余的 L0 tick 数 |
| 服务崩溃怎么办？ | 极端场景：锁被无限续期 → 需 `maxHoldDuration` 兜底（阶段三规划中） |
