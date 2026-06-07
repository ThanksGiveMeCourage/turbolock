# 对 sync.cond 的学习和思考

sync.Cond 是 Go 标准库提供的条件变量（condition variable），用于在多个 goroutine 之间进行基于条件的等待与唤醒通信。它本质上是一个信号通知机制，让一组 goroutine 可以在某个条件不满足时挂起等待，直到条件满足时被唤醒。

* sync.Cond 内部维护了三个关键要素：
* * 一个 Locker（通常是 sync.Mutex） — 保护共享条件
* * 一个等待队列（FIFO 通知列表） — 存储正在等待的 goroutine
* * 三个操作方法 — Wait()、Signal()、Broadcast()

* 三个核心方法：
* * Wait()	1. 自动释放持有的锁 2. 将当前 goroutine 放入等待队列挂起 3. 被唤醒后重新获取锁
* * Signal()	唤醒等待队列中的一个 goroutine（FIFO 顺序）
* * Broadcast()	唤醒等待队列中的所有 goroutine


* 经典使用模式：

```go
// 等待者（Waiter）
cond.L.Lock()
for !condition {
    cond.Wait()  // 自动释放锁并挂起，被唤醒后重新获取锁
}
// 条件满足，继续执行
cond.L.Unlock()

// 通知者（Notifier）
cond.L.Lock()
// 修改条件...
cond.L.Unlock()
cond.Signal()  // 或 cond.Broadcast()
```

* 在 turbolock_impl.go 中实现的本地锁槽领导者-追随者模式（Leader-Follower），正是 sync.Cond 的经典使用场景：

```
┌─────────────────────────────────────────────────┐
│                 Leader-Follower                   │
│                                                   │
│   Goroutine A (Leader) ──► 去 Redis 抢锁          │
│                                                   │
│   Goroutine B (Follower) ─┐                       │
│   Goroutine C (Follower)  ├─► cond.Wait() 挂起   │
│   Goroutine D (Follower) ─┘                       │
│                                                   │
│   Leader 回来后 ──► cond.Broadcast() 唤醒所有     │
└─────────────────────────────────────────────────┘
```

## turbolock_impl.go 中实现的本地锁槽领导者-追随者模式（Leader-Follower）的具体代码逻辑

```go
// 定义本地锁槽，每一个独立的 Key 都会对应一个锁槽
type localSlot struct {
	mu        sync.Mutex // 保护当前槽位内状态的局部锁
	cond      *sync.Cond // 用于挂起和唤醒当前 key 的追随者
	active    bool       // 是否已经有代表（Leader）去远程 Redis 抢锁了
	winnerVal string     // 如果 Leader 抢锁成功，把DNA留下来，供本地 Follower 共享释放（如果设计成共享锁模式的话）
}
```

```go
// 获取该 key 对应的本地闸门
	slot := t.getSlot(key)
	slot.mu.Lock()
	// 【核心拦截点】：如果已经有代表出发了，后来的协程全部原地卧倒
	for slot.active {
		slot.cond.Wait() // // 自动释放锁并挂起，被唤醒后重新获取锁
		// 惊醒（被唤醒）后，由于是排他分布式锁，第一版我们直接让追随者在被唤醒后重新参与竞争或返回失败
		// 为了让第一版最稳，被唤醒后跳出循环，重新往下走去博弈
	}
	slot.active = true
	slot.mu.Unlock()

	// 只要成为了 Leader，无论结果如何，退出时必须重置本地状态并唤醒所有卧倒的协程
	defer func() {
		slot.mu.Lock()
		slot.active = false
		slot.cond.Broadcast() // 广播，唤醒等待队列中的所有 goroutine
		slot.mu.Unlock()
	}()
```

其中需要注意的两点：

* 1、为什么 slot.cond.Wait() 必须放在 for slot.active 循环里，而不能用 if slot.active？
* * 原理： 这叫“**虚假唤醒（Spurious Wakeup）**”。
* * 当 Leader 释放锁并调用 Broadcast() 时，所有等待的 Follower 都会被唤醒，但它们是在同一时间被唤醒的。
* * 如果在外面加锁的不是 for 而是 if，协程醒来后会直接往下冲，导致成百上千个协程同时认为自己是 Leader，本地闸门直接崩塌。
* * 用 for 循环可以确保它们醒来后，必须重新检查一边条件，只有真正通过检查的那一个才能继续。

* 2、defer slot.cond.Broadcast() 的防死锁守护：
* * 原理： 只要抢锁逻辑结束（无论是因为抢到了、重试失败了、还是 ctx 超时退出了），Leader 必须执行 Broadcast()。
* * 如果不写在 defer 里，一旦中间发生 panic 或者未捕获的 return，本地挂起的 9999 个协程将永远死等，导致系统产生严重的协程死锁泄露。