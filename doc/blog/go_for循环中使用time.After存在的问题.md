# Go 高并发踩坑：关于在 for 循环里使用 time.After 后所进行的问题分析

引言：一场由 1006 次内存逃逸引发的血案

最近在学习 高性能分布式锁组件 时，为了测试极限性能，我写了一个 Benchmark：开启 1 万个 Goroutine 去争抢同一把锁，抢不到则进入重试循环。

跑分结果出来后，我盯着控制台的输出倒吸了一口凉气：

```
142537929 ns/op    57952 B/op    1006 allocs/op
```

单次抢锁耗时高达 142 毫秒，并且单次操作竟然触发了 1000 多次堆内存分配！

经过排查与性能剖析（Profiling），我发现 Go 标准库中的定时器：time.After 存在很大嫌疑！

```go
for i := 0; i < maxRetries; i++ {
    // ... 尝试获取锁失败 ...
	ok, err := t.client.SetNX(ctx, key, value, t.optsExpiry).Result()
    
    // 优雅地等待 50 毫秒后重试
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-time.After(50 * time.Millisecond): 
        // 继续下一轮循环
    }
}
```

* 首先是 client.SetNX
* 在 go-redis 的底层源码中，每次执行 SetNX，它不可避免地要在堆上做以下事情：
* * new 一个 *redis.BoolCmd 结构体来接收结果。
* * 创建一个 []interface{} 切片，把 "SET"、key、value、"NX"、"EX" 这些参数打包进去。
* * 字符串和接口类型转换（string to any）产生的逃逸。
* * 底层网络连接池（ConnPool）的上下文 Context 包装。
* 这样一次操作下来，预估也就 6~7 allocs，再叠加100次打满，那么最多不过 600-700 allocs
* 但实际测量下来的数据量（1000左右）明显超标了！
* 所以，必然是 time.After 中存在猫腻！


下面我们基于一个更为具体的实际测试案例进行分析

* bad_turbolock.go - 我们剥离了复杂的合流逻辑，只保留纯粹的重试循环。

```go
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

func (t *BadTurboLocker) Lock(ctx context.Context, key string) error {
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
```

* bad_turbolock_test.go

```go
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

	// ================= 核心 =================
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
```

* 需要确保本地启动了 Redis（localhost:6379）
* go test -bench=BenchmarkBadTurboLock_TimeAfter -benchmem -benchtime=3s
* 然后是示例数据的展示输出：

```
goos: linux
goarch: amd64
pkg: turbolock/example
cpu: Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
BenchmarkBadTurboLock_TimeAfter-8              1        5101094915 ns/op           56704 B/op        950 allocs/op
PASS
ok      turbolock/example       5.150s
```

我们先基于这个数据进行分析

* 5101094915 ns/op (约为 5.1 秒的耗时)，为什么是 5.1 秒呢？
* * 因为我们代码设定了重试 100 次，每次 time.After 沉睡 50 ms，那么就是 100 * 50ms = 5000ms（5秒）
* * 而剩下的 0.1秒则是 100 次 Redis网络请求的开销
* 这也证明了所有 goroutine 都全部撞满了 100 次南墙

* 56704 B/op 和 950 allocs/op
* * 就在这绝望的 5.1 秒里，每一个协程在堆上疯狂创建了**近 1000 个**垃圾对象，消耗了 **56 KB** 内存。
* * 如果有 1 万个并发，你的服务器瞬间就会多出 500 MB 的纯垃圾，导致 CPU 被垃圾回收器（GC）彻底榨干。

而溢出的分配次数和容量，我的初步怀疑，也就是上面也就说的 time.After

原因是：**每一轮重试的 time.After(50 * time.Millisecond) 都在底层向 Go 的 runtime 注册一个计时器（Timer）对象，这些对象全部逃逸到了堆上！重试 100 次，内存就暴涨 100 倍。**

time.After 的底层究竟是什么样的一个逻辑，才会造成了这个问题呢？以及我们该如何避免这样的问题出现呢？

接下来，我们将带着这两个问题，去解读 time 的源码，去解密我的猜想是否正确。

## time.After 的底层究竟是什么样的一个逻辑，才会造成了这个问题呢

需要注意的是，我这里使用的 Go 版本是：go version go1.25.10 linux/amd6

在 go1.23版本+ 中 timer 做了两个重大修复（这里只是提一下，后续单独出一个学习记录）

* 1、未停止定时器的内存泄漏”
* * newTimer 新 runtime 实现
* * 解除 runtime 对 Timer 的强引用 → Timer 可被 GC
* 2、重置定时器时的脏读陷阱
* * syncTimer(c)
* * 把通道从缓冲变同步 → 消除 Stop/Reset 后的脏数据

```go
func NewTimer(d Duration) *Timer {
	c := make(chan Time, 1)
	t := (*Timer)(newTimer(when(d), 0, sendTime, c, syncTimer(c)))
	t.C = c
	return t
}
```

但是！Go 1.23 的改进解决了 Timer 的**泄漏**和**脏数据**问题，但没有解决 time.After 在循环中的**堆分配**问题。

**它依然在疯狂地“分配（Allocate）”垃圾！**

* make(chan Time, 1)
* * makechan 本身就是 堆分配
* * * 在 runtime/chan.go 中，make(chan ...) 最终调用的 makechan 函数直接调用 mallocgc 从堆上申请内存。
* newTimer
* * 一次 newTimer 调用，其底层也是 runtimeTimer 的封装
* time.Timer 结构体
* * 由于需要回传给外层的 select 监听，所以最后也会 逃逸到堆
* 如此，一次 NewTimer 调用，底层会产生大约 3 个零碎的堆内存对象（包括 hchan 结构体、Timer 结构体、底层的 runtimeTimer 包装等）。
* 测试代码撞墙了 100 次，等于循环了 100 遍。100 次循环 × 单次调用的约 3 个堆对象 ≈ 300+ 次内存分配。 
* 如此，真相大白了。

明白了底层缘由，那我们又该如何去规避这样的问题呢？

## 解药：零分配（Zero-Allocation）重试机制

初始分配一次肯定是无法避免的，这里的零分配，指代的是在 for 循环内部重复使用，也就是：

**time.NewTimer + timer.Reset**

```go
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
```

```
go test -bench=BenchmarkBadTurboLock_TimeAfter -benchmem -benchtime=3s
goos: linux
goarch: amd64
pkg: turbolock/example
cpu: Intel(R) Core(TM) i7-7700HQ CPU @ 2.80GHz
BenchmarkBadTurboLock_TimeAfter-8              1        5086874740 ns/op           31544 B/op        649 allocs/op
PASS
ok      turbolock/example       5.136s
```

* allocs/op	
* * 950 ---> 649
* * 降幅：-32%
* B/op
* * 56704 ---> 31544
* * 降幅：-44%
* 相当可观的数据变化了

## 总结

从一次 Benchmark 的异常数据出发——单次抢锁操作触发 1000 次堆分配，56KB 内存消耗——一步步追溯到 time.After 的源码层，最终锁定了真凶：time.After 在 for 循环中每次迭代都在堆上新建 Timer 对象。

* 抽丝剥茧之后，结论其实很简洁：
* * Go 1.23 虽然修了 Timer 的 GC 泄漏和脏数据问题，但 "分配"本身并不会消失——只有改变代码模式才能从根本上减少分配。

一句话心得：**time.After 是给"用完即弃"的场景设计的——超时控制、一次性等待都很合适；一旦进入循环体，请毫不犹豫换成 NewTimer + Reset。**