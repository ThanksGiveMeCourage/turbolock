# Go 学习记录：关于 go1.23版本+ 中 Timer 底层做出的两个重大修复

需要注意的是，我这里使用的 Go 版本是：go version go1.25.10 linux/amd6

如果你使用的 Go 版本低于 go version 1.23，那么代码逻辑将存在额外问题，这里我们也会一并进行解读，也算是一个回顾吧

* 上层After 调度只是一个封装（没有变化），其核心差异在其内部的 NewTimer 实现上

```go
func After(d Duration) <-chan Time {
	return NewTimer(d).C
}
```


* go 1.21.11（Before Go 1.23）

```go
// NewTimer creates a new Timer that will send
// the current time on its channel after at least duration d.
func NewTimer(d Duration) *Timer {
	c := make(chan Time, 1)
	t := &Timer{
		C: c,
		r: runtimeTimer{
			when: when(d),
			f:    sendTime,
			arg:  c,
		},
	}
	startTimer(&t.r)
	return t
}
```
 
* go 1.25.10（go1.23+）

```go
// NewTimer creates a new Timer that will send
// the current time on its channel after at least duration d.
//
// Before Go 1.23, the garbage collector did not recover
// timers that had not yet expired or been stopped, so code often
// immediately deferred t.Stop after calling NewTimer, to make
// the timer recoverable when it was no longer needed.
// As of Go 1.23, the garbage collector can recover unreferenced
// timers, even if they haven't expired or been stopped.
// The Stop method is no longer necessary to help the garbage collector.
// (Code may of course still want to call Stop to stop the timer for other reasons.)
//
// Before Go 1.23, the channel associated with a Timer was
// asynchronous (buffered, capacity 1), which meant that
// stale time values could be received even after [Timer.Stop]
// or [Timer.Reset] returned.
// As of Go 1.23, the channel is synchronous (unbuffered, capacity 0),
// eliminating the possibility of those stale values.
//
// The GODEBUG setting asynctimerchan=1 restores both pre-Go 1.23
// behaviors: when set, unexpired timers won't be garbage collected, and
// channels will have buffered capacity. This setting may be removed
// in Go 1.27 or later.
func NewTimer(d Duration) *Timer {
	c := make(chan Time, 1)
	t := (*Timer)(newTimer(when(d), 0, sendTime, c, syncTimer(c)))
	t.C = c
	return t
}
```

首先，go1.23+ 的 NewTimer 从注释来看就知道信息量巨大，其中涉及了两个重大 timer 顽疾的修复：

* 1、未停止定时器的内存泄漏”
* 2、重置定时器时的脏读陷阱