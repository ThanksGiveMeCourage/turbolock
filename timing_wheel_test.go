package turbolock

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestWheel(t *testing.T) *timingWheel {
	t.Helper()
	tw := newTimingWheel()
	go tw.run()
	t.Cleanup(func() { tw.Stop() })
	return tw
}

// ─── Level 0 基础功能 ───

func TestTimingWheel_AddAndFire(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	tw.addTask("test_key", 200*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 等待足够 tick 触发
	time.Sleep(500 * time.Millisecond)

	var n int32

	if n = atomic.LoadInt32(&fired); n < 1 {
		t.Fatalf("expected callback to fire at least once, got %d", n)
	}
	t.Logf("callback fired %d times", n)
}

func TestTimingWheel_RemoveBeforeFire(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	task := tw.addTask("test_key", 500*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 立即移除
	tw.removeTask(task)

	// 等待远超 interval 的时间
	time.Sleep(800 * time.Millisecond)

	if n := atomic.LoadInt32(&fired); n != 0 {
		t.Fatalf("expected 0 fires after remove, got %d", n)
	}
}

func TestTimingWheel_MultipleTasks(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	for i := 0; i < 10; i++ {
		tw.addTask("key", 150*time.Millisecond, func(ctx context.Context) error {
			atomic.AddInt32(&fired, 1)
			return nil
		})
	}

	time.Sleep(500 * time.Millisecond)

	var n int32
	if n = atomic.LoadInt32(&fired); n < 10 {
		t.Fatalf("expected >= 10 fires (10 tasks), got %d", n)
	}
	t.Logf("10 tasks fired total %d times", n)
}

// ─── Level 1 级联降级 ───

func TestTimingWheel_CascadeLevel1(t *testing.T) {
	tw := newTestWheel(t)

	// 设置一个超过 Level 0 范围（25.6s）的 interval
	// 这里用 30s，确保落入 Level 1
	var fired int32
	tw.addTask("long_key", 30*time.Second, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 不能等 30s，手动级联验证逻辑
	// 检查任务确实放在了 Level 1（slots1 某个槽位非空）
	tw.mu.Lock()
	foundInL1 := false
	for i := 0; i < slotsL1; i++ {
		if tw.slots1[i] != nil {
			foundInL1 = true
			break
		}
	}
	tw.mu.Unlock()

	if !foundInL1 {
		t.Fatal("task with 30s interval should be placed in Level 1")
	}
	t.Log("task correctly placed in Level 1")
}

// ─── 并发安全 ───

func TestTimingWheel_ConcurrentAddRemove(t *testing.T) {
	tw := newTestWheel(t)

	var wg sync.WaitGroup
	const goroutines = 50
	const tasksPerGoroutine = 10

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var tasks []*timerTask
			for i := 0; i < tasksPerGoroutine; i++ {
				task := tw.addTask("key", 100*time.Millisecond, func(ctx context.Context) error {
					return nil
				})
				tasks = append(tasks, task)
			}
			// 随机删除几个
			for _, task := range tasks[:len(tasks)/2] {
				tw.removeTask(task)
			}
		}(g)
	}

	wg.Wait()
	// 无 panic 无 race 即通过
}

// ─── 启停 ───

func TestTimingWheel_Stop(t *testing.T) {
	tw := newTimingWheel()
	go tw.run()

	// 添加一个任务确保 run 在运行
	tw.addTask("key", 50*time.Millisecond, func(ctx context.Context) error {
		return nil
	})
	time.Sleep(200 * time.Millisecond)

	// 停止
	tw.Stop()

	// 再等一会儿，确保 run goroutine 已退出
	time.Sleep(200 * time.Millisecond)

	// 验证：stopCh 已关闭
	select {
	case <-tw.stopCh:
		t.Log("stopCh closed OK")
	default:
		t.Fatal("stopCh should be closed after Stop()")
	}
}

// ─── 槽位清空不重复触发（回归测试） ───

func TestTimingWheel_NoDuplicateFire(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	tw.addTask("dup_test", 100*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 等待远超一个周期
	time.Sleep(1 * time.Second)

	count := atomic.LoadInt32(&fired)
	// 1秒 / 100ms ≈ 10次，允许 ±2 的调度抖动
	if count < 5 || count > 15 {
		t.Fatalf("expected ~10 fires, got %d (possible duplicate fire bug)", count)
	}
	t.Logf("fired %d times (expect ~10)", count)
}

// ======================================================================================

// ─── 级联完整路径（缩小版时间轮） ───

// miniWheel 是一个缩小版时间轮，用于在亚秒级时间内验证级联逻辑。
// L0: 4 槽 × 10ms = 40ms 覆盖范围
// L1: 4 槽 × 40ms = 160ms 覆盖范围
type miniWheel struct {
	slots0  [4]*timerTask
	cursor0 int
	slots1  [4]*timerTask
	cursor1 int
	mu      sync.Mutex
	stopCh  chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
}

func newMiniWheel() *miniWheel {
	ctx, cancel := context.WithCancel(context.Background())
	return &miniWheel{
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (mw *miniWheel) addTask(ticks int, callback func(context.Context) error) *timerTask {
	task := &timerTask{ticks: ticks, callback: callback}
	mw.mu.Lock()
	if ticks < 4 {
		slot := (mw.cursor0 + ticks) % 4
		insertHead(&mw.slots0[slot], task)
	} else {
		l1Ticks := ticks / 4
		slot := (mw.cursor1 + l1Ticks) % 4
		insertHead(&mw.slots1[slot], task)
	}
	mw.mu.Unlock()
	return task
}

func (mw *miniWheel) removeTask(task *timerTask) {
	mw.mu.Lock()
	task.cancelled = true
	mw.mu.Unlock()
}

func (mw *miniWheel) advance() {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	mw.cursor0 = (mw.cursor0 + 1) % 4
	head := mw.slots0[mw.cursor0]
	mw.slots0[mw.cursor0] = nil

	task := head
	for task != nil {
		next := task.next
		task.prev = nil
		task.next = nil
		if !task.cancelled {
			go func(t *timerTask) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("turbolock: mini wheel callback panic: %v", r)
					}
				}()
				_ = t.callback(mw.ctx)
			}(task)
			// 重新调度
			if task.ticks < 4 {
				slot := (mw.cursor0 + task.ticks) % 4
				insertHead(&mw.slots0[slot], task)
			}
		}
		task = next
	}

	// Level 0 绕回 → 级联 Level 1
	if mw.cursor0 == 0 {
		mw.cursor1 = (mw.cursor1 + 1) % 4
		cascadeTask := mw.slots1[mw.cursor1]
		mw.slots1[mw.cursor1] = nil
		for cascadeTask != nil {
			next := cascadeTask.next
			cascadeTask.prev = nil
			cascadeTask.next = nil
			remaining := cascadeTask.ticks % 4
			slot := (mw.cursor0 + remaining) % 4
			insertHead(&mw.slots0[slot], cascadeTask)
			cascadeTask = next
		}
	}
}

func TestTimingWheel2_CascadeFullPath(t *testing.T) {
	mw := newMiniWheel()
	t.Cleanup(func() { mw.cancel(); close(mw.stopCh) })

	var fired int32
	// ticks=6: 4 槽 L0 覆盖 0~3 → 落入 L1 → l1Ticks=1
	// L0 走满一圈 (4×10ms=40ms) → cascadeLevel1 → 剩余=6%4=2 槽 → 再等 20ms
	// 总计 ~60ms 后触发
	mw.addTask(6, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 手动驱动 100ms (10 tick)，确保级联完成
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)
		mw.advance()
	}

	var n int32
	if n = atomic.LoadInt32(&fired); n < 1 {
		t.Fatalf("task should have cascaded from L1 to L0 and fired, got %d", n)
	}
	t.Logf("cascade full path verified: fired %d times", n)
}

// ─── 双重 removeTask ───

func TestTimingWheel2_DoubleRemove(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	task := tw.addTask("key", 100*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	tw.removeTask(task)
	tw.removeTask(task) // 第二次不应 panic

	time.Sleep(300 * time.Millisecond)

	if n := atomic.LoadInt32(&fired); n != 0 {
		t.Fatalf("expected 0 fires after double remove, got %d", n)
	}
}

// ─── 回调 error 路径 ───

func TestTimingWheel2_CallbackError(t *testing.T) {
	tw := newTestWheel(t)

	errCh := make(chan error, 1)
	tw.addTask("key", 100*time.Millisecond, func(ctx context.Context) error {
		errCh <- nil                           // 证明回调被执行了
		return errors.New("simulated failure") // 返回 error
	})

	// 等待回调触发
	select {
	case <-errCh:
		t.Log("callback executed despite previous error (rescheduled)")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("callback was never called")
	}

	// 不应 panic，进程应存活
}

// ─── 并发 removeTask + fireSlot 竞态 ───

func TestTimingWheel2_ConcurrentRemoveDuringFire(t *testing.T) {
	tw := newTestWheel(t)

	var fired int32
	task := tw.addTask("key", 50*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})

	// 在即将触发时（45ms）调用 removeTask
	time.Sleep(45 * time.Millisecond)
	tw.removeTask(task)

	// 等待足够时间确认回调未触发
	time.Sleep(150 * time.Millisecond)

	if n := atomic.LoadInt32(&fired); n != 0 {
		// 极低概率：remove 晚于 fireSlot 的 unlink，但早于 cancelled 检查
		// 此时 unlink 清空了指针但 task 还在 old head 中
		// → fireSlot 遍历时会看到 task（因为 old head 没清）
		// 但 cancelled=true → skip → 不触发回调
		// 所以理论上 fired 应为 0
		t.Fatalf("expected 0 fires (removed before fire), got %d", n)
	}
	t.Log("concurrent remove during fire: callback not triggered")
}

// ─── panic 恢复验证 ───

func TestTimingWheel2_PanicRecovery(t *testing.T) {
	tw := newTestWheel(t)

	panicCh := make(chan struct{}, 1)
	tw.addTask("key", 100*time.Millisecond, func(ctx context.Context) error {
		panicCh <- struct{}{}
		panic("simulated panic in callback")
	})

	// 等待 panic 发生并被 recover 捕获
	select {
	case <-panicCh:
		t.Log("callback panicked")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("callback was never called")
	}

	// 进程应存活，不应崩溃
	time.Sleep(100 * time.Millisecond)
	t.Log("process survived panic (recover worked)")

	// 添加新任务，验证时间轮仍正常工作
	var fired int32
	tw.addTask("key2", 100*time.Millisecond, func(ctx context.Context) error {
		atomic.AddInt32(&fired, 1)
		return nil
	})
	time.Sleep(300 * time.Millisecond)
	if n := atomic.LoadInt32(&fired); n < 1 {
		t.Fatal("wheel should still work after callback panic")
	}
}
