package turbolock

import (
	"context"
	"log"
	"sync"
	"time"
)

// ─── 时间轮常量 ───
const (
	tickMs = 100 * time.Millisecond // 基础 tick 间隔

	slotsL0 = 256 // Level 0 槽位数
	slotsL1 = 64  // Level 1 槽位数
	slotsL2 = 64  // Level 2 槽位数

	spanL0 = tickMs * slotsL0 // 25.6s
	spanL1 = spanL0 * slotsL1 // ~ 27.3 min
	spanL2 = spanL1 * slotsL2 // ~ 29.1 h
)

// ─── 时间轮任务节点（双向链表） ───

// timerTask 表示一个定期触发的续期任务。
// 同时作为双向链表的节点，挂在某个时间轮槽位中。
type timerTask struct {
	key        string                          // 锁的 Redis key
	callback   func(ctx context.Context) error // 执行 EXPIRE
	ticks      int                             // interval / tickMs，用于级联时重算槽位
	cancelled  bool                            // 标记任务已被删除
	prev, next *timerTask                      // 双向链表（同槽位内）
}

// ─── 层级时间轮 ───

// timingWheel 是一个三层层级时间轮，用于管理锁的自动续期。
//
//	Level 0: 256 槽, 100ms/tick,  覆盖 0 ~ 25.6s
//	Level 1:  64 槽, 25.6s/tick,  覆盖 0 ~ 27.3min
//	Level 2:  64 槽, 27.3min/tick, 覆盖 0 ~ 29.1h
//
//	续期间隔默认 ~2.6s（= Expiry/3），落在 Level 0 范围内。
//	Level 1/2 仅为极端配置兜底。
type timingWheel struct {
	// Level 0: 秒针层（主力）
	slots0  [slotsL0]*timerTask // 每个槽位是一个双向链表头
	cursor0 int

	// Level 1: 分针层
	slots1  [slotsL1]*timerTask // 每个槽位是一个双向链表头
	cursor1 int

	// Level 2: 时针层
	slots2  [slotsL2]*timerTask // 每个槽位是一个双向链表头
	cursor2 int

	mu     sync.Mutex    //
	stopCh chan struct{} //

	ctx    context.Context
	cancel context.CancelFunc
}

// ─── 构造 & 启停 ───

func newTimingWheel() *timingWheel {
	ctx, cancel := context.WithCancel(context.Background())
	return &timingWheel{
		stopCh: make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
}

// run 启动时间轮主循环。单 goroutine 运行，永不返回直到 Stop()。
func (tw *timingWheel) run() {
	ticker := time.NewTicker(tickMs)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.advance()
		case <-tw.stopCh:
			return
		}
	}
}

// Stop 优雅停止时间轮，释放 goroutine。
func (tw *timingWheel) Stop() {
	tw.cancel()
	close(tw.stopCh)
}

// ─── 公开 API ───

// addTask 向时间轮注册一个周期性续期任务。
// interval   续期间隔（如 Expiry/3）
// callback   续期时执行的回调
// 返回 *timerTask 作为句柄，用于后续 removeTask。

func (tw *timingWheel) addTask(key string, interval time.Duration, callback func(context.Context) error) *timerTask {
	task := &timerTask{
		key:      key,
		callback: callback,
		ticks:    int(interval / tickMs), // ← 关键：interval → tick 数
	}
	tw.mu.Lock()
	tw.insertTask(task)
	tw.mu.Unlock()

	return task
}

// removeTask 从时间轮中移除指定任务。
func (tw *timingWheel) removeTask(task *timerTask) {
	tw.mu.Lock()
	task.cancelled = true // 标记取消，fireSlot 遍历到时惰性摘除
	tw.mu.Unlock()
}

// ─── 内部：双向链表操作 ───

// insertHead 将 task 插入到槽位链表头部。
func insertHead(head **timerTask, task *timerTask) {
	task.prev = nil
	task.next = *head
	if *head != nil {
		(*head).prev = task
	}
	*head = task
}

// unlink 从链表中摘除 task。调用前需持有 tw.mu。
func (tw *timingWheel) unlink(task *timerTask) {
	if task.prev != nil {
		task.prev.next = task.next
	}
	if task.next != nil {
		task.next.prev = task.prev
	}
	// 清除指针，帮助GC
	task.prev = nil
	task.next = nil
}

// ─── 内部：任务调度 ───

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

// ─── 内部：指针推进 & 触发 ───

// advance 推进一格 tick：移动 Level 0 光标，触发到期任务，必要时级联。
func (tw *timingWheel) advance() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 1 - 推荐 Level 0 光标
	tw.cursor0 = (tw.cursor0 + 1) % slotsL0

	// 2 - 触发当前槽位的所有到期任务
	// 保存并清空，防止光标绕回时二次触发
	head := tw.slots0[tw.cursor0]
	tw.slots0[tw.cursor0] = nil
	tw.fireSlot(head)

	// 3 - level 0 走满一圈 -> 级联 Level 1
	if tw.cursor0 == 0 {
		tw.cascadeLevel1()
	}
}

// fireSlot 执行槽位中所有任务的回调，然后重新调度。
func (tw *timingWheel) fireSlot(head *timerTask) {
	task := head
	for task != nil {
		next := task.next // 先保存 next，回调可能修改链表
		tw.unlink(task)

		if task.cancelled {
			// 已取消：只摘除，不回调，不重新调度
			task = next
			continue
		}

		// 异步执行回调，避免阻塞时间轮推进
		go func(t *timerTask) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("turbolock: renew key=%s panic: %v", t.key, r)
				}
			}()
			if err := t.callback(tw.ctx); err != nil {
				log.Printf("turbolock: renew key=%s failed: %v", t.key, err)
				// 续期失败不重试，下次 tick 自然会再次触发；
				// 如果持续失败，Redis TTL 到期后锁自动释放（最终安全）。
			}
		}(task)

		// 重新调度下一次续期
		tw.insertTask(task)

		task = next
	}
}

// cascadeLevel1 将 Level 1 当前槽位的任务级联到 Level 0。
func (tw *timingWheel) cascadeLevel1() {
	tw.cursor1 = (tw.cursor1 + 1) % slotsL1

	// 将 Level 1 当前槽位的所有任务级联到 Level 0
	task := tw.slots1[tw.cursor1]
	tw.slots1[tw.cursor1] = nil //清空槽位
	for task != nil {
		next := task.next
		task.prev = nil
		task.next = nil

		// 重算 Level 0 槽位: 剩余 ticks = task.ticks % slotsL0
		remaining := task.ticks % slotsL0
		slot := (tw.cursor0 + remaining) % slotsL0
		insertHead(&tw.slots0[slot], task)

		task = next
	}

	// Level 1 走满一圈 → 级联 Level 2
	if tw.cursor1 == 0 {
		tw.cascadeLevel2()
	}
}

// cascadeLevel2 将 Level 2 当前槽位的任务级联到 Level 1。
func (tw *timingWheel) cascadeLevel2() {
	tw.cursor2 = (tw.cursor2 + 1) % slotsL2

	task := tw.slots2[tw.cursor2]
	tw.slots2[tw.cursor2] = nil
	for task != nil {
		next := task.next
		task.prev = nil
		task.next = nil

		// 重算 Level 1 槽位
		remaining := (task.ticks / slotsL0) % slotsL1
		slot := (tw.cursor1 + remaining) % slotsL1
		insertHead(&tw.slots1[slot], task)

		task = next
	}
}
