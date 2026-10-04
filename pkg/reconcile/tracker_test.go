package reconcile

import (
	"testing"
	"time"
)

// 退避档位与连续失败次数严格对应：第 N 次失败后等待 Backoff(base,max,N-1)，
// 逐次变长直到封顶；预算耗尽给出明确的 exhausted 信号。
func TestRetryTracker_BackoffGrowsWithFailureCount(t *testing.T) {
	base, max := 10*time.Millisecond, 50*time.Millisecond
	t0 := time.Now()
	tr := newRetryTracker(base, max, func() time.Time { return t0 })

	want := []time.Duration{10, 20, 40, 50, 50} // 毫秒，封顶后不再变长
	var prev time.Duration
	for i, w := range want {
		delay, exhausted := tr.recordFailure("k", 1, -1)
		if exhausted {
			t.Fatalf("failure %d: unexpected exhaustion with unlimited budget", i+1)
		}
		if delay != w*time.Millisecond {
			t.Fatalf("failure %d: want delay %v, got %v", i+1, w*time.Millisecond, delay)
		}
		if delay < prev {
			t.Fatalf("failure %d: delay must never shrink (%v < %v)", i+1, delay, prev)
		}
		prev = delay
		st, _ := tr.snapshot("k")
		if st.failures != i+1 {
			t.Fatalf("failure %d: consecutive count=%d", i+1, st.failures)
		}
		if st.lastDelay != delay {
			t.Fatalf("failure %d: tier %v does not match count", i+1, st.lastDelay)
		}
		if !st.nextRetryAt.Equal(t0.Add(delay)) {
			t.Fatalf("failure %d: nextRetryAt=%v want %v", i+1, st.nextRetryAt, t0.Add(delay))
		}
	}
}

// 预算耗尽：第 budget+1 次连续失败返回 exhausted，不再排定重试。
func TestRetryTracker_BudgetExhaustion(t *testing.T) {
	tr := newRetryTracker(time.Millisecond, 10*time.Millisecond, nil)
	for i := 1; i <= 3; i++ {
		if _, exhausted := tr.recordFailure("k", 1, 3); exhausted {
			t.Fatalf("failure %d within budget 3 must not exhaust", i)
		}
	}
	delay, exhausted := tr.recordFailure("k", 1, 3)
	if !exhausted {
		t.Fatal("4th consecutive failure must exhaust budget=3")
	}
	if delay != 0 {
		t.Fatalf("exhausted failure must not schedule a retry, delay=%v", delay)
	}
	st, _ := tr.snapshot("k")
	if !st.abandoned {
		t.Fatal("tracker must mark abandoned after exhaustion")
	}
	if !st.nextRetryAt.IsZero() {
		t.Fatal("exhausted object must have no pending retry")
	}
}

// 重新触发/重新排队（clearSchedule）只清排定时间，不回退失败计数与退避档位。
func TestRetryTracker_RetriggerDoesNotResetBackoff(t *testing.T) {
	base := 10 * time.Millisecond
	tr := newRetryTracker(base, 0, nil)
	tr.recordFailure("k", 1, -1) // 档位 base
	tr.recordFailure("k", 1, -1) // 档位 2*base

	tr.clearSchedule("k") // 模拟被新触发取代
	st, _ := tr.snapshot("k")
	if !st.nextRetryAt.IsZero() {
		t.Fatal("clearSchedule must clear pending retry time")
	}
	if st.failures != 2 {
		t.Fatalf("retrigger must not reset failure count, got %d", st.failures)
	}

	// 下一次失败继续变长（4*base），而不是回到最短等待 base。
	delay, _ := tr.recordFailure("k", 1, -1)
	if delay != 4*base {
		t.Fatalf("after retrigger, delay must keep growing: want %v, got %v", 4*base, delay)
	}
}

// 成功收敛与新一代 Spec 都会让计数归零、从最小等待重新开始。
func TestRetryTracker_ResetOnSuccessAndNewGeneration(t *testing.T) {
	base := 10 * time.Millisecond
	tr := newRetryTracker(base, 0, nil)
	tr.recordFailure("k", 1, -1)
	tr.recordFailure("k", 1, -1)

	tr.recordSuccess("k", 1)
	st, _ := tr.snapshot("k")
	if st.failures != 0 || st.lastDelay != 0 {
		t.Fatalf("success must reset backoff state: %+v", st)
	}
	delay, _ := tr.recordFailure("k", 1, -1)
	if delay != base {
		t.Fatalf("after success, first failure must wait minimum: got %v", delay)
	}

	// 新一代 Spec：计数归零。
	tr.recordFailure("k", 1, -1)
	delay, _ = tr.recordFailure("k", 2, -1)
	if delay != base {
		t.Fatalf("new generation must restart from minimum delay: got %v", delay)
	}
	st, _ = tr.snapshot("k")
	if st.failures != 1 || st.generation != 2 {
		t.Fatalf("new generation must reset count: %+v", st)
	}
}

// 显式重置：放弃状态与计数全部清除，重新从最小等待开始。
func TestRetryTracker_ExplicitReset(t *testing.T) {
	base := 10 * time.Millisecond
	tr := newRetryTracker(base, 0, nil)
	tr.recordFailure("k", 1, 1)
	if _, exhausted := tr.recordFailure("k", 1, 1); !exhausted {
		t.Fatal("budget=1 must exhaust on 2nd failure")
	}
	tr.reset("k")
	st, ok := tr.snapshot("k")
	if ok && (st.abandoned || st.failures != 0) {
		t.Fatalf("reset must clear abandoned state: %+v", st)
	}
	delay, exhausted := tr.recordFailure("k", 1, 1)
	if exhausted || delay != base {
		t.Fatalf("after reset, must restart from minimum delay: delay=%v exhausted=%v",
			delay, exhausted)
	}
}
