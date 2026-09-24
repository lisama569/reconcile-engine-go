package reconcile

import (
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

func TestWorkQueueDedupSameKey(t *testing.T) {
	q := NewWorkQueue()
	q.Add("a")
	q.Add("a")
	q.Add("a")
	if q.Len() != 1 {
		t.Fatalf("same key must fold into one queued item, Len=%d", q.Len())
	}
	k, attempt, ok := q.Get()
	if !ok || k != "a" || attempt != 1 {
		t.Fatalf("Get want a/1, got %s/%d/%v", k, attempt, ok)
	}
	// 处理期间重复加入：折叠为 dirty，且不进入排队计数。
	q.Add("a")
	q.Add("a")
	if q.Len() != 0 {
		t.Fatalf("add during processing must not create queued item, Len=%d", q.Len())
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		q.Done("a")
	}()
	k2, a2, ok2 := q.Get()
	if !ok2 || k2 != "a" || a2 != 1 {
		t.Fatalf("dirty requeue after Done want a/1(fresh), got %s/%d/%v", k2, a2, ok2)
	}
	q.Done("a")
}

func TestWorkQueueDelayed(t *testing.T) {
	q := NewWorkQueue()
	start := time.Now()
	q.AddAfter("late", 80*time.Millisecond)
	q.Add("now")

	k1, _, ok := q.Get()
	if !ok || k1 != "now" {
		t.Fatalf("want immediate 'now', got %s/%v", k1, ok)
	}
	q.Done("now")
	k2, _, ok := q.Get()
	if !ok || k2 != "late" {
		t.Fatalf("want delayed 'late', got %s/%v", k2, ok)
	}
	if elapsed := time.Since(start); elapsed < 60*time.Millisecond {
		t.Fatalf("delayed item taken too early after %v", elapsed)
	}
	q.Done("late")
	q.Stop()
}

func TestWorkQueueNoCrossConcurrency(t *testing.T) {
	// 并发狂打同一个键，并真正并发消费，断言任意时刻该键最多 1 个在途。
	q := NewWorkQueue()
	const producers = 8
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				q.Add(object.Key("hot"))
			}
		}()
	}

	var mu sync.Mutex
	current := 0
	maxConcurrent := 0
	var cwg sync.WaitGroup
	for w := 0; w < 4; w++ {
		cwg.Add(1)
		go func() {
			defer cwg.Done()
			for {
				k, _, ok := q.Get()
				if !ok {
					return
				}
				mu.Lock()
				current++
				if current > maxConcurrent {
					maxConcurrent = current
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				current--
				mu.Unlock()
				q.Done(k)
			}
		}()
	}
	wg.Wait()
	// 等待消费排空。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if q.Len() == 0 && q.Inflight() == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	q.Stop()
	cwg.Wait()
	if maxConcurrent > 1 {
		t.Fatalf("same key was processed concurrently: max=%d", maxConcurrent)
	}
}

func TestBackoff(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 10 * time.Millisecond},
		{1, 20 * time.Millisecond},
		{2, 40 * time.Millisecond},
		{3, 80 * time.Millisecond},
		{4, 100 * time.Millisecond}, // capped
		{10, 100 * time.Millisecond},
	}
	for _, c := range cases {
		if got := Backoff(10*time.Millisecond, 100*time.Millisecond, c.attempt); got != c.want {
			t.Errorf("attempt %d: want %v got %v", c.attempt, c.want, got)
		}
	}
}

func TestClassify(t *testing.T) {
	if _, ok := Classify(nil); ok {
		t.Fatal("nil should not be retriable")
	}
	if r, ok := Classify(InvalidConfig("bad")); r != ReasonConfig || ok {
		t.Fatalf("InvalidConfig want Config/non-retryable, got %s/%v", r, ok)
	}
	if r, ok := Classify(Transient(errBoom)); r != ReasonTransient || !ok {
		t.Fatalf("Transient want Transient/retryable, got %s/%v", r, ok)
	}
}
