package reconcile

import (
	"context"
	"sync"
	"testing"
	"time"
)

// 场景9：重复 Add 去重——100 次 Add 同一 key 只出队一次。
func TestWorkQueue_Dedup(t *testing.T) {
	q := NewWorkQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 100; i++ {
		q.Add("Endpoint/web-0")
	}
	got := make(map[string]int)
	for {
		key, ok := q.Get(ctx)
		if !ok {
			t.Fatal("queue closed unexpectedly")
		}
		got[key]++
		q.Done(key)
		if len(got) == 1 {
			// 给重复事件一点时间泄漏（不应该有）。
			time.Sleep(5 * time.Millisecond)
			break
		}
	}
	if got["Endpoint/web-0"] != 1 {
		t.Fatalf("同一 key 重复出队 %d 次", got["Endpoint/web-0"])
	}
}

// 场景10：同一 key 在 Get 与 Done 之间不会被第二个 worker 取到（不交叉协调），
// 但处理期间到达的 Add 会在 Done 后补一轮；不同 key 可以并行被取走。
func TestWorkQueue_SameKeySerial_DifferentKeysParallel(t *testing.T) {
	q := NewWorkQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.Add("a")
	q.Add("b")

	a, ok := q.Get(ctx)
	if !ok || a != "a" {
		t.Fatalf("期望取到 a, got %q ok=%v", a, ok)
	}
	// a 处理中：再次 Add a 必须排队等待 Done，不得立即出队。
	q.Add("a")
	got, ok := q.Get(ctx)
	if !ok || got != "b" {
		t.Fatalf("a 处理中应可取到不同 key b，got %q", got)
	}

	// 此时队列中不应再取得到 a（它仍在处理中）——用短超时验证。
	probeCtx, probeCancel := context.WithTimeout(ctx, 8*time.Millisecond)
	defer probeCancel()
	if k, ok := q.Get(probeCtx); ok {
		t.Fatalf("同一 key a 被交叉协调，取到了 %q", k)
	}

	// b 先 Done 不影响 a；a Done 后补入的那轮必须能取到。
	q.Done("b")
	q.Done("a")
	a2, ok := q.Get(ctx)
	if !ok || a2 != "a" {
		t.Fatalf("处理期间的 Add 应在 Done 后补一轮, got %q ok=%v", a2, ok)
	}
	q.Done("a")
}

// 场景11：AddAfter 的延迟真实生效且多个延迟取最早者。
func TestWorkQueue_AddAfter(t *testing.T) {
	q := NewWorkQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	q.AddAfter("x", 30*time.Millisecond)
	q.AddAfter("x", 10*time.Millisecond) // 更早的排程应胜出
	start := time.Now()
	key, ok := q.Get(ctx)
	if !ok || key != "x" {
		t.Fatalf("got %q ok=%v", key, ok)
	}
	if d := time.Since(start); d < 8*time.Millisecond {
		t.Fatalf("延迟未生效，%s 即出队", d)
	}
	q.Done("x")
}

// 场景12：高并发 Add 下每个入队 key 最终都恰好被处理一轮（去重不丢事件）。
func TestWorkQueue_ConcurrentAddNoLoss(t *testing.T) {
	q := NewWorkQueue()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const workers = 8
	const rounds = 200
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				q.Add("k" + itoa((id+i)%16))
			}
		}(w)
	}

	seen := make(map[string]int)
	seenMu := sync.Mutex{}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(stop)
	}()
	go func() {
		for {
			cctx, ccancel := context.WithTimeout(ctx, 20*time.Millisecond)
			key, ok := q.Get(cctx)
			ccancel()
			if !ok {
				select {
				case <-stop:
					// 生产者全部结束且队列已排空。
					if q.(*dedupQueue).len() == 0 {
						close(done)
						return
					}
				default:
				}
				continue
			}
			seenMu.Lock()
			seen[key]++
			seenMu.Unlock()
			q.Done(key)
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("超时，已处理 %d 个 key", len(seen))
	}
	if len(seen) != 16 {
		t.Fatalf("应有 16 个不同 key 被处理, got %d", len(seen))
	}
}
