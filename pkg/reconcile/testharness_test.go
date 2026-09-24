package reconcile

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// tHandler 把 slog 记录转成 t.Log，单测运行（-v）即可看到
// 每条协调的"输入 / 观测版本 / 判定依据"。
type tHandler struct {
	t     testing.TB
	attrs []slog.Attr
	mu    *sync.Mutex
}

func newTestLogger(t testing.TB) *slog.Logger {
	h := &tHandler{t: t, mu: &sync.Mutex{}}
	return slog.New(h)
}

func (h *tHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *tHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	line := r.Time.Format("15:04:05.000") + " " + r.Message
	for _, a := range h.attrs {
		line += " " + a.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		line += " " + a.String()
		return true
	})
	h.t.Log(line)
	return nil
}

func (h *tHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	nh.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &nh
}

func (h *tHandler) WithGroup(_ string) slog.Handler { return h }

// fixedClock 是测试用可控时钟。
type fixedClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fixedClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// harness 封装一次测试用到的 store / applier / engine。
type harness struct {
	t       *testing.T
	st      *store.Store
	counter *CountingApplier
	eng     *Engine
	log     *slog.Logger
}

func newHarness(t *testing.T, inner Applier, seed ...*object.Object) *harness {
	t.Helper()
	st := store.New(seed...)
	counter := NewCountingApplier(inner)
	log := newTestLogger(t)
	eng := New(Config{
		Store:     st,
		Applier:   counter,
		Logger:    log,
		Workers:   4,
		BaseDelay: 5 * time.Millisecond,
		MaxDelay:  40 * time.Millisecond,
		Clock:     SystemClock{},
	})
	return &harness{t: t, st: st, counter: counter, eng: eng, log: log}
}

func (h *harness) start() {
	h.eng.Start(context.Background())
	h.t.Cleanup(h.eng.Stop)
}

func (h *harness) waitIdle() {
	h.t.Helper()
	if !h.eng.WaitIdle(5 * time.Second) {
		h.t.Fatalf("engine did not become idle: queued=%d inflight=%d",
			h.eng.queue.Len(), h.eng.queue.Inflight())
	}
}

func (h *harness) get(k object.Key) *object.Object {
	h.t.Helper()
	o, err := h.st.Get(k)
	if err != nil {
		h.t.Fatalf("get %s: %v", k, err)
	}
	return o
}

func (h *harness) pollFor(d time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fn()
}
