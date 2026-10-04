package reconcile

import (
	"sync"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// selfWriteLog 记录引擎自己写入产生的 ResourceVersion（按 key 保留最近若干条）。
// 用途：Store 的变更通知无法区分"引擎自写"与"外部写入"，
// 而引擎自写（如记录失败状态）不应该取消刚刚排好的退避等待。
// watcher 收到通知时先查本日志：命中即消费并跳过，未命中才视为外部变更。
//
// ResourceVersion 全局单调递增、不复用，因此不会误伤外部写入。
type selfWriteLog struct {
	mu sync.Mutex
	m  map[object.Key][]int64
}

const selfWriteKeep = 8 // 每个 key 保留的最近自写版本数（有界，防泄漏）

func newSelfWriteLog() *selfWriteLog {
	return &selfWriteLog{m: map[object.Key][]int64{}}
}

// add 记录一次自写版本；超出保留窗口时丢弃最旧条目。
func (l *selfWriteLog) add(k object.Key, rv int64) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	s := append(l.m[k], rv)
	if len(s) > selfWriteKeep {
		s = s[len(s)-selfWriteKeep:]
	}
	l.m[k] = s
}

// consume 判断 rv 是否为已记录的自写版本；命中则移除并返回 true。
func (l *selfWriteLog) consume(k object.Key, rv int64) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.m[k]
	for i, v := range s {
		if v == rv {
			l.m[k] = append(s[:i], s[i+1:]...)
			if len(l.m[k]) == 0 {
				delete(l.m, k)
			}
			return true
		}
	}
	return false
}
