// Package store 提供版本化的内存对象存储。
// 所有写入都走乐观并发控制（CAS）：调用方必须基于观测到的
// ResourceVersion 提交，若版本已过期则返回 ErrConflict，
// 协调层据此以最新版本重新判定，绝不用过期快照覆盖新状态。
package store

import (
	"errors"
	"sync"

	"github.com/lisama569/reconcile-engine-go/pkg/object"
)

// ErrConflict 表示提交时对象版本已落后于存储中的最新版本。
var ErrConflict = errors.New("object resource version conflict")

// ErrNotFound 表示对象不存在。
var ErrNotFound = errors.New("object not found")

// Store 是并发安全的对象存储。
type Store struct {
	mu     sync.RWMutex
	data   map[object.Key]*object.Object
	nextRV int64
	notify chan object.Key
}

// New 创建空存储，可选灌入初始对象。
// 种子对象的 ResourceVersion 从 1 开始归一化。
func New(seed ...*object.Object) *Store {
	s := &Store{
		data:   make(map[object.Key]*object.Object),
		nextRV: 1,
		// 缓冲区较大：变更突发时不阻塞写入方；消费者是引擎的 watcher。
		notify: make(chan object.Key, 1024),
	}
	for _, o := range seed {
		obj := o.DeepCopy()
		obj.ResourceVersion = s.nextRV
		s.nextRV++
		s.data[obj.Key] = obj
	}
	return s
}

// Get 读取最新版本；不存在返回 ErrNotFound。
func (s *Store) Get(k object.Key) (*object.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.data[k]
	if !ok {
		return nil, ErrNotFound
	}
	return o.DeepCopy(), nil
}

// List 返回所有对象的快照副本。
func (s *Store) List() []*object.Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*object.Object, 0, len(s.data))
	for _, o := range s.data {
		out = append(out, o.DeepCopy())
	}
	return out
}

// ListChildren 返回某所有者的全部派生对象。
func (s *Store) ListChildren(owner object.Key) []*object.Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*object.Object
	for _, o := range s.data {
		if o.Owner == owner {
			out = append(out, o.DeepCopy())
		}
	}
	return out
}

// Create 新建对象；同键已存在返回 ErrConflict。
func (s *Store) Create(obj *object.Object) (*object.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[obj.Key]; ok {
		return nil, ErrConflict
	}
	stored := obj.DeepCopy()
	stored.ResourceVersion = s.nextRV
	s.nextRV++
	s.data[stored.Key] = stored
	s.signal(stored.Key)
	return stored.DeepCopy(), nil
}

// CAS 基于期望版本更新：
//   - expectRV 为 -1 表示无条件创建/覆盖（仅测试/引导使用）；
//   - 否则仅当存储中当前版本 == expectRV 时才提交，新版本自动 +1。
//
// 返回提交后的最新对象；版本不符返回 ErrConflict，存储内容不被改动。
func (s *Store) CAS(obj *object.Object, expectRV int64) (*object.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.data[obj.Key]
	if expectRV != -1 {
		if !ok || cur.ResourceVersion != expectRV {
			return nil, ErrConflict
		}
	}
	stored := obj.DeepCopy()
	stored.ResourceVersion = s.nextRV
	s.nextRV++
	s.data[stored.Key] = stored
	s.signal(stored.Key)
	return stored.DeepCopy(), nil
}

// Delete 物理移除（仅用于级联清理完成后的 child 回收）。
// 同样走版本校验；expectRV == -1 表示无条件删除。
func (s *Store) Delete(k object.Key, expectRV int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.data[k]
	if !ok {
		return ErrNotFound
	}
	if expectRV != -1 && cur.ResourceVersion != expectRV {
		return ErrConflict
	}
	delete(s.data, k)
	s.signal(k)
	return nil
}

// Version 返回当前版本号（不存在为 0），便于测试断言。
func (s *Store) Version(k object.Key) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if o, ok := s.data[k]; ok {
		return o.ResourceVersion
	}
	return 0
}

// Notify 返回版本变更事件通道（元素为被改动对象的 Key）。
func (s *Store) Notify() <-chan object.Key { return s.notify }

// signal 非阻塞地发出变更通知；通道满则丢弃（消费者做了定时重扫兜底，
// 且写入路径通常伴随显式 Trigger，丢弃不影响正确性）。
func (s *Store) signal(k object.Key) {
	select {
	case s.notify <- k:
	default:
	}
}
