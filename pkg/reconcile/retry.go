package reconcile

import (
	"errors"
	"time"

	"github.com/lisama569/reconcile-engine-go/pkg/store"
)

// FailureReason 是可区分的失败原因类别。
type FailureReason string

const (
	// ReasonNone 无失败。
	ReasonNone FailureReason = ""
	// ReasonTransient 可恢复失败：外部系统暂时不可用，按退避重试。
	ReasonTransient FailureReason = "Transient"
	// ReasonConfig 不可恢复失败：期望配置非法，终止重试。
	ReasonConfig FailureReason = "InvalidConfig"
	// ReasonConflict 协调期间版本被他人改写：立即用最新版本重新判定。
	ReasonConflict FailureReason = "Conflict"
)

// ErrInvalidConfig 是不可恢复配置错误的哨兵错误，可用 errors.Is 判断。
var ErrInvalidConfig = errors.New("invalid desired-state configuration")

// ClassifiedError 携带失败分类。
type ClassifiedError struct {
	Reason FailureReason
	Err    error
}

func (e *ClassifiedError) Error() string {
	if e.Err == nil {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Err.Error()
}

// Unwrap 支持 errors.As / errors.Is 穿透。
func (e *ClassifiedError) Unwrap() error { return e.Err }

// Transient 把一个错误包装为可恢复失败。
func Transient(err error) error {
	return &ClassifiedError{Reason: ReasonTransient, Err: err}
}

// InvalidConfig 构造不可恢复的配置失败。
func InvalidConfig(detail string) error {
	return &ClassifiedError{Reason: ReasonConfig, Err: errors.New(detail)}
}

// ConflictFailure 包装一次乐观锁冲突。
func ConflictFailure() error {
	return &ClassifiedError{Reason: ReasonConflict, Err: store.ErrConflict}
}

// Classify 把底层错误归类，返回原因与是否可重试。
// 分类规则（优先级从高到低）：
//  1. nil                          -> (None, false)
//  2. 包装了 store.ErrConflict     -> (Conflict, true)，立即重判
//  3. ClassifiedError              -> 按其标注归类
//  4. errors.Is ErrInvalidConfig   -> (Config, false)，终止
//  5. 其余未知错误                  -> (Transient, true)，保守重试
func Classify(err error) (FailureReason, bool) {
	if err == nil {
		return ReasonNone, false
	}
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.Reason, IsRetryable(ce.Reason)
	}
	if errors.Is(err, store.ErrConflict) {
		return ReasonConflict, true
	}
	if errors.Is(err, ErrInvalidConfig) {
		return ReasonConfig, false
	}
	return ReasonTransient, true
}

// IsRetryable 判断失败原因是否应继续重试。
// Conflict 立即重试；Transient 退避重试；Config 终止。
func IsRetryable(r FailureReason) bool {
	switch r {
	case ReasonTransient, ReasonConflict:
		return true
	default:
		return false
	}
}

// Backoff 计算指数退避：base * 2^attempt，上限 cap。
// 为保证测试可复现，不引入随机抖动。
// attempt <= 0 时返回 base；cap <= 0 表示不封顶。
func Backoff(base time.Duration, cap time.Duration, attempt int) time.Duration {
	if base < 0 {
		base = 0
	}
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if cap > 0 && d >= cap {
			return cap
		}
		// 防御溢出：一旦位移后异常变小，直接封顶。
		if d < 0 {
			if cap > 0 {
				return cap
			}
			return base
		}
	}
	if cap > 0 && d > cap {
		d = cap
	}
	return d
}
