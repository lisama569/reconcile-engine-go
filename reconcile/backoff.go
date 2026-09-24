package reconcile

// Backoff 为单个对象的连续可恢复失败计算退避时长。
// 成功（或对象已收敛）后必须由调用方 Reset。
type Backoff interface {
	// Next 返回第 failureCount 次连续失败后的等待时长（failureCount 从 1 开始）。
	Next(failureCount int) Duration
	Reset()
}

// ExponentialBackoff 是确定性指数退避（无抖动，保证测试可复现）：
// delay = min(Base * 2^(n-1), Max)。
type ExponentialBackoff struct {
	Base Duration
	Max  Duration
}

func (b ExponentialBackoff) Next(failureCount int) Duration {
	if failureCount < 1 {
		failureCount = 1
	}
	d := b.Base
	for i := 1; i < failureCount; i++ {
		d *= 2
		if d >= b.Max {
			return b.Max
		}
	}
	if d > b.Max {
		return b.Max
	}
	return d
}

func (b ExponentialBackoff) Reset() {}
