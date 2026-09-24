package reconcile

import (
	"errors"
	"fmt"
)

// ErrorClass 是失败分类。
type ErrorClass string

const (
	// ClassTransient 可恢复失败：按退避重试，并保留上次成功写入的状态。
	ClassTransient ErrorClass = "Transient"
	// ClassTerminal 不可恢复的配置错误：终止重试并写入可区分的失败原因。
	ClassTerminal ErrorClass = "Terminal"
	// ClassConflict 协调基于过期版本：以最新版本重新判断，不产生副作用。
	ClassConflict ErrorClass = "Conflict"
)

// ClassifiedError 携带失败分类的协调错误。
type ClassifiedError struct {
	Class ErrorClass
	Msg   string
}

func (e *ClassifiedError) Error() string {
	return string(e.Class) + ": " + e.Msg
}

// IsClass 判断 err 是否属于给定分类（支持被 %w 包装）。
func IsClass(err error, class ErrorClass) bool {
	var ce *ClassifiedError
	if errors.As(err, &ce) {
		return ce.Class == class
	}
	return false
}

func transientf(format string, args ...any) error {
	return &ClassifiedError{Class: ClassTransient, Msg: fmt.Sprintf(format, args...)}
}

func terminalf(format string, args ...any) error {
	return &ClassifiedError{Class: ClassTerminal, Msg: fmt.Sprintf(format, args...)}
}

// errConflict 是版本冲突的共享错误实例。
var errConflict = &ClassifiedError{Class: ClassConflict, Msg: "resource version conflict, retry with latest object"}
