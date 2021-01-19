package tierTimewheel

import "time"

type tasker interface {
	callbackHandler()
	getTimerId() uint64
	getDelay() time.Duration
}

// Task 延时任务
// delay 执行时间，unix纳秒时间戳
// timerId 定时器唯一标识, 用于删除定时器
// callback 回调函数
type task struct {
	delay    time.Duration
	timerId  uint64
	callback func()
}

// callbackHandler 执行回调函数
func (t *task) callbackHandler() {
	if t.callback == nil {
		return
	}
	t.callback()
}

func (t *task) getTimerId() uint64      { return t.timerId }
func (t *task) getDelay() time.Duration { return t.delay }
