package tierTimewheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// TimeWheel 时间轮
// interval 指针每隔多久往前移动一格
// ticker 时间轮计时器
// slots 存放各层级时间轮插槽
// timerId 定时器自增id
// timer key:定时器自增id value:定时器链表指针，用于删除定时器
// currentPos 存放各层级时间轮当前时间指针
// tier 时间轮层级，长度为分级级数，下标值为该级插槽数
// stopChan 停止定时器channel
// startOnce 定时器启动锁
// closeOnce 定时器关闭锁
// workerChan 工作通道
// workerNum 启动几个routine来处理指针转动
// maxTimerList 存放超出分层时间轮时间范围的计时器
type TimeWheel struct {
	interval     time.Duration
	ticker       *time.Ticker
	slots        [][]*list.List
	timerId      uint64
	timer        sync.Map
	currentPos   []int
	tier         []int
	stopChan     chan struct{}
	startOnce    sync.Once
	closeOnce    sync.Once
	workerChan   chan func()
	workerNum    int
	maxTimerList *list.List
}

// timerElement
// timerList 定时器所在链表
// listElement 链表元素
type timerElement struct {
	timerList   *list.List
	listElement *list.Element
}

// New 创建时间轮
// interval 指针转动间隔时间
// tier 时间轮层级，值为该层级插槽数
// workerNum 启动几个routine来处理指针转动
func New(interval time.Duration, tier []int, workerNum int) *TimeWheel {
	if interval < 1 || len(tier) < 1 || workerNum < 1 {
		return nil
	}
	tw := &TimeWheel{
		interval:     interval,
		slots:        make([][]*list.List, len(tier)),
		timerId:      0,
		timer:        sync.Map{},
		currentPos:   make([]int, len(tier)),
		tier:         tier,
		stopChan:     make(chan struct{}),
		workerChan:   make(chan func()),
		workerNum:    workerNum,
		maxTimerList: &list.List{},
	}
	tw.initSlots()
	return tw
}

// initSlots 初始化槽，每个槽指向一个链表
func (t *TimeWheel) initSlots() {
	tierLen := len(t.tier)
	for i := 0; i < tierLen; i++ {
		tmpList := make([]*list.List, t.tier[i])
		pos := t.tier[i]
		for j := 0; j < pos; j++ {
			tmpList[j] = &list.List{}
		}
		t.slots[i] = tmpList
	}
}

// Start 启动时间轮
func (t *TimeWheel) Start() {
	t.startOnce.Do(func() {
		for i := 0; i < t.workerNum; i++ {
			go t.wokerRoutine()
		}
		go t.start()
		go t.maxTaskHandle()
	})
}

// Stop 停止时间轮
func (t *TimeWheel) Stop() {
	t.closeOnce.Do(func() {
		close(t.stopChan)
	})
}

// wokerRoutine 工作routine
func (t *TimeWheel) wokerRoutine() {
	var execFunc func()
	for {
		select {
		case <-t.stopChan:
			return
		case execFunc = <-t.workerChan:
			execFunc()
		}
	}
}

// start 启动时间轮
func (t *TimeWheel) start() {
	t.ticker = time.NewTicker(t.interval)
	for {
		select {
		case <-t.stopChan:
			t.ticker.Stop()
			return
		case <-t.ticker.C:
			t.tickHandler()
		}
	}
}

// tickHandler 时间轮指针转动处理
func (t *TimeWheel) tickHandler() {
	list := t.slots[0][t.currentPos[0]]
	if t.currentPos[0] == t.tier[0]-1 {
		t.currentPos[0] = 0
		t.workerChan <- func() {
			t.scanTask(list)
			t.tierHandler(1)
		}
	} else {
		t.currentPos[0]++
		t.workerChan <- func() {
			t.scanTask(list)
		}
	}
	//插槽数大于10时,指针转到倒数第5个预处理第二层级
	if t.tier[0] > 10 && t.currentPos[0] == t.tier[0]-5 && len(t.tier) > 1 {
		t.prestrainList(t.slots[1][t.currentPos[1]])
	}
}

// prestrain 预处理
func (t *TimeWheel) prestrainList(taskList *list.List) {
	var (
		task        tasker
		currentTime = time.Duration(time.Now().UnixNano()) + (t.interval * 6)
	)
	if taskList.Front() == nil {
		return
	}
	_, ok := taskList.Front().Value.(tasker)
	if !ok {
		return
	}
	for e := taskList.Front(); e != nil; {
		task = e.Value.(tasker)
		e = t.prestrainAssign(taskList, e, task, currentTime)
	}
}

// prestrainAssign 预处理分配
func (t *TimeWheel) prestrainAssign(taskList *list.List, e *list.Element, task tasker, currentTime time.Duration) *list.Element {
	delay := task.getDelay()
	delayTime := int(delay - currentTime)
	timerId := task.getTimerId()
	var (
		next        *list.Element
		pos         int
		listElement *list.Element
	)
	if delayTime < 1 {
		return e.Next()
	}
	interval := int(t.interval)
	pos = 0 + delayTime/interval
	if pos > t.tier[0]-6 {
		return e.Next()
	}
	next = e.Next()
	taskList.Remove(e)
	listElement = t.slots[0][pos].PushBack(e.Value)
	t.timer.Store(timerId, timerElement{
		timerList:   t.slots[0][pos],
		listElement: listElement,
	})
	return next
}

// tierHandler 递归处理分级
// tier 层级
func (t *TimeWheel) tierHandler(tier int) {
	if tier == len(t.tier) {
		return
	}
	taskList := t.slots[tier][t.currentPos[tier]]
	if t.currentPos[tier] == len(t.slots[tier])-1 {
		t.currentPos[tier] = 0
		t.tierListHandler(taskList)
		tier++
		t.tierHandler(tier)
	} else {
		t.currentPos[tier]++
		t.tierListHandler(taskList)
	}
}

// tierHandler 处理分级链表
func (t *TimeWheel) tierListHandler(taskList *list.List) {
	var (
		task        tasker
		currentTime = time.Duration(time.Now().UnixNano())
	)
	if taskList.Front() == nil {
		return
	}
	_, ok := taskList.Front().Value.(tasker)
	if !ok {
		return
	}
	for e := taskList.Front(); e != nil; {
		task = e.Value.(tasker)
		e = t.timerAssign(taskList, e, task, currentTime)
	}
}

// timerAssign 计时器分配
func (t *TimeWheel) timerAssign(taskList *list.List, e *list.Element, task tasker, currentTime time.Duration) *list.Element {
	delay := task.getDelay()
	delayTime := int(delay - currentTime)
	timerId := task.getTimerId()
	var (
		next        *list.Element
		pos         int
		listElement *list.Element
	)
	// 延迟时间小于间隔时间的一半直接执行
	if delayTime < int(t.interval/2) {
		return t.callbackHandler(taskList, e, task)
	}
	// 每个槽间隔时间，等于底级时间轮一圈总时间
	interval := int(t.interval)
	tierLen := len(t.tier)
	for i := 0; i < tierLen; i++ {
		// 当前层级时间轮剩余时间
		timeRemain := (t.tier[i] - t.currentPos[i]) * interval
		if delayTime < timeRemain {
			pos = ((t.currentPos[i] + delayTime/interval) % t.tier[i]) - 1
			next = e.Next()
			taskList.Remove(e)
			listElement = t.slots[i][pos].PushBack(e.Value)
			t.timer.Store(timerId, timerElement{
				timerList:   t.slots[i][pos],
				listElement: listElement,
			})
			return next
		}
		interval = t.tier[i] * interval
	}
	// 没有层级满足存放要求，放在当前指针位置，重新处理
	pos = t.currentPos[0]
	next = e.Next()
	taskList.Remove(e)
	listElement = t.slots[0][pos].PushBack(e.Value)
	t.timer.Store(timerId, timerElement{
		timerList:   t.slots[0][pos],
		listElement: listElement,
	})
	return next
}

// scanTask 扫描链表中过期定时器,并执行回调函数
func (t *TimeWheel) scanTask(taskList *list.List) {
	var (
		task tasker
	)
	if taskList.Front() == nil {
		return
	}
	_, ok := taskList.Front().Value.(tasker)
	if !ok {
		return
	}
	for e := taskList.Front(); e != nil; {
		task = e.Value.(tasker)
		e = t.callbackHandler(taskList, e, task)
	}
}

// callbackHandler 执行回调函数
func (t *TimeWheel) callbackHandler(taskList *list.List, e *list.Element, task tasker) *list.Element {
	next := e.Next()
	taskList.Remove(e)
	t.timer.Delete(task.getTimerId())
	t.workerChan <- task.callbackHandler
	return next
}

// AddTimer 添加定时器
func (t *TimeWheel) AddTimer(delay time.Duration, callback func()) uint64 {
	if delay < 0 {
		return 0
	}
	//每个槽间隔时间，等于底级时间轮一圈总时间
	interval := t.interval
	tierLen := len(t.tier)
	for i := 0; i < tierLen; i++ {
		//当前层级时间轮剩余时间
		timeRemain := time.Duration(t.tier[i]-t.currentPos[i]) * interval
		if delay < timeRemain {
			return t.addTask(i, interval, delay, callback)
		}
		interval = time.Duration(t.tier[i]) * interval
	}

	return t.addMaxTask(delay, callback)
}

// addTask 新增任务到链表中
func (t *TimeWheel) addTask(tier int, interval, delay time.Duration, callback func()) uint64 {
	var timeTask tasker
	intervalTime := int(interval)
	delayTime := int(delay)
	id := atomic.AddUint64(&t.timerId, 1)
	timeTask = &task{
		delay:    time.Duration(time.Now().UnixNano()) + delay,
		timerId:  id,
		callback: callback,
	}

	pos := ((t.currentPos[tier] + delayTime/intervalTime) % t.tier[tier]) - 1
	if pos < t.currentPos[tier] {
		pos = t.currentPos[tier]
	}
	listElement := t.slots[tier][pos].PushBack(timeTask)
	t.timer.Store(id, timerElement{
		timerList:   t.slots[tier][pos],
		listElement: listElement,
	})
	return id
}

// addMaxTask 添加超出时间轮时间范围的计时器
func (t *TimeWheel) addMaxTask(delay time.Duration, callback func()) uint64 {
	var (
		timeTask    tasker
		timer       tasker
		listElement *list.Element
	)
	id := atomic.AddUint64(&t.timerId, 1)
	dealyTime := time.Duration(time.Now().UnixNano()) + delay
	timeTask = &task{
		delay:    dealyTime,
		timerId:  id,
		callback: callback,
	}
	for e := t.maxTimerList.Front(); e != nil; e = e.Next() {
		timer = e.Value.(tasker)
		if dealyTime < timer.getDelay() {
			listElement = t.maxTimerList.InsertBefore(timeTask, e)
			t.timer.Store(id, timerElement{
				timerList:   t.maxTimerList,
				listElement: listElement,
			})
			return id
		}
	}
	listElement = t.maxTimerList.PushBack(timeTask)
	t.timer.Store(id, timerElement{
		timerList:   t.maxTimerList,
		listElement: listElement,
	})
	return id
}

// RemoveTimer 删除定时器
func (t *TimeWheel) RemoveTimer(key uint64) {
	// 获取定时器所在位置
	e, ok := t.timer.Load(key)
	if !ok {
		return
	}
	timer, ok := e.(timerElement)
	if !ok {
		return
	}
	// 获取槽指向的链表
	l := timer.timerList
	t.timer.Delete(key)
	l.Remove(timer.listElement)
}

// scanMaxTask 扫描超出时间轮时间范围的定时器并执行
func (t *TimeWheel) maxTaskHandle() {
	ticker := time.NewTicker(t.interval)
	for {
		select {
		case <-t.stopChan:
			ticker.Stop()
			return
		case <-ticker.C:
			t.scanMaxTask()
		}
	}
}

// scanMaxTask 扫描超出时间轮时间范围的定时器并执行
func (t *TimeWheel) scanMaxTask() {
	var (
		task        tasker
		currentTime = time.Duration(time.Now().UnixNano()) + (t.interval / 2)
	)
	for e := t.maxTimerList.Front(); e != nil; e = e.Next() {
		task = e.Value.(tasker)
		if currentTime >= task.getDelay() {
			t.callbackHandler(t.maxTimerList, e, task)
			continue
		}
		return
	}
}
