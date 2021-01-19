# github.com/HQPPP/tierTimewheel

golang 分级时间轮，支持设置任意时间轮级数，时间轮间隔，工作routine数量

# 使用
    package main
    import (
        "github.com/HQPPP/tierTimewheel"
        "time"
    )
    func main() {
        // 时间轮层级，值为该层级插槽数
        tier := []int{
    	    60, 60, 12,
        }
        // 时间轮转动间隔时间，层级，启动几个routine来工作
        tw := timewheel.New(1*time.Second, tier, 3)
        tw.Start()
        tw.AddTimer(2*time.Second, func() {
    	    fmt.Println(time.Now().Unix())
        })
    
        tw.AddTimer(5*time.Second, func() {
    	    fmt.Println(time.Now().Unix())
    	    timerId := tw.AddTimer(2*time.Second, func() {
    		    fmt.Println(time.Now().Unix())
    	    })
    	    tw.RemoveTimer(timerId)
        })
        time.Sleep(100 * time.Second)
    }

