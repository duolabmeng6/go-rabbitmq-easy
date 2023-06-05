package main

import (
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC/redis"
	"github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
)

func main() {
	println("启动 LLRPC Redis客户端")
	时间统计 := ecore.New时间统计类()
	启动数量 := gtype.NewInt()
	错误数量 := gtype.NewInt()
	成功数量 := gtype.NewInt()
	ecore.E时钟_创建(func() bool {
		fmt.Println(
			"错误数量", 错误数量.Val(),
			"成功数量", 成功数量.Val(),
			"启动数量", 启动数量.Val(),
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			"qps", (成功数量.Val()+1)/(ecore.E取整(时间统计.E取秒())+1),
		)
		return true
	}, 1000)

	ecore.E时钟_创建(func() bool {
		成功数量.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	client := LLRPCRedis.NewClient("127.0.0.1:6379")
	线程池 := etool.New线程池(runtime.NumCPU() * 10)
	for i := 1; i <= 10000*10; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			启动数量.Add(1)
			ret, err := client.Call("func1", "hello", 10)
			if ret.Result != "hello ok" {
				fmt.Println("调用错误", "返回结果", ret.Result, "错误提示", err)
				错误数量.Add(1)
			} else {
				成功数量.Add(1)
			}
		}(i)
	}

	线程池.E等待任务完成()
	ecore.E延时(5 * 1000)
}
