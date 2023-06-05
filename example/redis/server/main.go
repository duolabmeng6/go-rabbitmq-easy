package main

import (
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC/redis"
	"github.com/duolabmeng6/goefun/ecore"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
)

func main() {
	println("启动 LLRPC Redis服务端")
	成功数量 := gtype.NewInt()
	时间统计 := ecore.New时间统计类()
	时间统计.E开始()
	ecore.E时钟_创建(func() bool {
		fmt.Println(
			"接收任务数量", 成功数量,
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

	server := LLRPCRedis.NewServer("127.0.0.1:6379")
	server.Router("func1", runtime.NumCPU(), func(data LLRPC.TaskData) (string, bool) {
		成功数量.Add(1)

		return data.Data + " ok", true
	})
	select {}
}
