# llrpc 超高性能的rpc

本模块是采用即时消息队列的方式实现的rpc，支持断线重连，消息重发，超高性能。

支持多种后端 rabbmitmq , redis, kafka

# 特性

- 支持断线重连
- 支持消息重发
- 超高性能




# RPC使用方法

# redis

## 服务端
```golang
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
```

## 客户端
```golang
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

```

## 性能

客户端

```
错误数量 0 成功数量 77044 启动数量 77204 协程数量 167 耗时 14.000 qps 5136
错误数量 0 成功数量 82571 启动数量 82731 协程数量 167 耗时 15.000 qps 5160
错误数量 0 成功数量 88448 启动数量 88608 协程数量 167 耗时 16.000 qps 5202
错误数量 0 成功数量 94675 启动数量 94835 协程数量 167 耗时 17.000 qps 5259
错误数量 0 成功数量 100000 启动数量 100000 协程数量 7 耗时 18.000 qps 5263
```

服务端

```api
接收任务数量 76514 协程数量 38 耗时 20.000 qps 3643
接收任务数量 81911 协程数量 37 耗时 21.000 qps 3723
接收任务数量 87815 协程数量 38 耗时 22.000 qps 3818
接收任务数量 94004 协程数量 38 耗时 23.000 qps 3916
接收任务数量 100000 协程数量 22 耗时 24.000 qps 4000
```