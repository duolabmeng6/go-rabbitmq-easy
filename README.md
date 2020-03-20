# go-rabbitmq-easy
容易使用的rabbitmq
以及超高性能的rpc

# 特性

- 支持断线重连
- 支持消息重发
- 超高性能


# RPC使用方法

```go
package LLRPCRabbmitMQ

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"fmt"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"
	. "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"runtime"
	"testing"
)

func TestClient(t *testing.T) {
	successCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		E调试输出(
			"客户端",
			"成功数据", successCount.Val(),
			"失败数量", errorCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)
	//amqp://admin:admin@182.92.84.229:5672/
	//amqp://guest:guest@127.0.0.1:5672/
	client := NewLRpcRabbmitMQClient("amqp://guest:guest@127.0.0.1:5672/")
	//等一会让监听结果的连上
	E延时(1000)
	线程池 := New线程池(100000)
	for i := 0; i < 10000*100; i++ {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			//fmt.Println("调用函数 func2")好的  我看看，我先在本地测一下
			//好吧?
			ret, err := client.Call("func2", E到文本(E取现行时间().E取时间戳毫秒()), 1000)
			if err != nil {
				fmt.Println("func2 调用错误", err)
				errorCount.Add(1)
			} else {
				//fmt.Println("func2 结果", ret.Result, err)
				if ret.Result != "" {
					successCount.Add(1)
				}
			}
		}()
	}

	select {}
}

func TestServer(t *testing.T) {
	successCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		E调试输出(
			"服务端",
			"成功数据", successCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)

	server := NewLRpcRabbmitMQServer("amqp://guest:guest@127.0.0.1:5672/")
	server.Router("func2", func(data LLRPC.TaskData) (string, bool) {
		successCount.Add(1)
		//fmt.Println("test", data.Data)
		//E延时(6000)
		time := E到整数(data.Data)
		nowtime := E取现行时间().E取时间戳毫秒()
		str := nowtime - time

		return E到文本(str), true
	})
	select {}
}


```

# 性能测试

## 服务端

```golang
服务端 成功数据 869482 协程数量 95727
服务端 成功数据 893600 协程数量 93586
服务端 成功数据 921375 协程数量 89362
服务端 成功数据 941614 协程数量 82450
服务端 成功数据 957765 协程数量 67899
服务端 成功数据 998649 协程数量 82950
服务端 成功数据 1000000 协程数量 55277
服务端 成功数据 1000000 协程数量 21469
```

```golang
客户端 成功数据 828172 失败数量 0 协程数量 100017
客户端 成功数据 849850 失败数量 0 协程数量 100017
客户端 成功数据 887470 失败数量 0 协程数量 100017
客户端 成功数据 915258 失败数量 0 协程数量 84759
客户端 成功数据 944763 失败数量 0 协程数量 55254
客户端 成功数据 978566 失败数量 0 协程数量 21451
客户端 成功数据 1000000 失败数量 0 协程数量 17
```

# 支持断线重连的rabbmit 使用方式

```golang

package LLRPCRabbmitMQ

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"fmt"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"
	. "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"runtime"
	"testing"
)

func TestClient(t *testing.T) {
	successCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		E调试输出(
			"客户端",
			"成功数据", successCount.Val(),
			"失败数量", errorCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)
	//amqp://admin:admin@182.92.84.229:5672/
	//amqp://guest:guest@127.0.0.1:5672/
	client := NewLRpcRabbmitMQClient("amqp://guest:guest@127.0.0.1:5672/")
	//等一会让监听结果的连上
	E延时(1000)
	线程池 := New线程池(100000)
	for i := 0; i < 10000*100; i++ {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			//fmt.Println("调用函数 func2")好的  我看看，我先在本地测一下
			//好吧?
			ret, err := client.Call("func2", E到文本(E取现行时间().E取时间戳毫秒()), 1000)
			if err != nil {
				fmt.Println("func2 调用错误", err)
				errorCount.Add(1)
			} else {
				//fmt.Println("func2 结果", ret.Result, err)
				if ret.Result != "" {
					successCount.Add(1)
				}
			}
		}()
	}

	select {}
}

func TestServer(t *testing.T) {
	successCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		E调试输出(
			"服务端",
			"成功数据", successCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)

	server := NewLRpcRabbmitMQServer("amqp://guest:guest@127.0.0.1:5672/")
	server.Router("func2", func(data LLRPC.TaskData) (string, bool) {
		successCount.Add(1)
		//fmt.Println("test", data.Data)
		//E延时(6000)
		time := E到整数(data.Data)
		nowtime := E取现行时间().E取时间戳毫秒()
		str := nowtime - time

		return E到文本(str), true
	})
	select {}
}

```