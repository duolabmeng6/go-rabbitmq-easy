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
	"encoding/json"
	. "github.com/duolabmeng6/goefun/ecore"
	"testing"
)

func TestNewLRpcRabbmit_server(t *testing.T) {
	server := NewLRpcRabbmit("amqp://guest:guest@127.0.0.1:5672/", func(this *LRpcRabbmit) {
		E调试输出("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			"test1", // 队列名称
			true,    // 是否需要持久化
			false,   //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
			false,   // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
			false,   // 是否等待服务器返回
			nil,     // arguments
		)
		if err != nil {
			E调试输出("QueueDeclare", err)
		}
		//监听队列
		this.msgs, err = this.channel.Consume(
			q.Name, // 消息要取得消息的队列名
			"",     // 消费者标签
			true,   // 服务器将确认 为true，使用者不应调用Delivery.Ack
			false,  // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
			false,  // no-local
			false,  // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
			nil,    // args
		)
		if err != nil {
			E调试输出("Consume", err)
		}
		go func() {
			for d := range this.msgs {
				//收到任务创建协程执行
				taskData := LLRPC.TaskData{}
				json.Unmarshal(d.Body, &taskData)

				this.fn(taskData)
			}
		}()

	})

	server.Subscribe(func(data LLRPC.TaskData) {
		E调试输出("收到数据", data.Data)
	})

	select {}
}

func TestNewLRpcRabbmit_client(t *testing.T) {
	server := NewLRpcRabbmit("amqp://guest:guest@127.0.0.1:5672/", func(this *LRpcRabbmit) {

	})
	taskData := LLRPC.TaskData{
		Data: "heello",
	}

	for {
		server.Publish("test1", &taskData)
		E延时(1)
	}

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
	 "github.com/duolabmeng6/goefun/ecore"

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
	线程池 := etool.New线程池(100000)
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