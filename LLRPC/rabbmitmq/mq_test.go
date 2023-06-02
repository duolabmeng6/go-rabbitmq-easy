package LLRPCRabbmitMQ

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"fmt"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
	"testing"
)

func TestClient(t *testing.T) {
	successCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		fmt.Println(
			"客户端",
			"成功数据", successCount.Val(),
			"失败数量", errorCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)
	//amqp://admin:admin@182.92.84.229:5672/
	//amqp://guest:guest@127.0.0.1:5672/
	client := NewLLRPCRabbmitMQClient("amqp://guest:guest@127.0.0.1:5672/")
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
		fmt.Println(
			"服务端",
			"成功数据", successCount.Val(),
			"协程数量", runtime.NumGoroutine(),
		)
		return true
	}, 1000)

	server := NewLLRPCRabbmitMQServer("amqp://guest:guest@127.0.0.1:5672/")
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

// 提取1条消息
func TestServer_one(t *testing.T) {
	server := NewLLRPCRabbmitMQServer("amqp://guest:guest@127.0.0.1:5672/")
	server.Router("func2", func(data LLRPC.TaskData) (string, bool) {
		//fmt.Println("test", data.Data)
		//E延时(6000)
		time := E到整数(data.Data)
		nowtime := E取现行时间().E取时间戳毫秒()
		str := nowtime - time

		return E到文本(str), true
	})
	select {}
}
