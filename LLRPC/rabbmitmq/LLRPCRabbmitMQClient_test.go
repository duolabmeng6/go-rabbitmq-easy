package LLRPCRabbmitMQ

import (
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
	"testing"
)

func TestClient(t *testing.T) {
	时间统计 := New时间统计类()
	stratCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	successCount := gtype.NewInt()
	E时钟_创建(func() bool {
		fmt.Println(
			"错误数量", errorCount.Val(),
			"成功数量", successCount.Val(),
			"启动数量", stratCount.Val(),
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			"qps", (successCount.Val()+1)/(E取整(时间统计.E取秒())+1),
		)
		return true
	}, 1000)

	E时钟_创建(func() bool {
		successCount.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	client := NewLLRPCRabbmitMQClient("amqp://guest:guest@127.0.0.1:5672/")
	//等一会让监听结果的连上
	E延时(1000)
	线程池 := etool.New线程池(100)
	for i := 0; i < 10000*100; i++ {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			//fmt.Println("调用函数 func2")好的  我看看，我先在本地测一下
			//好吧?
			ret, err := client.Call("func2", "hello", 1000)
			if ret.Result != "hello ok" {
				fmt.Println("调用错误", "返回结果", ret.Result, "错误提示", err)

				errorCount.Add(1)
			} else {
				successCount.Add(1)
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
			"接收任务数量", successCount,
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			"qps", (successCount.Val()+1)/(E取整(时间统计.E取秒())+1),
		)
		return true
	}, 1000)

	E时钟_创建(func() bool {
		successCount.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	server := NewLLRPCRabbmitMQServer("amqp://guest:guest@127.0.0.1:5672/")
	server.Router("func2", func(data LLRPC.TaskData) (string, bool) {
		successCount.Add(1)
		return data.Data + " ok", true
		//fmt.Println("test", data.Data)
		//E延时(6000)
		//time := E到整数(data.Data)
		//nowtime := E取现行时间().E取时间戳毫秒()
		//str := nowtime - time
		//
		//return E到文本(str), true
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
