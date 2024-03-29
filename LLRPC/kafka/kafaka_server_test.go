package kafka

import (
	"fmt"
	. "github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
	"testing"
)

func TestServer(t *testing.T) {
	server := NewLLRPCKafkaServer("182.92.84.229:9092")
	server.Router("func1", func(data TaskData) (string, bool) {
		fmt.Println("test", data.Data)
		//E延时(6000)

		return data.Data + " ok", true
	})
	select {}
}

func TestClient(t *testing.T) {
	client := NewLLRPCKafkaClient("182.92.84.229:9092")
	for i := 0; i < 1000; i++ {
		fmt.Println("调用函数 func1")
		ret, err := client.Call("func1", "hello", 10)
		if err != nil {
			fmt.Println("func1 调用错误", err)

			continue
		}
		fmt.Println("func1 结果", ret.Result, err)
	}
	select {}

}

// 服务端处理能力测试
func TestServerTongji(t *testing.T) {

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

	go func() {
		server := NewLLRPCKafkaServer("182.92.84.229:9092")
		server.Router("func1", func(data TaskData) (string, bool) {
			successCount.Add(1)
			//fmt.Println(1)
			return data.Data + " ok", true
		})
	}()

	go func() {
		server2 := NewLLRPCKafkaServer("182.92.84.229:9092")
		server2.Router("func1", func(data TaskData) (string, bool) {
			successCount.Add(1)
			//fmt.Println(2)
			return data.Data + " ok", true
		})
	}()

	select {}
}

func TestClientTongjiQps(t *testing.T) {
	client := NewLLRPCKafkaClient("182.92.84.229:9092")

	线程池 := etool.New线程池(10)
	for {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			ret, err := client.Call("func1", "hello", 10)

			fmt.Println("测试调用函数 func1 结果", ret.Result, err)

		}()
	}

	select {}
}

// 客户端统计
func TestCientTongji(t *testing.T) {
	client := NewLLRPCKafkaClient("182.92.84.229:9092")

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

	线程池 := etool.New线程池(1000)

	for i := 1; i <= 10000*2; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			//提交的时候写log
			//fmt.Println("测试调用函数")

			ret, err := client.Call("func1", "hello", 10)

			//fmt.Println("测试调用函数 func1 结果", ret, err)
			//fmt.Println(E到文本(res), err)
			if ret.Result != "hello ok" {
				fmt.Println("调用错误", "返回结果", ret.Result, "错误提示", err)

				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}

		}(i)
	}

	线程池.E等待任务完成()
	E延时(10 * 1000)
}
