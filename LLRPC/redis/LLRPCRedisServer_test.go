package redis

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"fmt"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"
	"runtime"
	"testing"
)

func TestServer(t *testing.T) {
	server := NewLLRPCRedisServer("127.0.0.1:6379")
	server.Router("func1", func(data LLRPC.TaskData) (string, bool) {
		fmt.Println("test", data.Data)

		return data.Data + " ok", true
	})
	select {}
}

func TestClient(t *testing.T) {
	client := NewLLRPCRedisClient("127.0.0.1:6379")
	for i := 0; i < 1; i++ {
		fmt.Println("调用函数 func1")
		ret, err := client.Call("func1", "hello")
		fmt.Println("func1 结果", ret.Result, err)
	}
}

// 服务端处理能力测试
func Test测试服务器能力(t *testing.T) {
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

	server := NewLLRPCRedisServer("127.0.0.1:6379")
	server.Router("func1", func(data LLRPC.TaskData) (string, bool) {
		successCount.Add(1)
		return data.Data + " ok", true
	})
	select {}
}

func Test测试服务器qps(t *testing.T) {
	client := NewLLRPCRedisClient("127.0.0.1:6379")

	线程池 := etool.New线程池(10)
	for {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			ret, err := client.Call("func1", "hello")

			fmt.Println("测试调用函数 func1 结果", ret.Result, err)

		}()
	}

	select {}
}

// 客户端统计
func Test客户端统计(t *testing.T) {
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

	client := NewLLRPCRedisClient("127.0.0.1:6379")
	线程池 := etool.New线程池(1000)
	for i := 1; i <= 10000*1; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			ret, err := client.Call("func1", "hello")
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
