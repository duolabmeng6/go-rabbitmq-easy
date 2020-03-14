package LRpc

import (
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"
	. "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"runtime"
	"testing"
)

func TestServer(t *testing.T) {
	server := NewLRpcRedisServer("aa")
	server.Router("func1", func(data TaskData) (string, bool) {
		E调试输出("test", data.Data)

		return data.Data + " ok", true
	})
	select {}
}

func TestClient(t *testing.T) {
	client := NewLRpcRedisClient("aa")
	for i := 0; i < 10000; i++ {
		E调试输出("测试调用函数 func1")
		ret, err := client.Call("func1", "hello")
		E调试输出("测试调用函数 func1 结果", ret.Result, err)
	}
}

//服务端处理能力测试
func TestServerTongji(t *testing.T) {

	successCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	E时钟_创建(func() bool {
		E调试输出(
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

	server := NewLRpcRedisServer("aa")
	server.Router("func1", func(data TaskData) (string, bool) {
		successCount.Add(1)

		return data.Data + " ok", true
	})
	select {}
}

func TestClientTongjiQps(t *testing.T) {
	client := NewLRpcRedisClient("aa")

	线程池 := New线程池(10)
	for {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			ret, err := client.Call("func1", "hello")

			E调试输出("测试调用函数 func1 结果", ret.Result, err)

		}()
	}

	select {}
}

//客户端统计
func TestCientTongji(t *testing.T) {
	client := NewLRpcRedisClient("aa")

	时间统计 := New时间统计类()

	stratCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	successCount := gtype.NewInt()
	E时钟_创建(func() bool {
		E调试输出(
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

	线程池 := New线程池(10)

	for i := 1; i <= 1000*1; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			//提交的时候写log
			//E调试输出("测试调用函数")

			ret, err := client.Call("func1", "hello")

			//E调试输出("测试调用函数 func1 结果", ret, err)
			//E调试输出(E到文本(res), err)
			if ret.Result != "hello ok" {
				E调试输出("调用错误", "返回结果", ret.Result, "错误提示", err)

				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}

		}(i)
	}

	线程池.E等待任务完成()
	E延时(10 * 1000)
}
