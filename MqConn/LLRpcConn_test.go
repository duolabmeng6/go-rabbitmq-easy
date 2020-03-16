package MqConn

import (
	"github.com/ThreeDotsLabs/watermill/message"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"
	os "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"runtime"
	"testing"
)

func TestPushQPS(t *testing.T) {

	successCount := gtype.NewInt()
	时间统计 := New时间统计类()
	时间统计.E开始()

	os.E时钟_创建(func() bool {
		E调试输出(
			"接收任务数量", successCount,
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			"qps", (successCount.Val()+1)/(E取整(时间统计.E取秒())+1),
		)

		return true
	}, 1000)
	os.E时钟_创建(func() bool {
		successCount.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	RpcServer := NewLLRpcServer("amqp://guest:guest@127.0.0.1:5672/")
	RpcServer.Router("testqps", 100000, func(messages *message.Message) ([]byte, bool) {
		successCount.Add(1)

		msg := string(messages.Payload)
		//E调试输出(msg)
		return E到字节集(msg + " ok"), false
	})

	//RpcServer2 := NewLLRpcServer("amqp://guest:guest@127.0.0.1:5672/")
	//RpcServer2.Router("testqps", 10000, func(messages *message.Message) ([]byte, bool) {
	//	successCount.Add(1)
	//
	//	msg := string(messages.Payload)
	//	//E调试输出(msg)
	//	return E到字节集(msg + " ok"), false
	//})

	select {}
}
func TestPushQPSSend(t *testing.T) {
	RpcClient := NewLLRpcClient("amqp://guest:guest@127.0.0.1:5672/")
	线程池 := New线程池(100000)
	for {
		线程池.E加入任务()
		go func() {
			defer 线程池.E完成()
			RpcClient.TestPush("testqps", E到字节集("666"), 10)
		}()
	}
}

func TestNewLLRpcServer(t *testing.T) {
	defer func() {
		recover()

	}()
	时间统计 := New时间统计类()

	RpcServer := NewLLRpcServer("amqp://guest:guest@127.0.0.1:5672/")

	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出(
			"接收任务数量", successCount,
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			//"qps", successCount.Val()/E取整(时间统计.E取秒())+1,
		)

		return true
	}, 1000)

	os.E时钟_创建(func() bool {
		successCount.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	RpcServer.Router("func1", 1000, func(messages *message.Message) ([]byte, bool) {
		successCount.Add(1)

		msg := string(messages.Payload)
		//E调试输出(msg)

		return E到字节集(msg + " ok"), true
	})

	select {}
}

func TestNewLLRpcClient(t *testing.T) {

	RpcClient := NewLLRpcClient("amqp://guest:guest@127.0.0.1:5672/")
	时间统计 := New时间统计类()

	stratCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出(
			"错误数量", errorCount.Val(),
			"成功数量", successCount.Val(),
			"启动数量", stratCount.Val(),
			"协程数量", runtime.NumGoroutine(),
			"耗时", 时间统计.E取秒(),
			//"qps", successCount.Val()/E取整(时间统计.E取秒())+1,
		)
		return true
	}, 1000)

	os.E时钟_创建(func() bool {
		successCount.Set(0)
		时间统计.E开始()
		return true
	}, 60*1000)

	线程池 := New线程池(2000)

	for i := 1; i <= 10000*1000; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			//提交的时候写log

			res, err := RpcClient.Call("func1", E到字节集("hello"), 10)
			//E调试输出(E到文本(res), err)
			if E到文本(res) != "hello ok" {
				E调试输出("调用错误", "返回结果", E到文本(res), "错误提示", err)

				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}

		}(i)
	}

	线程池.E等待任务完成()
	E延时(10 * 1000)
}
