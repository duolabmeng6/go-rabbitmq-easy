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

func TestNewLLRpcServer(t *testing.T) {

	RpcServer := NewLLRpcServer("amqp://admin:admin@182.92.84.229:5672/")

	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出("接收任务数量", successCount, "协程数量", runtime.NumGoroutine())
		return true
	}, 1000)

	RpcServer.Router("func1", 1000, func(messages *message.Message) ([]byte, bool) {
		successCount.Add(1)

		msg := string(messages.Payload)
		//E调试输出(msg)

		return E到字节集(msg + " ok"), true
	})

	select {}
}

func TestNewLLRpcClient(t *testing.T) {
	RpcClient := NewLLRpcClient("amqp://admin:admin@182.92.84.229:5672/")

	stratCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出("错误数量", errorCount.Val(), "成功数量", successCount.Val(), "启动数量", stratCount.Val(), "协程数量", runtime.NumGoroutine())
		return true
	}, 1000)

	线程池 := New线程池(2000)

	for i := 1; i <= 10000*10; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			//提交的时候写log

			res, err := RpcClient.Call("func1", E到字节集("hello"), 30)
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
	select {}

}
