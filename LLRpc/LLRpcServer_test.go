package LLRpc

import (
	. "github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/streadway/amqp"
	"testing"
)

//发布消息
func TestRabbitmqModel_Publishddaaaa(t *testing.T) {
	//连接
	task := NewLLRpcClient("amqp://admin:admin@182.92.84.229:5672/")
	E延时(1000)

	线程池 := coreUtil.New线程池(2)
	for i := 1; i <= 10000000000; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			//提交的时候写log
			res, err := task.Call("rpc_queue1", E到字节集("10,"+E到文本(E取现行时间().E取时间戳())), 10)
			failOnError(err, "Failed to handle RPC request")
			//有结果的时候写log
			E调试输出("发送数据 超时10", "返回结果", E到文本(res))

			res, err = task.Call("rpc_queue1", E到字节集("2,"+E到文本(E取现行时间().E取时间戳())), 2)
			failOnError(err, "Failed to handle RPC request")
			E调试输出("发送数据 超时2", "返回结果", E到文本(res))
		}(i)
	}
	线程池.E等待任务完成()
}

func TestRabbitmqModel_Subscribeddffdb(t *testing.T) {
	for i := 1; i <= 1; i++ {
		//连接
		go func() {
			task := NewLLRpcServer("amqp://admin:admin@182.92.84.229:5672/")
			task.Router("rpc_queue1", 1000000, func(delivery amqp.Delivery) ([]byte, bool) {
				//n := E到整数(E到文本(delivery.Body))
				时间统计 := coreUtil.New时间统计类()
				//response := fib(int(n))

				E延时(3)
				t.Log("收到任务数据", E到文本(delivery.Body), "rpc_queue1计算结果为", "耗时", 时间统计.E取秒())

				return E到字节集(E到文本(delivery.Body) + "," + 时间统计.E取秒() + "," + E到文本(E取现行时间().E取时间戳())), true
			})
		}()
	}

	E延时(1000 * 60 * 60)
}
