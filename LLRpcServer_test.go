package RabbitmqEasy

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
	//发布消息
	线程池 := coreUtil.New线程池(10)
	for i := 1; i <= 10000; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()

			res, err := task.Call("rpc_queue1", E到字节集(E到文本(i)), 20)
			failOnError(err, "Failed to handle RPC request")
			E调试输出("发送数据", E到文本(i), "rpc_queue1 计算结果", E到文本(res))

		}(i)
		//go func(i int) {
		//	res, err := task.Call("rpc_queue2", E到字节集(E到文本(i)))
		//	failOnError(err, "Failed to handle RPC request")
		//	E调试输出("发送数据", E到文本(i), "rpc_queue2 计算结果", E到文本(res))
		//	E延时(5*1000)
		//}(i)
	}

	线程池.E等待任务完成()
}

func TestRabbitmqModel_Subscribeddffdb(t *testing.T) {
	for i := 1; i <= 1; i++ {
		//连接
		go func() {
			task := NewLLRpcServer("amqp://admin:admin@182.92.84.229:5672/")
			task.Router("rpc_queue1", 10, func(delivery amqp.Delivery) ([]byte, bool) {
				//t.Log("时间",E取现行时间().E时间到文本(""), "收到任务数据")

				n := E到整数(E到文本(delivery.Body))
				//log.Printf(" [.] fib(%d)", n)
				时间统计 := coreUtil.New时间统计类()

				response := fib(int(n))

				//t.Log("收到任务数据", n, "rpc_queue1计算结果为", E到文本(response), "耗时", 时间统计.E取秒())
				//E延时(5*1000)

				return E到字节集(E到文本(response) + "," + 时间统计.E取秒()), true
			})
		}()
	}

	E延时(1000 * 60 * 60)
}
