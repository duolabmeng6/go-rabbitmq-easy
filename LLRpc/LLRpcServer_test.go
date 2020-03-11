package LLRpc

import (
	. "github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	os "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"github.com/streadway/amqp"
	"testing"
)

//发布消息
func TestRabbitmqModel_Publishddaaaa(t *testing.T) {
	//连接
	task := NewLLRpcClient("amqp://admin:admin@182.92.84.229:5672/")
	E延时(1000)

	线程池 := coreUtil.New线程池(1000)
	stratCount := gtype.NewInt()
	errorCount := gtype.NewInt()
	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出("错误数量", errorCount.Val(), "成功数量", successCount.Val(), "启动数量", stratCount.Val())
		return true
	}, 1000)

	for i := 1; i <= 100000; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()
			stratCount.Add(1)
			//提交的时候写log
			res, err := task.Call("rpc_queue1", E到字节集("10,"+E到文本(E取现行时间().E取时间戳())), 10)
			//failOnError(err, "Failed to handle RPC request")
			//有结果的时候写log
			//E调试输出("发送数据 超时10", "返回结果", E到文本(res))
			if err != nil {
				E调试输出("发送数据 超时10", "返回结果", E到文本(res), "错误提示", err)

				errorCount.Add(1)
			} else {
				successCount.Add(1)
			}

		}(i)
	}

	线程池.E等待任务完成()
	E延时(60 * 1000)

}

func TestRabbitmqModel_Subscribeddffdb(t *testing.T) {
	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		E调试输出("接收任务数量", successCount)
		return true
	}, 1000)

	for i := 1; i <= 1; i++ {
		//连接
		go func() {
			task := NewLLRpcServer("amqp://admin:admin@182.92.84.229:5672/")
			task.Router("rpc_queue1", 1000000, func(delivery amqp.Delivery) ([]byte, bool) {
				successCount.Add(1)
				//n := E到整数(E到文本(delivery.Body))
				时间统计 := coreUtil.New时间统计类()
				//response := fib(int(n))

				E延时(3)
				//t.Log("收到任务数据", E到文本(delivery.Body), "rpc_queue1计算结果为", "耗时", 时间统计.E取秒())

				return E到字节集(E到文本(delivery.Body) + "," + 时间统计.E取秒() + "," + E到文本(E取现行时间().E取时间戳())), true
			})
		}()
	}

	E延时(1000 * 60 * 60)
}
