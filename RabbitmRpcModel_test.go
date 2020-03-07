package RabbitmqEasy

import (
	. "github.com/duolabmeng6/goefun/core"
	"log"
	"strconv"
	"testing"
)

//发布消息
func TestRabbitmqModel_Publishaaaa(t *testing.T) {
	//连接
	task := NewRabbitmRpcModel("amqp://admin:admin@182.92.84.229:5672/", "rpc_queue")
	//发布消息
	for i := 1; i <= 1; i++ {
		res, err := task.Call(E到文本(i))
		failOnError(err, "Failed to handle RPC request")
		log.Printf(" [.] Got %d", res)

		E延时(100)
	}
}

func TestRabbitmqModel_Subscribedddb(t *testing.T) {
	//连接
	task := NewRabbitmRpcModel("amqp://admin:admin@182.92.84.229:5672/", "rpc_queue")
	//订阅
	task.Subscribe("rpc_queue")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			n, err := strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [.] fib(%d)", n)
			response := fib(n)

			task.Callfun(d, []byte(strconv.Itoa(response)))

			d.Ack(false)
		}
	}()

	E延时(1000 * 60 * 60)
}

func fib(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}
