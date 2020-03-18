package heightenMq

import (
	"duolabmeng6/go-rabbitmq-easy/LRpc"
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {
	client := NewLRpcRedisClient("amqp://admin:admin@182.92.84.229:5672/")
	for i := 0; i < 10000; i++ {
		go func() {
			for i := 0; i < 10000; i++ {
				fmt.Println("调用函数 func1")
				ret, err := client.Call("func1", "hello", 3)
				if err != nil {
					fmt.Println("func1 调用错误", err)

					continue
				}
				fmt.Println("func1 结果", ret.Result, err)
			}
		}()
	}
	select {}
}

func TestServer(t *testing.T) {
	server := NewLRpcRedisServer("amqp://admin:admin@182.92.84.229:5672/")
	server.Router("func1", func(data LRpc.TaskData) (string, bool) {
		fmt.Println("test", data.Data)
		//E延时(6000)

		return data.Data + " ok", true
	})
	select {}
}
