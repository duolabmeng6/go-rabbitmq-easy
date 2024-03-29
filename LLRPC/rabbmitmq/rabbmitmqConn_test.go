package LLRPCRabbmitMQ

import (
	"encoding/json"
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"
	"testing"
)

func TestNew_server(t *testing.T) {
	server := NewConn("amqp://guest:guest@127.0.0.1:5672/", func(this *Conn) {
		fmt.Println("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			"test1", // 队列名称
			true,    // 是否需要持久化
			false,   //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
			false,   // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
			false,   // 是否等待服务器返回
			nil,     // arguments
		)
		if err != nil {
			fmt.Println("QueueDeclare", err)
		}
		//监听队列
		this.msgs, err = this.channel.Consume(
			q.Name, // 消息要取得消息的队列名
			"",     // 消费者标签
			true,   // 服务器将确认 为true，使用者不应调用Delivery.Ack
			false,  // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
			false,  // no-local
			false,  // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
			nil,    // args
		)
		if err != nil {
			fmt.Println("Consume", err)
		}
		go func() {
			for d := range this.msgs {
				//收到任务创建协程执行
				taskData := LLRPC.TaskData{}
				json.Unmarshal(d.Body, &taskData)

				this.fn(taskData)
			}
		}()

	})

	server.Subscribe(func(data LLRPC.TaskData) {
		fmt.Println("收到数据", data.Data)
	})

	select {}
}

func TestNew_client(t *testing.T) {
	server := NewConn("amqp://guest:guest@127.0.0.1:5672/", func(this *Conn) {

	})
	taskData := LLRPC.TaskData{
		Data: "heello",
	}

	for {
		server.Publish("test1", &taskData)
		E延时(1)
	}

}
