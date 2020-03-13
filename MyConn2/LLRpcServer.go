package MqConn

import (
	"github.com/streadway/amqp"
)

type LLRpcServer struct {
	link     string
	sendConn *LLRpcConn
	receive  *LLRpcConn
}

func NewLLRpcServer(link string) *LLRpcServer {
	this := new(LLRpcServer)
	this.link = link
	this.sendConn = NewLLRpcConn(this.link, false, false)
	//this.receive = NewLLRpcConn(this.link, false, false)

	return this
}

//注册函数
func (this *LLRpcServer) Router(Path string, qos int, fn func(messages amqp.Delivery) ([]byte, bool)) {

	consumer := &Consumer{
		ExchangeName: "",
		QueueName:    Path,
		Fn: func(messages amqp.Delivery) {
			//开启协程处理任务
			go func(msg amqp.Delivery) {
				//data, flag := fn(msg)
				//core.E调试输出(
				//	"收到消息", core.E到文本(msg.Body),
				//	"返回消息", core.E到文本("data"),
				//	"返回结果队列", msg.ReplyTo,
				//	"返回标志", msg.CorrelationId,
				//)

				//if flag {
				//this.sendConn.Publish(msg.ReplyTo, msg.CorrelationId, data, "")
				//}
			}(messages)
		},
	}

	this.receive.Subscribe(Path, consumer)
}
