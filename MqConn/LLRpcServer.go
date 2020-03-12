package MqConn

import (
	"github.com/ThreeDotsLabs/watermill/message"
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
	this.receive = NewLLRpcConn(this.link, false, false)

	return this
}

//注册函数
func (this *LLRpcServer) Router(Path string, qos int, fn func(messages *message.Message) ([]byte, bool)) {

	this.receive.Subscribe(Path, qos, func(messages <-chan *message.Message) {
		for msg := range messages {
			//开启协程处理任务
			go func(msg *message.Message) {
				msg.Ack()

				data, _ := fn(msg)

				//core.E调试输出(
				//	"收到消息", core.E到文本(msg.Payload),
				//	"返回消息", core.E到文本(data),
				//	"返回结果队列", msg.Metadata["repTo"],
				//	"返回标志", msg.UUID,
				//)

				this.sendConn.Publish(msg.Metadata["repTo"], msg.UUID, data, "")

			}(msg)
		}
	})
}
