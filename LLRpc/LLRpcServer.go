package LLRpc

import (
	"fmt"
	"github.com/duolabmeng6/goefun/core"
	"github.com/streadway/amqp"
	"time"
)

type LLRpcServer struct {
	link       string
	connNotify chan *amqp.Error
	llConn     *LLRpcConn
}

func NewLLRpcServer(link string) *LLRpcServer {
	this := new(LLRpcServer)
	this.link = link

	return this
}

//注册函数
func (this *LLRpcServer) Router(Path string, qps int, fn func(amqp.Delivery) ([]byte, bool)) {
	producer := NewMq(Path, qps, this.link, func(mq *LLRpcConn) {
		mq.QueueDeclare(
			Path,
			false,
			false,
			false,
			false,
			nil,
		)
	})
	go func() {
		for d := range producer.ReceiveChan {
			//收到任务创建协程执行
			go func(d amqp.Delivery) {
				data, _ := fn(d)
				//fmt.Println(string(data))
				this.ReturnResult(producer, d, data)
			}(d)
		}
	}()

	for {
		err := producer.Receive()
		core.E调试输出(err)
		core.E延时(1000)
	}
}

//回调结果
func (this *LLRpcServer) ReturnResult(producer *LLRpcConn, d amqp.Delivery, data []byte) {
	if this.llConn == nil {
		this.llConn = NewMq("rpc_queue1", 10000, "amqp://admin:admin@182.92.84.229:5672/", func(mq *LLRpcConn) {

		})
	}

	fmt.Println(d.ReplyTo)
	this.llConn.Publish(
		"",        // Exchange
		d.ReplyTo, // Routing key
		false,     // Mandatory
		false,     // Immediate
		amqp.Publishing{
			DeliveryMode:  2,
			ContentType:   "text/plain",
			CorrelationId: d.CorrelationId,
			Body:          data,
			Timestamp:     time.Now(),
		},
	)
}
