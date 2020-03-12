package MqConn

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gogf/gf/container/gtype"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	_ "github.com/ThreeDotsLabs/watermill/message"
)

type LLRpcConn struct {
	amqpConfig amqp.Config
	publisher  *amqp.Publisher
	//publisher2 [15]*amqp.Publisher
	pushCount *gtype.Int
}

func NewLLRpcConn(amqpURI string, Exclusivet bool, AutoDelete bool) *LLRpcConn {
	this := new(LLRpcConn)
	var err error
	this.amqpConfig = amqp.NewDurableQueueConfig(amqpURI)
	this.amqpConfig.Consume.Exclusive = Exclusivet
	this.amqpConfig.Queue.AutoDelete = AutoDelete
	this.amqpConfig.Consume.Qos.PrefetchCount = 100
	this.amqpConfig.Queue.Durable = false
	this.amqpConfig.Consume.NoRequeueOnNack = true

	this.publisher, err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))
	if err != nil {
		panic("NewLLRpcConn " + err.Error())
	}

	//for i := 0; i < len(this.publisher2); i++ {
	//	this.publisher2[i], err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))
	//	if err != nil {
	//		panic("NewLLRpcConn2 " + err.Error())
	//	}
	//}

	this.pushCount = gtype.NewInt()

	return this
}

func (this *LLRpcConn) Publish(queue string, UUID string, data []byte, repTo string) {
	msg := message.NewMessage(UUID, data)
	if repTo != "" {
		msg.Metadata["repTo"] = repTo
	}
	//panic: cannot open channel: Exception (504) Reason: "channel id space exhausted"
	this.pushCount.Add(1)
	for {
		//有什么办法让这个代码同时只有2000个在执行 如果超过了 就切换另外一个连接
		//conn := core.E取整((this.pushCount.Val() / 1000))
		////fmt.Println("推送次数超过了2000次拉 需要换一个连接数推送", this.pushCount.Val(), "选择了第几个连接", conn)
		//
		//if err := this.publisher2[conn].Publish(queue, msg); err != nil {
		//	fmt.Println("第几个连接", conn, "重试 Publish2 "+err.Error())
		//	//core.E延时(2000)
		//} else {
		//	break
		//}
		if err := this.publisher.Publish(queue, msg); err != nil {
			//panic("Publish " + err.Error())
			fmt.Println("重试 Publish " + err.Error())
			core.E延时(1000)
		} else {
			break
		}
	}
	defer this.pushCount.Add(-1)

}

func (this *LLRpcConn) Subscribe(queue string, qos int, fn func(<-chan *message.Message)) {
	this.amqpConfig.Consume.Qos.PrefetchCount = qos

	subscriber, err := amqp.NewSubscriber(
		// This config is based on this example: https://www.rabbitmq.com/tutorials/tutorial-two-go.html
		// It works as a simple queue.
		//
		// If you want to implement a Pub/Sub style service instead, check
		// https://watermill.io/docs/pub-sub-implementations/#amqp-consumer-groups
		this.amqpConfig,
		watermill.NewStdLogger(false, false),
	)

	if err != nil {
		panic("Subscribe " + err.Error())
	}

	messages, err := subscriber.Subscribe(context.Background(), queue)
	if err != nil {
		panic("Subscribe2 " + err.Error())

	}

	go fn(messages)

}
