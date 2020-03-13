package MqConn

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	_ "github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gogf/gf/container/gtype"
)

type LLRpcConn struct {
	amqpConfig amqp.Config
	publisher  *amqp.Publisher
	pushCount  *gtype.Int
}

func NewLLRpcConn(amqpURI string, Exclusivet bool, AutoDelete bool) *LLRpcConn {
	this := new(LLRpcConn)
	var err error
	this.amqpConfig = amqp.NewDurableQueueConfig(amqpURI)
	this.amqpConfig.Consume.Exclusive = Exclusivet
	this.amqpConfig.Queue.AutoDelete = AutoDelete
	this.amqpConfig.Consume.Qos.PrefetchCount = 100
	this.amqpConfig.Queue.Durable = true
	this.amqpConfig.Consume.NoRequeueOnNack = false
	this.amqpConfig.Consume.NoWait = true

	this.publisher, err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))
	if err != nil {
		panic("NewLLRpcConn " + err.Error())
	}

	this.pushCount = gtype.NewInt()
	this.publisher.NewChannel()
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
