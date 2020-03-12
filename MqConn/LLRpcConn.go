package MqConn

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	_ "github.com/ThreeDotsLabs/watermill/message"
)

type LLRpcConn struct {
	amqpConfig amqp.Config
	publisher  *amqp.Publisher
}

func NewLLRpcConn(amqpURI string, Exclusivet bool, AutoDelete bool) *LLRpcConn {
	this := new(LLRpcConn)
	var err error

	this.amqpConfig = amqp.NewDurableQueueConfig(amqpURI)
	this.amqpConfig.Consume.Exclusive = Exclusivet
	this.amqpConfig.Queue.AutoDelete = AutoDelete
	this.amqpConfig.Consume.Qos.PrefetchCount = 100

	this.publisher, err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))
	if err != nil {
		panic("NewLLRpcConn " + err.Error())
	}

	return this
}

func (this *LLRpcConn) Publish(queue string, UUID string, data []byte, repTo string) {
	msg := message.NewMessage(UUID, data)
	if repTo != "" {
		msg.Metadata["repTo"] = repTo
	}
	//panic: cannot open channel: Exception (504) Reason: "channel id space exhausted"

	if err := this.publisher.Publish(queue, msg); err != nil {
		//panic("Publish " + err.Error())
		fmt.Println("Publish " + err.Error())

	}
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
