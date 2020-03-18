package heightenMq

import (
	"context"
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gogf/gf/container/gtype"
	amqp2 "github.com/streadway/amqp"
	"log"
)

var (
	serverNotifyClose chan *amqp2.Error
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer

	amqpConfig amqp.Config
	publisher  *amqp.Publisher
	pushCount  *gtype.Int
	amqpURI    string
	channel    *amqp2.Channel
}

//初始化消息队列
func NewLRpcRedisServer(amqpURI string) *LRpcRedisServer {
	this := new(LRpcRedisServer)
	this.amqpURI = amqpURI
	this.init()

	return this
}

//连接服务器
func (this *LRpcRedisServer) init() *LRpcRedisServer {
	core.E调试输出("连接到服务端")
	var err error
	this.amqpConfig = amqp.NewDurableQueueConfig(this.amqpURI)
	this.amqpConfig.Consume.Exclusive = false
	this.amqpConfig.Queue.AutoDelete = false
	this.amqpConfig.Consume.Qos.PrefetchCount = 100
	this.amqpConfig.Queue.Durable = true
	this.amqpConfig.Consume.NoRequeueOnNack = false
	this.amqpConfig.Consume.NoWait = true

	this.publisher, err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))
	if err != nil {
		panic("NewLLRpcConn " + err.Error())
	}

	this.pushCount = gtype.NewInt()
	if this.channel, err = this.publisher.Connection().Channel(); err != nil {
		panic("channel" + err.Error())
	}

	serverNotifyClose = make(chan *amqp2.Error)
	this.channel.NotifyClose(serverNotifyClose)
	go this.handleReconnect()

	return this
}

func (this *LRpcRedisServer) handleReconnect() {
	for err := range serverNotifyClose {

		log.Println("断开了连接,", err.Code)
		core.E延时(10000)
		this.init()
	}
}

//发布
func (this *LRpcRedisServer) publish(taskData *TaskData) error {
	//core.E调试输出("发布")
	jsondata, _ := json.Marshal(taskData)
	msg := message.NewMessage(taskData.UUID, jsondata)
	for {
		//core.E调试输出(taskData.ReportTo)
		if err := this.publisher.Publish(taskData.ReportTo, msg); err != nil {
			//panic("Publish " + err.Error())
			core.E调试输出("重试 Publish " + err.Error())
			core.E延时(1000)
		} else {
			break
		}
	}
	return nil
}

//订阅
func (this *LRpcRedisServer) subscribe(funcName string, fn func(TaskData)) error {
	core.E调试输出("订阅函数事件", funcName)

	this.amqpConfig.Consume.Qos.PrefetchCount = 100

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

	messages, err := subscriber.Subscribe(context.Background(), funcName)
	if err != nil {
		panic("Subscribe2 " + err.Error())

	}
	for msg := range messages {
		msg.Ack()
		//开启协程处理任务
		go func(msg *message.Message) {
			taskData := TaskData{}
			json.Unmarshal(msg.Payload, &taskData)
			//core.E调试输出("收到数据", taskData)

			fn(taskData)
		}(msg)
	}

	return nil
}

//订阅
func (this *LRpcRedisServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	core.E调试输出("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//core.E调试输出("收到任务数据", data)
		if data.StartTime/1000+data.TimeOut < efun.E取时间戳() {
			efun.E调试输出格式化("任务超时抛弃 %s \r\n", data.Fun)
			return
		}

		redata, flag := fn(data)
		data.Result = redata
		//core.E调试输出("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			//将结果返回给调用的客户端
			this.publish(&data)
		}

	})

}
