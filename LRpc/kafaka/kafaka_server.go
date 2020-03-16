package kafaka

import (
	"context"
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gogf/gf/container/gtype"
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer
	subscriber *kafka.Subscriber
	publisher  *kafka.Publisher
	pushCount  *gtype.Int
	link       string
}

//初始化消息队列
func NewLRpcRedisServer(link string) *LRpcRedisServer {
	this := new(LRpcRedisServer)
	this.link = link
	this.init()

	return this
}

//连接服务器
func (this *LRpcRedisServer) init() *LRpcRedisServer {
	core.E调试输出("连接到服务端")
	var err error
	saramaSubscriberConfig := kafka.DefaultSaramaSubscriberConfig()
	// equivalent of auto.offset.reset: earliest
	saramaSubscriberConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	this.subscriber, err = kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               []string{this.link},
			Unmarshaler:           kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaSubscriberConfig,
			ConsumerGroup:         "test_consumer_group",
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	this.publisher, err = kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:   []string{this.link},
			Marshaler: kafka.DefaultMarshaler{},
		},
		watermill.NewStdLogger(false, false),
	)
	if err != nil {
		panic(err)
	}

	return this
}

//发布
func (this *LRpcRedisServer) publish(taskData *TaskData) error {
	//core.E调试输出("发布")
	jsondata, _ := json.Marshal(taskData)
	msg := message.NewMessage(taskData.UUID, jsondata)
	for {
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

	messages, err := this.subscriber.Subscribe(context.Background(), funcName)
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
		core.E调试输出("收到任务数据", data)
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
