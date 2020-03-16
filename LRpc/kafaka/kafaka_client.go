package kafaka

import (
	"context"
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/gogf/gf/container/gtype"
	"sync"
	"time"
)

type LRpcRedisClient struct {
	LRpcPubSub
	LRpcClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	link    string

	subscriber *kafka.Subscriber
	publisher  *kafka.Publisher
	pushCount  *gtype.Int
}

//初始化消息队列
func NewLRpcRedisClient(link string) *LRpcRedisClient {
	this := new(LRpcRedisClient)
	this.link = link
	this.keychan = map[string]chan TaskData{}

	this.init()
	this.listen()

	return this
}

//连接服务器
func (this *LRpcRedisClient) init() *LRpcRedisClient {
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
func (this *LRpcRedisClient) publish(taskData *TaskData) error {
	//core.E调试输出("发布")
	jsondata, _ := json.Marshal(taskData)
	msg := message.NewMessage(taskData.UUID, jsondata)
	for {
		if err := this.publisher.Publish(taskData.Fun, msg); err != nil {
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
func (this *LRpcRedisClient) subscribe(funcName string, fn func(TaskData)) error {
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

func (this *LRpcRedisClient) listen() {
	go func() {
		core.E调试输出("注册回调结果监听", "return")
		this.subscribe("return", func(data TaskData) {
			//core.E调试输出("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()

}

func (this *LRpcRedisClient) Call(funcName string, data string, timeout int64) (TaskData, error) {
	var err error
	taskData := TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = coreUtil.E取uuid()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = timeout
	//任务加入时间
	taskData.StartTime = efun.E取毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = "return"

	//注册通道
	mychan := this.newChan(taskData.UUID)

	this.publish(&taskData)

	//core.E调试输出("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := this.waitResult(mychan, taskData.UUID, taskData.TimeOut)
	if flag == false {
		err = errors.New(core.E到文本(value.Result))
	}

	return value, err
}

func (this *LRpcRedisClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LRpcRedisClient) returnChan(uuid string, data TaskData) {
	this.lock.RLock()
	funchan, ok := this.keychan[uuid]
	this.lock.RUnlock()
	if ok {
		funchan <- data
	} else {
		//E调试输出格式化("fun not find %s", fun)
	}
}

//等待任务结果
func (this *LRpcRedisClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
	//注册监听通道
	var value TaskData

	breakFlag := false
	timeOutFlag := false

	for {
		select {

		case data := <-mychan:
			//收到结果放进RUnlock()
			value = data
			breakFlag = true
		case <-time.After(time.Duration(timeOut) * time.Second):
			//超时跳出并且删除
			breakFlag = true
			timeOutFlag = true
		}
		if breakFlag {
			break
		}
	}
	//将通道的key删除
	this.lock.Lock()
	delete(this.keychan, key)
	this.lock.Unlock()

	if timeOutFlag {
		return TaskData{Result: "timeout"}, false
	}
	return value, true
}
