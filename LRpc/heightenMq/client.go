package heightenMq

import (
	"context"
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"errors"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/gogf/gf/container/gtype"
	amqp2 "github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

var (
	clientNotifyClose chan *amqp2.Error
)

type LRpcRedisClient struct {
	LRpcPubSub
	LRpcClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	amqpURI string

	amqpConfig amqp.Config
	//	publisher  *amqp.Publisher

	pushCount *gtype.Int
	conn      *amqp2.Connection
	channel   *amqp2.Channel

	receive_result_name string
}

//初始化消息队列
func NewLRpcRedisClient(amqpURI string) *LRpcRedisClient {

	this := new(LRpcRedisClient)
	this.amqpURI = amqpURI
	this.keychan = map[string]chan TaskData{}

	this.init()
	this.listen()

	return this
}

//连接服务器
func (this *LRpcRedisClient) init() *LRpcRedisClient {
	core.E调试输出("连接到服务端")
	var err error

	this.amqpConfig = amqp.NewDurableQueueConfig(this.amqpURI)
	this.amqpConfig.Consume.Exclusive = true
	this.amqpConfig.Queue.AutoDelete = true
	this.amqpConfig.Consume.Qos.PrefetchCount = 100
	this.amqpConfig.Queue.Durable = true
	this.amqpConfig.Consume.NoRequeueOnNack = false
	this.amqpConfig.Consume.NoWait = true
	/*
		this.publisher, err = amqp.NewPublisher(this.amqpConfig, watermill.NewStdLogger(false, false))

		if err != nil {
			panic("NewLLRpcConn " + err.Error())
		}
		if this.channel,err = this.publisher.Connection().Channel() ;err != nil{
			panic("channel" + err.Error())
		}

		clientNotifyClose = make(chan * amqp2.Error)
		this.channel.NotifyClose(clientNotifyClose);

		this.pushCount = gtype.NewInt()
	*/

	if this.conn, err = amqp2.Dial("amqp://admin:admin@182.92.84.229:5672/"); err != nil {
		panic("Final to conn  :" + err.Error())
	}

	if this.channel, err = this.conn.Channel(); err != nil {
		panic("Final to channel :" + err.Error())
	}
	clientNotifyClose = make(chan *amqp2.Error)
	this.channel.NotifyClose(clientNotifyClose)

	//q, err := this.channel.QueueDeclare("rpc_queue1", false, false, false, false, nil)

	go this.handleReconnect()
	return this
}

func (this *LRpcRedisClient) handleReconnect() {
	for err := range clientNotifyClose {

		log.Println("断开了连接,", err.Code)
		core.E延时(int64(ReconnectDelay))
		this.init()
	}
}

//发布
func (this *LRpcRedisClient) publish(taskData *TaskData) (err error) {
	//core.E调试输出("发布")
	jsondata, _ := json.Marshal(taskData)
	msg := message.NewMessage(taskData.UUID, jsondata)
	/*
		for {
			if err := this.publisher.Publish(taskData.Fun, msg); err != nil {
				//panic("Publish " + err.Error())
				core.E调试输出("重试 Publish " + err.Error())
				core.E延时(5000)
			} else {
				break
			}
		}*/
	m := Marshal(msg)
	errCount := 0
	for {
		if err = this.channel.Publish("", taskData.Fun, false, false, m); err != nil {
			core.E调试输出("重试 Publish " + err.Error())
			core.E延时(int64(ResendDelay))
			if errCount < ResendCount {
				errCount++
			} else {
				break
			}
		} else {
			break
		}
	}
	return
}

func Marshal(msg *message.Message) amqp2.Publishing {
	headers := make(amqp2.Table, len(msg.Metadata)+1) // metadata + plus uuid

	for key, value := range msg.Metadata {
		headers[key] = value
	}
	headers[amqp.MessageUUIDHeaderKey] = msg.UUID

	publishing := amqp2.Publishing{
		Body:    msg.Payload,
		Headers: headers,
	}
	/*
		if !d.NotPersistentDeliveryMode {
			publishing.DeliveryMode = amqp.Persistent
		}

		if d.PostprocessPublishing != nil {
			publishing = d.PostprocessPublishing(publishing)
		}*/

	return publishing
}

//订阅
func (this *LRpcRedisClient) subscribe(funcName string, fn func(TaskData)) error {
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

func (this *LRpcRedisClient) listen() {
	go func() {
		this.receive_result_name = "receive_result_" + coreUtil.E取uuid()

		core.E调试输出("注册回调结果监听", this.receive_result_name)
		this.subscribe(this.receive_result_name, func(data TaskData) {
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
	taskData.ReportTo = this.receive_result_name

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
