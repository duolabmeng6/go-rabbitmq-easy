package LLRPCRabbmitMQ

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"

	"encoding/json"
	"github.com/duolabmeng6/goefun/core"
	"github.com/streadway/amqp"
	"log"
)

type LRpcRabbmit struct {
	LRpcPubSub
	amqpURI           string
	conn              *amqp.Connection
	channel           *amqp.Channel
	clientNotifyClose chan *amqp.Error

	funcName string
	msgs     <-chan amqp.Delivery
	fn       func(TaskData)
	success  func(channel *LRpcRabbmit)
}

//初始化消息队列
func NewLRpcRabbmit(amqpURI string, success func(this *LRpcRabbmit)) *LRpcRabbmit {
	this := new(LRpcRabbmit)
	this.amqpURI = amqpURI
	this.success = success

	this.init()

	return this
}

//连接服务器
func (this *LRpcRabbmit) init() bool {
	core.E调试输出("连接到服务端")
	var err error

	if this.conn, err = amqp.Dial(this.amqpURI); err != nil {
		//panic("Final to conn  :" + err.Error())
		core.E调试输出("重连 amqp.Dial")
		core.E延时(int64(ReconnectDelay))

		return this.init()
	}

	if this.channel, err = this.conn.Channel(); err != nil {
		//panic("Final to channel :" + err.Error())
		core.E调试输出("重连 Channel")
		core.E延时(int64(ReconnectDelay))
		this.init()
		return this.init()
	}
	this.clientNotifyClose = make(chan *amqp.Error)
	this.channel.NotifyClose(this.clientNotifyClose)
	go this.handleReconnect()

	this.success(this)

	return true
}

func (this *LRpcRabbmit) handleReconnect() {
	for err := range this.clientNotifyClose {
		log.Println("断开了连接,", err.Code)
		core.E延时(3000)
		if this.init() {
			log.Println("重新连接订阅")

		}

	}
}

//发布
func (this *LRpcRabbmit) Publish(queueName string, taskData *TaskData) (err error) {
	//core.E调试输出("发布消息", queueName)

	jsondata, _ := json.Marshal(taskData)
	errCount := 0
	for {
		if err = this.channel.Publish("", queueName, false, false, amqp.Publishing{
			Body: jsondata,
		}); err != nil {
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

//订阅
func (this *LRpcRabbmit) Subscribe(fn func(TaskData)) error {
	this.fn = fn

	return nil
}
