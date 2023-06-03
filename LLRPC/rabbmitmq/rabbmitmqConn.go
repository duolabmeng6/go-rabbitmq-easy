package LLRPCRabbmitMQ

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	"fmt"
	. "github.com/duolabmeng6/goefun/ecore"

	"encoding/json"
	"github.com/streadway/amqp"
	"log"
)

type LLRPCRabbmitConn struct {
	LLRPCPubSub
	amqpURI           string
	conn              *amqp.Connection
	channel           *amqp.Channel
	clientNotifyClose chan *amqp.Error

	funcName string
	msgs     <-chan amqp.Delivery
	fn       func(TaskData)
	success  func(channel *LLRPCRabbmitConn)
}

// 初始化消息队列
func NewLLRPCRabbmitConn(amqpURI string, success func(this *LLRPCRabbmitConn)) *LLRPCRabbmitConn {
	this := new(LLRPCRabbmitConn)
	this.amqpURI = amqpURI
	this.success = success

	this.InitConnection()

	return this
}

// 连接服务器
func (this *LLRPCRabbmitConn) InitConnection() bool {
	fmt.Println("连接到服务端")
	var err error

	if this.conn, err = amqp.Dial(this.amqpURI); err != nil {
		//panic("Final to conn  :" + err.Error())
		fmt.Println("重连 amqp.Dial")
		E延时(int64(ReconnectDelay))

		return this.InitConnection()
	}

	if this.channel, err = this.conn.Channel(); err != nil {
		//panic("Final to channel :" + err.Error())
		fmt.Println("重连 Channel")
		E延时(int64(ReconnectDelay))
		this.InitConnection()
		return this.InitConnection()
	}
	this.clientNotifyClose = make(chan *amqp.Error)
	this.channel.NotifyClose(this.clientNotifyClose)
	go this.handleReconnect()

	this.success(this)

	return true
}

func (this *LLRPCRabbmitConn) handleReconnect() {
	for err := range this.clientNotifyClose {
		log.Println("断开了连接,", err.Code)
		E延时(3000)
		if this.InitConnection() {
			log.Println("重新连接订阅")

		}

	}
}

// 发布
func (this *LLRPCRabbmitConn) Publish(queueName string, taskData *TaskData) (err error) {
	//fmt.Println("发布消息", queueName)

	jsondata, _ := json.Marshal(taskData)
	errCount := 0
	for {
		if err = this.channel.Publish("", queueName, false, false, amqp.Publishing{
			Body: jsondata,
		}); err != nil {
			fmt.Println("重试 Publish " + err.Error())
			E延时(int64(ResendDelay))
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

// 订阅
func (this *LLRPCRabbmitConn) Subscribe(fn func(TaskData)) error {
	this.fn = fn

	return nil
}
