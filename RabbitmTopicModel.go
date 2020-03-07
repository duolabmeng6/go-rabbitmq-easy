package RabbitmqEasy

import (
	"github.com/duolabmeng6/goefun/core"
	"github.com/streadway/amqp"
	"log"
)

type RabbitmTopicModel struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	q            amqp.Queue
	ReceivedChan <-chan amqp.Delivery
	link         string
	exchangeName string
}

//link 连接 地址
//exchangeName 转发器的名称
func NewRabbitmTopicModel(link string, exchangeName string) *RabbitmTopicModel {
	this := new(RabbitmTopicModel)
	this.link = link
	this.exchangeName = exchangeName

	this.Init()
	return this
}

//连接
func (this *RabbitmTopicModel) Init() *RabbitmTopicModel {
	var err error
	this.conn, err = amqp.Dial(this.link)
	failOnError(err, "Failed to connect to RabbitMQ")

	if this.conn != nil {
		//defer conn.Close()
		this.ch, err = this.conn.Channel()
		failOnError(err, "Failed to open a channel")

		//创建转发器
		err = this.ch.ExchangeDeclare(
			this.exchangeName, // name
			"topic",           // type
			true,              // durable
			false,             // auto-deleted
			false,             // internal
			false,             // no-wait
			nil,               // arguments
		)
		failOnError(err, "Failed to declare an exchange")
		err = this.ch.Qos(1, 0, true)
		failOnError(err, "Failed to Qos")
	}
	return this
}

//发布
func (this *RabbitmTopicModel) Publish(key string, msg string) *RabbitmTopicModel {

	err := this.ch.Publish(this.exchangeName, key, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)})
	failOnError(err, "")
	return this
}

//订阅
//queueName如果是"" 则会随机创建队列 如果填写则监听您锁填写的队列名称  相当于有多少个消费者
//keys 监听的kyes 例如 user.* logs.info.*
func (this *RabbitmTopicModel) Subscribe(queueName string, keys ...interface{}) *RabbitmTopicModel {
	var err error

	this.q, err = this.ch.QueueDeclare(queueName, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	for _, p := range keys {
		key := core.E到文本(p)
		log.Printf("监听队列%s 主题 %s", this.q.Name, key)
		err = this.ch.QueueBind(
			"",                // queue name
			key,               // routing key
			this.exchangeName, // exchange
			false,
			nil,
		)
	}

	this.ReceivedChan, err = this.ch.Consume(
		this.q.Name, // queue
		"",          // consumer
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	failOnError(err, "Failed to register a consumer")
	//go func() {
	//	for d := range ReceivedChan {
	//		log.Printf("Received a message: %s", d.Body)
	//	}
	//}()

	return this
}

func (this *RabbitmTopicModel) Receive() <-chan amqp.Delivery {
	return this.ReceivedChan
}
