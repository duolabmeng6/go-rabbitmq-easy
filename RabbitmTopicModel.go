package Service

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

	}
	return this
}

//发布
func (this *RabbitmTopicModel) Publish(msg string, key string) *RabbitmTopicModel {

	err := this.ch.Publish(this.exchangeName, key, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)})
	failOnError(err, "")
	return this
}

//订阅
func (this *RabbitmTopicModel) Subscribe(queueName string, keys ...interface{}) *RabbitmTopicModel {
	var err error

	this.q, err = this.ch.QueueDeclare(queueName, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	for _, p := range keys {
		key := core.E到文本(p)
		log.Printf("监听主题 %s", key)
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
