package RabbitmqEasy

import (
	"github.com/streadway/amqp"
	"log"
)

type RabbitmFanoutModel struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	q            amqp.Queue
	ReceivedChan <-chan amqp.Delivery
	link         string
	exchangeName string
}

func NewRabbitmFanoutModel(link string, exchangeName string) *RabbitmFanoutModel {
	this := new(RabbitmFanoutModel)
	this.link = link
	this.exchangeName = exchangeName

	this.Init()
	return this
}

//连接
func (this *RabbitmFanoutModel) Init() *RabbitmFanoutModel {
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
			"fanout",          // type
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
func (this *RabbitmFanoutModel) Publish(msg string) *RabbitmFanoutModel {
	err := this.ch.Publish(this.exchangeName, "", false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)})
	failOnError(err, "")
	return this
}

//订阅
func (this *RabbitmFanoutModel) Subscribe(queueName string) *RabbitmFanoutModel {
	var err error
	this.q, err = this.ch.QueueDeclare(queueName, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")
	log.Printf("监听队列%s 监听转发器 %s", this.q.Name, this.exchangeName)
	err = this.ch.QueueBind(
		this.q.Name,       // queue name
		"",                // routing key
		this.exchangeName, // exchange
		false,
		nil,
	)

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

func (this *RabbitmFanoutModel) Receive() <-chan amqp.Delivery {
	return this.ReceivedChan
}
