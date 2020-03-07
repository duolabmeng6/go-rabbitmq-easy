package RabbitmqEasy

import (
	"github.com/streadway/amqp"
	"log"
)

type RabbitmModel struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	q            amqp.Queue
	ReceivedChan <-chan amqp.Delivery
	link         string
	exchangeName string
}

func NewRabbitmModel(link string, exchangeName string) *RabbitmModel {
	this := new(RabbitmModel)

	this.link = link
	this.exchangeName = exchangeName

	this.Init()
	return this
}

//连接
func (this *RabbitmModel) Init() *RabbitmModel {
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
			"direct",          // type
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
func (this *RabbitmModel) Publish(queueName string, msg string) *RabbitmModel {

	err := this.ch.Publish(this.exchangeName, queueName, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)})
	failOnError(err, "")
	return this
}

//订阅
func (this *RabbitmModel) Subscribe(queueName string) *RabbitmModel {
	var err error
	this.q, err = this.ch.QueueDeclare(queueName, false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

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

func (this *RabbitmModel) Receive() <-chan amqp.Delivery {
	return this.ReceivedChan
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
