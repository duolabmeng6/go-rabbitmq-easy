package RabbitmqEasy

import (
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/streadway/amqp"
	"log"
	"strconv"
)

type RabbitmRpcModel struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	q            amqp.Queue
	ReceivedChan <-chan amqp.Delivery
	link         string
	exchangeName string
}

func NewRabbitmRpcModel(link string, exchangeName string) *RabbitmRpcModel {
	this := new(RabbitmRpcModel)
	this.link = link
	this.exchangeName = exchangeName

	this.Init()
	return this
}

//连接
func (this *RabbitmRpcModel) Init() *RabbitmRpcModel {
	var err error
	this.conn, err = amqp.Dial(this.link)
	failOnError(err, "Failed to connect to RabbitMQ")

	if this.conn != nil {
		//defer conn.Close()
		this.ch, err = this.conn.Channel()
		failOnError(err, "Failed to open a channel")

	}
	return this
}

//发布
func (this *RabbitmRpcModel) Call(data string) (res int, err error) {
	q, err := this.ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := this.ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := coreUtil.E取uuid()

	err = this.ch.Publish(
		"",          // exchange
		"rpc_queue", // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(data),
		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")
			break
		}
	}
	return
}

//订阅
func (this *RabbitmRpcModel) Subscribe(queueName string) *RabbitmRpcModel {
	var err error
	this.q, err = this.ch.QueueDeclare("rpc_queue", false, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")
	log.Printf("监听队列%s 监听转发器 %s", this.q.Name, this.exchangeName)

	err = this.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

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
func (this *RabbitmRpcModel) Callfun(d amqp.Delivery, data []byte) {

	err := this.ch.Publish(
		"",        // exchange
		d.ReplyTo, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: d.CorrelationId,
			Body:          data,
		})
	failOnError(err, "Failed to publish a message")

}
func (this *RabbitmRpcModel) Receive() <-chan amqp.Delivery {
	return this.ReceivedChan
}
