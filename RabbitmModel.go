package Service

import (
	"github.com/streadway/amqp"
	"log"
)

type RabbitmModel struct {
	conn         *amqp.Connection
	ch           *amqp.Channel
	q            amqp.Queue
	ReceivedChan <-chan amqp.Delivery
	queueName    string
	link         string
}

func NewRabbitmModel(link string, queueName string) *RabbitmModel {
	this := new(RabbitmModel)
	this.queueName = queueName
	this.link = link
	this.Init()

	return this
}

//连接
func (this *RabbitmModel) Init() *RabbitmModel {
	if this.conn == nil {
		this.conn, _ = amqp.Dial(this.link)
	}
	if this.conn != nil {
		//defer conn.Close()
		if this.ch == nil {
			this.ch, _ = this.conn.Channel()
		}
		//defer ch.Close()
		if len(this.q.Name) <= 0 {
			this.q, _ = this.ch.QueueDeclare(this.queueName, false, false, false, false, nil)
		}
	}

	return this
}

//发布
func (this *RabbitmModel) Publish(msg string) *RabbitmModel {

	err := this.ch.Publish("", this.q.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte(msg)})
	FailOnError(err, "")

	return this
}

//订阅
func (this *RabbitmModel) Subscribe() *RabbitmModel {
	q, err := this.ch.QueueDeclare(
		this.queueName, // name
		false,          // durable
		false,          // delete when unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	this.ReceivedChan, err = this.ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	FailOnError(err, "Failed to register a consumer")
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

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
