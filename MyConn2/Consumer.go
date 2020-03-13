package MqConn

import (
	"context"
	"github.com/streadway/amqp"
	"log"
)

// Consumer implement rabbitroutine.Consumer interface.
type Consumer struct {
	ExchangeName string
	QueueName    string
	Fn           func(amqp.Delivery)
}

// Declare implement rabbitroutine.Consumer.(Declare) interface method.
func (c *Consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	var err error
	//err := ch.ExchangeDeclare(
	//	c.ExchangeName, // name
	//	"direct",       // type
	//	true,           // durable
	//	false,          // auto-deleted
	//	false,          // internal
	//	false,          // no-wait
	//	nil,            // arguments
	//)
	//if err != nil {
	//	log.Printf("failed to declare exchange %v: %v", c.ExchangeName, err)
	//
	//	return err
	//}

	_, err = ch.QueueDeclare(
		c.QueueName, // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,
	)
	if err != nil {
		log.Printf("failed to declare queue %v: %v", c.QueueName, err)

		return err
	}
	//
	//err = ch.QueueBind(
	//	c.QueueName,    // queue name
	//	c.QueueName,    // routing key
	//	c.ExchangeName, // exchange
	//	false,          // no-wait
	//	nil,            // arguments
	//)
	//if err != nil {
	//	log.Printf("failed to bind queue %v: %v", c.QueueName, err)
	//
	//	return err
	//}

	return nil
}

// Consume implement rabbitroutine.Consumer.(Consume) interface method.
func (c *Consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	defer log.Println("consume method finished")

	err := ch.Qos(
		1000,  // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		log.Printf("failed to set qos: %v", err)

		return err
	}

	msgs, err := ch.Consume(
		c.QueueName, // queue
		"",          // consumer name
		true,        // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Printf("failed to consume %v: %v", c.QueueName, err)

		return err
	}

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			//content := string(msg.Body)

			//fmt.Println("New message:", content)

			c.Fn(msg)

			err := msg.Ack(false)
			if err != nil {
				log.Printf("failed to Ack message: %v", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
