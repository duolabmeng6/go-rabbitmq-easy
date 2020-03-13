package MqConn

import (
	"context"
	"errors"
	_ "github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/goefun/core"
	"github.com/furdarius/rabbitroutine"
	"github.com/gogf/gf/container/gtype"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type LLRpcConn struct {
	pushCount *gtype.Int
	link      string
	pub       *rabbitroutine.RetryPublisher
	ctx       context.Context
}

func NewLLRpcConn(amqpURI string, Exclusivet bool, AutoDelete bool) *LLRpcConn {
	this := new(LLRpcConn)

	this.link = amqpURI

	this.ctx = context.Background()

	url := amqpURI

	conn := rabbitroutine.NewConnector(rabbitroutine.Config{
		// Max reconnect attempts
		ReconnectAttempts: 20,
		// How long wait between reconnect
		Wait: 2 * time.Second,
	})

	pool := rabbitroutine.NewPool(conn)
	ensurePub := rabbitroutine.NewEnsurePublisher(pool)
	this.pub = rabbitroutine.NewRetryPublisher(
		ensurePub,
		rabbitroutine.PublishMaxAttemptsSetup(16),
		rabbitroutine.PublishDelaySetup(rabbitroutine.LinearDelay(10*time.Millisecond)),
	)

	go func() {
		err := conn.Dial(this.ctx, url)
		if err != nil {
			log.Fatalf("failed to establish RabbitMQ connection: %v", err)
		}
	}()

	return this
}

var (
	// ErrTermSig used to notify that termination signal received.
	ErrTermSig = errors.New("termination signal caught")
)

// TermSignalTrap used to catch termination signal from OS
// and return error to golang.org/x/sync/errgroup
func TermSignalTrap(ctx context.Context) error {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, os.Interrupt, syscall.SIGTERM)

	select {
	case <-sigc:
		log.Println("termination signal caught")
		return ErrTermSig
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (this *LLRpcConn) Publish(queue string, UUID string, data []byte, repTo string) {
	timeoutCtx, cancel := context.WithTimeout(this.ctx, 100*time.Millisecond)
	core.E调试输出("ReplyTo", repTo)

	err := this.pub.Publish(timeoutCtx,
		"",
		queue,
		amqp.Publishing{
			Body:          data,
			ReplyTo:       repTo,
			CorrelationId: UUID,
		})

	if err != nil {
		log.Println("failed to publish:", err)
	}

	//Expiration:    core.E到文本(timeOut * 1000),
	//	ContentType:   "text/plain",
	//		CorrelationId: corrId,
	//		ReplyTo:       this.listenQueueName,
	//		Body:          data,
	//this.llConn.Publish(
	//	"",        // Exchange
	//	d.ReplyTo, // Routing key
	//	false,     // Mandatory
	//	false,     // Immediate
	//	amqp.Publishing{
	//		DeliveryMode:  2,
	//		ContentType:   "text/plain",
	//		CorrelationId: d.CorrelationId,
	//		Body:          data,
	//		Timestamp:     time.Now(),
	//	},
	//)
	cancel()

}

func (this *LLRpcConn) Subscribe(queue string, Consumer rabbitroutine.Consumer) {

	g, ctx := errgroup.WithContext(context.Background())

	url := this.link

	conn := rabbitroutine.NewConnector(rabbitroutine.Config{
		// How long wait between reconnect
		Wait: 2 * time.Second,
	})

	conn.AddRetriedListener(func(r rabbitroutine.Retried) {
		log.Printf("try to connect to RabbitMQ: attempt=%d, error=\"%v\"",
			r.ReconnectAttempt, r.Error)
	})

	conn.AddDialedListener(func(_ rabbitroutine.Dialed) {
		log.Printf("RabbitMQ connection successfully established")
	})

	conn.AddAMQPNotifiedListener(func(n rabbitroutine.AMQPNotified) {
		log.Printf("RabbitMQ error received: %v", n.Error)
	})

	g.Go(func() error {
		log.Println("conn.Start starting")
		defer log.Println("conn.Start finished")

		return conn.Dial(ctx, url)
	})

	//consumer := &Consumer{
	//	ExchangeName: "",
	//	QueueName:    queue,
	//	Fn:           fn,
	//}
	g.Go(func() error {
		log.Println("consumers starting")
		defer log.Println("consumers finished")

		return conn.StartMultipleConsumers(ctx, Consumer, 5)
	})

	g.Go(func() error {
		log.Println("signal trap starting")
		defer log.Println("signal trap finished")

		return TermSignalTrap(ctx)
	})

	if err := g.Wait(); err != nil && err != ErrTermSig {
		log.Fatal(
			"failed to wait goroutine group: ",
			err)
	}

}
