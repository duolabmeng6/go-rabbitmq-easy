package LLRpc

import (
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/streadway/amqp"
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
	//连接队列
	this.conn, err = amqp.Dial(this.link)
	failOnError(err, "Failed to connect to RabbitMQ")

	if this.conn != nil {
		//defer conn.Close()
		//连接通道
		this.ch, err = this.conn.Channel()
		failOnError(err, "Failed to open a channel")

	}
	return this
}

//发布
func (this *RabbitmRpcModel) Call(data string) (res int, err error) {
	//声明队列
	q, err := this.ch.QueueDeclare(
		"",    // 队列名称
		false, // 是否需要持久化
		false, //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
		true,  // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
		false, // 是否等待服务器返回
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	//监听队列
	msgs, err := this.ch.Consume(
		q.Name, // 消息要取得消息的队列名
		"",     // 消费者标签
		true,   // 服务器将确认 为true，使用者不应调用Delivery.Ack
		false,  // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
		false,  // no-local
		false,  // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := coreUtil.E取uuid()

	//发送消息到任务队列
	err = this.ch.Publish(
		"",          // 交换机名称
		"rpc_queue", // 路由kyes
		false,       // 当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，那么会调用basic.return方法将消息返还给生产者；当mandatory设为false时，出现上述情形broker会直接将消息扔掉。
		false,       // 当immediate标志位设置为true时，如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，那么这条消息不会放入队列中。
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       q.Name,
			Body:          []byte(data),
		})
	failOnError(err, "Failed to publish a message")

	//接受队列消息
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
func (this *RabbitmRpcModel) Subscribe() *RabbitmRpcModel {
	var err error

	//声明队列
	this.q, err = this.ch.QueueDeclare(
		"rpc_queue", // 队列名称
		false,       // 是否需要持久化
		false,       //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
		true,        // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
		false,       // 是否等待服务器返回
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = this.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	this.ReceivedChan, err = this.ch.Consume(
		this.q.Name, // 消息要取得消息的队列名
		"",          // 消费者标签
		false,       // 服务器将确认 为true，使用者不应调用Delivery.Ack
		false,       // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
		false,       // no-local
		false,       // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
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
func (this *RabbitmRpcModel) ReturnResult(d amqp.Delivery, data []byte) {

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

	d.Ack(false)

}
func (this *RabbitmRpcModel) Receive() <-chan amqp.Delivery {
	return this.ReceivedChan
}
