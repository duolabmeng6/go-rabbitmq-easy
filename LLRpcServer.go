package RabbitmqEasy

import (
	"github.com/duolabmeng6/goefun/core"
	"github.com/streadway/amqp"
	"time"
)

type LLRpcServer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
	link string
}

func NewLLRpcServer(link string) *LLRpcServer {
	this := new(LLRpcServer)
	this.link = link

	this.Init()
	return this
}

//连接
func (this *LLRpcServer) Init() *LLRpcServer {
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

//注册函数
func (this *LLRpcServer) Router(Path string, qps int, fn func(amqp.Delivery) ([]byte, bool)) {
	var err error

	arg := make(map[string]interface{}, 3)

	//arg["x-message-ttl"] = int64(5 * 60 * 1000)

	//声明队列
	this.q, err = this.ch.QueueDeclare(
		Path,  // 队列名称
		false, // 是否需要持久化
		false, //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
		false, // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
		false, // 是否等待服务器返回
		arg,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = this.ch.Qos(
		qps,   // prefetch count
		0,     // prefetch size
		false, // global
	)

	failOnError(err, "Failed to set QoS")

	ReceivedChan, err := this.ch.Consume(
		this.q.Name, // 消息要取得消息的队列名
		"",          // 消费者标签
		false,       // 服务器将确认 为true，使用者不应调用Delivery.Ack
		false,       // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
		false,       // no-local
		false,       // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
		nil,         // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for d := range ReceivedChan {
			//收到任务创建协程执行
			go func() {
				core.E调试输出("时间 收到数据", core.E取现行时间().E取时间戳毫秒())

				//回调函数  调用这个函数如果超时30秒 怎么让他停止这个函数的执行

				service := func() []byte {
					data, _ := fn(d)
					return data
				}
				data := AsyncServiceOut(service, 3*time.Second)
				core.E调试输出("处理结果", core.E到文本(data))

				this.ReturnResult(d, data)
				//d.Ack(flag == false)
				d.Ack(false)

			}()

		}
	}()
}

func AsyncService(service func() []byte) chan []byte {
	retCh := make(chan []byte, 1)
	go func() {
		ret := service()
		core.E调试输出("service()执行结束.")
		retCh <- ret
		core.E调试输出("service()返回值塞进通道.")
	}()
	return retCh
}

func AsyncServiceOut(service func() []byte, duration time.Duration) []byte {
	select {
	case ret := <-AsyncService(service):
		core.E调试输出("====", ret)
		return ret
	case <-time.After(duration):
		core.E调试输出("time out")
		return []byte{}

	}

}

//回调结果
func (this *LLRpcServer) ReturnResult(d amqp.Delivery, data []byte) {
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
