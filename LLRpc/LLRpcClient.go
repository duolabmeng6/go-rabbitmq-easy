package LLRpc

import (
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type LLRpcClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
	link string
	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan         map[string]chan []byte
	listenQueueName string
	producer        *LLRpcConn
	producer2       *LLRpcConn
}

func NewLLRpcClient(link string) *LLRpcClient {
	this := new(LLRpcClient)
	this.link = link
	this.keychan = map[string]chan []byte{}

	this.Init()
	return this
}

//连接
func (this *LLRpcClient) Init() *LLRpcClient {
	this.producer = NewMq("", this.link)

	//var err error
	////连接队列
	//this.conn, err = amqp.Dial(this.link)
	//failOnError(err, "Failed to connect to RabbitMQ")
	//
	//if this.conn != nil {
	//	//defer conn.Close()
	//	//连接通道
	//	this.ch, err = this.conn.Channel()
	//	failOnError(err, "Failed to open a channel")
	//
	//}
	//开始调用结果监听队列
	this.listen()

	return this
}

//开始调用结果监听队列

func (this *LLRpcClient) listen() {
	this.listenQueueName = "receive_result_" + coreUtil.E取uuid()
	this.producer2 = NewMq(this.listenQueueName, this.link)
	go func() {
		for d := range this.producer2.ReceiveChan {
			//收到任务创建协程执行
			go func(d amqp.Delivery) {
				fun := d.CorrelationId
				this.lock.RLock()
				funchan, ok := this.keychan[fun]
				this.lock.RUnlock()
				if ok {
					funchan <- d.Body
				} else {
					//E调试输出格式化("fun not find %s", fun)
				}
			}(d)
		}
	}()
	go func() {
		for {
			err := this.producer2.Receive()
			core.E调试输出(err)
			core.E延时(1000)
		}
	}()

	//
	////声明队列
	//q, err := this.ch.QueueDeclare(
	//	"",    // 队列名称
	//	false, // 是否需要持久化
	//	false, //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
	//	true,  // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
	//	false, // 是否等待服务器返回
	//	nil,   // arguments
	//)
	//failOnError(err, "Failed to declare a queue")
	//this.listenQueueName = q.Name
	//
	//core.E调试输出("开始调用结果监听队列", this.listenQueueName)
	////监听队列
	//msgs, err := this.ch.Consume(
	//	q.Name, // 消息要取得消息的队列名
	//	"",     // 消费者标签
	//	true,   // 服务器将确认 为true，使用者不应调用Delivery.Ack
	//	false,  // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
	//	false,  // no-local
	//	false,  // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
	//	nil,    // args
	//)
	//failOnError(err, "Failed to register a consumer")
	//
	//go func() {
	//	//接受队列消息
	//	for d := range msgs {
	//		fun := d.CorrelationId
	//		this.lock.RLock()
	//		funchan, ok := this.keychan[fun]
	//		this.lock.RUnlock()
	//		if ok {
	//			funchan <- d.Body
	//		} else {
	//			//E调试输出格式化("fun not find %s", fun)
	//		}
	//
	//	}
	//}()

}

//发布
func (this *LLRpcClient) Call(Path string, data []byte, timeOut int64) (res []byte, err error) {
	if timeOut == 0 {
		timeOut = 60
	}
	corrId := coreUtil.E取uuid()

	//发送消息到任务队列
	err = this.producer.Publish(
		"",    // 交换机名称
		Path,  // 路由kyes
		false, // 当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，那么会调用basic.return方法将消息返还给生产者；当mandatory设为false时，出现上述情形broker会直接将消息扔掉。
		false, // 当immediate标志位设置为true时，如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，那么这条消息不会放入队列中。
		amqp.Publishing{
			Expiration:    core.E到文本(timeOut * 1000),
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       this.listenQueueName,
			Body:          data,
		})
	failOnError(err, "Failed to publish a message")

	value, _ := this.waitResult(corrId, timeOut)

	//if timeOut == 0 {
	//	timeOut = 60
	//}
	//corrId := coreUtil.E取uuid()
	//
	////发送消息到任务队列
	//err = this.ch.Publish(
	//	"",    // 交换机名称
	//	Path,  // 路由kyes
	//	false, // 当mandatory标志位设置为true时，如果exchange根据自身类型和消息routeKey无法找到一个符合条件的queue，那么会调用basic.return方法将消息返还给生产者；当mandatory设为false时，出现上述情形broker会直接将消息扔掉。
	//	false, // 当immediate标志位设置为true时，如果exchange在将消息route到queue(s)时发现对应的queue上没有消费者，那么这条消息不会放入队列中。
	//	amqp.Publishing{
	//		Expiration:    core.E到文本(timeOut * 1000),
	//		ContentType:   "text/plain",
	//		CorrelationId: corrId,
	//		ReplyTo:       this.listenQueueName,
	//		Body:          data,
	//	})
	//failOnError(err, "Failed to publish a message")
	//
	//value, _ := this.waitResult(corrId, timeOut)

	return value, nil
}

//等待任务结果
func (this *LLRpcClient) waitResult(key string, timeOut int64) ([]byte, bool) {
	//注册监听通道
	this.lock.Lock()
	this.keychan[key] = make(chan []byte)
	mychan := this.keychan[key]
	this.lock.Unlock()

	var value []byte

	breakFlag := false
	timeOutFlag := false

	for {
		select {

		case data := <-mychan:
			//收到结果放进RUnlock()
			value = data
			breakFlag = true
		case <-time.After(time.Duration(timeOut) * time.Second):
			//超时跳出并且删除
			breakFlag = true
			timeOutFlag = true
		}
		if breakFlag {
			break
		}
	}
	//将通道的key删除
	this.lock.Lock()
	delete(this.keychan, key)
	this.lock.Unlock()

	if timeOutFlag {
		return core.E到字节集("time out"), false
	}
	return value, true
}
