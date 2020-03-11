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
	this.producer = NewMq("", 1, this.link, func(mq *LLRpcConn) {

	})

	//开始调用结果监听队列
	this.listen()
	return this
}

//开始调用结果监听队列

func (this *LLRpcClient) listen() {
	this.listenQueueName = "receive_result_" + coreUtil.E取uuid()
	this.producer2 = NewMq(this.listenQueueName, 10000, this.link, func(mq *LLRpcConn) {
		mq.QueueDeclare(
			this.listenQueueName,
			false,
			false,
			true,
			false,
			nil,
		)
	})

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
	core.E调试输出(err)

	failOnError(err, "Failed to publish a message")

	value, _ := this.waitResult(corrId, timeOut)

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
