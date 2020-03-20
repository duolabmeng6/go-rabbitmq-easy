package LLRPCRabbmitMQ

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"

	"encoding/json"
	"errors"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	"sync"
	"time"
)

type LRpcRabbmitMQClient struct {
	LRpcPubSub
	LRpcClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	amqpURI string

	//发送用的
	send *LRpcRabbmit

	receive_result_name string
}

//初始化消息队列
func NewLRpcRabbmitMQClient(amqpURI string) *LRpcRabbmitMQClient {

	this := new(LRpcRabbmitMQClient)
	this.amqpURI = amqpURI
	this.keychan = map[string]chan TaskData{}

	this.init()
	this.listen()

	return this
}

//连接服务器
func (this *LRpcRabbmitMQClient) init() *LRpcRabbmitMQClient {
	core.E调试输出("连接到服务端")
	this.send = NewLRpcRabbmit(this.amqpURI, func(this *LRpcRabbmit) {

	})
	return this
}

//发布
func (this *LRpcRabbmitMQClient) publish(taskData *TaskData) (err error) {
	return this.send.Publish(taskData.Fun, taskData)
}

//订阅
func (this *LRpcRabbmitMQClient) subscribe(funcName string, fn func(TaskData)) error {

	NewLRpcRabbmit(this.amqpURI, func(this *LRpcRabbmit) {
		core.E调试输出("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			funcName, // 队列名称
			false,    // 是否需要持久化
			true,     //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
			false,    // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
			false,    // 是否等待服务器返回
			nil,      // arguments
		)
		if err != nil {
			core.E调试输出("QueueDeclare", err)
		}
		//监听队列
		this.msgs, err = this.channel.Consume(
			q.Name, // 消息要取得消息的队列名
			"",     // 消费者标签
			true,   // 服务器将确认 为true，使用者不应调用Delivery.Ack
			false,  // true 服务器将确保这是唯一的使用者 为false时，服务器将公平地分配跨多个消费者交付。
			false,  // no-local
			false,  // true时，不要等待服务器确认请求和立即开始交货。如果不能消费，一个渠道将引发异常并关闭通道
			nil,    // args
		)
		if err != nil {
			core.E调试输出("Consume", err)
		}
		go func() {
			for d := range this.msgs {
				//收到任务创建协程执行
				taskData := TaskData{}
				json.Unmarshal(d.Body, &taskData)
				//回调
				fn(taskData)
			}
		}()
	})

	return nil
}

func (this *LRpcRabbmitMQClient) listen() {
	go func() {
		this.receive_result_name = "receive_result_" + coreUtil.E取uuid()
		core.E调试输出("注册回调结果监听", this.receive_result_name)
		this.subscribe(this.receive_result_name, func(data TaskData) {
			//core.E调试输出("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()
}

func (this *LRpcRabbmitMQClient) Call(funcName string, data string, timeout int64) (TaskData, error) {
	var err error
	taskData := TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = coreUtil.E取uuid()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = timeout
	//任务加入时间
	taskData.StartTime = efun.E取毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = this.receive_result_name

	//注册通道
	mychan := this.newChan(taskData.UUID)

	this.publish(&taskData)

	//core.E调试输出("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := this.waitResult(mychan, taskData.UUID, taskData.TimeOut)
	if flag == false {
		err = errors.New(core.E到文本(value.Result))
	}

	return value, err
}

func (this *LRpcRabbmitMQClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LRpcRabbmitMQClient) returnChan(uuid string, data TaskData) {
	this.lock.RLock()
	funchan, ok := this.keychan[uuid]
	this.lock.RUnlock()
	if ok {
		funchan <- data
	} else {
		//E调试输出格式化("fun not find %s", fun)
	}
}

//等待任务结果
func (this *LRpcRabbmitMQClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
	//注册监听通道
	var value TaskData

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
		return TaskData{Result: "timeout"}, false
	}
	return value, true
}
