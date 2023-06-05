package LLRPCRabbmitMQ

import (
	"fmt"
	. "github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"

	"encoding/json"
	"errors"
	"sync"
	"time"
)

type LLRPCRabbmitMQClient struct {
	LLRPCPubSub
	LLRPCClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	amqpURI string

	//发送用的
	send *LLRPCRabbmitConn

	receive_result_name string
}

// 初始化消息队列
func NewLLRPCRabbmitMQClient(amqpURI string) *LLRPCRabbmitMQClient {

	this := new(LLRPCRabbmitMQClient)
	this.amqpURI = amqpURI
	this.keychan = map[string]chan TaskData{}

	this.InitConnection()
	this.listen()

	return this
}

// 连接服务器
func (this *LLRPCRabbmitMQClient) InitConnection() *LLRPCRabbmitMQClient {
	fmt.Println("连接到服务端")
	this.send = NewLLRPCRabbmitConn(this.amqpURI, func(this *LLRPCRabbmitConn) {

	})
	return this
}

// 发布
func (this *LLRPCRabbmitMQClient) publish(taskData *TaskData) (err error) {
	return this.send.Publish(taskData.Fun, taskData)
}

// 订阅
func (this *LLRPCRabbmitMQClient) subscribe(funcName string, fn func(TaskData)) error {

	NewLLRPCRabbmitConn(this.amqpURI, func(this *LLRPCRabbmitConn) {
		fmt.Println("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			funcName, // 队列名称
			false,    // 是否需要持久化
			true,     //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
			false,    // 如果为真 当连接关闭时connection.close()该队列是否会自动删除 其他通道channel是不能访问的
			false,    // 是否等待服务器返回
			nil,      // arguments
		)
		if err != nil {
			fmt.Println("QueueDeclare", err)
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
			fmt.Println("Consume", err)
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

func (this *LLRPCRabbmitMQClient) listen() {
	go func() {
		this.receive_result_name = "receive_result_" + etool.E取UUID()
		fmt.Println("注册回调结果监听", this.receive_result_name)
		this.subscribe(this.receive_result_name, func(data TaskData) {
			//fmt.Println("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()
}

func (this *LLRPCRabbmitMQClient) Call(funcName string, data string, timeout int64) (TaskData, error) {
	var err error
	taskData := TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = etool.E取UUID()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = timeout
	//任务加入时间
	taskData.StartTime = E取现行时间().E取时间戳毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = this.receive_result_name

	//注册通道
	mychan := this.newChan(taskData.UUID)

	this.publish(&taskData)

	//fmt.Println("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := this.waitResult(mychan, taskData.UUID, taskData.TimeOut)
	if flag == false {
		err = errors.New(E到文本(value.Result))
	}

	return value, err
}

func (this *LLRPCRabbmitMQClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LLRPCRabbmitMQClient) returnChan(uuid string, data TaskData) {
	this.lock.RLock()
	funchan, ok := this.keychan[uuid]
	this.lock.RUnlock()
	if ok {
		funchan <- data
	} else {
		//fmt.Println格式化("fun not find %s", fun)
	}
}

// 等待任务结果
func (this *LLRPCRabbmitMQClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
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
