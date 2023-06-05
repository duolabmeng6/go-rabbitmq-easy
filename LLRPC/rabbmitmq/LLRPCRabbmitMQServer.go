package LLRPCRabbmitMQ

import (
	"fmt"
	. "github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"

	"encoding/json"
)

type Server struct {
	LLRPCPubSub
	LLRPCServer

	//发送用的
	send *Conn

	amqpURI string
}

// 初始化消息队列
func NewServer(amqpURI string) *Server {
	this := new(Server)
	this.amqpURI = amqpURI
	this.InitConnection()

	return this
}

// 连接服务器
func (this *Server) InitConnection() *Server {
	this.send = NewConn(this.amqpURI, func(this *Conn) {

	})
	return this
}

// 发布
func (this *Server) publish(taskData *TaskData) (err error) {

	return this.send.Publish(taskData.ReportTo, taskData)

}

// 订阅
func (this *Server) subscribe(funcName string, fn func(TaskData)) error {
	NewConn(this.amqpURI, func(this *Conn) {
		fmt.Println("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			funcName, // 队列名称
			true,     // 是否需要持久化
			false,    //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
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
				go fn(taskData)
			}
		}()
	})

	return nil
}

// 订阅
func (this *Server) Router(funcName string, fn func(TaskData) (string, bool)) {
	fmt.Println("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//fmt.Println("收到任务数据", data)
		if data.StartTime/1000+data.TimeOut < E取现行时间().E取时间戳() {
			//fmt.Println格式化("任务超时抛弃 %s \r\n", data.Fun)
			return
		}

		redata, flag := fn(data)
		data.Result = redata
		//fmt.Println("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			//将结果返回给调用的客户端
			this.publish(&data)
		}

	})

}
