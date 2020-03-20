package heightenMq

import (
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
)

type LRpcRabbmitMQServer struct {
	LRpcPubSub
	LRpcServer

	//发送用的
	send *LRpcRabbmit

	amqpURI string
}

//初始化消息队列
func NewLRpcRabbmitMQServer(amqpURI string) *LRpcRabbmitMQServer {
	this := new(LRpcRabbmitMQServer)
	this.amqpURI = amqpURI
	this.init()

	return this
}

//连接服务器
func (this *LRpcRabbmitMQServer) init() *LRpcRabbmitMQServer {
	core.E调试输出("连接到服务端")
	this.send = NewLRpcRabbmit(this.amqpURI, func(this *LRpcRabbmit) {

	})

	return this
}

//发布
func (this *LRpcRabbmitMQServer) publish(taskData *TaskData) (err error) {

	return this.send.Publish(taskData.ReportTo, taskData)

}

//订阅
func (this *LRpcRabbmitMQServer) subscribe(funcName string, fn func(TaskData)) error {
	NewLRpcRabbmit(this.amqpURI, func(this *LRpcRabbmit) {
		core.E调试输出("连接成功开始订阅队列")
		q, err := this.channel.QueueDeclare(
			funcName, // 队列名称
			true,     // 是否需要持久化
			false,    //是否自动删除，当最后一个消费者断开连接之后队列是否自动被删除
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
				go fn(taskData)
			}
		}()
	})

	return nil
}

//订阅
func (this *LRpcRabbmitMQServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	core.E调试输出("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//core.E调试输出("收到任务数据", data)
		if data.StartTime/1000+data.TimeOut < efun.E取时间戳() {
			//efun.E调试输出格式化("任务超时抛弃 %s \r\n", data.Fun)
			return
		}

		redata, flag := fn(data)
		data.Result = redata
		//core.E调试输出("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			//将结果返回给调用的客户端
			this.publish(&data)
		}

	})

}
