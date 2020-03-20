package example

import (
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gomodule/redigo/redis"
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer

	//redis客户端
	redisPool *redis.Pool
	link      string
}

//初始化消息队列
func NewLRpcRedisServer(link string) *LRpcRedisServer {
	this := new(LRpcRedisServer)
	this.link = link
	this.init()

	//t := &TaskData{
	//	Fun:   "aaa",
	//	Queue: "func1",
	//}
	//
	//this.publish(t)
	//
	//this.subscribe("aaa", func(data TaskData) {
	//	core.E调试输出("收到数据")
	//	core.E调试输出(data)
	//
	//})

	return this
}

//连接服务器
func (this *LRpcRedisServer) init() *LRpcRedisServer {
	core.E调试输出("连接到服务端")

	return this
}

//发布
func (this *LRpcRedisServer) publish(funcname string, taskData *TaskData) error {
	core.E调试输出("发布")

	return nil
}

//订阅
func (this *LRpcRedisServer) subscribe(funcName string, fn func(TaskData)) error {
	core.E调试输出("订阅函数事件", funcName)

	t := TaskData{}
	t.Fun = funcName
	t.ReportTo = "return"
	fn(t)
	//core.E延时(1000)
	//core.E调试输出("测试调用函数 func1", funcName)
	//ret, err := this.Call("func1", "hello")
	//core.E调试输出("测试调用函数 func1 结果", ret, err)

	return nil
}

//订阅
func (this *LRpcRedisServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	core.E调试输出("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//core.E调试输出("收到任务数据", data)

		redata, flag := fn(data)
		data.Result = redata
		core.E调试输出("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			this.publish(data.ReportTo, &data)
		}

	})

}
