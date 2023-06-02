package example

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"
)

type LRpcExampleServer struct {
	LRpcPubSub
	LRpcServer

	//Example客户端
	link string
}

// 初始化消息队列
func NewLRpcExampleServer(link string) *LRpcExampleServer {
	this := new(LRpcExampleServer)
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
	//	fmt.Println("收到数据")
	//	fmt.Println(data)
	//
	//})

	return this
}

// 连接服务器
func (this *LRpcExampleServer) init() *LRpcExampleServer {
	fmt.Println("连接到服务端")

	return this
}

// 发布
func (this *LRpcExampleServer) publish(funcname string, taskData *TaskData) error {
	fmt.Println("发布")

	return nil
}

// 订阅
func (this *LRpcExampleServer) subscribe(funcName string, fn func(TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	t := TaskData{}
	t.Fun = funcName
	t.ReportTo = "return"
	fn(t)
	//E延时(1000)
	//fmt.Println("测试调用函数 func1", funcName)
	//ret, err := this.Call("func1", "hello")
	//fmt.Println("测试调用函数 func1 结果", ret, err)

	return nil
}

// 订阅
func (this *LRpcExampleServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	fmt.Println("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//fmt.Println("收到任务数据", data)

		redata, flag := fn(data)
		data.Result = redata
		fmt.Println("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			this.publish(data.ReportTo, &data)
		}

	})

}
