package LLRPC

var (
	ResendCount    = 10    // 消息重发次数
	ResendDelay    = 5000  // 消息发送失败后，多久重发
	ReconnectDelay = 10000 // 连接断开后多久重连

)

// 调用任务的结构
type TaskData struct {
	//函数名称
	Fun string `json:"fun"`
	//成功后返回给谁
	ReportTo string `json:"report_to"`
	//uuid标识
	UUID string `json:"uuid"`
	//任务数据
	Data string `json:"data"`
	//加入任务时间
	StartTime int64 `json:"start_time"`
	//超时时间
	TimeOut int64 `json:"timeout"`
	//执行完成结果
	Result string `json:"result"`
	//完成时间
	CompleteTime int64 `json:"complete_time"`
}
type LLRPCPubSub interface {
	//初始化连接
	InitConnection()

	publish(*TaskData) error
	subscribe(funcName string, fn func(TaskData)) error
}

type LLRPCServer interface {
	//初始化连接
	LLRPCPubSub

	//注册函数
	Router(funcName string, fn func(TaskData) (string, bool))
}

type LLRPCClient interface {
	//初始化连接
	LLRPCPubSub

	//调用函数
	Call(funcName string) (TaskData, error)

	//监听订阅回调的结果
	listen()

	//创建一个通道接受结果
	newChan()

	//将结果返回给通道
	returnChan()

	//等待通道的结果
	waitResult()
}
