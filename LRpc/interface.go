package LRpc

//调用任务的结构
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
type LRpcPubSub interface {
	//初始化连接
	init()

	publish(*TaskData) error
	subscribe(funcName string, fn func(TaskData)) error
}

type LRpcServer interface {
	//初始化连接
	LRpcPubSub

	//注册函数
	Router(funcName string, fn func(TaskData) (string, bool))
}

type LRpcClient interface {
	//初始化连接
	LRpcPubSub

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
