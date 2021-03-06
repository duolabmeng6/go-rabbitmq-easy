package example

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	"errors"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"

	"sync"
	"time"
)

type LRpcExampleClient struct {
	LRpcPubSub
	LRpcClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	link    string
}

//初始化消息队列
func NewLRpcExampleClient(link string) *LRpcExampleClient {
	this := new(LRpcExampleClient)
	this.link = link
	this.keychan = map[string]chan TaskData{}

	this.init()
	this.listen()
	//t := &TaskData{
	//	Fun:   "aaa",
	//	Queue: "func1",
	//}
	//
	//this.publish(t)
	//
	//this.subscribe("aaa", func(data TaskData) {
	//	E调试输出("收到数据")
	//	E调试输出(data)
	//
	//})

	return this
}

//连接服务器
func (this *LRpcExampleClient) init() *LRpcExampleClient {
	E调试输出("连接到服务端")

	return this
}

//发布
func (this *LRpcExampleClient) publish(taskData *TaskData) error {
	E调试输出("发布")

	return nil
}

//订阅
func (this *LRpcExampleClient) subscribe(funcName string, fn func(TaskData)) error {
	E调试输出("订阅函数事件", funcName)

	return nil
}

func (this *LRpcExampleClient) listen() {
	go func() {
		E调试输出("注册回调结果监听", "return")
		this.subscribe("return", func(data TaskData) {
			E调试输出("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()

}

func (this *LRpcExampleClient) Call(funcName string, data string) (TaskData, error) {
	var err error
	taskData := TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = E取uuid()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = 10
	//任务加入时间
	taskData.StartTime = E取现行时间().E取毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = "return"

	//注册通道
	mychan := this.newChan(taskData.UUID)

	this.publish(&taskData)

	E调试输出("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := this.waitResult(mychan, taskData.UUID, 10)
	if flag == false {
		err = errors.New(E到文本(value))
	}

	return value, err
}

func (this *LRpcExampleClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LRpcExampleClient) returnChan(uuid string, data TaskData) {
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
func (this *LRpcExampleClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
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
		return TaskData{}, false
	}
	return value, true
}
