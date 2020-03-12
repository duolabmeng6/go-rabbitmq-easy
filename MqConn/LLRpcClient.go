package MqConn

import (
	"errors"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/duolabmeng6/goefun/core"
	"github.com/duolabmeng6/goefun/coreUtil"
	os "github.com/duolabmeng6/goefun/os/定时任务"
	"github.com/gogf/gf/container/gtype"
	"runtime"
	"sync"
	"time"
)

type LLRpcClient struct {
	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan         map[string]chan []byte
	listenQueueName string

	link     string
	sendConn *LLRpcConn
	receive  *LLRpcConn
}

func NewLLRpcClient(link string) *LLRpcClient {
	this := new(LLRpcClient)
	this.link = link
	this.keychan = map[string]chan []byte{}

	//发送消息连接
	this.sendConn = NewLLRpcConn(this.link, true, true)
	//接受回调结果队列
	this.receive = NewLLRpcConn(this.link, true, true)

	this.listen()

	return this
}

//开始调用结果监听队列

func (this *LLRpcClient) listen() {
	successCount := gtype.NewInt()
	os.E时钟_创建(func() bool {
		core.E调试输出("回调结果接收数量", successCount.Val(), "协程数量", runtime.NumGoroutine())
		return true
	}, 1000)

	this.listenQueueName = "listen_result_" + coreUtil.E取uuid()

	core.E调试输出("订阅回调结果队列", this.listenQueueName)
	this.receive.Subscribe(this.listenQueueName, 100, func(messages <-chan *message.Message) {
		for msg := range messages {
			successCount.Add(1)

			//log.Printf("process_client received message: %s, payload: %s", msg.UUID, string(msg.Payload))

			fun := msg.UUID
			this.lock.RLock()
			funchan, ok := this.keychan[fun]
			this.lock.RUnlock()
			if ok {
				funchan <- msg.Payload
			} else {
				//E调试输出格式化("fun not find %s", fun)
			}

			msg.Ack()
		}
	})

}

//发布
func (this *LLRpcClient) Call(Path string, data []byte, timeOut int64) (res []byte, err error) {
	if timeOut == 0 {
		timeOut = 60
	}
	corrId := coreUtil.E取uuid()

	//注册通道
	mychan := this.returnChan(corrId)

	go this.sendConn.Publish(Path, corrId, data, this.listenQueueName)

	//等待通道的结果回调
	value, flag := this.waitResult(mychan, corrId, timeOut)
	if flag == false {
		err = errors.New(core.E到文本(value))
	}
	return value, err
}

func (this *LLRpcClient) returnChan(key string) chan []byte {
	this.lock.Lock()
	this.keychan[key] = make(chan []byte)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

//等待任务结果
func (this *LLRpcClient) waitResult(mychan chan []byte, key string, timeOut int64) ([]byte, bool) {
	//注册监听通道
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
