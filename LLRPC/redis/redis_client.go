package redis

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	"encoding/json"
	"errors"
	. "github.com/duolabmeng6/goefun/core"
	. "github.com/duolabmeng6/goefun/coreUtil"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

type LRpcRedisClient struct {
	LRpcPubSub
	LRpcClient

	//redis客户端
	redisPool *redis.Pool
	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	link    string
}

//初始化消息队列
func NewLRpcRedisClient(link string) *LRpcRedisClient {
	this := new(LRpcRedisClient)
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
func (this *LRpcRedisClient) init() *LRpcRedisClient {
	E调试输出("连接到服务端")
	this.redisPool = &redis.Pool{
		MaxIdle:     100,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", this.link,
				//redis.DialPassword(conf["Password"].(string)),
				redis.DialDatabase(int(0)),
				redis.DialConnectTimeout(240*time.Second),
				redis.DialReadTimeout(240*time.Second),
				redis.DialWriteTimeout(240*time.Second))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}

	return this
}

//发布
func (this *LRpcRedisClient) publish(taskData *TaskData) error {
	//E调试输出("发布")

	conn := this.redisPool.Get()
	defer conn.Close()

	jsondata, _ := json.Marshal(taskData)
	//E调试输出(string(jsondata))

	_, err := conn.Do("lpush", taskData.Fun, string(jsondata))
	if err != nil {
		E调试输出("PUBLISH Error", err.Error())
	}

	return nil
}

//订阅
func (this *LRpcRedisClient) subscribe(funcName string, fn func(TaskData)) error {
	E调试输出("订阅函数事件", funcName)

	go func() {
		for {
			taskData := TaskData{}

			conn := this.redisPool.Get()
			defer conn.Close()

			ret, _ := redis.Strings(conn.Do("brpop", funcName, 10))
			if len(ret) == 0 {
			} else {
				//E调试输出格式化("subscribe message: %s\n", ret[1])

				json.Unmarshal([]byte(ret[1]), &taskData)
				fn(taskData)
			}
		}
	}()

	//go func() {
	//	psc := redis.PubSubConn{Conn: this.redisPool.Get()}
	//	psc.subscribe(funcName)
	//	for {
	//		switch v := psc.Receive().(type) {
	//		case redis.Message:
	//			E调试输出格式化("%s: message: %s\n", v.Channel, v.Data)
	//
	//			taskData := TaskData{}
	//			json.Unmarshal(TaskData( v.Data), &taskData)
	//
	//			fn(taskData)
	//
	//		case redis.Subscription:
	//			E调试输出格式化("%s: %s %d\n", v.Channel, v.Kind, v.Count)
	//		case error:
	//			E调试输出("subscribe error", v)
	//			//return v
	//
	//			psc = redis.PubSubConn{Conn: this.redisPool.Get()}
	//			psc.subscribe(funcName)
	//		}
	//
	//	}
	//
	//}()

	return nil
}

func (this *LRpcRedisClient) listen() {
	go func() {
		E调试输出("注册回调结果监听", "return")
		this.subscribe("return", func(data TaskData) {
			E调试输出("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()

}

func (this *LRpcRedisClient) Call(funcName string, data string) (TaskData, error) {
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

func (this *LRpcRedisClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LRpcRedisClient) returnChan(uuid string, data TaskData) {
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
func (this *LRpcRedisClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
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
