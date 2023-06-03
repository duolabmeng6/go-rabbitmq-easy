package redis

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

type LLRPCRedisClient struct {
	LLRPC.LLRPCPubSub
	LLRPC.LLRPCClient

	//redis客户端
	redisPool *redis.Pool
	//等待消息回调的通道
	//keychan map[string]chan LLRPC.TaskData
	keychan sync.Map
	link    string
}

// 初始化消息队列
func NewLLRPCRedisClient(link string) *LLRPCRedisClient {
	c := new(LLRPCRedisClient)
	c.link = link

	c.InitConnection()
	c.listen()

	return c
}

// 连接服务器
func (c *LLRPCRedisClient) InitConnection() *LLRPCRedisClient {
	fmt.Println("连接到服务端")
	c.redisPool = &redis.Pool{
		MaxIdle:     1000,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial(
				"tcp",
				c.link,
				redis.DialDatabase(0),
				redis.DialConnectTimeout(240*time.Second),
				redis.DialReadTimeout(240*time.Second),
				redis.DialWriteTimeout(240*time.Second))
			if err != nil {
				return nil, err
			}
			return con, nil
		},
	}

	return c
}

// 发布
func (c *LLRPCRedisClient) publish(taskData *LLRPC.TaskData) error {
	//fmt.Println("发布")

	conn := c.redisPool.Get()

	jsondata, err := json.Marshal(taskData)
	if err != nil {
		fmt.Println("PUBLISH json Marshal Error", err.Error())
	}

	_, err = conn.Do("lpush", taskData.Fun, string(jsondata))
	if err != nil {
		fmt.Println("PUBLISH Error", err.Error())
	}
	conn.Close()

	return nil
}

// 订阅
func (c *LLRPCRedisClient) subscribe(funcName string, fn func(LLRPC.TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	go func() {
		for {
			taskData := LLRPC.TaskData{}

			conn := c.redisPool.Get()
			defer conn.Close()

			ret, _ := redis.Strings(conn.Do("brpop", funcName, 10))
			if len(ret) == 0 {
			} else {
				//fmt.Println格式化("subscribe message: %s\n", ret[1])

				json.Unmarshal([]byte(ret[1]), &taskData)
				fn(taskData)
			}
		}
	}()

	return nil
}

func (c *LLRPCRedisClient) listen() {
	go func() {
		fmt.Println("注册回调结果监听", "return")
		c.subscribe("return", func(data LLRPC.TaskData) {
			//fmt.Println("收到回调结果:", data)
			c.returnChan(data.UUID, data)

		})
	}()

}

func (c *LLRPCRedisClient) Call(funcName string, data string) (LLRPC.TaskData, error) {
	var err error
	taskData := LLRPC.TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = etool.E取UUID()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = 10
	//任务加入时间
	taskData.StartTime = ecore.E取现行时间().E取毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = "return"

	//注册通道
	mychan := c.newChan(taskData.UUID)

	c.publish(&taskData)

	//fmt.Println("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := c.waitResult(mychan, taskData.UUID, 10)
	if flag == false {
		err = errors.New(ecore.E到文本(value))
	}

	return value, err
}

func (c *LLRPCRedisClient) newChan(key string) chan LLRPC.TaskData {
	mychan := make(chan LLRPC.TaskData)
	c.keychan.Store(key, mychan)
	return mychan
}

func (c *LLRPCRedisClient) returnChan(uuid string, data LLRPC.TaskData) {
	value, ok := c.keychan.Load(uuid)
	if ok {
		funchan := value.(chan LLRPC.TaskData)
		funchan <- data
	}
}

// 等待任务结果
func (c *LLRPCRedisClient) waitResult(mychan chan LLRPC.TaskData, key string, timeOut int64) (LLRPC.TaskData, bool) {
	//注册监听通道
	var value LLRPC.TaskData

	breakFlag := false
	timeOutFlag := false

	for {
		select {
		case data := <-mychan:
			value = data
			breakFlag = true
		case <-time.After(time.Duration(timeOut) * time.Second):
			breakFlag = true
			timeOutFlag = true
		}

		if breakFlag {
			break
		}
	}

	c.keychan.Delete(key)

	if timeOutFlag {
		return LLRPC.TaskData{}, false
	}
	return value, true
}
