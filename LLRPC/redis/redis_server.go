package redis

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"time"
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer

	//redis客户端
	redisPool *redis.Pool
	link      string
}

// 初始化消息队列
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
	//	fmt.Println("收到数据")
	//	fmt.Println(data)
	//
	//})

	return this
}

// 连接服务器
func (this *LRpcRedisServer) init() *LRpcRedisServer {
	fmt.Println("连接到服务端")
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

// 发布
func (this *LRpcRedisServer) publish(funcname string, taskData *TaskData) error {
	//fmt.Println("发布")

	conn := this.redisPool.Get()
	defer conn.Close()

	jsondata, _ := json.Marshal(taskData)
	//fmt.Println(string(jsondata))

	_, err := conn.Do("lpush", funcname, string(jsondata))
	if err != nil {
		fmt.Println("PUBLISH Error", err.Error())
	}

	return nil
}

// 订阅
func (this *LRpcRedisServer) subscribe(funcName string, fn func(TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	go func() {
		for {
			taskData := TaskData{}

			conn := this.redisPool.Get()
			defer conn.Close()

			ret, _ := redis.Strings(conn.Do("brpop", funcName, 10))
			if len(ret) == 0 {
			} else {
				json.Unmarshal([]byte(ret[1]), &taskData)
				fmt.Println("收到数据", taskData)
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
	//			fmt.Println格式化("%s: message: %s\n", v.Channel, v.Data)
	//
	//			taskData := TaskData{}
	//			json.Unmarshal([]byte( v.Data), &taskData)
	//
	//			fn(taskData)
	//
	//		case redis.Subscription:
	//			fmt.Println格式化("%s: %s %d\n", v.Channel, v.Kind, v.Count)
	//		case error:
	//			fmt.Println("subscribe error", v)
	//			//return v
	//
	//			psc = redis.PubSubConn{Conn: this.redisPool.Get()}
	//			psc.subscribe(funcName)
	//		}
	//
	//	}
	//
	//}()

	//E延时(1000)
	//fmt.Println("测试调用函数 func1", funcName)
	//ret, err := this.Call("func1", "hello")
	//fmt.Println("测试调用函数 func1 结果", ret, err)

	return nil
}

// 订阅
func (this *LRpcRedisServer) Router(funcName string, fn func(TaskData) (string, bool)) {
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
