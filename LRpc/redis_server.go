package LRpc

import (
	"encoding/json"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gomodule/redigo/redis"
	"time"
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer

	//redis客户端
	redisPool *redis.Pool
}

//初始化消息队列
func NewLRpcRedisServer(link string) *LRpcRedisServer {
	this := new(LRpcRedisServer)
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
	this.redisPool = &redis.Pool{
		MaxIdle:     1000,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", "127.0.0.1:6379",
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
func (this *LRpcRedisServer) publish(funcname string, taskData *TaskData) error {
	//core.E调试输出("发布")

	conn := this.redisPool.Get()
	defer conn.Close()

	jsondata, _ := json.Marshal(taskData)
	//core.E调试输出(string(jsondata))

	_, err := conn.Do("lpush", funcname, string(jsondata))
	if err != nil {
		core.E调试输出("PUBLISH Error", err.Error())
	}

	return nil
}

//订阅
func (this *LRpcRedisServer) subscribe(funcName string, fn func(TaskData)) error {
	core.E调试输出("订阅函数事件", funcName)

	go func() {
		for {
			taskData := TaskData{}

			conn := this.redisPool.Get()
			defer conn.Close()

			ret, _ := redis.Strings(conn.Do("brpop", funcName, 10))
			if len(ret) == 0 {
			} else {
				json.Unmarshal([]byte(ret[1]), &taskData)
				//core.E调试输出("收到数据", taskData)
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
	//			core.E调试输出格式化("%s: message: %s\n", v.Channel, v.Data)
	//
	//			taskData := TaskData{}
	//			json.Unmarshal([]byte( v.Data), &taskData)
	//
	//			fn(taskData)
	//
	//		case redis.Subscription:
	//			core.E调试输出格式化("%s: %s %d\n", v.Channel, v.Kind, v.Count)
	//		case error:
	//			core.E调试输出("subscribe error", v)
	//			//return v
	//
	//			psc = redis.PubSubConn{Conn: this.redisPool.Get()}
	//			psc.subscribe(funcName)
	//		}
	//
	//	}
	//
	//}()

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
		//core.E调试输出("处理完成", data)

		if flag {
			this.publish(data.Channel, &data)
		}

	})

}
