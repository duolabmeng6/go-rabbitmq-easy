package redis

import (
	"duolabmeng6/go-rabbitmq-easy/LLRPC"
	"encoding/json"
	"fmt"

	"github.com/gomodule/redigo/redis"
	"time"
)

type LLRPCRedisServer struct {
	LLRPC.LLRPCPubSub
	LLRPC.LLRPCServer

	//redis客户端
	redisPool *redis.Pool
	link      string
}

// 初始化消息队列
func NewLLRPCRedisServer(link string) *LLRPCRedisServer {
	c := new(LLRPCRedisServer)
	c.link = link
	c.InitConnection()

	return c
}

// 连接服务器
func (c *LLRPCRedisServer) InitConnection() *LLRPCRedisServer {
	fmt.Println("连接到服务端")
	c.redisPool = &redis.Pool{
		MaxIdle:     100,
		MaxActive:   0,
		IdleTimeout: 240 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			con, err := redis.Dial("tcp", c.link,
				//redis.DialPassword(conf["Password"].(string)),
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
func (c *LLRPCRedisServer) publish(funcname string, taskData *LLRPC.TaskData) error {
	//fmt.Println("发布")

	conn := c.redisPool.Get()
	defer conn.Close()

	jsondata, err := json.Marshal(taskData)
	if err != nil {
		return err
	}

	_, err = conn.Do("lpush", funcname, string(jsondata))
	if err != nil {
		fmt.Println("PUBLISH Error", err.Error())
	}

	return nil
}

// 订阅
func (c *LLRPCRedisServer) subscribe(funcName string, fn func(LLRPC.TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	go func() {
		for {
			taskData := LLRPC.TaskData{}

			conn := c.redisPool.Get()
			defer conn.Close()

			ret, err := redis.Strings(conn.Do("BRPOP", funcName, 10))
			if err != nil {
				fmt.Println("subscribe BRPOP Error:", err)
			}
			if len(ret) > 0 {
				err := json.Unmarshal([]byte(ret[1]), &taskData)
				if err != nil {
					fmt.Println("subscribe json Unmarshal Error:", err)
				}
				fmt.Println("收到数据", taskData)
				fn(taskData)
			}

		}
	}()
	return nil
}

// 订阅
func (c *LLRPCRedisServer) Router(funcName string, fn func(LLRPC.TaskData) (string, bool)) {
	fmt.Println("注册函数", funcName)
	c.subscribe(funcName, func(data LLRPC.TaskData) {
		//fmt.Println("收到任务数据", data)

		redata, flag := fn(data)
		data.Result = redata
		fmt.Println("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			c.publish(data.ReportTo, &data)
		}

	})

}
