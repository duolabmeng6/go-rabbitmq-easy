package redis

import (
	"encoding/json"
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	"net"
	"runtime"
	"time"

	"github.com/go-redis/redis"
)

type LLRPCRedisServer struct {
	LLRPC.LLRPCPubSub
	LLRPC.LLRPCServer

	//redis客户端
	redisPool *redis.Client
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
	fmt.Println("LLRPC Redis 服务器启动")
	c.redisPool = redis.NewClient(&redis.Options{
		//连接信息
		Network:  "tcp",            //网络类型，tcp or unix，默认tcp
		Addr:     "127.0.0.1:6379", //主机名+冒号+端口，默认localhost:6379
		Password: "",               //密码
		DB:       0,                // redis数据库index

		//连接池容量及闲置连接数量
		PoolSize:     4 * runtime.NumCPU(), // 连接池最大socket连接数，默认为4倍CPU数， 4 * runtime.NumCPU
		MinIdleConns: runtime.NumCPU(),     //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。

		//超时
		DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
		IdleTimeout:        5 * time.Minute,  //闲置超时，默认5分钟，-1表示取消闲置超时检查
		MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接

		//命令执行失败时的重试策略
		MaxRetries:      0,                      // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

		//可自定义连接函数
		Dialer: func() (net.Conn, error) {
			netDialer := &net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 5 * time.Minute,
			}
			return netDialer.Dial("tcp", c.link)
		},

		//钩子函数
		OnConnect: func(conn *redis.Conn) error { //仅当客户端执行命令时需要从连接池获取连接时，如果连接池需要新建连接时则会调用此钩子函数
			//fmt.Printf("conn=%v\n", conn)
			return nil
		},
	})

	return c
}

// 发布
func (c *LLRPCRedisServer) publish(funcname string, taskData *LLRPC.TaskData) error {
	//fmt.Println("发布")

	jsondata, err := json.Marshal(taskData)
	if err != nil {
		return err
	}

	err = c.redisPool.LPush(funcname, string(jsondata)).Err()
	if err != nil {
		fmt.Println("PUBLISH Error", err.Error())
	}
	//设置过期时间 由于是即时消息队列 所以过期时间设置短一点
	c.redisPool.Expire(funcname, time.Second*300)

	return nil
}

// 订阅
func (c *LLRPCRedisServer) subscribe(funcName string, fn func(LLRPC.TaskData)) error {
	fmt.Println("订阅函数事件", funcName, runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			for {
				ret, err := c.redisPool.BRPop(time.Second*60, funcName).Result()

				if err != nil {
					fmt.Println("subscribe BRPOP Error:", err)
				}
				if len(ret) > 0 {
					go func() {
						taskData := LLRPC.TaskData{}
						err := json.Unmarshal([]byte(ret[1]), &taskData)
						if err != nil {
							fmt.Println("subscribe json Unmarshal Error:", err)
						}
						//fmt.Println("收到数据", taskData)
						fn(taskData)
					}()
				}

			}
		}()
	}

	return nil
}

// 订阅
func (c *LLRPCRedisServer) Router(funcName string, fn func(LLRPC.TaskData) (string, bool)) {
	fmt.Println("注册函数", funcName)
	c.subscribe(funcName, func(data LLRPC.TaskData) {
		//fmt.Println("收到任务数据", data)

		redata, flag := fn(data)
		data.Result = redata
		//fmt.Println("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			c.publish(data.ReportTo, &data)
		}

	})

}
