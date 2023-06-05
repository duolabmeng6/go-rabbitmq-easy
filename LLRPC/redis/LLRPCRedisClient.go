package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	"github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/go-redis/redis"
	"net"
	"runtime"
	"sync"
	"time"
)

type LLRPCRedisClient struct {
	LLRPC.LLRPCPubSub
	LLRPC.LLRPCClient

	//redis客户端
	redisPool *redis.Client

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
	fmt.Println("连接到服务端 线程池:", runtime.NumCPU())
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
func (c *LLRPCRedisClient) publish(taskData *LLRPC.TaskData) error {
	//fmt.Println("发布")
	jsondata, err := json.Marshal(taskData)
	if err != nil {
		fmt.Println("PUBLISH json Marshal Error", err.Error())
	}
	err = c.redisPool.LPush(taskData.Fun, string(jsondata)).Err()
	if err != nil {
		fmt.Println("PUBLISH Error", err.Error())
	}
	return nil
}

// 订阅
func (c *LLRPCRedisClient) subscribe(funcName string, fn func(LLRPC.TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	go func() {
		for {
			ret, err := c.redisPool.BRPop(time.Second*60, funcName).Result()

			if err != nil {
				fmt.Println("subscribe BRPOP Error:", err)
				ecore.E延时(1)
			}
			if len(ret) > 0 {
				taskData := LLRPC.TaskData{}

				err := json.Unmarshal([]byte(ret[1]), &taskData)
				if err != nil {
					fmt.Println("subscribe json Unmarshal Error:", err)
				}
				//fmt.Println("收到数据", taskData)
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
