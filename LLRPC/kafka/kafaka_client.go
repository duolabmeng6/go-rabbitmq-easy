package kafka

import (
	. "duolabmeng6/go-rabbitmq-easy/LLRPC"
	"encoding/json"
	"errors"
	"github.com/Shopify/sarama"
	. "github.com/duolabmeng6/goefun/ecore"
	"github.com/duolabmeng6/goefun/etool"
	"github.com/gogf/gf/v2/container/gtype"

	"sync"
	"time"
)

type LRpcKafkaClient struct {
	LRpcPubSub
	LRpcClient

	//读写锁用于keychan的
	lock sync.RWMutex
	//等待消息回调的通道
	keychan map[string]chan TaskData
	link    string

	consumer  sarama.Consumer
	producer  sarama.AsyncProducer
	pushCount *gtype.Int
}

// 初始化消息队列
func NewLRpcKafkaClient(link string) *LRpcKafkaClient {
	this := new(LRpcKafkaClient)
	this.link = link
	this.keychan = map[string]chan TaskData{}

	this.init()
	this.listen()

	return this
}

// 连接服务器
func (this *LRpcKafkaClient) init() *LRpcKafkaClient {
	E调试输出("连接到服务端")
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	this.consumer, err = sarama.NewConsumer([]string{this.link}, config)
	if err != nil {
		E调试输出格式化("consumer_test create consumer error %s\n", err.Error())
		return this
	}

	config2 := sarama.NewConfig()
	config2.Producer.RequiredAcks = sarama.WaitForAll
	config2.Producer.Partitioner = sarama.NewRandomPartitioner
	config2.Producer.Return.Successes = true
	config2.Producer.Return.Errors = true
	config2.Version = sarama.V0_11_0_2

	this.producer, err = sarama.NewAsyncProducer([]string{this.link}, config2)
	if err != nil {
		E调试输出格式化("producer_test create producer error :%s\n", err.Error())
		return this
	}

	return this
}

// 发布
func (this *LRpcKafkaClient) publish(taskData *TaskData) error {
	//E调试输出("发布")
	// send message
	msg := &sarama.ProducerMessage{
		Topic: taskData.Fun,
		Key:   sarama.StringEncoder("go_test"),
	}

	jsondata, _ := json.Marshal(taskData)

	msg.Value = sarama.ByteEncoder(jsondata)

	// send to chain
	this.producer.Input() <- msg

	select {
	case _ = <-this.producer.Successes():
		//E调试输出格式化("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
	case _ = <-this.producer.Errors():
		//E调试输出格式化("err: %s\n", fail.Err.Error())
	}
	return nil
}

// 订阅
func (this *LRpcKafkaClient) subscribe(funcName string, fn func(TaskData)) error {
	E调试输出("订阅函数事件", funcName)

	partition_consumer, err := this.consumer.ConsumePartition(funcName, 0, sarama.OffsetNewest)
	if err != nil {
		E调试输出格式化("try create partition_consumer error %s\n", err.Error())
		return nil
	}

	for {
		select {
		case msg := <-partition_consumer.Messages():
			//E调试输出格式化("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
			//	msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))

			taskData := TaskData{}
			json.Unmarshal(msg.Value, &taskData)
			//E调试输出("收到数据", taskData)
			go fn(taskData)

		case err := <-partition_consumer.Errors():
			E调试输出格式化("err :%s\n", err.Error())
		}
	}

	return nil
}

func (this *LRpcKafkaClient) listen() {
	go func() {
		E调试输出("注册回调结果监听", "return")
		this.subscribe("return", func(data TaskData) {
			//E调试输出("收到回调结果:", data)
			this.returnChan(data.UUID, data)

		})
	}()

}

func (this *LRpcKafkaClient) Call(funcName string, data string, timeout int64) (TaskData, error) {
	var err error
	taskData := TaskData{}
	//任务id
	taskData.Fun = funcName
	//UUID
	taskData.UUID = etool.E取UUID()
	//任务数据
	taskData.Data = data
	//超时时间 1.pop 取出任务超时了 就放弃掉 2.任务在规定时间内未完成 超时 退出
	taskData.TimeOut = timeout
	//任务加入时间
	taskData.StartTime = E取现行时间().E取毫秒()
	//任务完成以后回调的频道名称
	taskData.ReportTo = "return"

	//注册通道
	mychan := this.newChan(taskData.UUID)

	this.publish(&taskData)

	//E调试输出("uuid", taskData.UUID)
	//等待通道的结果回调
	value, flag := this.waitResult(mychan, taskData.UUID, taskData.TimeOut)
	if flag == false {
		err = errors.New(E到文本(value.Result))
	}

	return value, err
}

func (this *LRpcKafkaClient) newChan(key string) chan TaskData {
	this.lock.Lock()
	this.keychan[key] = make(chan TaskData)
	mychan := this.keychan[key]
	this.lock.Unlock()
	return mychan
}

func (this *LRpcKafkaClient) returnChan(uuid string, data TaskData) {
	this.lock.RLock()
	funchan, ok := this.keychan[uuid]
	this.lock.RUnlock()
	if ok {
		funchan <- data
	} else {
		//E调试输出格式化("fun not find %s", fun)
	}
}

// 等待任务结果
func (this *LRpcKafkaClient) waitResult(mychan chan TaskData, key string, timeOut int64) (TaskData, bool) {
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
		return TaskData{Result: "timeout"}, false
	}
	return value, true
}
