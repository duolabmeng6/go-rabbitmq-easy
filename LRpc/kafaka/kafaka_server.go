package kafaka2

import (
	. "duolabmeng6/go-rabbitmq-easy/LRpc"
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/duolabmeng6/efun/efun"
	"github.com/duolabmeng6/goefun/core"
	"github.com/gogf/gf/container/gtype"
)

type LRpcRedisServer struct {
	LRpcPubSub
	LRpcServer
	consumer  sarama.Consumer
	producer  sarama.AsyncProducer
	pushCount *gtype.Int
	link      string
}

//初始化消息队列
func NewLRpcRedisServer(link string) *LRpcRedisServer {
	this := new(LRpcRedisServer)
	this.link = link
	this.init()

	return this
}

//连接服务器
func (this *LRpcRedisServer) init() *LRpcRedisServer {
	core.E调试输出("连接到服务端")
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	this.consumer, err = sarama.NewConsumer([]string{this.link}, config)
	if err != nil {
		//efun.E调试输出格式化("consumer_test create consumer error %s\n", err.Error())
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
		//efun.E调试输出格式化("producer_test create producer error :%s\n", err.Error())
		return this
	}

	return this
}

//发布
func (this *LRpcRedisServer) publish(taskData *TaskData) error {
	//core.E调试输出("发布")
	// send message
	msg := &sarama.ProducerMessage{
		Topic: taskData.ReportTo,
		Key:   sarama.StringEncoder("go_test"),
	}

	jsondata, _ := json.Marshal(taskData)

	msg.Value = sarama.ByteEncoder(jsondata)

	// send to chain
	this.producer.Input() <- msg

	select {
	case _ = <-this.producer.Successes():
		//efun.E调试输出格式化("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
	case _ = <-this.producer.Errors():
		//efun.E调试输出格式化("err: %s\n", fail.Err.Error())
	}

	return nil
}

//订阅
func (this *LRpcRedisServer) subscribe(funcName string, fn func(TaskData)) error {
	core.E调试输出("订阅函数事件", funcName)

	partition_consumer, err := this.consumer.ConsumePartition(funcName, 0, sarama.OffsetNewest)
	if err != nil {
		//efun.E调试输出格式化("try create partition_consumer error %s\n", err.Error())
		return nil
	}

	for {
		select {
		case msg := <-partition_consumer.Messages():
			//efun.E调试输出格式化("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
			//	msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))

			taskData := TaskData{}
			json.Unmarshal(msg.Value, &taskData)
			//core.E调试输出("收到数据", taskData)
			go fn(taskData)

		case err := <-partition_consumer.Errors():
			efun.E调试输出格式化("err :%s\n", err.Error())
		}
	}

	return nil
}

//订阅
func (this *LRpcRedisServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	core.E调试输出("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//core.E调试输出("收到任务数据", data)
		if data.StartTime/1000+data.TimeOut < efun.E取时间戳() {
			//efun.E调试输出格式化("任务超时抛弃 %s \r\n", data.Fun)
			return
		}

		redata, flag := fn(data)
		data.Result = redata
		//core.E调试输出("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			//将结果返回给调用的客户端
			this.publish(&data)
		}

	})

}
