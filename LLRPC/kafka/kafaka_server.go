package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	. "github.com/duolabmeng6/go-rabbitmq-easy/LLRPC"
	. "github.com/duolabmeng6/goefun/ecore"

	"github.com/gogf/gf/v2/container/gtype"
)

type LLRPCKafkaServer struct {
	LLRPCPubSub
	LLRPCServer
	consumer  sarama.Consumer
	producer  sarama.AsyncProducer
	pushCount *gtype.Int
	link      string
}

// 初始化消息队列
func NewLLRPCKafkaServer(link string) *LLRPCKafkaServer {
	this := new(LLRPCKafkaServer)
	this.link = link
	this.InitConnection()

	return this
}

// 连接服务器
func (this *LLRPCKafkaServer) InitConnection() *LLRPCKafkaServer {
	fmt.Println("连接到服务端")
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	// consumer
	this.consumer, err = sarama.NewConsumer([]string{this.link}, config)
	if err != nil {
		//fmt.Println格式化("consumer_test create consumer error %s\n", err.Error())
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
		//fmt.Println格式化("producer_test create producer error :%s\n", err.Error())
		return this
	}

	return this
}

// 发布
func (this *LLRPCKafkaServer) publish(taskData *TaskData) error {
	//fmt.Println("发布")
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
		//fmt.Println格式化("offset: %d,  timestamp: %s", suc.Offset, suc.Timestamp.String())
	case _ = <-this.producer.Errors():
		//fmt.Println格式化("err: %s\n", fail.Err.Error())
	}

	return nil
}

// 订阅
func (this *LLRPCKafkaServer) subscribe(funcName string, fn func(TaskData)) error {
	fmt.Println("订阅函数事件", funcName)

	partition_consumer, err := this.consumer.ConsumePartition(funcName, 0, sarama.OffsetNewest)
	if err != nil {
		//fmt.Println格式化("try create partition_consumer error %s\n", err.Error())
		return nil
	}

	for {
		select {
		case msg := <-partition_consumer.Messages():
			//fmt.Println格式化("msg offset: %d, partition: %d, timestamp: %s, value: %s\n",
			//	msg.Offset, msg.Partition, msg.Timestamp.String(), string(msg.Value))

			taskData := TaskData{}
			json.Unmarshal(msg.Value, &taskData)
			//fmt.Println("收到数据", taskData)
			go fn(taskData)

		case err := <-partition_consumer.Errors():
			fmt.Printf("err :%s\n", err.Error())
		}
	}

	return nil
}

// 订阅
func (this *LLRPCKafkaServer) Router(funcName string, fn func(TaskData) (string, bool)) {
	fmt.Println("注册函数", funcName)
	this.subscribe(funcName, func(data TaskData) {
		//fmt.Println("收到任务数据", data)
		if data.StartTime/1000+data.TimeOut < E取现行时间().E取时间戳() {
			//fmt.Println格式化("任务超时抛弃 %s \r\n", data.Fun)
			return
		}

		redata, flag := fn(data)
		data.Result = redata
		//fmt.Println("处理完成", data, "将结果发布到", data.ReportTo)

		if flag {
			//将结果返回给调用的客户端
			this.publish(&data)
		}

	})

}
