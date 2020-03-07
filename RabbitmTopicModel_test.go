package RabbitmqEasy

import (
	. "github.com/duolabmeng6/goefun/core"
	"log"
	"testing"
)

//发布消息
func TestRabbitmqTopicModel_Publish(t *testing.T) {
	//连接
	task := NewRabbitmTopicModel("amqp://admin:admin@182.92.84.229:5672/", "logs_topic")

	//发布消息
	for i := 1; i <= 10; i++ {
		task.Publish("user.info", E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"))
		task.Publish("user.order", E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"))

		task.Publish("user.error", E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"))
		task.Publish("core.error", E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"))

		E延时(100)
	}
}

//订阅消息
func TestRabbitmqTopicModel_Subscribe2(t *testing.T) {
	//连接
	task := NewRabbitmTopicModel("amqp://admin:admin@182.92.84.229:5672/", "logs_topic")
	server1 := 0

	//queueName 如果填写为 server1 相当于有名称为server1的1个队列 3个服务端一起处理 server1中的消息
	//最终 server1 消息会被 3个服务端消费掉
	//有100条消息 3个服务端均分消息
	//服务端1 收到33条
	//服务端2 收到33条
	//服务端3 收到34条

	for i := 0; i < 3; i++ {
		go func(i int) {
			//订阅
			task.Subscribe("", "core.error")
			//接受订阅数据
			for d := range task.Receive() {
				server1++
				log.Printf("收到次数%d 服务端%d core.error 的信息: %s", server1, i, d.Body)
			}
		}(i)
	}

	E延时(1000 * 60 * 60)

}

//模拟3个客户端同时处理的情况
func TestRabbitmqTopicModel_Subscribe3(t *testing.T) {
	//连接
	task := NewRabbitmTopicModel("amqp://admin:admin@182.92.84.229:5672/", "logs_topic")
	server1 := 0

	//queueName 如果填写为 server1 相当于有名称为server1的1个队列 3个服务端一起处理 server1中的消息
	//最终 server1 消息会被 3个服务端消费掉
	//有100条消息 3个服务端均分消息
	//服务端1 收到33条
	//服务端2 收到33条
	//服务端3 收到34条

	for i := 0; i < 3; i++ {
		go func(i int) {
			//订阅
			task.Subscribe("client", "core.error")
			//接受订阅数据
			for d := range task.Receive() {
				server1++
				log.Printf("收到次数%d 服务端%d core.error 的信息: %s", server1, i, d.Body)
			}
		}(i)
	}

	E延时(1000 * 60 * 60)

}
