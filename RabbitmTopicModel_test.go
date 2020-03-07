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
	for i := 1; i <= 100; i++ {
		//task.Publish(E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"), "user.info")
		//task.Publish(E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"), "user.order")

		//task.Publish(E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"), "user.error")
		task.Publish("core.error", E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"))

		E延时(100)
	}
}

//订阅消息
func TestRabbitmqTopicModel_Subscribe(t *testing.T) {
	//连接
	task := NewRabbitmTopicModel("amqp://admin:admin@182.92.84.229:5672/", "logs_topic")
	//订阅
	task.Subscribe("", "*.info", "*.error")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			log.Printf("a收到 *.info,*.error 的信息: %s", d.Body)
		}
	}()
	//订阅
	task.Subscribe("", "core.error")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			log.Printf("a收到 core.error 的信息: %s", d.Body)
		}
	}()

	E延时(1000 * 60 * 60)
}

//订阅消息
func TestRabbitmqTopicModel_Subscribe2(t *testing.T) {
	//连接
	task := NewRabbitmTopicModel("amqp://admin:admin@182.92.84.229:5672/", "logs_topic")
	server1 := 0
	server2 := 0
	server3 := 0

	//queueName 不填写的话 相当于有3个随机队列 每个队列都会收到一样的消息
	//假如有100条消息
	//那么3个服务端都会收到100消息
	//服务端1 收到100条
	//服务端2 收到100条
	//服务端3 收到100条

	//queueName 如果填写为 server1 相当于有名称为server1的1个队列 3个服务端一起处理 server1中的消息
	//最终 server1 消息会被 3个服务端消费掉
	//有100条消息 3个服务端均分消息
	//服务端1 收到33条
	//服务端2 收到33条
	//服务端3 收到34条

	//订阅
	task.Subscribe("", "core.error")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server1++
			log.Printf("收到次数%d 服务端1 core.error 的信息: %s", server1, d.Body)
		}
	}()

	//订阅
	task.Subscribe("", "core.error")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server2++
			log.Printf("收到次数%d 服务端2 core.error 的信息: %s", server2, d.Body)
		}
	}()

	//订阅
	task.Subscribe("", "core.error")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server3++
			log.Printf("收到次数%d 服务端3 core.error 的信息: %s", server3, d.Body)
		}
	}()

	E延时(1000 * 60 * 60)

}
