package Service

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
		task.Publish(E到文本(i)+" hello"+E取现行时间().E时间到文本("Y-m-d H:i:s"), "core.error")

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
