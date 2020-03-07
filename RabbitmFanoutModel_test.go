package RabbitmqEasy

import (
	. "github.com/duolabmeng6/goefun/core"
	"log"
	"testing"
)

//发布消息
func TestRabbitmqModel_Publishaa(t *testing.T) {
	//连接
	task := NewRabbitmFanoutModel("amqp://admin:admin@182.92.84.229:5672/", "logs_fanout")
	//发布消息
	for i := 1; i <= 1; i++ {
		task.Publish(E到文本(i) + " hello" + E取现行时间().E时间到文本("Y-m-d H:i:s"))
		task.Publish(E到文本(i) + " ok" + E取现行时间().E时间到文本("Y-m-d H:i:s"))
		E延时(100)
	}
}

//订阅消息
//等于2个客户端同时接一样的消息 例如2条信息 那么 一共就是 4次
func TestRabbitmqModel_Subscribebb(t *testing.T) {
	//连接
	task := NewRabbitmFanoutModel("amqp://admin:admin@182.92.84.229:5672/", "logs_fanout")
	server1 := 0
	server2 := 0
	//订阅
	task.Subscribe("")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server1++
			log.Printf("收到次数%d 服务端%d 的信息: %s", server1, 0, d.Body)

		}
	}()

	//订阅
	task.Subscribe("")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server2++
			log.Printf("收到次数%d 服务端%d hello 的信息: %s", server2, 1, d.Body)

		}
	}()

	E延时(1000 * 60 * 60)
}

//订阅消息 设置为 client 一样的话 他就只会被消费1次
//等于2个客户端同时消费1个队列 例如 2条消息 那么一共就是2次
func TestRabbitmqModel_Subscribebb333(t *testing.T) {
	//连接
	task := NewRabbitmFanoutModel("amqp://admin:admin@182.92.84.229:5672/", "logs_fanout")
	server1 := 0
	server2 := 0

	task.Subscribe("client")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server1++
			log.Printf("收到次数%d 服务端%d 的信息: %s", server1, 0, d.Body)

		}
	}()

	//订阅
	task.Subscribe("client")
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			server2++
			log.Printf("收到次数%d 服务端%d 的信息: %s", server2, 1, d.Body)

		}
	}()

	E延时(1000 * 60 * 60)
}
