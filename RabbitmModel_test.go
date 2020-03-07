package Service

import (
	. "github.com/duolabmeng6/goefun/core"
	"log"
	"testing"
)

//发布消息
func TestRabbitmqModel_Publish(t *testing.T) {
	//连接
	task := NewRabbitmModel("amqp://admin:admin@182.92.84.229:5672/", "hello_queue")
	//发布消息
	for i := 1; i < 10; i++ {
		task.Publish("hello" + E取现行时间().E时间到文本("Y-m-d H:i:s"))
		E延时(1000)
	}
}

//订阅消息
func TestRabbitmqModel_Subscribe(t *testing.T) {
	//连接
	task := NewRabbitmModel("amqp://admin:admin@182.92.84.229:5672/", "hello_queue")
	//订阅
	task.Subscribe()
	//接受订阅数据
	go func() {
		for d := range task.Receive() {
			log.Printf("Received a message: %s", d.Body)
		}
	}()
	forever := make(chan bool)
	<-forever
}
