package main

import (
	"duolabmeng6/go-rabbitmq-easy/MqConn"
	"fmt"
	"github.com/duolabmeng6/goefun/coreUtil"
)

func Rabbitmq_Run() {
	var mq1 *MqConn.BaseMq
	mq1 = MqConn.GetConnection("client")
	channleContxt := MqConn.ChannelContext{
		Exchange:     "",
		ExchangeType: "",
		RoutingKey:   "rpc_queue1",
		Reliable:     true,
		Durable:      false,
	}

	线程池 := coreUtil.New线程池(10000)
	for i := 1; i <= 100000; i++ {
		线程池.E加入任务()
		go func(i int) {
			defer 线程池.E完成()

			fmt.Println("sending message")
			//mq1.Publish(&channleContxt, "helllosaga")
		}(i)
	}

}
