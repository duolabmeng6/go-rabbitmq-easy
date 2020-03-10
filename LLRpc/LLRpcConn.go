package LLRpc

import (
	"errors"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

type LLRpcConn struct {
	name           string
	logger         *log.Logger
	connection     *amqp.Connection
	channel        *amqp.Channel
	done           chan bool
	notifyClose    chan *amqp.Error
	notifyConfirm  chan amqp.Confirmation
	isConnected    bool
	reconnectCount int
	ReceiveChan    chan amqp.Delivery
}

const (
	reconnectDelay         = 2 * time.Second // 连接断开后多久重连
	MaxWaitConnErrorNumber = 5               // 调用推送命令时,连接断开后 等待多少次重连失败 返回错误
	resendDelay            = 2 * time.Second // 消息发送失败后，多久重发
	resendTime             = 6               // 消息重发次数
)

var (
	errNotConnected  = errors.New("not connected to the producer")
	errAlreadyClosed = errors.New("already closed: not connected to the producer")
)

func NewMq(name string, addr string) *LLRpcConn {
	mq := LLRpcConn{
		logger: log.New(os.Stdout, "", log.LstdFlags),
		name:   name,
		done:   make(chan bool),
	}
	mq.reconnectCount = 0
	mq.ReceiveChan = make(chan amqp.Delivery)
	go mq.handleReconnect(addr)
	return &mq
}

// 如果连接失败会不断重连
// 如果连接断开会重新连接
func (mq *LLRpcConn) handleReconnect(addr string) {
	for {
		mq.isConnected = false
		log.Println("Attempting to connect")
		for !mq.connect(addr) {
			log.Println("连接失败，重试中...")
			time.Sleep(reconnectDelay)
		}
		select {
		case <-mq.done:
			return
		case <-mq.notifyClose:
		}
	}
}

// 连接rabbitmq，以生产者的name定义一个队列
func (mq *LLRpcConn) connect(addr string) bool {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return false
	}
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	ch.Confirm(false)
	_, err = ch.QueueDeclare(
		mq.name,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return false
	}
	mq.changeConnection(conn, ch)
	mq.isConnected = true
	mq.reconnectCount = 0
	log.Println("Connected!")
	return true
}

// 监听Rabbit channel的状态
func (mq *LLRpcConn) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	mq.connection = connection
	mq.channel = channel
	// channels没有必要主动关闭。如果没有协程使用它，它会被垃圾收集器收拾
	mq.notifyClose = make(chan *amqp.Error)
	mq.notifyConfirm = make(chan amqp.Confirmation)
	mq.channel.NotifyClose(mq.notifyClose)
	mq.channel.NotifyPublish(mq.notifyConfirm)
}

func (mq *LLRpcConn) Send(data []byte) error {
	if !mq.isConnected {
		return errors.New("推送失败，未连接到服务器")
	}
	var currentTime = 0
	for {
		err := mq.UnsafePush(data)
		if err != nil {
			mq.logger.Println("推送失败 ，重试中...")
			currentTime += 1
			if currentTime < resendTime {
				continue
			} else {
				return err
			}
		}
		ticker := time.NewTicker(resendDelay)
		select {
		case confirm := <-mq.notifyConfirm:
			if confirm.Ack {
				mq.logger.Println("推送成功!")
				return nil
			}
		case <-ticker.C:
		}
		mq.logger.Println("推送失败 ，重试中...")
	}
}

func (mq *LLRpcConn) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if !mq.isConnected {
		for i := 0; i < MaxWaitConnErrorNumber; i++ {
			mq.logger.Println("等待连接服务器", i, "次")
			time.Sleep(reconnectDelay)
			if mq.isConnected {
				break
			}
		}
		if !mq.isConnected {
			return errors.New("推送失败，未连接到服务器")
		}
	}
	var currentTime = 0
	for {
		err := mq.UnsafePush3(exchange, key, mandatory, immediate, msg)
		if err != nil {
			mq.logger.Println("推送失败 ，重试中...")
			currentTime += 1
			if currentTime < resendTime {
				continue
			} else {
				return err
			}
		}
		ticker := time.NewTicker(resendDelay)
		select {
		case confirm := <-mq.notifyConfirm:
			if confirm.Ack {
				mq.logger.Println("推送成功!")
				return nil
			}
		case <-ticker.C:
		}
		mq.logger.Println("推送失败 ，重试中...")
	}
}

// 发送出去，不管是否接受的到
func (mq *LLRpcConn) UnsafePush3(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if !mq.isConnected {
		return errNotConnected
	}
	return mq.channel.Publish(exchange, key, mandatory, immediate, msg)
}

// 发送出去，不管是否接受的到
func (mq *LLRpcConn) UnsafePush(data []byte) error {
	if !mq.isConnected {
		return errNotConnected
	}
	return mq.channel.Publish(
		"",      // Exchange
		mq.name, // Routing key
		false,   // Mandatory
		false,   // Immediate
		amqp.Publishing{
			DeliveryMode: 2,
			ContentType:  "application/json",
			Body:         data,
			Timestamp:    time.Now(),
		},
	)
}

func (mq *LLRpcConn) Receive() error {
	if !mq.isConnected {
		mq.reconnectCount++
		if mq.reconnectCount < resendTime {
			time.Sleep(reconnectDelay)
			return mq.Receive()
		} else {
			mq.logger.Println("接收失败：###断开连接")
			return errors.New("接收失败：###断开连接")
		}
	}
	var c = make(chan bool)
	//var currentTime = 0
	for {

		if delivery, err := mq.UnsafeReceive(); err != nil {
			mq.logger.Println("接收失败：", err, ",重连中...")

			mq.reconnectCount++
			if mq.reconnectCount < resendTime {

				time.Sleep(reconnectDelay)
				return mq.Receive()
			} else {
				mq.logger.Println("接收失败：###断开连接")
				return err
			}
		} else {
			mq.reconnectCount = 0
			go func(delivery <-chan amqp.Delivery) {
				for d := range delivery {
					//fmt.Println(string(d.Body))
					mq.ReceiveChan <- d
				}
				c <- true
			}(delivery)
		}
	}
	select {
	case <-c:
		mq.logger.Println("接收失败：###重连中...")
		time.Sleep(reconnectDelay)
		mq.Receive()
	}
	return errors.New("连接断开 ，重试中2...")

}

func (mq *LLRpcConn) UnsafeReceive() (<-chan amqp.Delivery, error) {
	if !mq.isConnected {
		return nil, errNotConnected
	}
	return mq.channel.Consume(mq.name, "", true, false, false, false, nil)
}

// 关闭连接/信道
func (mq *LLRpcConn) Close() error {
	if !mq.isConnected {
		return errAlreadyClosed
	}
	err := mq.channel.Close()
	if err != nil {
		return err
	}
	err = mq.connection.Close()
	if err != nil {
		return err
	}
	close(mq.done)
	mq.isConnected = false
	return nil
}

func main() {
	producer := NewMq("rpc_queue1", "amqp://admin:admin@182.92.84.229:5672/")
	producer.Receive()
	//fmt.Println("over")
	//
	//for {
	//	producer.Send([]byte("6666"))
	//}

}
