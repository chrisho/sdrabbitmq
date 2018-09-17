package sdrabbitmq

import (
	"github.com/streadway/amqp"
)

// 临时消息队列
type EmitTempLogTopic struct {
	key string
	q amqp.Queue
	conn *amqp.Connection // 连接实例
	ch *amqp.Channel // 管道实例
	exchange string // 交换器名称
}


// 生产一条消息
// @param body string 消息体
// @param exchange string 交换器名称
// @param key string Routing Key
// @return err error
func (e *EmitTempLogTopic) Publish(body string, exchange string, key string) (err error){
	if e.ch == nil {
		e.ch, err = channel(e.conn)
		if err != nil {
			return err
		}
	}

	e.key = key
	e.exchange = exchange
	err = e.exchangeDeclare()
	if err != nil {
		return err
	}

	err = e.publish(body)
	return
}


// 声明交换器
func (e *EmitTempLogTopic) exchangeDeclare() (err error) {
	err = e.ch.ExchangeDeclare(
		e.exchange, // name
		SDRabbitmqExchangeTypeTopic, // type
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return failOnError("Failed to declare an exchange", err)
	}

	return
}



// 生产一条消息
// @param body string 消息体
// @param key string Routing Key
// @return err error
func (e *EmitTempLogTopic) publish(body string) (err error) {
	//forever := make(chan bool)
	err = e.ch.Publish(
		e.exchange,          // exchange
		e.key, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			DeliveryMode: amqp.Persistent,
		})
	if err != nil {
		failOnError("Failed to publish a message", err)
	}
	e.ch.Close()
	//<-forever
	return
}


// NewEmitLogTopic 实例化emitLogTopic
// @param url string 连接rabbitmq服务器地址
// @return e *emitLogTopic
//         err error
func NewEmitTempLogTopic(url string) (e *EmitTempLogTopic, err error) {
	e = &EmitTempLogTopic{}
	e.conn, e.ch, err = connect(url)
	if err != nil {
		return nil, err
	}

	return e, nil
}


// NewEmitLogTopicWithConn 实例化emitLogTopic
// @param conn *amqp.Connection 已定义连接，用于共享连接
// @return e *emitLogTopic
//         err error
func NewEmitTempLogTopicWithConn(conn *amqp.Connection) (e *EmitTempLogTopic, err error) {
	e = &EmitTempLogTopic{}
	e.conn = conn
	e.ch, err = channel(e.conn)
	if err != nil {
		return nil, err
	}
	return e, err
}