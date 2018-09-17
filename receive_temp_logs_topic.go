package sdrabbitmq

import (
	"github.com/streadway/amqp"
)

type ReceiveTempLogsTopic struct {
	conn *amqp.Connection  // 连接
	ch *amqp.Channel  // 管道
	q amqp.Queue  // 队列
	exchange string // 交换器名称
}


// 获取消息交付实例
// @param exchange string 交换器名称
// @return <-chan amqp.Delivery 交付实例
//         error
func (r *ReceiveTempLogsTopic) GetMsgs(exchange string) (<-chan amqp.Delivery, error) {
	r.exchange = exchange
	err := r.exchangeDeclare()
	if err != nil {
		return nil, err
	}

	err = r.queueDeclare()
	if err != nil {
		return nil, err
	}

	err = r.queueBind()
	if err != nil {
		return nil, err
	}

	return r.consume()
}


// 回复对应key的消息
// @param msg string 消息内容body
// @param key string Routing Key
// @return err error
func (r *ReceiveTempLogsTopic) Replay(msg string, key string) (err error){
	e, err := NewEmitLogTopicWithConn(r.conn)
	if err != nil {
		return err
	}
	e.Publish(msg, r.exchange, key)
	return
}


// 声明交换器
func (r *ReceiveTempLogsTopic) exchangeDeclare() (err error) {
	err = r.ch.ExchangeDeclare(
		r.exchange, // name
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


// 声明消息队列
func (r *ReceiveTempLogsTopic) queueDeclare() (err error) {
	r.q, err = r.ch.QueueDeclare(
		"",
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return failOnError("Failed to declare a queue", err)
	}

	return
}


// 绑定消息队列
func (r *ReceiveTempLogsTopic) queueBind() (err error) {
	err = r.ch.QueueBind(
		"",       // queue name
		SDRabbitmqRoutingKeyProducer,   // routing key
		r.exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return failOnError("Failed to bind a queue", err)
	}
	return
}


// 消费消息队列
// @return msgs <-chan amqp.Delivery 消息交付实例
//         err error
func (r *ReceiveTempLogsTopic) consume() (msgs <-chan amqp.Delivery, err error) {
	msgs, err = r.ch.Consume(
		r.q.Name, // queue
		"",     // consumer
		false,   // auto ack
		false,  // exclusive
		false,  // no local
		false,  // no wait
		nil,    // args
	)
	if err != nil {
		return nil, failOnError("Failed to register a consume", err)
	}
	return
}


// 关闭连接，关闭管道
func (r *ReceiveTempLogsTopic) Close() {
	r.conn.Close()
	r.ch.Close()
}


// NewReceiveLogsTopic 实例化receiveLogsTopic
// @param url string 连接rabbitmq服务器地址
// @return r *receiveLogsTopic
//         err error
func NewReceiveTempLogsTopic(url string) (r *ReceiveTempLogsTopic, err error) {
	r = &ReceiveTempLogsTopic{}
	r.conn, r.ch, err = connect(url)
	if err != nil {
		return nil, err
	}

	return r, nil
}


// NewReceiveLogsTopicWithConn 实例化receiveLogsTopic
// @param conn *amqp.Connection 已定义连接，用于共享连接
// @return r *receiveLogsTopic
//         err error
func NewReceiveTempLogsTopicWithConn(conn *amqp.Connection) (r *ReceiveTempLogsTopic, err error) {
	r = &ReceiveTempLogsTopic{}
	r.ch, err = channel(conn)
	if err != nil {
		return nil, err
	}
	return r, err
}