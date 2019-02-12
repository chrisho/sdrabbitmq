package sdrabbitmq

import (
	"github.com/streadway/amqp"
)

type ReceiveLogsTopic struct {
	conn *amqp.Connection  // 连接
	ch *amqp.Channel  // 管道
	q amqp.Queue  // 队列
	exchange string // 交换器名称
}


// 获取消息交付实例
// @param exchange string 交换器名称
// @return <-chan amqp.Delivery 交付实例
//         error
func (r *ReceiveLogsTopic) GetMsgs(exchange string, durable bool) (<-chan amqp.Delivery, error) {
	r.exchange = exchange
	err := r.exchangeDeclareWithParams(durable)
	if err != nil {
		return nil, err
	}

	err = r.queueDeclareWithParams(durable)
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
func (r *ReceiveLogsTopic) Replay(msg string, key string) (err error){
	e, err := NewEmitLogTopicWithConn(r.conn)
	if err != nil {
		return err
	}
	e.Publish(msg, r.exchange+".callback", key, true)
	return
}

// 声明交换器
func (r *ReceiveLogsTopic) exchangeDeclare() (err error) {
	return r.exchangeDeclareWithParams(false)
}
// 声明交换器
func (r *ReceiveLogsTopic) exchangeDeclareWithParams(durable bool) (err error) {
	err = r.ch.ExchangeDeclare(
		r.exchange, // name
		SDRabbitmqExchangeTypeTopic, // type
		durable,        // durable
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

// 声明持久化队列
func (r *ReceiveLogsTopic) queueDeclare() (err error) {
	return r.queueDeclareWithParams(false)
}
// 声明消息队列
func (r *ReceiveLogsTopic) queueDeclareWithParams(durable bool) (err error) {
	r.q, err = r.ch.QueueDeclare(
		 r.exchange,
		durable,
		false,
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
func (r *ReceiveLogsTopic) queueBind() (err error) {
	err = r.ch.QueueBind(
		r.q.Name,       // queue name
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
func (r *ReceiveLogsTopic) consume() (msgs <-chan amqp.Delivery, err error) {
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
func (r *ReceiveLogsTopic) Close() {
	if r.conn != nil {
		r.conn.Close()
	}
	r.ch.Close()
}


// NewReceiveLogsTopic 实例化ReceiveLogsTopic
// @param url string 连接rabbitmq服务器地址
// @return r *ReceiveLogsTopic
//         err error
func NewReceiveLogsTopic(hostPort string, username string, password string) (r *ReceiveLogsTopic, err error) {
	r = &ReceiveLogsTopic{}
	r.conn, r.ch, err = connect(hostPort, username, password)
	if err != nil {
		return nil, err
	}

	return r, nil
}


// NewReceiveLogsTopicWithConn 实例化ReceiveLogsTopic
// @param conn *amqp.Connection 已定义连接，用于共享连接
// @return r *ReceiveLogsTopic
//         err error
func NewReceiveLogsTopicWithConn(conn *amqp.Connection) (r *ReceiveLogsTopic, err error) {
	r = &ReceiveLogsTopic{}
	r.ch, err = channel(conn)
	if err != nil {
		return nil, err
	}
	return r, err
}