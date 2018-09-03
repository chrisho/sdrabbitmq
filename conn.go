package sdrabbitmq

import (
	"github.com/streadway/amqp"
)


// 连接rabbitmq
// @param url string 连接地址：amqp://guest:guest@127.0.0.1:5672/
// @return conn *amqp.Connection 连接实例
//         ch *amqp.Channel 管道实例
//         err error
func connect(url string) (conn *amqp.Connection, ch *amqp.Channel, err error) {

	conn, err = amqp.Dial(url)
	if err != nil {
		return nil, nil, failOnError("Failed to connect", err)
	}

	ch, err = channel(conn)
	if err != nil {
		return nil, nil, err
	}
	return
}


// 连接获取管道
// @param conn *amqp.Connection 连接实例
// @return ch *amqp.Channel 管理实例
//         err error
func channel(conn *amqp.Connection) (ch *amqp.Channel, err error) {
	ch, err = conn.Channel()
	if err != nil {
		return nil, failOnError("Failed to open a channel", err)
	}
	
	return
}
