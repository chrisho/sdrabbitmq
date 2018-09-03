package main

import (
	"fmt"
	"github.com/chrisho/sdrabbitmq"
	"github.com/chrisho/sdrabbitmq/test/common"
)

func main() {
	rabbiturl := "amqp://guest:guest@192.168.0.193:5672"
	receive, err := sdrabbitmq.NewReceiveLogsTopic(rabbiturl)
	if err != nil {
		common.FailOrError(err)
	}

	msgs, err := receive.GetMsgs("test.sdrabbitmq")
	if err != nil {
		common.FailOrError(err)
	}

	forever := make(chan bool)
	go func() {
		fmt.Println("start listen rabbitmq")
		for d := range msgs {
			fmt.Printf("receive %s \n", d.Body)
			d.Ack(false)
		}
	}()
	<-forever
}