package main

import (
	"fmt"
	"github.com/chrisho/sdrabbitmq"
	"github.com/chrisho/sdrabbitmq/test/common"
)

func main() {
	rabbiturl := "amqp://guest:guest@127.0.0.1:5672"
	receive, err := sdrabbitmq.NewReceiveTempLogsTopic(rabbiturl)
	if err != nil {
		common.FailOrError(err)
	}

	msgs, err := receive.GetMsgs("test.sdrabbitmq.temp")
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