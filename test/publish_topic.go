package main

import (
	"github.com/chrisho/sdrabbitmq"
	"github.com/chrisho/sdrabbitmq/test/common"
)



func main() {
	rabbiturl := "amqp://guest:guest@192.168.0.193:5672"
	emit, err := sdrabbitmq.NewEmitLogTopic(rabbiturl)
	if err != nil {
		common.FailOrError(err)
	}


	err = emit.Publish("Hello World Test", "test.sdrabbitmq", "producer")
	if err != nil {
		common.FailOrError(err)
	}
}