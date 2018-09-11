package main

import (
	"github.com/chrisho/sdrabbitmq"
	"github.com/chrisho/sdrabbitmq/test/common"
)



func main() {
	rabbiturl := "amqp://guest:guest@127.0.0.1:5672"
	emit, err := sdrabbitmq.NewEmitTempLogTopic(rabbiturl)
	if err != nil {
		common.FailOrError(err)
	}


	err = emit.Publish("Hello World Test", "test.sdrabbitmq.temp", "producer")
	if err != nil {
		common.FailOrError(err)
	}
}