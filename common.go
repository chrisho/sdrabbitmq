package sdrabbitmq

import (
	"fmt"
	"errors"
)

// 返回错误格式
// @param msg string 内容
// @param err error
// @return error
func failOnError(msg string, err error) error {
	if err != nil {
		return errors.New(fmt.Sprintf("%s: %s", msg, err))
	}
	return nil
}

