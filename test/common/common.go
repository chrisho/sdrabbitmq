package common

import (
	"fmt"
	"os"
)

func FailOrError(err error) {
	fmt.Println(err)
	os.Exit(0)
}