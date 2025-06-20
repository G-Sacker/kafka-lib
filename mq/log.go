package mq

import "fmt"

func NewLogger() Logger {
	return logImpl{}
}

type logImpl struct{}

func (logImpl) Info(args ...interface{}) {
	fmt.Println(args...)
}

func (logImpl) Warn(args ...interface{}) {
	fmt.Println(args...)
}

func (logImpl) Error(args ...interface{}) {
	fmt.Println(args...)
}

func (logImpl) Errorf(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}

func (logImpl) Infof(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
