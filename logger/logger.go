package logger

import "fmt"

var Default = &defaultLogger{}

type Logger interface {
	Error(v any)
}

type defaultLogger struct {
}

func (d *defaultLogger) Error(v any) {
	fmt.Println(v)
}
