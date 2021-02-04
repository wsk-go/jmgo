package logger

import "fmt"

var Default = &defaultLogger{}

type Logger interface {
    Error(v interface{})
}

type defaultLogger struct {

}

func (d *defaultLogger) Error(v interface{}) {
    fmt.Println(v)
}



