package jmongo

import "fmt"

var DefaultLogger = &defaultLogger{}

type defaultLogger struct {

}

func (d defaultLogger) Debug(msg string) {
    fmt.Println(msg)
}

func (d defaultLogger) Info(msg string) {
    fmt.Println(msg)
}

func (d defaultLogger) Warn(msg string) {
    fmt.Println(msg)
}

func (d defaultLogger) Error(msg string) {
    fmt.Println(msg)
}

func (d defaultLogger) DPanic(msg string) {
    fmt.Println(msg)
}

func (d defaultLogger) Panic(msg string) {
    fmt.Println(msg)
}

type Logger interface {

    Debug(msg string)

    Info(msg string)

    Warn(msg string)

    Error(msg string)

    DPanic(msg string)

    Panic(msg string)

}

func logError(message string)  {
    DefaultLogger.Error(message)
}
