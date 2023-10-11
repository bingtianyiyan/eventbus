package logx

import (
	"fmt"
	"log"
)

type ILogger interface {
	Log(logLevel LogLevel, template string, args ...interface{})
}

type LogLevel int

var (
	TraceLevel LogLevel = 0
	DebugLevel LogLevel = 1
	InfoLevel  LogLevel = 2
	WarnLevel  LogLevel = 3
	ErrorLevel LogLevel = 4
	FatalLevel LogLevel = 5
)

type LogCallback func(logLevel LogLevel, template string, args ...interface{}) error

/*
默认日志
*/
type DefaultLog struct {
	callBack LogCallback
}

func NewDefaultLog(callBack LogCallback) ILogger {
	return DefaultLog{
		callBack: callBack,
	}
}

func (p DefaultLog) Log(logLevel LogLevel, template string, args ...interface{}) {
	if p.callBack == nil {
		if logLevel == FatalLevel {
			log.Panicf(fmt.Sprintf(template, args...))
		} else {
			log.Println(fmt.Sprintf(template, args...))
		}
		return
	}
	p.callBack(logLevel, template, args)
}
