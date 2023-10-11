package logx

import (
	"fmt"
	"testing"
)

func TestLog(t *testing.T) {
	logx := NewDefaultLog(nil)
	logx.Log(InfoLevel, "test info %s", "测试日志")
}

func TestLogCallBack(t *testing.T) {
	logx := NewDefaultLog(func(logLevel LogLevel, template string, args ...interface{}) error {
		fmt.Printf("打印日志")
		return nil
	})
	logx.Log(InfoLevel, "test info %s", "测试日志")
}
