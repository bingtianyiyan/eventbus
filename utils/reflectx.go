package utils

import "reflect"

func GetTypeName(data interface{}) string {
	return reflect.TypeOf(data).Elem().Name()
}
