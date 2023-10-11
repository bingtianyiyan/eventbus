package contracts

import (
	"context"
	"errors"
)

// ErrMissingEvent is when there is no event to be handled.
var ErrMissingEvent = errors.New("missing event")

// EventHandlerType is the type of an event handler, used as its unique identifier.
type EventHandlerType string

// String returns the string representation of an event handler type.
func (ht EventHandlerType) String() string {
	return string(ht)
}

// EventHandler is a handler of events. If registered on a bus as a handler only
// one handler of the same type will receive each event. If registered on a bus
// as an observer all handlers of the same type will receive each event.
type EventHandler interface {
	// HandlerType is the type of the handler.
	HandlerType() EventHandlerType
	// 各个事件实际处理 HandleEvent handles an event.
	HandleEvent(context.Context, Event) error
}
