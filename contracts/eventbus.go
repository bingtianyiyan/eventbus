package contracts

import (
	"context"
	"errors"
)

type EventBus interface {
	// EventHandler actual execute func
	EventHandler

	// Register EventHander
	AddHandler(EventType, EventHandler, interface{}) error

	// Errors returns an error channel where async handling errors are sent.
	Errors() <-chan error

	// Close closes the EventBus and waits for all handlers to finish.
	Close() error
}

var (
	ErrMissingEventType = errors.New("missing eventType")
	// ErrMissingHandler is returned when calling AddHandler with a nil handler.
	ErrMissingHandler = errors.New("missing handler")
	// ErrHandlerAlreadyAdded is returned when calling AddHandler weth the same handler twice.
	ErrHandlerAlreadyAdded = errors.New("handler already added")
)

// EventBusError is an async error containing the error returned from a handler
// and the event that it happened on.
type EventBusError struct {
	// Err is the error.
	Err error
	// Ctx is the context used when the error happened.
	Ctx context.Context
	// Event is the event handeled when the error happened.
	Event Event
}

// Error implements the Error method of the error interface.
func (e *EventBusError) Error() string {
	str := "event bus: "

	if e.Err != nil {
		str += e.Err.Error()
	} else {
		str += "unknown error"
	}

	if e.Event != nil {
		str += " [" + e.Event.String() + "]"
	}

	return str
}

// Unwrap implements the errors.Unwrap method.
func (e *EventBusError) Unwrap() error {
	return e.Err
}

// Cause implements the github.com/pkg/errors Unwrap method.
func (e *EventBusError) Cause() error {
	return e.Unwrap()
}
