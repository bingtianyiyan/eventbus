package codec

import (
	"context"
	eh "github.com/bingtianyiyan/eventbus/contracts"
)

// EventCodec is a codec for marshaling and unmarshaling events to and from bytes.
type EventCodec interface {
	// MarshalEvent marshals an event and the supported parts of context into bytes.
	MarshalEvent(context.Context, eh.Event) ([]byte, error)
	// UnmarshalEvent unmarshals an event and supported parts of context from bytes.
	UnmarshalEvent(context.Context, eh.EventType, []byte) (eh.Event, context.Context, error)
}
