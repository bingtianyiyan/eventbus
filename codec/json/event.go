package json

import (
	"context"
	"encoding/json"
	"fmt"
	eh "github.com/bingtianyiyan/eventbus/contracts"
)

// EventCodec is a codec for marshaling and unmarshaling events
// to and from bytes in JSON format.
type EventCodec struct{}

// MarshalEvent marshals an event into bytes in JSON format.
func (c *EventCodec) MarshalEvent(ctx context.Context, event eh.Event) ([]byte, error) {
	e := evt{
		EventType: event.EventType(),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		if e.RawData, err = json.Marshal(event.Data()); err != nil {
			return nil, fmt.Errorf("could not marshal event data: %w", err)
		}
	}

	// Marshal the event (using JSON for now).
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("could not marshal event: %w", err)
	}

	return b, nil
}

// UnmarshalEvent unmarshals an event from bytes in JSON format.
func (c *EventCodec) UnmarshalEvent(ctx context.Context, eventType eh.EventType, b []byte) (eh.Event, context.Context, error) {
	// Decode the raw JSON event data.
	var e = evt{
		EventType: eventType,
	}
	// Create an event of the correct type and decode from raw JSON.
	var err error
	if len(b) > 0 {
		if e.data, err = eh.CreateEventData(eventType); err != nil {
			return nil, nil, fmt.Errorf("could not create event data: %w", err)
		}
		if err := json.Unmarshal(b, e.data); err != nil {
			return nil, nil, fmt.Errorf("could not unmarshal event data: %w", err)
		}
		e.RawData = nil
	}
	// Build the event.
	event := eh.NewEvent(
		e.EventType,
		e.data,
	)
	return event, ctx, nil
}

// evt is the internal event used on the wire only.
type evt struct {
	EventType eh.EventType    `json:"event_type"`
	RawData   json.RawMessage `json:"data,omitempty"`
	data      eh.EventData    `json:"-"` // 数据类型解析
}
