package contracts

import (
	"errors"
	"fmt"
	"sync"
)

// Event event data interface
type Event interface {
	// EventType returns the type of the event.
	EventType() EventType
	// The data attached to the event.
	Data() EventData
	// A string representation of the event.
	String() string
}

// EventType is the type of an event, used as its unique identifier.
type EventType string

// String returns the string representation of an event type.
func (et EventType) String() string {
	return string(et)
}

// EventData is any additional data for an event.
type EventData interface{}

// NewEvent creates a new event with a type and data
func NewEvent(eventType EventType, data EventData) Event {
	e := &event{
		eventType: eventType,
		data:      data,
	}
	return e
}

// event is an internal representation of an event implementing Event.
type event struct {
	eventType EventType
	data      EventData
}

// EventType implements the EventType method of the Event interface.
func (e event) EventType() EventType {
	return e.eventType
}

// Data implements the Data method of the Event interface.
func (e event) Data() EventData {
	return e.data
}

// String implements the String method of the Event interface.
func (e event) String() string {
	str := string(e.eventType)
	return str
}

// ErrEventDataNotRegistered is when no event data factory was registered.
var ErrEventDataNotRegistered = errors.New("event data not registered")

// RegisterEventData registers an event data factory for a type. The factory is
// used to create concrete event data structs when loading from the database.
//
// An example would be:
//
//	RegisterEventData(MyEventType, func() Event { return &MyEventData{} })
func RegisterEventData(eventType EventType, factory func() EventData) error {
	if eventType == EventType("") {
		errors.New("event: attempt to register empty event type")
	}

	eventDataFactoriesMu.Lock()
	defer eventDataFactoriesMu.Unlock()

	// a factory func.
	if _, ok := eventDataFactories[eventType]; ok {
		errors.New(fmt.Sprintf("event: registering duplicate types for %q", eventType))
	}

	eventDataFactories[eventType] = factory
	return nil
}

// UnregisterEventData removes the registration of the event data factory for
// a type
func UnregisterEventData(eventType EventType) error {
	if eventType == EventType("") {
		errors.New("eventhorizon: attempt to unregister empty event type")
	}

	eventDataFactoriesMu.Lock()
	defer eventDataFactoriesMu.Unlock()

	if _, ok := eventDataFactories[eventType]; !ok {
		errors.New(fmt.Sprintf("eventhorizon: unregister of non-registered type %q", eventType))
	}

	delete(eventDataFactories, eventType)
	return nil
}

// CreateEventData creates an event data of a type using the factory registered
// with RegisterEventData.
func CreateEventData(eventType EventType) (EventData, error) {
	eventDataFactoriesMu.RLock()
	defer eventDataFactoriesMu.RUnlock()

	if factory, ok := eventDataFactories[eventType]; ok {
		return factory(), nil
	}

	return nil, ErrEventDataNotRegistered
}

var eventDataFactories = make(map[EventType]func() EventData)
var eventDataFactoriesMu sync.RWMutex
