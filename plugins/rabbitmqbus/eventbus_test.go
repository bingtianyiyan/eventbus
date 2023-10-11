package rabbitmqbus

import (
	"context"
	"encoding/json"
	"fmt"
	eh "github.com/bingtianyiyan/eventbus/contracts"
	"github.com/bingtianyiyan/eventbus/rabbitmqpools"
	"github.com/bingtianyiyan/eventbus/utils"
	"sync"
	"testing"
)

func TestRabbitMqEventBus(t *testing.T) {
	setup()
}

func setup() {
	var wg sync.WaitGroup
	var eventHandler = NewTestMqEventHandler()
	bus, _ := NewEventBus("amqp://guest:guest@127.0.0.1:5672")
	//集成事件订阅
	eh.RegisterEventData(eventHandler.Type, eventHandler.Data)

	//m eh.EventType, h eh.EventHandler
	var attr = rabbitmqpools.RabbitAttribute{
		ExchangeName: "MyTestExchange1",
		WorkModel:    "direct",
		RoutingKey:   "TestMqEventRouteKey1",
		Queue:        "MyTestEventQueue1",
	}
	_ = bus.AddHandler(eh.EventType(utils.GetTypeName(&TestMqEvent{})), NewTestMqEventHandler(), attr)
	wg.Add(1)
	wg.Wait()
}

type TestMqEvent struct {
	Name string `json:"name,omitempty"`
}

type TestMqEventHandler struct {
	Type eh.EventType
	Data func() eh.EventData
}

var _ = eh.EventHandler(&TestMqEventHandler{})

// NewEventHandler creates a new EventHandler.
func NewTestMqEventHandler() *TestMqEventHandler {
	return &TestMqEventHandler{
		Type: eh.EventType(utils.GetTypeName(&TestMqEvent{})),
		Data: func() eh.EventData {
			return &TestMqEvent{}
		},
	}
}

// HandlerType implements the HandlerType method of the contracts.EventHandler interface.
func (m *TestMqEventHandler) HandlerType() eh.EventHandlerType {
	return eh.EventHandlerType(m.Type)
}

// HandleEvent implements the HandleEvent method of the contracts.EventHandler interface.
func (m *TestMqEventHandler) HandleEvent(ctx context.Context, event eh.Event) error {
	var data TestMqEvent
	//data, ok := event.Data().(TestMqEvent)
	resByre, err := json.Marshal(event.Data())
	if err != nil {
		fmt.Println("fail msg")
		return nil
	}
	err = json.Unmarshal(resByre, &data)
	if err != nil {
		fmt.Println("fail msg")
		return nil
	}
	fmt.Println("event print msg-->" + data.Name)
	return nil
}
