package rabbitmqpools

import "sync"

// 事件event配置的基本应用交换机队列数据
type RabbitAttribute struct {
	//交换机名称
	ExchangeName string `json:"exchangeName"`
	// 交换机模式[fanout 发布订阅模式,direct 路由模式，topic 主题模式，x-delayed-message 延时队列模式
	WorkModel string `json:"workModel"`
	//路由键《路由键和队列名称配合使用》
	RoutingKey string `json:"routingKey"`
	//队列名称《队列名称和路由键配合使用》
	Queue string `json:"queue"`
	//队列订阅消费的模式
	Modes []QueueConsumerMode `json:"modes"`
}

var (
	queueConsumerModeMap map[QueueConsumerMode]string
	onceLock             sync.Once
)

type QueueConsumerMode string

// QueueConsumerMode
const (
	Normal QueueConsumerMode = "Normal"
	Retry  QueueConsumerMode = "Retry"
	Fail   QueueConsumerMode = "Fail"
)

func (p QueueConsumerMode) ToString() string {
	return string(p)
}

func QueueConsumerModeList() map[QueueConsumerMode]string {
	if queueConsumerModeMap == nil {
		onceLock.Do(func() {
			queueConsumerModeMap = make(map[QueueConsumerMode]string)
			queueConsumerModeMap[Normal] = Normal.ToString()
			queueConsumerModeMap[Retry] = Retry.ToString()
			queueConsumerModeMap[Fail] = Fail.ToString()
		})
	}
	return queueConsumerModeMap
}

func GetQueueConsumerMode(mode QueueConsumerMode) string {
	if queueConsumerModeMap == nil {
		QueueConsumerModeList()
	}
	data, _ := queueConsumerModeMap[mode]
	return data
}

func GetQueueConsumerModeContains(modes []QueueConsumerMode, compareMode QueueConsumerMode) bool {
	for _, v := range modes {
		if v == compareMode {
			return true
		}
	}
	return false
}
