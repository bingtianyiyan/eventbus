package rabbitmqbus

import (
	"context"
	"errors"
	"fmt"
	msgCodec "github.com/bingtianyiyan/eventbus/codec"
	"github.com/bingtianyiyan/eventbus/codec/json"
	eh "github.com/bingtianyiyan/eventbus/contracts"
	"github.com/bingtianyiyan/eventbus/logx"
	"github.com/bingtianyiyan/eventbus/rabbitmqpools"
	"github.com/bingtianyiyan/eventbus/utils"
	"github.com/streadway/amqp"
	"strconv"
	"sync"
	"time"
)

type EventBus struct {
	url                    string // address
	messageTimeToAlive     int64  // 消息存活时间
	rollbackCount          int32  // 上限次数，重试次数大于最大约定上限次数则停止重试
	maxConsumeChannelCount int32  // 消费者数量
	instanceConsumePool    *rabbitmqpools.RabbitPool
	instanceProductPool    *rabbitmqpools.RabbitPool
	registered             map[eh.EventHandlerType]struct{}               // 事件是否已注册
	subscriptionManager    map[eh.EventType][]*eh.EventHandler            // 订阅事件类型和事件处理管理 建立映射
	handlerQueueList       map[eh.EventType]rabbitmqpools.RabbitAttribute //事件类型队列注册信息
	handlerNewQueueList    map[string]rabbitmqpools.RabbitAttribute
	registeredMu           sync.RWMutex
	errCh                  chan error
	cctx                   context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	codec                  msgCodec.EventCodec
	logger                 logx.ILogger
}

// NewEventBus creates an EventBus, with optional settings.
func NewEventBus(url string, options ...Option) (*EventBus, error) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &EventBus{
		url:                    url,
		messageTimeToAlive:     0,
		rollbackCount:          3,
		maxConsumeChannelCount: rabbitmqpools.DEFAULT_MAX_CONSUME_CHANNEL,
		registered:             map[eh.EventHandlerType]struct{}{},
		subscriptionManager:    map[eh.EventType][]*eh.EventHandler{},
		handlerQueueList:       map[eh.EventType]rabbitmqpools.RabbitAttribute{},
		handlerNewQueueList:    map[string]rabbitmqpools.RabbitAttribute{},
		errCh:                  make(chan error, 100),
		cctx:                   ctx,
		cancel:                 cancel,
		codec:                  &json.EventCodec{},
		logger:                 logx.NewDefaultLog(nil),
	}

	// Apply configuration options.
	for _, option := range options {
		if option == nil {
			continue
		}

		if err := option(b); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	//消费者Pool
	b.instanceConsumePool = rabbitmqpools.NewConsumePool()
	b.instanceConsumePool.SetMaxConsumeChannel(b.maxConsumeChannelCount)
	err := b.instanceConsumePool.ConnectUrl(url)
	if err != nil {
		b.logger.Log(logx.ErrorLevel, "rabbitMq consumerPool error-->: %w", err)
		return nil, err
	}
	//生产者Pool
	b.instanceProductPool = rabbitmqpools.NewProductPool()
	err = b.instanceProductPool.ConnectUrl(url)
	if err != nil {
		b.logger.Log(logx.ErrorLevel, "rabbitMq productPool error-->: %w", err)
		return nil, err
	}
	return b, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventBus) error

func WithUrl(url string) Option {
	return func(b *EventBus) error {
		b.url = url
		return nil
	}
}

// messageTime to alive
func WithMessageTimeToAlive(timeAlive int64) Option {
	return func(b *EventBus) error {
		b.messageTimeToAlive = timeAlive
		return nil
	}
}

// retry count
func WithRollBackCount(count int32) Option {
	return func(b *EventBus) error {
		b.rollbackCount = count
		return nil
	}
}

// consume count
func WithMaxConsumeCount(consumeCount int32) Option {
	return func(b *EventBus) error {
		b.maxConsumeChannelCount = consumeCount
		return nil
	}
}

// WithCodec uses the specified codec for encoding events.
func WithCodec(codec msgCodec.EventCodec) Option {
	return func(b *EventBus) error {
		b.codec = codec
		return nil
	}
}

func WithLogger(logger logx.ILogger) Option {
	return func(b *EventBus) error {
		b.logger = logger
		return nil
	}
}

// HandlerType implements the HandlerType method of the EventHandler interface.
func (b *EventBus) HandlerType() eh.EventHandlerType {
	return "eventbus"
}

// publish message
func (b *EventBus) Publish(exChangeName string, exChangeType string, queueName string, route string, msg string) error {
	data := rabbitmqpools.GetRabbitMqDataFormat(exChangeName, exChangeType, queueName, route, msg)
	err := b.instanceProductPool.Push(data)
	return err
}

//implement eventbus interface

// AddHandler implements the AddHandler method of the contracts.EventBus interface.
func (b *EventBus) AddHandler(m eh.EventType, h eh.EventHandler, args interface{}) error {
	if len(m) == 0 {
		return eh.ErrMissingEventType
	}

	if h == nil {
		return eh.ErrMissingHandler
	}

	// Check handler existence.
	b.registeredMu.Lock()
	defer b.registeredMu.Unlock()
	// if have register ,then return
	if _, ok := b.registered[h.HandlerType()]; ok {
		return eh.ErrHandlerAlreadyAdded
	}

	// Register handler.
	b.registered[h.HandlerType()] = struct{}{}
	// Add eventType and eventHandler
	b.subscriptionManager[m] = append(b.subscriptionManager[m], &h)
	// 增加 注册的队列参数,一种eventType 只有一种队列配置
	queueParam, ok := args.(rabbitmqpools.RabbitAttribute)
	if !ok {
		return errors.New("args convert err to RabbitAttribute")
	}
	//add modes 目前先固定下来
	queueParam.Modes = []rabbitmqpools.QueueConsumerMode{
		rabbitmqpools.Normal, rabbitmqpools.Retry, rabbitmqpools.Fail,
	}
	queueConsumerModeList := rabbitmqpools.QueueConsumerModeList()
	for index, mode := range queueParam.Modes {
		modeName := queueConsumerModeList[mode]
		var queueName = convertMode(mode, queueParam.Queue, modeName)
		var exchangeName = convertMode(mode, queueParam.ExchangeName, modeName)
		var routeKeyName = convertMode(mode, queueParam.RoutingKey, modeName)
		if _, ok := b.handlerQueueList[m]; !ok {
			b.handlerQueueList[m] = queueParam
		}
		if _, ok := b.handlerNewQueueList[m.String()+"_"+modeName]; !ok {
			b.handlerNewQueueList[m.String()+"_"+modeName] = rabbitmqpools.RabbitAttribute{
				Queue:        queueName,
				ExchangeName: exchangeName,
				RoutingKey:   routeKeyName,
			}
		}
		//create consumer channel
		tagName := convertMode(mode, m.String(), modeName)
		receive := &rabbitmqpools.ConsumeReceive{
			TagName:      tagName,              //方法唯一标识
			ExchangeName: exchangeName,         //队列名称
			ExchangeType: queueParam.WorkModel, //
			Route:        routeKeyName,
			QueueName:    queueName,
			IsTry:        true,            //是否重试
			IsAutoAck:    false,           //自动消息确认
			MaxReTry:     b.rollbackCount, //最大重试次数
			EventFail: func(code int, e error, data []byte) {
				b.logger.Log(logx.ErrorLevel, fmt.Sprintf("EventFail->处理消息失败->code:%d,", code), e.Error())
			},
			EventSuccess: func(data *amqp.Delivery, retryClient rabbitmqpools.RetryClientInterface) bool { //如果返回true 则无需重试
				return true
			},
		}

		//create consumer channel
		err := b.createConsumerChannel(queueParam, mode, m, receive, int32(index))
		if err != nil {
			return errors.New(fmt.Sprintf("createConsumerChannel error:%s", err.Error()))
		}
		var i int32 = 0
		for i = 0; i < b.instanceConsumePool.GetMaxConsumerChannel(); i++ {
			itemI := i
			b.wg.Add(1)
			// Handle until context is cancelled.
			go func(b *EventBus, m eh.EventType, h eh.EventHandler, mode rabbitmqpools.QueueConsumerMode, receive *rabbitmqpools.ConsumeReceive, num int32) {
				b.handle(m, h, mode, receive, num+1)
			}(b, m, h, mode, receive, itemI)
		}
	}

	return nil
}

func convertMode(mode rabbitmqpools.QueueConsumerMode, data string, modeName string) string {
	if mode != rabbitmqpools.Normal {
		return data + "@" + modeName
	}
	return data
}

func (b *EventBus) createConsumerChannel(args rabbitmqpools.RabbitAttribute, mode rabbitmqpools.QueueConsumerMode, eventType eh.EventType, receive *rabbitmqpools.ConsumeReceive, num int32) error {
	var err error
	var bindFailConsumer = rabbitmqpools.GetQueueConsumerModeContains(args.Modes, mode)
	switch mode {
	case rabbitmqpools.Normal:
		return b.createActualConsumerChannel(args, mode, eventType, bindFailConsumer, receive, num)
	case rabbitmqpools.Retry:
		return b.createActualConsumerChannel(args, mode, eventType, bindFailConsumer, receive, num)
	case rabbitmqpools.Fail:
		return b.createActualConsumerChannel(args, mode, eventType, bindFailConsumer, receive, num)
	default:
		err = errors.New("无效的mode")
	}
	return err
}

func (b *EventBus) createActualConsumerChannel(args rabbitmqpools.RabbitAttribute, mode rabbitmqpools.QueueConsumerMode, eventType eh.EventType, bindConsumer bool, receive *rabbitmqpools.ConsumeReceive, num int32) error {
	var err error
	var modeName = rabbitmqpools.GetQueueConsumerMode(mode)
	queueTempTupleItem, ok := b.handlerNewQueueList[eventType.String()+"_"+modeName]
	if !ok {
		return errors.New("未找到相应的queueArgs 配置")
	}
	exchangeTable := map[string]interface{}{}
	exchange := queueTempTupleItem.ExchangeName
	routingKey := queueTempTupleItem.RoutingKey
	exchangeType := args.WorkModel
	queueName := queueTempTupleItem.Queue

	//获取请求连接
	conn := b.instanceConsumePool.GetRConnection()
	//生成处理channel 根据最大channel数处理
	channel, err := b.instanceConsumePool.CreateChannel(conn)
	if err != nil {
		if receive.EventFail != nil {
			receive.EventFail(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, rabbitmqpools.NewRabbitMqError(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, "channel create error", err.Error()), nil)
		}
		return err
	}
	defer func() {
		if err != nil {
			_ = channel.Close()
			_ = b.instanceConsumePool.CloseConn(conn)
		}
	}()
	rChannels := b.instanceConsumePool.CreateRChannel(channel, num)

	queue_args := map[string]interface{}{}
	//重试队列绑定死信
	if mode == rabbitmqpools.Retry {
		var failModeName = rabbitmqpools.GetQueueConsumerMode(rabbitmqpools.Fail)
		var failQueueName = convertMode(rabbitmqpools.Fail, args.Queue, failModeName)
		var failExchangeName = convertMode(rabbitmqpools.Fail, args.ExchangeName, failModeName)
		var failRouteKeyName = convertMode(rabbitmqpools.Fail, args.RoutingKey, failModeName)
		queue_args["x-dead-letter-exchange"] = failExchangeName
		if b.messageTimeToAlive > 0 {
			queue_args["x-message-ttl"] = b.messageTimeToAlive
		}
		queue_args["x-dead-letter-routing-key"] = failRouteKeyName
		b.logger.Log(logx.TraceLevel, "创建死信消费者通道")
		deadChannel, deadErr := b.instanceConsumePool.CreateChannel(conn)
		if deadErr != nil {
			if receive.EventFail != nil {
				receive.EventFail(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, rabbitmqpools.NewRabbitMqError(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, "dead channel create error", err.Error()), nil)
			}
			return deadErr
		}
		defer func() {
			_ = deadChannel.Close()
		}()
		deadRChannels := b.instanceConsumePool.CreateRChannel(deadChannel, num)
		//创建死信交换机
		err = b.instanceConsumePool.DeclareExchange(conn, deadRChannels, failExchangeName, args.WorkModel, nil)
		if err != nil {
			b.logger.Log(logx.ErrorLevel, "创建死信rabbitmq交换机-"+failExchangeName+"失败", err.Error())
			return err
		}
		deadChannel = b.instanceConsumePool.GetRChannelCh(deadRChannels)
		//创建死信队列
		_, err = b.instanceConsumePool.DeclareQueue(conn, deadRChannels, failQueueName, nil)
		if err != nil {
			b.logger.Log(logx.ErrorLevel, "创建死信rabbitmq队列-"+failQueueName+"失败", err.Error())
			return err
		}
		deadChannel = b.instanceConsumePool.GetRChannelCh(deadRChannels)
		exchangeTable = queue_args
	}

	err = b.instanceConsumePool.DeclareExchange(conn, rChannels, exchange, exchangeType, nil)
	if err != nil {
		b.logger.Log(logx.ErrorLevel, "创建rabbitmq交换机"+exchange+"失败", err.Error())
		return err
	}
	channel = b.instanceConsumePool.GetRChannelCh(rChannels)
	_, err = b.instanceConsumePool.DeclareQueue(conn, rChannels, queueName, exchangeTable)
	if err != nil {
		b.logger.Log(logx.ErrorLevel, "创建rabbitmq队列"+queueName+"失败", err.Error())
		return err
	}
	channel = b.instanceConsumePool.GetRChannelCh(rChannels)
	err = b.instanceConsumePool.BindQueue(conn, rChannels, queueName, routingKey, exchange, nil)
	if err != nil {
		b.logger.Log(logx.ErrorLevel, "绑定rabbitmq队列"+queueName+"失败", err.Error())
		return err
	}
	b.logger.Log(logx.WarnLevel, "集成事件订阅:交换机:"+exchange+"-->队列:"+queueName+"-->路由:"+routingKey+"-->success")
	return err
}

// Handles all events coming in on the channel.
func (b *EventBus) handle(m eh.EventType, h eh.EventHandler, mode rabbitmqpools.QueueConsumerMode, receive *rabbitmqpools.ConsumeReceive, num int32) {
	defer b.wg.Done()
	//获取请求连接
	closeFlag := false
	conn := b.instanceConsumePool.GetRConnection()
	//生成处理channel 根据最大channel数处理
	channel, err := b.instanceConsumePool.CreateChannel(conn)
	if err != nil {
		if receive.EventFail != nil {
			receive.EventFail(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, rabbitmqpools.NewRabbitMqError(rabbitmqpools.RCODE_CHANNEL_CREATE_ERROR, "channel create error", err.Error()), nil)
		}
		return
	}
	defer func() {
		_ = channel.Close()
		_ = b.instanceConsumePool.CloseConn(conn)
	}()
	//defer
	notifyClose := make(chan *amqp.Error)
	closeChan := make(chan *amqp.Error, 1)
	b.logger.Log(logx.TraceLevel, "处理事件-->"+m.String())
	//事件处理函数
	handler := b.handler(m, h, mode, receive, channel)
	//args
	//args := b.handlerQueueList[m]
	//确保rabbitmq会一个一个发消息
	_ = channel.Qos(1, 0, false)
	msgs, err := channel.Consume(
		receive.QueueName, // queue
		receive.TagName,   // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		if receive.EventFail != nil {
			receive.EventFail(rabbitmqpools.RCODE_GET_CHANNEL_ERROR, rabbitmqpools.NewRabbitMqError(rabbitmqpools.RCODE_GET_CHANNEL_ERROR, fmt.Sprintf("获取队列 %s 的消费通道失败", receive.QueueName), err.Error()), nil)
		}
		return
	}

	if errors.Is(err, context.Canceled) {
		b.logger.Log(logx.FatalLevel, "获取rabbitmq消息失败 cancel", err.Error())
	} else if err != nil {
		err = fmt.Errorf("could not fetch message: %w", err)
		select {
		case b.errCh <- &eh.EventBusError{Err: err}:
		default:
			b.logger.Log(logx.ErrorLevel, "event: missed error in RabbitMq event bus: %s", err)
		}
	}

	//一旦消费者的channel有错误，产生一个amqp.Error，channel监听并捕捉到这个错误
	notifyClose = channel.NotifyClose(closeChan)
	// 处理消息
	for {
		select {
		case msg := <-msgs:
			//实际执行
			if err := handler(b.cctx, &msg); err != nil {
				select {
				case b.errCh <- err:
				default:
					b.logger.Log(logx.ErrorLevel, "event: handler missed error in rabbitMq event bus: %s", err)
				}
			}
			//一但有错误直接返回 并关闭信道
		case e := <-notifyClose:
			if receive.EventFail != nil {
				receive.EventFail(rabbitmqpools.RCODE_CONNECTION_ERROR, rabbitmqpools.NewRabbitMqError(rabbitmqpools.RCODE_CONNECTION_ERROR, fmt.Sprintf("消息处理中断: queue:%s\n", receive.QueueName), e.Error()), nil)
			}
			b.instanceConsumePool.SetConnectionError(e.Code, fmt.Sprintf("消息处理中断: %s \n", e.Error()))
			closeFlag = true
		case <-b.cctx.Done():
			b.logger.Log(logx.InfoLevel, "rabbitMq订阅服务退出")

			return
		}
		if closeFlag {
			break
		}
	}
}

// 消息解析和处理函数返回
func (b *EventBus) handler(m eh.EventType, h eh.EventHandler, mode rabbitmqpools.QueueConsumerMode, receive *rabbitmqpools.ConsumeReceive, channel *amqp.Channel) func(ctx context.Context, msg *amqp.Delivery) *eh.EventBusError {
	return func(ctx context.Context, msg *amqp.Delivery) *eh.EventBusError {
		// Use a new context to always finish the commit.
		if receive.IsAutoAck { //如果是自动确认,否则需使用回调用 newRetryClient Ack
			_ = msg.Ack(true)
		} else {
			b.Ack(receive, msg)
		}
		event, ctx, err := b.codec.UnmarshalEvent(ctx, m, msg.Body)
		if err != nil {
			return &eh.EventBusError{
				Err:   err,
				Ctx:   ctx,
				Event: nil,
			}
		}
		// Handle the event
		if err := h.HandleEvent(ctx, event); err != nil {
			var errMsg string
			if receive.IsTry {
				retryNum, ok := msg.Headers["retry_nums"]
				var retryNums int32
				if !ok {
					retryNums = 0
				} else {
					retryNums = retryNum.(int32)
				}
				retryNums += 1
				msgStr, _ := utils.MarshalToString(event.Data())
				//只有在死信队列或者失败队列处理超过最大重试则打印具体Msg
				if mode == rabbitmqpools.Fail || retryNums > b.rollbackCount {
					errMsg = fmt.Sprintf("handle event (%s): err (%s),数据消费失败->ModeName:%s,EventName:%s,Message:%s,RetryCount:%d", h.HandlerType(), err.Error(), rabbitmqpools.GetQueueConsumerMode(mode), m.String(), msgStr, retryNums)
				} else {
					errMsg = fmt.Sprintf("handle event (%s): err (%s),数据消费失败->ModeName:%s,EventName:%s,RetryCount:%d", h.HandlerType(), err.Error(), rabbitmqpools.GetQueueConsumerMode(mode), m.String(), retryNums)
				}
				if mode != rabbitmqpools.Fail {
					tempMode := rabbitmqpools.Retry
					if retryNums > b.rollbackCount {
						tempMode = rabbitmqpools.Fail
					}
					tempModeName := rabbitmqpools.GetQueueConsumerMode(tempMode)
					queueTempTupleItem, ok := b.handlerNewQueueList[m.String()+"_"+tempModeName]
					if ok {
						var tempExchangeName = queueTempTupleItem.ExchangeName
						var tempRouteKeyName = queueTempTupleItem.RoutingKey
						if retryNums > b.rollbackCount {
							retryNums = b.rollbackCount
						}
						go func(tryNum int32, exchangeName, routeKeyName string, msg *amqp.Delivery) {
							time.Sleep(time.Millisecond * 200)
							header := make(map[string]interface{}, 1)
							header["retry_nums"] = tryNum

							err = channel.Publish(exchangeName, routeKeyName, false, false, amqp.Publishing{
								ContentType:  "text/plain",
								Body:         msg.Body,
								Expiration:   strconv.FormatInt(b.messageTimeToAlive, 10),
								Headers:      header,
								DeliveryMode: amqp.Persistent,
							})
							//logger.Logger.Info(fmt.Sprintf("重发消费失败数据->ModeName:%s,EventName:%s,ExchangeName:%s,RouteKey:%s", tempModeName, m.String(), exchangeName, routeKeyName))
						}(retryNums, tempExchangeName, tempRouteKeyName, msg)

					}
				}
			}
			return &eh.EventBusError{
				Err:   fmt.Errorf(errMsg), //fmt.Errorf("could not handle event (%s): %w", h.HandlerType(), err),
				Ctx:   ctx,
				Event: event,
			}
		}
		return nil
	}
}

func (b *EventBus) ack(retryClient rabbitmqpools.RetryClientInterface) {
	// Use a new context to always finish the commit.
	if err := retryClient.Ack(); err != nil {
		err = fmt.Errorf("could not ack message: %w", err)
		select {
		case b.errCh <- &eh.EventBusError{Err: err}:
		default:
			b.logger.Log(logx.ErrorLevel, "event: ack missed error in rabbitMq event bus: %s", err.Error())
		}
	}
}

func (b *EventBus) Ack(receive *rabbitmqpools.ConsumeReceive, msg *amqp.Delivery) error {
	//如果是非自动确认消息 手动进行确认
	if !receive.IsAutoAck {
		if msg.Body != nil {
			return msg.Ack(true)
		}
		return rabbitmqpools.ACK_DATA_NIL
	}
	return nil
}

func (b *EventBus) ErrorPrint() {
	// 处理错误消息
	for {
		select {
		case errMsg := <-b.Errors():
			b.logger.Log(logx.ErrorLevel, "event: handler error in rabbitMq event bus: %s", errMsg.Error())
		case errMsg := <-b.instanceConsumePool.GetErrorChannel():
			b.logger.Log(logx.ErrorLevel, "event: consumer channel error in rabbitMq event bus: %s", errMsg.Error())
		case <-b.cctx.Done():
			b.logger.Log(logx.InfoLevel, "rabbitMq订阅服务退出")
			return
		}
	}
}

// Errors implements the Errors method of the contracts.EventBus interface.
func (b *EventBus) Errors() <-chan error {
	return b.errCh
}

// Close implements the Close method of the contracts.EventBus interface.
func (b *EventBus) Close() error {
	b.cancel()
	b.wg.Wait()
	b.instanceConsumePool = nil
	b.instanceProductPool = nil
	return nil
}
