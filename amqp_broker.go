package go_celery_client

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQPExchange 表示AMQP交换。
type AMQPExchange struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
}

func NewAMQPExchange(name string, exchangeType string, durable bool, autoDelete bool) *AMQPExchange {
	return &AMQPExchange{
		Name:       name,
		Type:       exchangeType,
		Durable:    durable,
		AutoDelete: autoDelete,
	}
}

// AMQPQueue 表示AMQP队列。
type AMQPQueue struct {
	Name       string
	Durable    bool
	AutoDelete bool
}

func NewAMQPQueue(name string, durable bool, autoDelete bool) *AMQPQueue {
	return &AMQPQueue{
		Name:       name,
		Durable:    durable,
		AutoDelete: autoDelete,
	}
}

// AMQPCeleryBroker 表示AMQP Celery Broker。
type AMQPCeleryBroker struct {
	*amqp.Channel
	Connection       *amqp.Connection
	Exchange         *AMQPExchange
	Queue            *AMQPQueue
	consumingChannel <-chan amqp.Delivery
	Rate             int
}

func NewAMQPConnection(host string) (*amqp.Connection, *amqp.Channel) {
	connection, err := amqp.Dial(host)
	if err != nil {
		panic(err)
	}

	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}
	return connection, channel
}

// NewAMQPCeleryBroker creates new AMQPCeleryBroker
func NewAMQPCeleryBroker(host string, exchangeName, exchangeType, queue string) *AMQPCeleryBroker {
	if exchangeType == "" {
		exchangeType = "direct"
	}
	if queue == "" {
		queue = "celery"
	}
	if exchangeName == "" {
		exchangeName = "celery"
	}
	conn, channel := NewAMQPConnection(host)
	return NewAMQPCeleryBrokerByConnAndChannel(exchangeName, exchangeType, queue, conn, channel)
}

// NewAMQPCeleryBrokerByConnAndChannel creates new AMQPCeleryBroker using AMQP conn and channel
func NewAMQPCeleryBrokerByConnAndChannel(
	exchangeName,
	exchangeType,
	queue string,
	conn *amqp.Connection,
	channel *amqp.Channel,
) *AMQPCeleryBroker {
	broker := &AMQPCeleryBroker{
		Channel:    channel,
		Connection: conn,
		Exchange:   NewAMQPExchange(exchangeName, exchangeType, true, false),
		Queue:      NewAMQPQueue(queue, true, false),
		Rate:       4,
	}
	if err := broker.CreateExchange(); err != nil {
		panic(err)
	}
	if err := broker.CreateQueue(); err != nil {
		panic(err)
	}
	if err := broker.Qos(broker.Rate, 0, false); err != nil {
		panic(err)
	}
	if err := broker.StartConsumingChannel(); err != nil {
		panic(err)
	}
	return broker
}

// StartConsumingChannel 在AMQP队列上生成接收通道
func (b *AMQPCeleryBroker) StartConsumingChannel() error {
	channel, err := b.Consume(b.Queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	b.consumingChannel = channel
	return nil
}

// CreateExchange 使用存储配置声明AMQP交换
func (b *AMQPCeleryBroker) CreateExchange() error {
	return b.ExchangeDeclare(
		b.Exchange.Name,
		b.Exchange.Type,
		b.Exchange.Durable,
		b.Exchange.AutoDelete,
		false,
		false,
		nil,
	)
}

// CreateQueue 使用存储配置声明AMQP队列
func (b *AMQPCeleryBroker) CreateQueue() error {
	_, err := b.QueueDeclare(
		b.Queue.Name,
		b.Queue.Durable,
		b.Queue.AutoDelete,
		false,
		false,
		nil,
	)
	return err
}

func (b *AMQPCeleryBroker) SendCeleryMessage(ctx context.Context, msg *CeleryMessage) error {
	headers := amqp.Table{
		"lang":     msg.Headers.Lang,
		"task":     msg.Headers.Task,
		"id":       msg.Headers.ID,
		"argsrepr": msg.Headers.Argsrepr,
		"origin":   msg.Headers.Origin,
	}
	props := amqp.Publishing{
		Headers:         headers,
		ContentType:     msg.Properties.ContentType,
		ContentEncoding: msg.Properties.ContentEncoding,
		ReplyTo:         msg.Properties.ReplyTo,
		Body:            msg.Body,
	}
	return b.PublishWithContext(ctx, "", b.Queue.Name, false, false, props)
}
func (b *AMQPCeleryBroker) GetTaskMessage(_ context.Context) (*TaskMessage, error) {
	return nil, nil
}
