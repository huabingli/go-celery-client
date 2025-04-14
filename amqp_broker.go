package go_celery_client

import (
	"context"
	"fmt"
	"sync"

	"github.com/wagslane/go-rabbitmq"
)

// AMQPQueue 封装了每个 Celery Task 对应的队列及发布器
type AMQPQueue struct {
	QueueName string
	Publisher *rabbitmq.Publisher
}

// AMQPCeleryBroker 管理 AMQP 连接和多个任务队列的 Publisher
type AMQPCeleryBroker struct {
	Connection *rabbitmq.Conn
	AMQPQueue  sync.Map
}

// Close 关闭连接与所有发布器
func (b *AMQPCeleryBroker) Close() {
	if b.Connection != nil {
		_ = b.Connection.Close()
	}
	b.AMQPQueue.Range(
		func(_, value any) bool {
			queue := value.(*AMQPQueue)
			queue.Publisher.Close()
			return true
		},
	)
}

// AddQueue 注册一个任务对应的队列及其 Publisher
func (b *AMQPCeleryBroker) AddQueue(queueName string) error {
	// 如果已经存在，不重复添加
	if _, ok := b.AMQPQueue.Load(queueName); ok {
		return nil
	}

	publisher, err := rabbitmq.NewPublisher(
		b.Connection,
		rabbitmq.WithPublisherOptionsLogging,
		rabbitmq.WithPublisherOptionsExchangeName(queueName),
		rabbitmq.WithPublisherOptionsConfirm,
	)
	if err != nil {
		return fmt.Errorf("failed to create publisher for: %w", err)
	}
	b.AMQPQueue.Store(
		queueName, &AMQPQueue{
			QueueName: queueName,
			Publisher: publisher,
		},
	)
	return nil
}

// NewAMQPConnection 创建一个新的 AMQP 连接
func NewAMQPConnection(host string) (*rabbitmq.Conn, error) {
	return rabbitmq.NewConn(
		host,
		rabbitmq.WithConnectionOptionsLogger(NewSlogLogger()),
		rabbitmq.WithConnectionOptionsLogging,
	)
}

// NewAMQPCeleryBroker 创建一个新的 AMQP Celery Broker（带默认参数）
func NewAMQPCeleryBroker(host string) (*AMQPCeleryBroker, error) {

	conn, err := NewAMQPConnection(host)
	if err != nil {
		return nil, fmt.Errorf("failed to create AMQP connection: %w", err)
	}
	return NewAMQPCeleryBrokerByConn(conn), nil
}

// NewAMQPCeleryBrokerByConn  基于已有连接创建 Broker
func NewAMQPCeleryBrokerByConn(conn *rabbitmq.Conn) *AMQPCeleryBroker {
	broker := &AMQPCeleryBroker{
		Connection: conn,
	}
	return broker
}

// SendCeleryMessage 发送Celery消息
func (b *AMQPCeleryBroker) SendCeleryMessage(ctx context.Context, msg *CeleryMessage) error {
	v, ok := b.AMQPQueue.Load(msg.QueueName)
	if !ok {
		return fmt.Errorf("no queue found for task: %s", msg.Headers.Task)
	}
	queue := v.(*AMQPQueue)

	headers := rabbitmq.Table{
		"lang":     msg.Headers.Lang,
		"task":     msg.Headers.Task,
		"id":       msg.Headers.ID,
		"argsrepr": msg.Headers.Argsrepr,
		"origin":   msg.Headers.Origin,
	}

	confirms, err := queue.Publisher.PublishWithDeferredConfirmWithContext(
		ctx,
		msg.Body,
		[]string{queue.QueueName},
		rabbitmq.WithPublishOptionsContentType(msg.Properties.ContentType),
		rabbitmq.WithPublishOptionsContentEncoding(msg.Properties.ContentEncoding),
		rabbitmq.WithPublishOptionsMandatory,
		rabbitmq.WithPublishOptionsPersistentDelivery,
		rabbitmq.WithPublishOptionsExchange(queue.QueueName),
		rabbitmq.WithPublishOptionsReplyTo(msg.Properties.ReplyTo),
		rabbitmq.WithPublishOptionsHeaders(headers),
	)
	if err != nil {
		return fmt.Errorf("publish error: %w", err)
	}
	ok, err = confirms[0].WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirm wait error: %w", err)
	}
	if !ok {
		return fmt.Errorf("message not acknowledged by broker")
	}
	return nil
}
func (b *AMQPCeleryBroker) GetTaskMessage(_ context.Context) (*TaskMessage, error) {
	return nil, nil
}
