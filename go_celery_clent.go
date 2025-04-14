package go_celery_client

import (
	"context"
	"log/slog"
)

type CeleryClient struct {
	Broker CeleryBroker
}

// CeleryBroker interface
type CeleryBroker interface {
	SendCeleryMessage(context.Context, *CeleryMessage) error
	GetTaskMessage(context.Context) (*TaskMessage, error)
}

func NewCeleryClient(broker CeleryBroker) *CeleryClient {
	return &CeleryClient{
		Broker: broker,
	}
}

type AsyncResult struct {
	TaskID string
	result *ResultMessage
}

func (cc *CeleryClient) Delay(
	ctx context.Context,
	taskName, queueName string,
	args ...interface{},
) (*AsyncResult, error) {
	celeryTask := getTaskMessage(taskName)
	celeryTask.Args = args
	celeryTask.QueryName = queueName
	return cc.delay(ctx, celeryTask)
}

func (cc *CeleryClient) DelayKwargs(
	ctx context.Context,
	taskName, queueName string,
	kwargs map[string]interface{},
) (*AsyncResult, error) {
	celeryTask := getTaskMessage(taskName)
	celeryTask.Kwargs = kwargs
	celeryTask.QueryName = queueName
	return cc.delay(ctx, celeryTask)
}

func (cc *CeleryClient) delay(
	ctx context.Context,
	task *TaskMessage,
	args ...interface{},
) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	for _, arg := range args {
		switch i := arg.(type) {
		case *CeleryProperties:
			celeryMessage.Properties.Merge(i)
		case *CeleryHeaders:
			celeryMessage.Headers.Merge(i)
		default:
			slog.WarnContext(ctx, "unknown type", slog.Any("type", i))
		}
	}
	celeryMessage.Headers.Task = task.Task
	celeryMessage.QueueName = task.QueryName
	defer releaseCeleryMessage(celeryMessage)
	err = cc.Broker.SendCeleryMessage(ctx, celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID: task.ID,
	}, nil
}
