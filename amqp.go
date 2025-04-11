package go_celery_client

import (
	"context"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

func deliveryAck(ctx context.Context, delivery amqp.Delivery) {
	var err error

	for retryCout := 3; retryCout > 0; retryCout-- {
		err = delivery.Ack(false)
		if err == nil {
			break
		}
	}
	if err != nil {
		slog.ErrorContext(
			ctx, "amqp_backend: failed to acknowledge result message", slog.Any("err", err),
			slog.String("messageId", delivery.MessageId),
		)
	}
}
