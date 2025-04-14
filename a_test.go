package go_celery_client

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	rabbitmq1 "github.com/wagslane/go-rabbitmq"
)

func TestGorabbitmq(t *testing.T) {
	message := StwAccessLog{
		ClientIP:   "192.168.1.1",
		TimeLocal:  "2023-04-01 12:00:00",
		UserName:   "testuser",
		RequestUrl: "/test",
		UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
	}
	a := TaskMessage{Args: []interface{}{message}}
	conn, err := rabbitmq1.NewConn(
		"amqp://admin:zwjy.com@192.168.81.99:32386/test-devops",
		rabbitmq1.WithConnectionOptionsLogger(NewSlogLogger()),
		rabbitmq1.WithConnectionOptionsLogging,
		rabbitmq1.WithConnectionOptionsConfig(
			rabbitmq1.Config{
				Vhost: "test-devops",
			},
		),
	)
	if err != nil {
		t.Error(err)
	}
	defer conn.Close()

	publisher, err := rabbitmq1.NewPublisher(
		conn,
		rabbitmq1.WithPublisherOptionsLogging,
		rabbitmq1.WithPublisherOptionsExchangeName("stw"),
		rabbitmq1.WithPublisherOptionsConfirm,
		// rabbitmq1.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		t.Error(err)
		return
	}
	defer publisher.Close()

	publisher.NotifyReturn(
		func(r rabbitmq1.Return) {
			// log.Printf("message returned from server: %s", string(r.Body))
			t.Logf("message returned from server: %s", string(r.Body))
		},
	)

	publisher.NotifyPublish(
		func(c rabbitmq1.Confirmation) {
			// log.Printf("message confirmed from server. tag: %v, ack: %v", c.DeliveryTag, c.Ack)
			t.Logf("message confirmed from server. tag: %v, ack: %v", c.DeliveryTag, c.Ack)
		},
	)

	// block main thread - wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		fmt.Println()
		t.Log(sig)
		done <- true
	}()

	t.Log("awaiting signal")
	messageByte, err := json.Marshal(a)
	if err != nil {
		t.Errorf("json marshal error %v", err)
		return
	}
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			confirms, err := publisher.PublishWithDeferredConfirmWithContext(
				context.Background(),
				messageByte,
				[]string{"stw"},
				rabbitmq1.WithPublishOptionsContentType("application/json"),
				rabbitmq1.WithPublishOptionsContentEncoding("utf-8"),
				rabbitmq1.WithPublishOptionsMandatory,
				rabbitmq1.WithPublishOptionsPersistentDelivery,
				rabbitmq1.WithPublishOptionsExchange("stw"),
				rabbitmq1.WithPublishOptionsReplyTo("id-ssdsddssd"),
				rabbitmq1.WithPublishOptionsHeaders(
					map[string]interface{}{
						"lang":     "py",
						"task":     "stw_models.tasks.analyzing_stw_uc_user_ben_logs",
						"id":       "id-ssdsddssd",
						"argsrepr": string(messageByte),
					},
				),
			)
			if err != nil {
				t.Errorf("publish error %v", err)
				break
				// log.Println(err)
			}
			ok, err := confirms[0].WaitContext(context.Background())
			if err != nil {
				t.Errorf("confirm error %v", err)
				// log.Println(err)
				break
			}
			if ok {
				t.Log("message publishing confirmed")
			} else {
				t.Log("message publishing not confirmed")
			}
		case <-done:
			t.Log("stopping publisher")
			return
		}
	}
}
