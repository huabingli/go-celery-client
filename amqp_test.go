package go_celery_client

import (
	"context"
	"strconv"
	"testing"
)

type StwAccessLog struct {
	ClientIP   string `json:"client_ip"`
	TimeLocal  string `json:"time_local"`
	UserName   string `json:"user_name"`
	RequestUrl string `json:"request_url"`
	UserAgent  string `json:"user_agent"`
}

func TestAMQPBroker(t *testing.T) {
	message := StwAccessLog{
		ClientIP:   "192.168.1.1",
		TimeLocal:  "2023-04-01 12:00:00",
		UserName:   "testuser",
		RequestUrl: "/test",
		UserAgent:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
	}
	amqp, err := NewAMQPCeleryBroker("amqp://admin:zwjy.com@192.168.81.99:32386/test-devops")
	if err != nil {
		t.Error(err)
	}
	defer amqp.Close()
	err = amqp.AddQueue("stw")
	if err != nil {
		t.Error(err)
		return
	}
	celery := NewCeleryClient(
		amqp,
	)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		message.UserName = "testuser" + strconv.Itoa(i)
		_, err := celery.Delay(ctx, "stw_models.tasks.analyzing_stw_uc_user_ben_logs", "stw", message)
		if err != nil {
			t.Error(err)
		}
	}
	_, err = celery.Delay(ctx, "stw_models.tasks.analyzing_stw_uc_user_ben_logs", "stw", message)
	if err != nil {
		t.Error(err)
	}
}
