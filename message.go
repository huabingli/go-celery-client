package go_celery_client

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/huabingli/go-common"
)

type CeleryProperties struct {
	ContentType     string `json:"content_type,omitempty"`
	ContentEncoding string `json:"content_encoding,omitempty"`
	CorrelationId   string `json:"correlation_id,omitempty"`
	ReplyTo         string `json:"reply_to,omitempty"`
}

// Merge 对比判断是否存在字段，如果存在，则赋值
func (cp *CeleryProperties) Merge(other *CeleryProperties) {
	if other == nil {
		return
	}
	if other.ContentType != "" {
		cp.ContentType = other.ContentType
	}
	if other.ContentEncoding != "" {
		cp.ContentEncoding = other.ContentEncoding
	}
	if other.CorrelationId != "" {
		cp.CorrelationId = other.CorrelationId
	}
	if other.ReplyTo != "" {
		cp.ReplyTo = other.ReplyTo
	}
}

var propertiesPool = sync.Pool{
	New: func() interface{} {
		return &CeleryProperties{}
	},
}

func getProperties() *CeleryProperties {
	p := propertiesPool.Get().(*CeleryProperties)
	p.CorrelationId = common.GenerateRequestID()
	p.ReplyTo = common.GenerateRequestID()
	p.ContentEncoding = "utf-8"
	p.ContentType = "application/json"
	return p
}

func releaseProperties(p *CeleryProperties) {
	*p = CeleryProperties{}
	propertiesPool.Put(p)
}

type CeleryHeaders struct {
	Lang                string `json:"lang,omitempty"`
	Task                string `json:"task,omitempty"`
	ID                  string `json:"id,omitempty"`
	RootId              string `json:"root_id,omitempty"`
	ParentId            string `json:"parent_id,omitempty"`
	Group               string `json:"group,omitempty"`
	Meth                string `json:"meth,omitempty"`
	Shadow              string `json:"shadow,omitempty"`
	Eta                 string `json:"eta,omitempty"`
	Expires             string `json:"expires,omitempty"`
	Retries             int    `json:"retries,omitempty"`
	Timestamp           string `json:"timestamp,omitempty"`
	Argsrepr            string `json:"argsrepr,omitempty"`
	Kwargsrepr          string `json:"kwargsrepr,omitempty"`
	Origin              string `json:"origin,omitempty"`
	ReplacedTaskNesting string `json:"replaced_task_nesting,omitempty"`
}

// Merge 对比判断是否存在字段，如果存在，则赋值
func (ch *CeleryHeaders) Merge(other *CeleryHeaders) {
	if other == nil {
		return
	}
	if other.Lang != "" {
		ch.Lang = other.Lang
	}
	if other.Task != "" {
		ch.Task = other.Task
	}
	if other.ID != "" {
		ch.ID = other.ID
	}
	if other.RootId != "" {
		ch.RootId = other.RootId
	}
	if other.ParentId != "" {
		ch.ParentId = other.ParentId
	}
	if other.Group != "" {
		ch.Group = other.Group
	}
	if other.Meth != "" {
		ch.Meth = other.Meth
	}
	if other.Shadow != "" {
		ch.Shadow = other.Shadow
	}
	if other.Eta != "" {
		ch.Eta = other.Eta
	}
	if other.Expires != "" {
		ch.Expires = other.Expires
	}
	if other.Retries != 0 {
		ch.Retries = other.Retries
	}
	if other.Timestamp != "" {
		ch.Timestamp = other.Timestamp
	}
	if other.Argsrepr != "" {
		ch.Argsrepr = other.Argsrepr
	}
	if other.Kwargsrepr != "" {
		ch.Kwargsrepr = other.Kwargsrepr
	}
	if other.Origin != "" {
		ch.Origin = other.Origin
	}
	if other.ReplacedTaskNesting != "" {
		ch.ReplacedTaskNesting = other.ReplacedTaskNesting
	}
}

var cachedHostname = os.Getenv("HOSTNAME")
var origin = fmt.Sprintf("%d@%s", os.Getgid(), cachedHostname)

var headersPool = sync.Pool{
	New: func() interface{} {
		return &CeleryHeaders{}
	},
}

func getHeaders() *CeleryHeaders {
	h := headersPool.Get().(*CeleryHeaders)
	h.Lang = "py"
	h.ID = common.GenerateRequestID()
	h.Origin = origin
	return h
}

func releaseHeaders(h *CeleryHeaders) {
	*h = CeleryHeaders{}
	headersPool.Put(h)
}

type CeleryMessage struct {
	Body            []byte            `json:"body,omitempty"`
	Headers         *CeleryHeaders    `json:"headers,omitempty"`
	Properties      *CeleryProperties `json:"properties,omitempty"`
	QueueName       string            `json:"queue_name,omitempty"`
	ContentType     string            `json:"content_type,omitempty"`
	ContentEncoding string            `json:"content_encoding,omitempty"`
}

func (cm *CeleryMessage) reset() {
	cm.ContentEncoding = "utf-8"
	cm.ContentType = "application/json"
	cm.Body = nil

	// 先释放旧的
	if cm.Headers != nil {
		releaseHeaders(cm.Headers)
	}

	if cm.Properties != nil {
		releaseProperties(cm.Properties)
	}

	cm.Headers = getHeaders()
	cm.Properties = getProperties()

}

var celeryMessagePool = sync.Pool{
	New: func() interface{} {
		msg := &CeleryMessage{}
		msg.reset()
		return msg
	},
}

func getCeleryMessage(encodedTaskMessage []byte) *CeleryMessage {
	msg := celeryMessagePool.Get().(*CeleryMessage)
	msg.Body = encodedTaskMessage
	msg.Headers.Argsrepr = string(encodedTaskMessage)
	return msg
}

func releaseCeleryMessage(v *CeleryMessage) {
	v.reset()
	celeryMessagePool.Put(v)
}

type ResultMessage struct {
	ID        string        `json:"task_id"`
	Status    string        `json:"status"`
	Traceback interface{}   `json:"traceback"`
	Result    interface{}   `json:"result"`
	Children  []interface{} `json:"children"`
}

type TaskMessage struct {
	ID        string                 `json:"-"`
	QueryName string                 `json:"-"`
	Task      string                 `json:"-"`
	Retries   int                    `json:"-"`
	Args      []interface{}          `json:"args,omitempty"`
	Kwargs    map[string]interface{} `json:"kwargs,omitempty"`
	Embed     interface{}            `json:"-"`
}

func (tm *TaskMessage) reset() {
	tm.ID = common.GenerateRequestID()
	tm.Task = ""
	tm.QueryName = ""
	tm.Args = nil
	tm.Kwargs = nil
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &TaskMessage{
			Args:   nil,
			Kwargs: nil,
		}
	},
}

func getTaskMessage(taskName string) *TaskMessage {
	msg := taskMessagePool.Get().(*TaskMessage)
	msg.Task = taskName
	msg.ID = common.GenerateRequestID()
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	return msg
}
func releaseTaskMessage(v *TaskMessage) {
	v.reset()
	taskMessagePool.Put(v)
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() ([]byte, error) {
	if tm.Args == nil {
		tm.Args = make([]interface{}, 0)
	}
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return nil, err
	}
	return jsonData, err
}
