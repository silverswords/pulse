package whisper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/silverswords/whisper/internal"
	"time"
)

//// DirectMessaging is the API interface for invoking a remote app
//type DirectMessaging interface {
//	Invoke(ctx context.Context, targetAppID string, req *invokev1.InvokeMethodRequest) (*invokev1.InvokeMethodResponse, error)
//}

// Message format maybe below
//{
//"specversion": "1.x-wip",
//"type": "coolevent",
//"id": "xxxx-xxxx-xxxx",
//"source": "bigco.com",
//"data": { ... }
//}
type Message struct {
	Id   string
	Data []byte // Message data

	// Where the message from and to. what codec is the message have. when and why have this message.
	Attributes internal.Header // Message Header use to specific message and how to handle it.

	// Logic is represents the fields that don't need initialize by the message producer.
	L Logic
}

type Logic struct {
	AckID string
	// Timestamp
	publishTime time.Time
	receiveTime time.Time

	// DeliveryAttempt is the number of times a message has been delivered.
	// This is part of the dead lettering feature that forwards messages that
	// fail to be processed (from nack/ack deadline timeout) to a dead letter topic.
	// If dead lettering is enabled, this will be set on all attempts, starting
	// with value 1. Otherwise, the value will be nil.
	// This field is read-only.
	DeliveryAttempt *int
	calledDone      bool
	doneFunc        func(string, bool, time.Time)

	size        int
	OrderingKey string
}

func (l *Logic) String() string {
	return fmt.Sprintf("AckID: %s PublishTime: %v ReceiveTime %v DeliveryAttempt: %d calledDone: %v doneFunc: %T size: %d OrderingKey: %s", l.AckID, l.publishTime, l.receiveTime, l.DeliveryAttempt, l.calledDone, l.doneFunc, l.size, l.OrderingKey)
}

// note that id should be uuid.
func NewMessage(id string, data []byte) *Message {
	return &Message{
		Id:         id,
		Data:       data,
		Attributes: make(internal.Header),
		L: Logic{
			publishTime: time.Now(),
		},
	}
}

// Ack indicates successful processing of a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// If message acknowledgement fails, the Message will be redelivered.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *Message) Ack() {
	m.done(true)
}

// Nack indicates that the client will not or cannot process a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// Nack will result in the Message being redelivered more quickly than if it were allowed to expire.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *Message) Nack() {
	m.done(false)
}

func (m *Message) done(ack bool) {
	if m.L.calledDone {
		return
	}
	m.L.calledDone = true
	m.L.doneFunc(m.L.AckID, ack, m.L.receiveTime)
}

func (m *Message) String() string {
	return fmt.Sprintf("Id: %s Data: %s Attributes: %v ", m.Id, m.Data, m.Attributes)
}

func ToByte(m *Message) []byte {
	mb, _ := Encode(m)
	return mb
}

func ToMessage(bytes []byte) (*Message, error) {
	m := &Message{}
	err := Decode(bytes, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func Encode(data interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(data)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte, to interface{}) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	return dec.Decode(to)
}
