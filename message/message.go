package message

import (
	"bytes"
	"encoding/gob"
)

// Message format maybe below
//{
//"specversion": "1.x-wip",
//"type": "coolevent",
//"id": "xxxx-xxxx-xxxx",
//"source": "bigco.com",
//"data": { ... }
//}
type Message struct {
	// Where the message from and to. what codec is the message have. when and why have this message.
	context Header // Message Header use to specific message and how to handle it.
	data    []byte // Message data
}

func (m *Message) SetTopic(topic string) {
	m.context.SetTopic(topic)
}

func (m *Message) Topic() string {
	return m.context.GetTopic()
}

func ToByte(m *Message) []byte {
	bytes, _ := Encode(m)
	return bytes
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
