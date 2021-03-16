package protocol

import (
	"context"
	"github.com/silverswords/pulse/pkg/visitor"
)

type Message struct {
	UUID string
	Data []byte

	Topic       string
	OrderingKey string
}

func NewMessage(topic, orderingKey string, data []byte) *Message {
	return &Message{
		UUID:        uidGen.Next(),
		Data:        data,
		Topic:       topic,
		OrderingKey: orderingKey,
	}
}

func (m *Message) Do(fn visitor.DoFunc) error {
	return fn(context.Background(), m)
}

// PublishRequest is the request to publish a message
type PublishRequest struct {
	Data       []byte            `json:"data"`
	PubsubName string            `json:"pubsubname"`
	Topic      string            `json:"topic"`
	Metadata   map[string]string `json:"metadata"`

	OrderingKey string `json:"orderingkey"`
}

// SubscribeRequest is the request to subscribe to a topic
type SubscribeRequest struct {
	Topic    string   `json:"topic"`
	Metadata Metadata `json:"metadata"`
}

type Metadata struct {
	Properties map[string]string
}

func NewMetadata() *Metadata {
	return &Metadata{Properties: make(map[string]string)}
}

func (m *Metadata) Clone() *Metadata {
	newProperties := make(map[string]string)
	for k, v := range m.Properties {
		newProperties[k] = v
	}
	return &Metadata{Properties: newProperties}
}

// if driverName is empty, use default local driver. which couldn't cross process
func (m *Metadata) GetDriverName() string {
	var noDriver = ""
	if driverName, ok := m.Properties["DriverName"]; ok {
		return driverName
	}
	return noDriver
}

func (m *Metadata) SetDriver(driverName string) {
	m.Properties["DriverName"] = driverName
}
