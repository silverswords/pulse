// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.
// ------------------------------------------------------------

// from https://github.com/dapr/components-contrib/blob/master/pubsub/envelope_test.go

package message

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nuid"
	"log"
)

var uidGen = nuid.New()

const (
	// DefaultCloudEventType is the default event type for an pulse published event
	DefaultCloudEventType = "com.pulse.event.sent"
	// CloudEventsSpecVersion is the specversion used by pulse for the cloud events implementation
	CloudEventsSpecVersion = "1.0"
	//ContentType is the Cloud Events HTTP content type
	ContentType = "application/cloudevents+json"
	// DefaultCloudEventSource is the default event source
	DefaultCloudEventSource = "pulse"
	// DefaultCloudEventWebhook
	DefaultCloudEventWebhook = ""
)

// CloudEventsEnvelope describes the Dapr implementation of the Cloud Events spec
// Spec details: https://github.com/cloudevents/spec/blob/master/spec.md
type CloudEventsEnvelope struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	Type        string `json:"type"`
	SpecVersion string `json:"specversion"`
	// See DataContentType https://tools.ietf.org/html/rfc2046
	DataContentType string `json:"datacontenttype"`
	Data            []byte `json:"data"`
	Topic           string `json:"topic"`
	WebhookURL      string `json:"webhookUrl"`
	OrderingKey     string `json:"orderingKey"` // for test, order id, would be ordered consume by the consumer.

	// Where the message from and to. what gob is the message have. when and why have this message.

	// Logic is represents the fields that don't need initialize by the message producer.
	Size int
	// DeliveryAttempt is the number of times a message has been delivered.
	// This is part of the dead lettering feature that forwards messages that
	// fail to be processed (from nack/ack deadline timeout) to a dead letter topic.
	// If dead lettering is enabled, this will be set on all attempts, starting
	// with value 1. Otherwise, the value will be nil.
	// This field is read-only.
	DeliveryAttempt *int
	calledDone      bool
	DoneFunc        func(string, bool) `json:"-"`
}

// NewSimpleByteMessage -
func NewSimpleByteMessage(data []byte) *CloudEventsEnvelope {
	e, err := NewCloudEventsEnvelope("", "", "", "", "", "", "", data)
	if err != nil {
		log.Println(err)
	}
	return e
}

// NewCloudEventsEnvelope returns CloudEventsEnvelope from data or a new one when data content was not
func NewCloudEventsEnvelope(id, source, datacontentType, eventType, topic, webhook, orderingKey string, data []byte) (*CloudEventsEnvelope, error) {
	// defaults
	if id == "" {
		id = uidGen.Next()
	}
	if source == "" {
		source = DefaultCloudEventSource
	}
	if eventType == "" {
		eventType = DefaultCloudEventType
	}
	if webhook == "" {
		webhook = DefaultCloudEventWebhook
	}

	e := &CloudEventsEnvelope{
		ID:              id,
		SpecVersion:     CloudEventsSpecVersion,
		DataContentType: datacontentType,
		Source:          source,
		Type:            eventType,
		Topic:           topic,
		Data:            data,
		WebhookURL:      webhook,
		OrderingKey:     orderingKey,
	}
	// Use a PublishRequest with only the Messages field to calculate the size
	// of an individual message. This accurately calculates the size of the
	// encoded proto message by accounting for the length of an individual
	// PubSubMessage and Data/Attributes field.
	// TODO(hongalex): if this turns out to take significant time, try to approximate it.
	b, err := jsoniter.ConfigFastest.Marshal(e)
	if err != nil {
		return nil, err
	}
	e.Size = len(b)
	return e, nil
}

// hint: now message string just print the event
func (m *CloudEventsEnvelope) String() string {
	return fmt.Sprintf("Id: %s Data: %s OrderingKey: %s DeliveryAttempt: %d calledDone: %v DoneFunc: %T size: %d", m.ID, m.Data, m.OrderingKey, m.DeliveryAttempt, m.calledDone, m.DoneFunc, m.Size)
}

// Ack indicates successful processing of a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// If message acknowledgement fails, the Message will be redelivered.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *CloudEventsEnvelope) Ack() {
	m.done(true)
}

// Nack indicates that the client will not or cannot process a Message passed to the Subscriber.Receive callback.
// It should not be called on any other Message value.
// Nack will result in the Message being redelivered more quickly than if it were allowed to expire.
// Client code must call Ack or Nack when finished for each received Message.
// Calls to Ack or Nack have no effect after the first call.
func (m *CloudEventsEnvelope) Nack() {
	m.done(false)
}

func (m *CloudEventsEnvelope) done(ack bool) {
	if m.calledDone {
		return
	}
	m.calledDone = true
	m.DoneFunc(m.ID, ack)
}
