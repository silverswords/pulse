package whisper

import (
	"errors"
	"github.com/nats-io/nats.go/encoders/builtin"
	"reflect"
	"sync"
)
// Copyright 2012-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	// Default Encoders
	"github.com/nats-io/nats.go/encoders/builtin"
)

// Encoder interface is for all register encoders
type Encoder interface {
	Encode(subject string, v interface{}) ([]byte, error)
	Decode(subject string, data []byte, vPtr interface{}) error
}

var encMap map[string]Encoder
var encLock sync.Mutex

// Indexed names into the Registered Encoders.
const (
	JSON_ENCODER    = "json"
	GOB_ENCODER     = "gob"
	DEFAULT_ENCODER = "default"
)

func init() {
	encMap = make(map[string]Encoder)
	// Register json, gob and default encoder
	RegisterEncoder(JSON_ENCODER, &builtin.JsonEncoder{})
	RegisterEncoder(GOB_ENCODER, &builtin.GobEncoder{})
	RegisterEncoder(DEFAULT_ENCODER, &builtin.DefaultEncoder{})
}

type EncodedConn struct {
	Encoder Encoder
	Conn Conn
}

func (c *EncodedConn) BindSendChan(subject string, channel interface{}) error {
	chVal := reflect.ValueOf(channel)
	if chVal.Kind() != reflect.Chan {
		return ErrChanArg
	}
	go chPublish(c, chVal, subject)
	return nil
}

// Publish all values that arrive on the channel until it is closed or we
// encounter an error.
func chPublish(c *EncodedConn, chVal reflect.Value, subject string) {
	for {
		val, ok := chVal.Recv()
		if !ok {
			// Channel has most likely been closed.
			return
		}
		if e := c.Publish(subject, val.Interface()); e != nil {
			// Do this under lock.
			c.Conn.mu.Lock()
			defer c.Conn.mu.Unlock()

			if c.Conn.Opts.AsyncErrorCB != nil {
				// FIXME(dlc) - Not sure this is the right thing to do.
				// FIXME(ivan) - If the connection is not yet closed, try to schedule the callback
				if c.Conn.isClosed() {
					go c.Conn.Opts.AsyncErrorCB(c.Conn, nil, e)
				} else {
					c.Conn.ach.push(func() { c.Conn.Opts.AsyncErrorCB(c.Conn, nil, e) })
				}
			}
			return
		}
	}
}

// BindRecvChan binds a channel for receive operations from NATS.
func (c *EncodedConn) BindRecvChan(subject string, channel interface{}) (*Subscription, error) {
	return c.bindRecvChan(subject, _EMPTY_, channel)
}

// BindRecvQueueChan binds a channel for queue-based receive operations from NATS.
func (c *EncodedConn) BindRecvQueueChan(subject, queue string, channel interface{}) (*Subscription, error) {
	return c.bindRecvChan(subject, queue, channel)
}

// Internal function to bind receive operations for a channel.
func (c *EncodedConn) bindRecvChan(subject, queue string, channel interface{}) (*Subscription, error) {
	chVal := reflect.ValueOf(channel)
	if chVal.Kind() != reflect.Chan {
		return nil, ErrChanArg
	}
	argType := chVal.Type().Elem()

	cb := func(m *Msg) {
		var oPtr reflect.Value
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.New(argType)
		} else {
			oPtr = reflect.New(argType.Elem())
		}
		if err := c.Enc.Decode(m.Subject, m.Data, oPtr.Interface()); err != nil {
			c.Conn.err = errors.New("nats: Got an error trying to unmarshal: " + err.Error())
			if c.Conn.Opts.AsyncErrorCB != nil {
				c.Conn.ach.push(func() { c.Conn.Opts.AsyncErrorCB(c.Conn, m.Sub, c.Conn.err) })
			}
			return
		}
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.Indirect(oPtr)
		}
		// This is a bit hacky, but in this instance we may be trying to send to a closed channel.
		// and the user does not know when it is safe to close the channel.
		defer func() {
			// If we have panicked, recover and close the subscription.
			if r := recover(); r != nil {
				m.Sub.Unsubscribe()
			}
		}()
		// Actually do the send to the channel.
		chVal.Send(oPtr)
	}

	return c.Conn.subscribe(subject, queue, cb, nil, false)
}
