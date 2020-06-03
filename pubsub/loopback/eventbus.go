package loopback

import (
	"errors"
	"sync"
	"github.com/silverswords/whisper"
)

var Loopback = simpleDriver{}
var subscribers = make(map[string][]chan *whisper.Message)
var rm sync.RWMutex

type simpleDriver struct{}

func (d *simpleDriver) Pub(topic string, msg *whisper.Message) error {
	rm.RLock()
	if chans, found := subscribers[topic]; found {
		channels := append([]chan *whisper.Message{}, chans...)
		go func(msg *whisper.Message, dataChannelSlices []chan *whisper.Message) {
			for _, ch := range dataChannelSlices {
				if ch == nil {
					continue
				}
				ch <- msg
			}
		}(msg, channels)
	}
	rm.RUnlock()
	return nil
}

type unSubscriber struct {
	topic string
	serial int
	channel chan *whisper.Message
}

func (u *unSubscriber) Unsubscribe() error {
	if u == nil || u.channel == nil {return errors.New("nil subscriber")}
	rm.Lock()
	subscribers[u.topic][u.serial] = nil
	rm.Unlock()
	return nil
}

func (d *simpleDriver) Sub(topic string, handler func(*whisper.Message) ) (whisper.UnSubscriber,error) {
	suber := &unSubscriber{
		topic:topic,
		channel: make(chan *whisper.Message,100),
		serial: 0,
	}

	rm.Lock()
	if prev, found := subscribers[topic]; found {
		suber.serial = len(prev)
		subscribers[topic] = append(prev, suber.channel)
	} else {
		suber.serial = 0
		subscribers[topic] = append([]chan *whisper.Message{}, suber.channel)
	}
	rm.Unlock()

	go func (){
		for suber.channel != nil {
			msg := <- suber.channel
			handler(msg)
		}
	}()

	return suber, nil
}