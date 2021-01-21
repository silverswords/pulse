package main

import (
	"strings"
	"time"

	r "github.com/dancannon/gorethink"
)

type Exchange struct {
	asserted bool
	name     string
	table    r.Term
	session  *r.Session
	subs     map[string]chan struct{}
}

func NewExchange(name string, session *r.Session) *Exchange {
	return &Exchange{
		name:    name,
		table:   r.Table(name),
		session: session,
		subs:    make(map[string]chan struct{}),
	}
}

func (e *Exchange) Publish(topicKey string, payload string) error {
	resp, err := e.table.Filter(map[string]interface{}{
		"topic": topicKey,
	}).Update(map[string]interface{}{
		"topic": topicKey, "updated_on": r.Now(), "payload": payload,
	}).RunWrite(e.session)
	if err != nil {
		return err
	}
	if resp.Replaced == 0 {
		err := e.table.Insert(map[string]interface{}{
			"topic": topicKey, "payload": payload, "updated_on": r.Now(),
		}).Exec(e.session, r.ExecOpts{NoReply: true})
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Exchange) fullQuery(matchRegexp string) r.Term {
	return e.table.Changes().Field("new_val").Filter(func(row r.Term) r.Term {
		return row.Field("topic").Match(matchRegexp)
	})
}

func (e *Exchange) assertTable() error {
	if e.asserted {
		return nil
	}
	_, err := r.TableCreate(e.name, r.TableCreateOpts{
		Durability: "soft",
	}).RunWrite(e.session)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		return err
	}
	e.asserted = true
	return nil
}

type Message struct {
	Topic   string
	Payload string
}

func (e *Exchange) Subscribe(topicKey string, match string, ch chan Message) error {
	err := e.assertTable()
	if err != nil {
		return err
	}
	messages, err := e.fullQuery(match).Run(e.session)
	if err != nil {
		return err
	}
	closeCh := make(chan struct{})
	e.subs[topicKey] = closeCh
	go func() {
		defer messages.Close()
		var message Message
		for messages.Next(&message) {
			select {
			case ch <- message:
			case <-closeCh:
				return
			}
		}
	}()
	return nil
}

func (e *Exchange) Unsubscribe(topicKey string) {
	if ch, ok := e.subs[topicKey]; ok {
		close(ch)
	}
}

func main() {
	ch := make(chan Message, 100000)

	var session *r.Session
	session, err := r.Connect(r.ConnectOpts{
		Address: "39.105.141.168:28015",
	})
	if err != nil {
		panic(err)
	}

	exch := NewExchange("subscribe_demo", session)
	err = exch.Subscribe("channel2", "channel2", ch)
	if err != nil {
		panic(err)
	}

	count := 0
	done := make(chan struct{})

	go func() {
		for msg := range ch {
			if msg.Topic == "" {
				panic("empty topic")
			}
			count++
			if count%1000 == 0 {
				println(count)
			}
			if count == 100000 {
				close(done)
			}
		}
	}()

	// 1 Core 2G MaxCPU 97% no memory pressure
	// Reads/sec: 10.2K
	// Writes/sec: 10.2K
	// every publisher write 3k/sec CPU 30~40%
	numPublishers := 2

	for i := 0; i < numPublishers; i++ {
		if i%30 == 0 {
			println(i)
		}
		pubSession, err := r.Connect(r.ConnectOpts{
			Address: "39.105.141.168:28015",
		})
		if err != nil {
			panic(err)
		}

		pubExch := NewExchange("subscribe_demo", pubSession)

		go func(ex *Exchange) {
			for {
				err := ex.Publish("channel2", "opa1")
				if err != nil {
					println(err)
				}
				time.Sleep(10 * time.Millisecond)
			}
		}(pubExch)
	}

	<-done
}
