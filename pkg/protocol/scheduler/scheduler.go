// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//edited from the https://github.com/googleapis/google-cloud-go

package scheduler

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"google.golang.org/api/support/bundler"
)

// PublishScheduler is a scheduler which is designed for Pub/Sub's Publish flow.
// It bundles items before handling them. All items in this PublishScheduler use
// the same handler.
//
// Each item is added with a given key. Items added to the empty string key are
// handled in random order. Items added to any other key are handled
// sequentially.
type PublishScheduler struct {
	// Settings passed down to each bundler that gets created.
	DelayThreshold time.Duration
	// Once a bundle has this many items, handle the bundle. Since only one
	// item at a time is added to a bundle, no bundle will exceed this
	// threshold, so it also serves as a limit. The default is
	// DefaultBundleCountThreshold.
	BundleCountThreshold int
	// Once the number of bytes in current bundle reaches this threshold, handle
	// the bundle. The default is DefaultBundleByteThreshold. This triggers handling,
	// but does not cap the total size of a bundle.
	BundleByteThreshold int
	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow. The default is DefaultBufferedByteLimit.
	BundleByteLimit   int
	BufferedByteLimit int

	mu          sync.Mutex
	bundlers    map[string]*bundler.Bundler
	outstanding map[string]int

	keysMu sync.RWMutex
	// keysWithErrors tracks ordering keys that cannot accept new messages.
	// A bundler might not accept new messages if publishing has failed
	// for a specific ordering key, and can be resumed with topic.ResumePublish().
	keysWithErrors map[string]struct{}

	// workers is a channel that represents workers. Rather than a pool, where
	// worker are "removed" until the pool is empty, the channel is more like a
	// set of work desks, where workers are "added" until all the desks are full.
	//
	// workers does not restrict the amount of goroutines in the bundlers.
	// Rather, it acts as the flow control for completion of bundler work.
	workers chan struct{}
	handle  func(bundle interface{})
	done    chan struct{}
}

// NewPublishScheduler returns a new PublishScheduler.
//
// The workers arg is the number of workers that will operate on the queue of
// work. A reasonably large number of workers is highly recommended. If the
// workers arg is 0, then a healthy default of 10 workers is used.
//
// The scheduler does not use a parent context. If it did, canceling that
// context would immediately stop the scheduler without waiting for
// undelivered messages.
//
// The scheduler should be stopped only with FlushAndStop.
func NewPublishScheduler(workers int, handle func(bundle interface{})) *PublishScheduler {
	if workers == 0 {
		workers = 10
	}

	s := PublishScheduler{
		bundlers:       make(map[string]*bundler.Bundler),
		outstanding:    make(map[string]int),
		keysWithErrors: make(map[string]struct{}),
		workers:        make(chan struct{}, workers),
		handle:         handle,
		done:           make(chan struct{}),
	}

	return &s
}

// Add adds an item to the scheduler at a given key.
//
// Add never blocks. Buffering happens in the scheduler's publishers. There is
// no flow control.
//
// Since ordered keys require only a single outstanding RPC at once, it is
// possible to send ordered key messages to Topic.Publish (and subsequently to
// PublishScheduler.Add) faster than the bundler can publish them to the
// Pub/Sub service, resulting in a backed up queue of Pub/Sub bundles. Each
// item in the bundler queue is a goroutine.
func (s *PublishScheduler) Add(key string, item interface{}, size int) error {
	select {
	case <-s.done:
		return errors.New("draining")
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bundlers[key]
	if !ok {
		s.outstanding[key] = 1
		b = bundler.NewBundler(item, func(bundle interface{}) {
			//log.Println("add the message in the publish queue and ready to pub with handle: ",bundle)
			s.workers <- struct{}{}
			s.handle(bundle)
			<-s.workers

			nlen := reflect.ValueOf(bundle).Len()
			s.mu.Lock()
			s.outstanding[key] -= nlen
			if s.outstanding[key] == 0 {
				delete(s.outstanding, key)
				delete(s.bundlers, key)
			}
			s.mu.Unlock()
		})
		b.DelayThreshold = s.DelayThreshold
		b.BundleCountThreshold = s.BundleCountThreshold
		b.BundleByteThreshold = s.BundleByteThreshold
		b.BundleByteLimit = s.BundleByteLimit
		b.BufferedByteLimit = s.BufferedByteLimit

		if b.BufferedByteLimit == 0 {
			b.BufferedByteLimit = 1e9
		}

		if key == "" {
			// There's no way to express "unlimited" in the bundler, so we use
			// some high number.
			b.HandlerLimit = 1e9
		} else {
			// HandlerLimit=1 causes the bundler to act as a sequential queue.
			b.HandlerLimit = 1
		}

		s.bundlers[key] = b
	}
	s.outstanding[key]++
	return b.Add(item, size)
}

// FlushAndStop begins flushing items from bundlers and from the scheduler. It
// blocks until all items have been flushed.
func (s *PublishScheduler) FlushAndStop() {
	close(s.done)
	for _, b := range s.bundlers {
		b.Flush()
	}
}

// IsPaused checks if the bundler associated with an ordering keys is
// paused.
func (s *PublishScheduler) IsPaused(orderingKey string) bool {
	s.keysMu.RLock()
	defer s.keysMu.RUnlock()
	_, ok := s.keysWithErrors[orderingKey]
	return ok
}

// Pause pauses the bundler associated with the provided ordering key,
// preventing it from accepting new messages. Any outstanding messages
// that haven't been published will error. If orderingKey is empty,
// this is a no-op.
func (s *PublishScheduler) Pause(orderingKey string) {
	if orderingKey != "" {
		s.keysMu.Lock()
		defer s.keysMu.Unlock()
		s.keysWithErrors[orderingKey] = struct{}{}
	}
}

// Resume resumes accepting message with the provided ordering key.
func (s *PublishScheduler) Resume(orderingKey string) {
	s.keysMu.Lock()
	defer s.keysMu.Unlock()
	delete(s.keysWithErrors, orderingKey)
}

// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// ReceiveScheduler is a scheduler which is designed for Pub/Sub's Receive flow.
//
// Each item is added with a given key. Items added to the empty string key are
// handled in random order. Items added to any other key are handled
// sequentially.
type ReceiveScheduler struct {
	// workers is a channel that represents workers. Rather than a pool, where
	// worker are "removed" until the pool is empty, the channel is more like a
	// set of work desks, where workers are "added" until all the desks are full.
	//
	// A worker taking an item from the unordered queue (key="") completes a
	// single item and then goes back to the pool.
	//
	// A worker taking an item from an ordered queue (key="something") completes
	// all work in that queue until the queue is empty, then deletes the queue,
	// then goes back to the pool.
	workers chan struct{}
	done    chan struct{}

	mu sync.Mutex
	m  map[string][]func()
}

// NewReceiveScheduler creates a new ReceiveScheduler.
//
// The workers arg is the number of concurrent calls to handle. If the workers
// arg is 0, then a healthy default of 10 workers is used. If less than 0, this
// will be set to an large number, similar to PublishScheduler's handler limit.
func NewReceiveScheduler(workers int) *ReceiveScheduler {
	if workers == 0 {
		workers = 10
	} else if workers < 0 {
		workers = 1e9
	}

	return &ReceiveScheduler{
		workers: make(chan struct{}, workers),
		done:    make(chan struct{}),
		m:       make(map[string][]func()),
	}
}

// Add adds the item to be handled. Add may block.
//
// Buffering happens above the ReceiveScheduler in the form of a flow controller
// that requests batches of messages to pull. A backed up ReceiveScheduler.Add
// call causes pushback to the pubsub service (less Receive calls on the
// long-lived stream), which keeps memory footprint stable.
func (s *ReceiveScheduler) Add(key string, item interface{}, handle func(item interface{})) error {
	select {
	case <-s.done:
		return errors.New("draining")
	default:
	}

	if key == "" {
		// Spawn a worker.
		s.workers <- struct{}{}
		go func() {
			// Unordered keys can be handled immediately.
			handle(item)
			<-s.workers

		}()
		return nil
	}

	// Add it to the queue. This has to happen before we enter the goroutine
	// below to prevent a race from the next iteration of the key-loop
	// adding another item before this one gets queued.

	s.mu.Lock()
	_, ok := s.m[key]
	s.m[key] = append(s.m[key], func() {
		handle(item)
	})
	s.mu.Unlock()
	if ok {
		// Someone is already working on this key.
		return nil
	}

	// Spawn a worker.
	s.workers <- struct{}{}

	go func() {
		defer func() { <-s.workers }()

		// Key-Loop: loop through the available items in the key's queue.
		for {
			s.mu.Lock()
			if len(s.m[key]) == 0 {
				// We're done processing items - the queue is empty. Delete
				// the queue from the map and free up the worker.
				delete(s.m, key)
				s.mu.Unlock()
				return
			}
			// Pop an item from the queue.
			next := s.m[key][0]
			s.m[key] = s.m[key][1:]
			s.mu.Unlock()

			next() // Handle next in queue.
		}
	}()

	return nil
}

// Shutdown begins flushing messages and stops accepting new Add calls. Shutdown
// does not block, or wait for all messages to be flushed.
func (s *ReceiveScheduler) Shutdown() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}
