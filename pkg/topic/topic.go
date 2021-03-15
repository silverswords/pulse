// Publish logic select the code form Google Code with MIT. https://github.com/googleapis/google-cloud-go/blob/master/pubsub/topic.go
package topic

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/protocol/aresult"
	"github.com/silverswords/pulse/pkg/protocol/retry"
	"github.com/silverswords/pulse/pkg/pubsub"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"github.com/silverswords/pulse/pkg/scheduler"
	"github.com/silverswords/pulse/pkg/visitor"
	"golang.org/x/sync/errgroup"

	//"go.opencensus.io/stats"
	//"github.com/golang/protobuf/proto"

	"runtime"
	"sync"
	"time"

	"go.opencensus.io/tag"
)

const (
	// MaxPublishRequestCount is the maximum number of messages that can be in
	// a single publish request, as defined by the PubSub service.
	MaxPublishRequestCount = 1000

	// MaxPublishRequestBytes is the maximum size of a single publish request
	// in bytes, as defined by the PubSub service.
	MaxPublishRequestBytes = 1e7 // 10m

	// prefix use to specific to other client use the same MQ.
	AckTopicPrefix = "ack_"
	PulsePrefix    = "p_"
)

var (
	log = logger.NewLogger("pulse")

	errTopicOrderingDisabled = errors.New("BundleTopic.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on BundleTopic.EnableMessageOrdering")
	errTopicStopped          = errors.New("pubsub: Stop has been called for this topic")
)

type BundleTopic struct {
	mu sync.RWMutex

	pubsub.PubSub
	conns map[uint64]driver.Conn

	stopped bool
	// Settings for publishing messages. All changes must be made before the
	// first call to Publish. The default is DefaultPublishSettings.
	// it means could not dynamically change and hot start.
	Settings
	scheduler *scheduler.BundleScheduler
}

// Settings control the bundling of published messages.
type Settings struct {
	// EnableMessageOrdering enables delivery of ordered keys.
	EnableMessageOrdering bool

	// Publish a non-empty batch after this delay has passed.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. The maximum is
	// MaxPublishRequestCount.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value.
	ByteThreshold int

	// The number of goroutines used in each of the data structures that are
	// involved along the the Publish path. Adjusting this value adjusts
	// concurrency along the publish path.
	//
	// Defaults to a multiple of GOMAXPROCS.
	NumGoroutines int

	// The maximum time that the client will attempt to publish a bundle of messages.
	Timeout time.Duration

	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow.
	//
	// Defaults to DefaultPublishSettings.BufferedByteLimit.
	BufferedByteLimit int
}

// DefaultPublishSettings holds the default values for topics' Settings.
var DefaultPublishSettings = Settings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	NumGoroutines:  100 * runtime.GOMAXPROCS(0),
	Timeout:        60 * time.Second,
	// By default, limit the bundler to 10 times the max protocol size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	// default linear increase retry interval and 10 times.
}

// NewTopic new a topic and init it with the connection options
func NewTopic(driverMetadata protocol.Metadata, options ...Option) (*BundleTopic, error) {
	d, err := pubsub.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	t := &BundleTopic{
		topicOptions: options,
		d:            d,
		Settings:     DefaultPublishSettings,
		pendingAcks:  make(map[string]bool),
		deadQueue:    nil,
	}

	if err := t.applyOptions(options...); err != nil {
		return nil, err
	}

	if err := t.d.Init(driverMetadata); err != nil {
		return nil, err
	}
	if err := t.startAck(context.Background()); err != nil {
		_ = t.d.Close()
		return nil, err
	}
	return t, nil
}

func (t *BundleTopic) startAck(_ context.Context) error {
	// open a client, receive and then ack the protocol.
	// protocol should check itself and then depend on topic RetryParams to retry.
	if !t.EnableAck {
		return nil
	}

	subCloser, err := t.d.Subscribe(AckTopicPrefix+t.name, func(out []byte) {
		e := &protocol.CloudEventsEnvelope{}
		err := jsoniter.ConfigFastest.Unmarshal(out, e)
		if err != nil {
			log.Error("topic", t.name, "error in ack protocol decode: ", err)
			//	 not our pulse protocol, just ignore it
			return
		}
		log.Debug("ACK: topic ", t.name, " received ackId ", e.ID)
		t.done(e.ID, true, time.Now())
	})
	log.Debug("try to subscribe the ack topic")
	if err != nil {
		if subCloser != nil {
			_ = subCloser.Close()
		}
		return err
	}
	return nil
}

// Publish publishes msg to the topic asynchronously. Messages are batched and
// sent according to the topic's Settings. Publish never blocks.
//
// Publish returns a non-nil aresult.Result which will be ready when the
// protocol has been sent (or has failed to be sent) to the server.
//
// Publish creates goroutines for batching and sending messages. These goroutines
// need to be stopped by calling t.Stop(). Once stopped, future calls to Publish
// will immediately return a aresult.Result with an error.
// advice: don't resume so quickly like less than 10* millisecond of Bundler ticker flush setting.
// that's ensure all the protocol in bundle with the key return error when scheduler paused.
//
// Warning: do not use incoming protocol pointer again that if protocol had been successfully
// add in the scheduler, protocol would be equal nil to gc.
//
// Warning: when use ordering feature, recommend to limit the QPS to 100, or use synchronous
func (t *BundleTopic) Publish(ctx context.Context, msg *protocol.CloudEventsEnvelope) *aresult.Result {
	r := aresult.NewResult()
	if !t.EnableMessageOrdering && msg.OrderingKey != "" {
		r.Set(errTopicOrderingDisabled)
		return r
	}

	t.start()
	t.mu.RLock()
	defer t.mu.RUnlock()
	// TODO(aboulhosn) [from bcmills] consider changing the semantics of bundler to perform this logic so we don't have to do it here
	if t.stopped {
		r.Set(errTopicStopped)
		return r
	}

	// TODO(jba) [from bcmills] consider using a shared channel per bundle
	// (requires Bundler API changes; would reduce allocations)
	err := t.scheduler.Add(msg.OrderingKey, &bundledMessage{msg, r}, msg.Size)
	if err != nil {
		t.scheduler.Pause(msg.OrderingKey)
		r.Set(err)
	}
	return r
}

// ResumePublish resumes accepting messages for the provided ordering key.
// Publishing using an ordering key might be paused if an error is
// encountered while publishing, to prevent messages from being published
// out of order.
func (t *BundleTopic) ResumePublish(orderingKey string) {
	t.mu.RLock()
	noop := t.scheduler == nil
	t.mu.RUnlock()
	if noop {
		return
	}
	t.scheduler.Resume(orderingKey)
}

// Stop sends all remaining published messages and stop goroutines created for handling
// publishing. Returns once all outstanding messages have been sent or have
// failed to be sent.
func (t *BundleTopic) Stop() {
	t.mu.Lock()
	noop := t.stopped || t.scheduler == nil
	t.stopped = true
	t.mu.Unlock()
	if noop {
		return
	}
	t.scheduler.FlushAndStop()
}

func (t *BundleTopic) done(ackId string, ack bool, _ time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ack {
		t.pendingAcks[ackId] = true
	}
}

// use to start the topic sender and acker
func (t *BundleTopic) start() {
	t.mu.RLock()
	onceStart := t.stopped || t.scheduler != nil
	t.mu.RUnlock()
	if onceStart {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Must re-check, since we released the lock.
	if t.stopped || t.scheduler != nil {
		return
	}

	timeout := t.Settings.Timeout
	workers := t.Settings.NumGoroutines
	// Unless overridden, allow many goroutines per CPU to call the Publish RPC
	// concurrently. The default value was determined via extensive load
	// testing (see the loadtest subdirectory).
	if t.Settings.NumGoroutines == 0 {
		workers = 25 * runtime.GOMAXPROCS(0)
	}

	t.scheduler = scheduler.NewPublishScheduler(workers, func(bundle interface{}) {
		// TODO(jba): use a context detached from the one passed to NewClient.
		ctx := context.TODO()
		if timeout != 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		t.publishMessageBundle(ctx, bundle.([]*bundledMessage))
	})
	t.scheduler.DelayThreshold = t.Settings.DelayThreshold
	t.scheduler.BundleCountThreshold = t.Settings.CountThreshold
	if t.scheduler.BundleCountThreshold > MaxPublishRequestCount {
		t.scheduler.BundleCountThreshold = MaxPublishRequestCount
	}
	t.scheduler.BundleByteThreshold = t.Settings.ByteThreshold

	bufferedByteLimit := DefaultPublishSettings.BufferedByteLimit
	if t.Settings.BufferedByteLimit > 0 {
		bufferedByteLimit = t.Settings.BufferedByteLimit
	}
	t.scheduler.BufferedByteLimit = bufferedByteLimit

	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow. The default is DefaultBufferedByteLimit.
	t.scheduler.BundleByteLimit = MaxPublishRequestBytes // - calcFieldSizeString(t.name)
}

// publishMessageBundle just handle the send logic
func (t *BundleTopic) publishMessageBundle(ctx context.Context, bms []*publisher.AsyncResultActor) {
	ctx, err := tag.New(ctx, tag.Insert(keyStatus, "OK"), tag.Upsert(keyTopic, t.name))
	if err != nil {
		log.Errorf("pubsub: cannot create context with tag in publishMessageBundle: %v", err)
	}
	group, gCtx := errgroup.WithContext(ctx)

	for _, bm := range bms {
		if bm.msg.OrderingKey != "" && t.scheduler.IsPaused(bm.msg.OrderingKey) {
			err = fmt.Errorf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", bm.msg.OrderingKey)
			bm.res.set("", err)
			continue
		}

		closureBundleMessage := &bundledMessage{
			msg: bm.msg,
			res: bm.res,
		}

		if bm.msg.OrderingKey == "" {
			group.Go(func() error {
				return t.publishMessage(gCtx, closureBundleMessage)
			})
			// if no order, send as far as possible
			continue
		}
		_ = t.publishMessage(ctx, bm)
	}
	_ = group.Wait()
}

type OrderingKeyActor struct {
	msg *protocol.Message
}

func (a *OrderingKeyActor) Do(fn visitor.DoFunc) error {

}

// publishMessage block until ack or an error occurs and pass the error by aresult.Result
func (t *BundleTopic) publishMessage(ctx context.Context, bm *publisher.AsyncResultActor) error {
	bm.Do(func(msg *protocol.Message, err error) error {
		log.Debug("sending: the bundle key is ", msg.OrderingKey, " with id")

		return nil
	})

	id := bm.msg.ID

	b, err := jsoniter.ConfigFastest.Marshal(bm.msg)
	if err != nil {
		log.Error("In publishMessage:", err)
		goto CheckError
	}

	// if pub error, would return it in result and terminate the scheduler if ordering.

	err = t.d.Publish(t.name, b)
	if err != nil {
		goto CheckError
	}

	// if no ack logic, just return
	if !t.EnableAck {
		goto CheckError
	}

	// handle the wait ack and retry logic. note that topic set ack true in startAck() function.
	// until the t.Timeout(default: 60s) cancel the ctx.
	for i := 0; i < t.MaxRetryTimes; i++ {
		checkTimes := 0
		// check loop
		for {
			checkTimes++
			// check on every loop until retry max times.
			if t.checkAck(bm.msg) {
				goto CheckError
			}

			// every internal time
			err = t.RetryParams.Backoff(ctx, checkTimes)
			if err != nil && err == retry.ErrCancel {
				goto CheckError
			} else if err == retry.ErrMaxRetry {
				break
			}
		}
		err = t.d.Publish(t.name, b)
		log.Error("Resend protocol: ", bm.msg.ID)
		if err != nil {
			goto CheckError
		}
	}
	// if reach here, run out of the retry times.
	err = errors.New(fmt.Sprint("over the retry times limit: ", t.MaxRetryTimes))

	//	no error check until here
CheckError:
	if err != nil {
		t.scheduler.Pause(bm.msg.OrderingKey)
		// Update context with error tag for OpenCensus,
		// using same stats.Record() call as success case.
		_, _ = tag.New(ctx, tag.Upsert(keyStatus, "ERROR"),
			tag.Upsert(keyError, err.Error()))
	}
	// error handle
	if err != nil {
		log.Error("error in publish protocol:", err)
		bm.res.set("", err)
	} else {
		bm.msg = nil
		bm.res.set(id, nil)
	}
	// this error return to cancel the group of sending goroutines if not nil.
	return err
}
