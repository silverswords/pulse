package topic

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/pkg/components/mq"
	"github.com/silverswords/whisper/pkg/deadpolicy"
	"github.com/silverswords/whisper/pkg/logger"
	"github.com/silverswords/whisper/pkg/message"
	"github.com/silverswords/whisper/pkg/retry"
	"github.com/silverswords/whisper/pkg/scheduler"
	"golang.org/x/sync/errgroup"

	//"go.opencensus.io/stats"
	//"github.com/golang/protobuf/proto"

	"go.opencensus.io/tag"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
	WhisperPrefix  = "w_"
)

var (
	log = logger.NewLogger("pulse")

	errTopicOrderingDisabled = errors.New("Topic.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on Topic.EnableMessageOrdering")
	errTopicStopped          = errors.New("pubsub: Stop has been called for this topic")
)

type Topic struct {
	topicOptions []Option

	d    mq.Driver
	name string

	endpoints []func(ctx context.Context, m *message.Message) error
	// Settings for publishing messages. All changes must be made before the
	// first call to Publish. The default is DefaultPublishSettings.
	// it means could not dynamically change and hot start.
	PublishSettings

	mu        sync.RWMutex
	stopped   bool
	scheduler *scheduler.PublishScheduler

	pendingAcks map[string]bool
	deadQueue   chan *message.Message
}

// PublishSettings control the bundling of published messages.
type PublishSettings struct {
	// EnableMessageOrdering enables delivery of ordered keys.
	EnableMessageOrdering bool
	EnableAck             bool

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

	AckMapTicker  time.Duration
	MaxRetryTimes int
	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow.
	//
	// Defaults to DefaultPublishSettings.BufferedByteLimit.
	BufferedByteLimit int

	// if nil, no retry if no ack.
	RetryParams *retry.Params
	// if nil, drop deadletter.
	DeadLetterPolicy *deadpolicy.DeadLetterPolicy
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	NumGoroutines:  100 * runtime.GOMAXPROCS(0),
	Timeout:        60 * time.Second,
	AckMapTicker:   5 * time.Second,
	MaxRetryTimes:  3,
	// By default, limit the bundler to 10 times the max message size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	// default linear increase retry interval and 10 times.
	RetryParams: &retry.DefaultRetryParams,
	// default nil and drop letter.
	DeadLetterPolicy: nil,
}

// new a topic and init it with the connection options
func NewTopic(topicName string, driverMetadata mq.Metadata, options ...Option) (*Topic, error) {
	d, err := mq.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	t := &Topic{
		name:            WhisperPrefix + topicName,
		topicOptions:    options,
		d:               d,
		PublishSettings: DefaultPublishSettings,
		pendingAcks:     make(map[string]bool),
		deadQueue:       nil,
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

func (t *Topic) startAck(_ context.Context) error {
	// todo: now the ack logic's stability rely on mq subscriber implements. but not pulse implements.
	// open a subscriber, receive and then ack the message.
	// message should check itself and then depend on topic RetryParams to retry.
	if !t.EnableAck {
		return nil
	}

	subCloser, err := t.d.Subscribe(AckTopicPrefix+t.name, func(out []byte) {
		m, err := message.ToMessage(out)
		if err != nil {
			log.Error("topic", t.name, "error in ack message decode: ", err)
			//	 not our Whisper message, just ignore it
			return
		}
		log.Debug("ACK: topic ", t.name, " received ackId ", m.Id)
		t.done(m.Id, true, time.Now())
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
// sent according to the topic's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Publish creates goroutines for batching and sending messages. These goroutines
// need to be stopped by calling t.Stop(). Once stopped, future calls to Publish
// will immediately return a PublishResult with an error.
// advice: don't resume so quickly like less than 10* millisecond of Bundler ticker flush setting.
// that's ensure all the message in bundle with the key return error when scheduler paused.
//
// Warning: do not use incoming message pointer again that if message had been successfully
// add in the scheduler, message would be equal nil to gc.
//
// Warning: when use ordering feature, recommend to limit the QPS to 100, or use synchronous
func (t *Topic) Publish(ctx context.Context, msg *message.Message) *PublishResult {
	r := &PublishResult{ready: make(chan struct{})}
	if !t.EnableMessageOrdering && msg.OrderingKey != "" {
		r.set("", errTopicOrderingDisabled)
		return r
	}

	// -------------Set the send logic parameters------------
	// With Chain handlers to handle.
	for _, handler := range t.endpoints {
		err := handler(ctx, msg)
		if err != nil {
			r.set("", err)
			return r
		}
	}
	// Use a PublishRequest with only the Messages field to calculate the size
	// of an individual message. This accurately calculates the size of the
	// encoded proto message by accounting for the length of an individual
	// PubSubMessage and Data/Attributes field.
	// TODO(hongalex): if this turns out to take significant time, try to approximate it.
	// TODO: consider wrap with a newLogicFromMessage()
	msg.Size = len(message.ToByte(msg))
	// --------------------Set Over---------------------------

	t.start()
	t.mu.RLock()
	defer t.mu.RUnlock()
	// TODO(aboulhosn) [from bcmills] consider changing the semantics of bundler to perform this logic so we don't have to do it here
	if t.stopped {
		r.set("", errTopicStopped)
		return r
	}

	// TODO(jba) [from bcmills] consider using a shared channel per bundle
	// (requires Bundler API changes; would reduce allocations)
	err := t.scheduler.Add(msg.OrderingKey, &bundledMessage{msg, r}, msg.Size)
	if err != nil {
		t.scheduler.Pause(msg.OrderingKey)
		r.set("", err)
	}
	return r
}

// ResumePublish resumes accepting messages for the provided ordering key.
// Publishing using an ordering key might be paused if an error is
// encountered while publishing, to prevent messages from being published
// out of order.
func (t *Topic) ResumePublish(orderingKey string) {
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
func (t *Topic) Stop() {
	t.mu.Lock()
	noop := t.stopped || t.scheduler == nil
	t.stopped = true
	t.mu.Unlock()
	if noop {
		return
	}
	t.scheduler.FlushAndStop()
}

func (t *Topic) done(ackId string, ack bool, _ time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ack {
		t.pendingAcks[ackId] = true
	}
}

// use to start the topic sender and acker
func (t *Topic) start() {
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

	timeout := t.PublishSettings.Timeout
	workers := t.PublishSettings.NumGoroutines
	// Unless overridden, allow many goroutines per CPU to call the Publish RPC
	// concurrently. The default value was determined via extensive load
	// testing (see the loadtest subdirectory).
	if t.PublishSettings.NumGoroutines == 0 {
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
	t.scheduler.DelayThreshold = t.PublishSettings.DelayThreshold
	t.scheduler.BundleCountThreshold = t.PublishSettings.CountThreshold
	if t.scheduler.BundleCountThreshold > MaxPublishRequestCount {
		t.scheduler.BundleCountThreshold = MaxPublishRequestCount
	}
	t.scheduler.BundleByteThreshold = t.PublishSettings.ByteThreshold

	bufferedByteLimit := DefaultPublishSettings.BufferedByteLimit
	if t.PublishSettings.BufferedByteLimit > 0 {
		bufferedByteLimit = t.PublishSettings.BufferedByteLimit
	}
	t.scheduler.BufferedByteLimit = bufferedByteLimit

	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow. The default is DefaultBufferedByteLimit.
	t.scheduler.BundleByteLimit = MaxPublishRequestBytes // - calcFieldSizeString(t.name)
}

type bundledMessage struct {
	msg *message.Message
	res *PublishResult
}

// PublishResult help to know error because of sending goroutine is another goroutine.
type PublishResult struct {
	ready    chan struct{}
	serverID string
	err      error
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *PublishResult) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish call.
// Get blocks until the Publish call completes or the context is done.
func (r *PublishResult) Get(ctx context.Context) (serverID string, err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return r.serverID, r.err
	default:
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.Ready():
		return r.serverID, r.err
	}
}

func (r *PublishResult) set(sid string, err error) {
	r.serverID = sid
	r.err = err
	close(r.ready)
}

// The following keys are used to tag requests with a specific topic/subscription ID.
var (
	keyTopic = tag.MustNewKey("topic")
	//keySubscription = tag.MustNewKey("subscription")
)

// In the following, errors are used if status is not "OK".
var (
	keyStatus = tag.MustNewKey("status")
	keyError  = tag.MustNewKey("error")
)

// choose to skip ack logic. would delete key when ack.
func (t *Topic) checkAck(m *message.Message) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	//log.Info("checking message ", m.Id, t.pendingAcks)
	if !t.pendingAcks[m.Id] {
		return false
	}
	// clear the map
	delete(t.pendingAcks, m.Id)
	return true
}

// publishMessageBundle just handle the send logic
func (t *Topic) publishMessageBundle(ctx context.Context, bms []*bundledMessage) {
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

// publishMessage block until ack or an error occurs and pass the error by PublishResult
func (t *Topic) publishMessage(ctx context.Context, bm *bundledMessage) error {
	log.Debug("sending: the bundle key is ", bm.msg.OrderingKey, " with id", bm.msg.Id)
	// comment the trace of google api
	mb := message.ToByte(bm.msg)
	// todo: make this id ordered or generated on mq
	id := bm.msg.Id
	// if pub error, would return it in result.
	// terminate the scheduler if ordering.

	err := t.d.Publish(t.name, mb)
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
		err = t.d.Publish(t.name, mb)
		log.Error("Resend message: ", bm.msg.Id)
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
		ctx, _ = tag.New(ctx, tag.Upsert(keyStatus, "ERROR"),
			tag.Upsert(keyError, err.Error()))
	}
	// error handle
	if err != nil {
		log.Error("error in publish message:", err)
		bm.res.set("", err)
	} else {
		bm.msg = nil
		bm.res.set(id, nil)
	}
	// this error return to cancel the group of sending goroutines if not nil.
	return err
}

// WithRequiredACK would turn on the ack function.
func WithRequiredACK() Option {
	return func(t *Topic) error {
		t.EnableAck = true
		return nil
	}
}

func WithCount() Option {
	return func(t *Topic) error {
		var count uint64
		t.endpoints = append(t.endpoints, func(ctx context.Context, m *message.Message) error {
			atomic.AddUint64(&count, 1)
			log.Info("count: ", count)
			return nil
		})
		return nil
	}
}

// WithRequiredACK would turn on the ack function.
func WithOrdered() Option {
	return func(t *Topic) error {
		t.EnableMessageOrdering = true
		return nil
	}
}

type Option func(*Topic) error

func (t *Topic) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}
