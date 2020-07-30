package whisper

// below code edit from github.com/googleapi/google-cloud-go
import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/whisper/driver"
	"github.com/silverswords/whisper/internal"
	"github.com/silverswords/whisper/internal/scheduler"
	"strconv"
	//"go.opencensus.io/stats"
	//"github.com/golang/protobuf/proto"

	"go.opencensus.io/tag"
	"log"
	"runtime"
	"sync"
	"time"
)

const (
	// MaxPublishRequestCount is the maximum number of messages that can be in
	// a single publish request, as defined by the PubSub service.
	MaxPublishRequestCount = 1000

	// MaxPublishRequestBytes is the maximum size of a single publish request
	// in bytes, as defined by the PubSub service.
	MaxPublishRequestBytes = 1e7 // 10m

	DeadQueueSize = 100

	AckTopicPrefix = "ack_"
)

var (
	errTopicOrderingDisabled = errors.New("Topic.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on Topic.EnableMessageOrdering")
	errTopicStopped          = errors.New("pubsub: Stop has been called for this topic")
)

type Topic struct {
	topicOptions []topicOption

	d    driver.Driver
	name string

	endpoints []func(m *Message) error
	// Settings for publishing messages. All changes must be made before the
	// first call to Publish. The default is DefaultPublishSettings.
	// it means could not dynamically change and hot start.
	PublishSettings

	mu        sync.RWMutex
	stopped   bool
	scheduler *scheduler.PublishScheduler

	// ackid map to *int . if 0 ack.
	pendingAcks map[string]bool
	// todo: consider if need a ack delay deadline holding. It gives more time to store the message in the Topic and not to retry or drop.
	// pendingAckDeadline map[string]time.Time
	deadQueue chan *Message
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

	// After AckTimeout to confirm if message has been acknowledged, and decide whether to retry.
	AckTimeout time.Duration
	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow.
	//
	// Defaults to DefaultPublishSettings.BufferedByteLimit.
	BufferedByteLimit int

	// if nil, no retry if no ack.
	RetryParams *internal.RetryParams
	// if nil, drop deadletter.
	DeadLetterPolicy *internal.DeadLetterPolicy
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	AckTimeout:     1 * time.Second,
	// By default, limit the bundler to 10 times the max message size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
	// default linear increase retry interval and 10 times.
	RetryParams: &internal.DefaultRetryParams,
	// default nil and drop letter.
	DeadLetterPolicy: nil,
}

// new a topic and init it with the connection options
func NewTopic(topicName string, driverMetadata driver.Metadata, options ...topicOption) (*Topic, error) {
	driver, err := driver.Registry.Create(driverMetadata.GetDriverName())
	if err != nil {
		return nil, err
	}
	t := &Topic{
		name:            topicName,
		topicOptions:    options,
		d:               driver,
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
	if err := t.startAck(); err != nil {
		t.d.Close()
		return nil, err
	}
	return t, nil
}

func (t *Topic) startAck() error {
	// todo: now the ack logic's stability rely on driver subscriber implements. but not whisper implements.
	// open a subscriber, receive and then ack the message.
	// message should check itself and then depend on topic RetryParams to retry.
	if !t.EnableAck {
		return nil
	}
	subCloser, err := t.d.Subscribe(AckTopicPrefix+t.name, func(out []byte) {
		m, err := ToMessage(out)
		if err != nil {
			fmt.Println("error in message decode: ", err)
		}
		t.done(m.L.AckID, true, time.Now())
	})
	if err != nil {
		subCloser.Close()
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
func (t *Topic) Publish(_ context.Context, msg *Message) *PublishResult {
	r := &PublishResult{ready: make(chan struct{})}
	if !t.EnableMessageOrdering && msg.L.OrderingKey != "" {
		r.set(errTopicOrderingDisabled)
		return r
	}

	// With Chain handlers to handle.
	for _, handler := range t.endpoints {
		err := handler(msg)
		if err != nil {
			r.set(err)
			return r
		}
	}

	// -------------Set the send logic parameters------------
	// Use a PublishRequest with only the Messages field to calculate the size
	// of an individual message. This accurately calculates the size of the
	// encoded proto message by accounting for the length of an individual
	// PubSubMessage and Data/Attributes field.
	// TODO(hongalex): if this turns out to take significant time, try to approximate it.
	// TODO: consider wrap with a newLogicFromMessage()
	msg.L.size = len(ToByte(msg))
	var tryTimes int
	msg.L.DeliveryAttempt = &tryTimes

	// --------------------Set Over---------------------------
	t.start()
	t.mu.RLock()
	defer t.mu.RUnlock()
	// TODO(aboulhosn) [from bcmills] consider changing the semantics of bundler to perform this logic so we don't have to do it here
	if t.stopped {
		r.set(errTopicStopped)
		return r
	}

	// TODO(jba) [from bcmills] consider using a shared channel per bundle
	// (requires Bundler API changes; would reduce allocations)
	err := t.scheduler.Add(msg.L.OrderingKey, &bundledMessage{msg, r}, msg.L.size)
	if err != nil {
		t.scheduler.Pause(msg.L.OrderingKey)
		r.set(err)
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

func (t *Topic) done(ackId string, ack bool, receiveTime time.Time) {
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
	msg *Message
	res *PublishResult
}

// PublishResult help to know error because of sending goroutine is another goroutine.
type PublishResult struct {
	ready chan struct{}
	err   error
}

// Ready returns a channel that is closed when the result is ready.
// When the Ready channel is closed, Get is guaranteed not to block.
func (r *PublishResult) Ready() <-chan struct{} { return r.ready }

// Get returns the server-generated message ID and/or error result of a Publish call.
// Get blocks until the Publish call completes or the context is done.
func (r *PublishResult) Get(ctx context.Context) (err error) {
	// If the result is already ready, return it even if the context is done.
	select {
	case <-r.Ready():
		return r.err
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.Ready():
		return r.err
	}
}

func (r *PublishResult) set(err error) {
	r.err = err
	close(r.ready)
}

// The following keys are used to tag requests with a specific topic/subscription ID.
var (
	keyTopic        = tag.MustNewKey("topic")
	keySubscription = tag.MustNewKey("subscription")
)

// In the following, errors are used if status is not "OK".
var (
	keyStatus = tag.MustNewKey("status")
	keyError  = tag.MustNewKey("error")
)

// choose to skip ack logic. would delete key when ack.
func (t *Topic) checkAck(m *Message) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if !t.pendingAcks[m.L.AckID] {
		return false
	}
	delete(t.pendingAcks, m.L.AckID)
	return true
}

// publishMessageBundle just handle the send logic
func (t *Topic) publishMessageBundle(ctx context.Context, bms []*bundledMessage) {
	ctx, err := tag.New(ctx, tag.Insert(keyStatus, "OK"), tag.Upsert(keyTopic, t.name))
	if err != nil {
		log.Printf("pubsub: cannot create context with tag in publishMessageBundle: %v", err)
	}
	bm := bms[0]
	var orderingKey = bm.msg.L.OrderingKey

	if orderingKey != "" && t.scheduler.IsPaused(orderingKey) {
		err = fmt.Errorf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", orderingKey)
	} else {
		// comment the trace of google api
		mb := ToByte(bm.msg)

		// if pub error, would return it in result.
		// terminate the scheduler if ordering.
		err = t.d.Publish(t.name, mb)
		if err != nil {
			goto NoRetry
		}

		// if no ack logic, just return
		if !t.EnableAck {
			goto NoRetry
		}
		// todo: consider wrap it with a function.
		// handle the retry logic.
		ticker := time.After(t.AckTimeout)
		<-ticker
		for *bm.msg.L.DeliveryAttempt+1 <= t.RetryParams.MaxTries {
			//check if need ack. if not enabled ack. just break out of retry logic loop.
			if t.checkAck(bm.msg) {
				break
			}
			// checkAck false and need to retry.
			err = t.d.Publish(t.name, mb)
			if err != nil {
				break
			}

			err = t.RetryParams.Backoff(context.TODO(), *bm.msg.L.DeliveryAttempt+1)
			if err != nil {
				break
			}
			*bm.msg.L.DeliveryAttempt++
		}


		//end := time.Now()
		//stats.Record(ctx,
		//	PublishLatency.M(float64(end.Sub(start)/time.Millisecond)),
		//	PublishedMessages.M(int64(len(bms))))
	}
	NoRetry :
	if err != nil {
		t.scheduler.Pause(orderingKey)
		// Update context with error tag for OpenCensus,
		// using same stats.Record() call as success case.
		ctx, _ = tag.New(ctx, tag.Upsert(keyStatus, "ERROR"),
			tag.Upsert(keyError, err.Error()))
	}
	// error handle
	if err != nil {
		bm.res.set(err)
	} else {
		bm.msg = nil
		bm.res.set(nil)
	}
}

// WithPubACK would turn on the ack function.
func WithPubACK() topicOption {
	return func(t *Topic) error {
		t.EnableAck = true
		// gen AckId
		t.endpoints = append(t.endpoints, func(m *Message) error {
			m.L.AckID = m.Id + strconv.FormatInt(time.Now().Unix(), 10)
			return nil
		})

		return nil
	}
}

// WithPubACK would turn on the ack function.
func WithOrdered() topicOption {
	return func(t *Topic) error {
		t.EnableMessageOrdering = true
		return nil
	}
}

type topicOption func(*Topic) error

func (t *Topic) applyOptions(opts ...topicOption) error {
	for _, fn := range opts {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}
