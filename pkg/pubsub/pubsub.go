package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/message"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"sync"
	"sync/atomic"
	"time"
)

var log = logger.NewLogger("pulse.driver")
var nowFunc = time.Now
var Registry = pubsubRegistry{
	buses: make(map[string]func() driver.Driver),
}

type pubsubRegistry struct {
	buses map[string]func() driver.Driver
}

func (r *pubsubRegistry) Register(name string, factory func() driver.Driver) {
	r.buses[name] = factory
}

// Create instantiates a pub/sub based on `name`.
func (r *pubsubRegistry) Create(name string) (driver.Driver, error) {
	if name == "" {
		log.Info("Create default in-process driver")
	} else {
		log.Infof("Create a driver %s", name)
	}
	if method, ok := r.buses[name]; ok {
		return method(), nil
	}
	return nil, fmt.Errorf("couldn't find message bus %s", name)
}

type Metadata struct {
	Properties map[string]interface{}
}

func NewMetadata() *Metadata {
	return &Metadata{Properties: make(map[string]interface{})}
}

// if driverName is empty, use default local driver. which couldn't cross process
func (m *Metadata) GetDriverName() string {
	var noDriver = ""
	if driverName, ok := m.Properties["DriverName"]; ok {
		if nameString, ok := driverName.(string); ok {
			return nameString
		}
		return noDriver
	}
	return noDriver
}

func (m *Metadata) SetDriver(driverName string) {
	m.Properties["DriverName"] = driverName
}

// todo: PubSub isn't a pool like database/sql db. but just conn collector.
type PubSub struct {
	connector driver.Connector
	// numClosed is an atomic counter which represents a total number of
	// closed connections. Stmt.openStmt checks it before cleaning closed
	// connections in Stmt.css.
	numClosed uint64

	mu           sync.Mutex
	freeConn     []*driverConn
	connRequests map[uint64]connRequest
	nextRequest  uint64
	numOpen      int
	// Used to signal the need for new connections
	// a goroutine running connectionOpener() reads on this chan and
	// maybeOpenNewConnections sends on the chan (one send per needed connection)
	// It is closed during db.Close(). The close tells the connectionOpener
	// goroutine to exit.
	openerCh          chan struct{}
	closed            bool
	dep               map[finalCloser]depSet
	maxIdleCount      int
	maxOpen           int
	maxLifetime       time.Duration
	maxIdleTime       time.Duration
	cleanerCh         chan struct{}
	waitCount         int64 // Total number of connections waited for.
	maxIdleClosed     int64 // Total number of connections closed due to idle count.
	maxIdleTimeClosed int64 // Total number of connections closed due to idle time.
	maxLifetimeClosed int64 // Total number of connections closed due to max connection lifetime limit.

	stop func() // stop cancels the connection opener.
}

func (db *PubSub) removeDepLocked(x finalCloser, dep interface{}) func() error {

	xdep, ok := db.dep[x]
	if !ok {
		panic(fmt.Sprintf("unpaired removeDep: no deps for %T", x))
	}

	l0 := len(xdep)
	delete(xdep, dep)

	switch len(xdep) {
	case l0:
		// Nothing removed. Shouldn't happen.
		panic(fmt.Sprintf("unpaired removeDep: no %T dep on %T", dep, x))
	case 0:
		// No more dependencies.
		delete(db.dep, x)
		return x.finalClose
	default:
		// Dependencies remain.
		return func() error { return nil }
	}
}

type driverConn struct {
	pubSub    *PubSub
	createdAt time.Time

	sync.Mutex       // guards following
	ci               driver.Conn
	closed           bool
	finalClosed      bool // ci.Close has been called
	openSubscription map[*driverSubscription]bool

	// guarded by pubsub.mu
	inUse          bool
	returnedAt     time.Time // Time the connection was created or returned.
	onPut          []func()  // code (with db.mu held) run when conn is next returned
	pubsubmuClosed bool      // same as closed, but guarded by pubsub.mu, for removeClosedStmtLocked
}

func (dc *driverConn) Close() error {
	dc.Lock()
	if dc.closed {
		dc.Unlock()
		return errors.New("pubsub: duplicate driverConn close")
	}
	dc.closed = true
	dc.Unlock() // not defer; removeDep finalClose calls may need to lock

	// And now updates that require holding dc.mu.Lock.
	dc.pubSub.mu.Lock()
	dc.pubsubmuClosed = true
	fn := dc.pubSub.removeDepLocked(dc, dc)
	dc.pubSub.mu.Unlock()
	return fn()
}

func (dc *driverConn) finalClose() error {
	var err error

	// Each *driverStmt has a lock to the dc. Copy the list out of the dc
	// before calling close on each stmt.
	var openSubscription []*driverSubscription
	withLock(dc, func() {
		openSubscription = make([]*driverSubscription, 0, len(dc.openSubscription))
		for ds := range dc.openSubscription {
			openSubscription = append(openSubscription, ds)
		}
		dc.openSubscription = nil
	})
	for _, ds := range openSubscription {
		ds.Close()
	}
	withLock(dc, func() {
		dc.finalClosed = true
		err = dc.ci.Close()
		dc.ci = nil
	})

	dc.pubSub.mu.Lock()
	dc.pubSub.numOpen--
	//dc.pubSub.maybeOpenNewConnections()
	dc.pubSub.mu.Unlock()

	atomic.AddUint64(&dc.pubSub.numClosed, 1)
	return err
}

func (dc *driverConn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return dc.createdAt.Add(timeout).Before(nowFunc())
}

// depSet is a finalCloser's outstanding dependencies
type depSet map[interface{}]bool // set of true bools

// The finalCloser interface is used by (*PubSub).addDep and related
// dependency reference counting.
type finalCloser interface {
	// finalClose is called when the reference count of an object
	// goes to zero. (*PubSub).mu is not held while calling it.
	finalClose() error
}

// driverSubscription associates a driver.Subscription with the
// *driverConn from which it came, so the driverConn's lock can be
// held during calls.
type driverSubscription struct {
	sync.Locker // the *driverConn
	si          driver.Subscription
	closed      bool
	closeErr    error // return value of previous Close call
}

func (ds *driverSubscription) Close() error {
	return ds.si.Close()
}

func (pubsub *PubSub) conn(ctx context.Context, metadata Metadata) (*driverConn, error) {
	pubsub.mu.Lock()
	if pubsub.closed {
		pubsub.mu.Unlock()
		return nil, errPubSubClosed
	}

	// Check if the context is expired.
	select {
	default:
	case <-ctx.Done():
		pubsub.mu.Unlock()
		return nil, ctx.Err()
	}

	// todo: Prefer a free connection, if possible.
	//	todo: Prefer control connection lifetime and max number.
	pubsub.numOpen++
	pubsub.mu.Unlock()
	ci, err := pubsub.connector.Connect(ctx, metadata)
	if err != nil {
		pubsub.mu.Lock()
		pubsub.numOpen--
		//	may append connect request to pubsub.connRequests

		pubsub.mu.Unlock()
		return nil, err
	}
	pubsub.mu.Lock()
	dc := &driverConn{
		pubSub:     pubsub,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
		inUse:      true,
	}
	return dc, nil
}

// SubscribeContext subscribe with context control.
func (pubsub *PubSub) SubscribeContext(ctx context.Context, md Metadata, fn message.DoFunc) (driver.Subscription, error) {
	return pubsub.subscribe(ctx, md, fn)
}

// Subscribe will open a connection or reuse connection to subscribe
// Pass Metadata to control subscribe options.
func (pubsub *PubSub) Subscribe(md Metadata, fn message.DoFunc) (driver.Subscription, error) {
	return pubsub.subscribe(context.Background(), md, fn)
}

func (pubsub *PubSub) subscribe(ctx context.Context, md Metadata, fn message.DoFunc) (driver.Subscription, error) {
	dc, err := pubsub.conn(ctx, md)
	if err != nil {
		return nil, err
	}
	return dc.ci.Subscribe(ctx, md, fn)
}

func (pubsub *PubSub) PublishContext(ctx context.Context, md Metadata, m *message.Message) error {
	return pubsub.publish(ctx, md, m)
}

func (pubsub *PubSub) publish(ctx context.Context, md Metadata, m *message.Message) error {
	dc, err := pubsub.conn(ctx, md)
	if err != nil {
		return err
	}
	return dc.ci.Publish(m, ctx, err)
}

// connRequest represents one request for a new connection
// When there are no idle connections available, PubSub.conn will create
// a new connRequest and put it on the db.connRequests list.
type connRequest struct {
	conn *driverConn
	err  error
}

var errPubSubClosed = errors.New("sql: pubsub is closed")

// withLock runs while holding lk.
func withLock(lk sync.Locker, fn func()) {
	lk.Lock()
	defer lk.Unlock() // in case fn panics
	fn()
}
