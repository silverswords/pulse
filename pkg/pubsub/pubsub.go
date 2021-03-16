package pubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/silverswords/pulse/pkg/logger"
	"github.com/silverswords/pulse/pkg/protocol"
	"github.com/silverswords/pulse/pkg/pubsub/driver"
	"sync"
	"time"
)

var log = logger.NewLogger("pulse.driver")
var nowFunc = time.Now
var driversMu = sync.RWMutex{}
var Registry = pubsubRegistry{
	buses: make(map[string]func(logger logger.Logger) driver.Driver),
}

type pubsubRegistry struct {
	buses map[string]func(logger logger.Logger) driver.Driver
}

func (r *pubsubRegistry) Register(name string, factory func(logger logger.Logger) driver.Driver) {
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
		return method(log), nil
	}
	return nil, fmt.Errorf("couldn't find protocol bus %s", name)
}

type dsnConnector struct {
	dsn    protocol.Metadata
	driver driver.Driver
}

func (t dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dsn)
}

func (t dsnConnector) Driver() driver.Driver {
	return t.driver
}

// OpenPubSub opens a database using a Connector, allowing drivers to
// bypass a string based data source name.
//
// Most users will open a database via a driver-specific connection
// helper function that returns a *DB. No database drivers are included
// in the Go standard library. See https://golang.org/s/sqldrivers for
// a list of third-party drivers.
//
// OpenPubSub may just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// Ping.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the OpenPubSub
// function should be called just once. It is rarely necessary to
// close a DB.
func OpenPubSub(c driver.Connector) *PubSub {
	_, cancel := context.WithCancel(context.Background())
	db := &PubSub{
		connector:   c,
		stop:        cancel,
		connections: make(map[*DriverConn]bool),
	}

	return db
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a
// database name and connection information.
//
// Most users will open a database via a driver-specific connection
// helper function that returns a *DB. No database drivers are included
// in the Go standard library. See https://golang.org/s/sqldrivers for
// a list of third-party drivers.
//
// Open may just validate its arguments without creating a connection
// to the database. To verify that the data source name is valid, call
// Ping.
//
// The returned DB is safe for concurrent use by multiple goroutines
// and maintains its own pool of idle connections. Thus, the Open
// function should be called just once. It is rarely necessary to
// close a DB.
func Open(driverName string, metadata protocol.Metadata) (*PubSub, error) {
	driversMu.RLock()
	driveri, err := Registry.Create(driverName)
	driversMu.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("sql: unknown driver %q (forgotten import?)", driverName)
	}

	if driverCtx, ok := driveri.(driver.DriverContext); ok {
		connector, err := driverCtx.OpenConnector(metadata)
		if err != nil {
			return nil, err
		}
		return OpenPubSub(connector), nil
	}

	return OpenPubSub(dsnConnector{dsn: metadata, driver: driveri}), nil
}

// todo: PubSub isn't a pool like database/sql db. but just conn collector.
type PubSub struct {
	connector driver.Connector

	mu            sync.Mutex
	publisherConn *DriverConn

	connections map[*DriverConn]bool

	closed            bool
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

type DriverConn struct {
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

func (dc *DriverConn) Publish(ctx context.Context, r *protocol.PublishRequest) error {
	return dc.ci.Publish(ctx, r)
}

func (dc *DriverConn) Subscribe(ctx context.Context, r *protocol.SubscribeRequest, handler func(ctx context.Context, r *protocol.Message) error) (driver.Subscription, error) {
	return dc.ci.Subscribe(ctx, r, handler)
}

func (dc *DriverConn) Close() error {
	dc.Lock()
	if dc.closed {
		dc.Unlock()
		return errors.New("pubsub: duplicate DriverConn close")
	}
	dc.closed = true
	dc.Unlock() // not defer; removeDep finalClose calls may need to lock

	// And now updates that require holding dc.mu.Lock.
	dc.pubSub.mu.Lock()
	dc.pubsubmuClosed = true
	dc.pubSub.mu.Unlock()
	return dc.finalClose()
}

func (dc *DriverConn) finalClose() error {
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
	// close all the Subscriptions
	for _, ds := range openSubscription {
		_ = ds.Close()
	}
	withLock(dc, func() {
		dc.finalClosed = true
		// Close the connection
		err = dc.ci.Close()
		dc.ci = nil
	})

	return err
}

func (dc *DriverConn) expired(timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	return dc.createdAt.Add(timeout).Before(nowFunc())
}

// The finalCloser interface is used by (*PubSub).addDep and related
// dependency reference counting.
type finalCloser interface {
	// finalClose is called when the reference count of an object
	// goes to zero. (*PubSub).mu is not held while calling it.
	finalClose() error
}

// driverSubscription associates a driver.Subscription with the
// *DriverConn from which it came, so the DriverConn's lock can be
// held during calls.
type driverSubscription struct {
	driverConn  *DriverConn
	sync.Locker // the *DriverConn
	si          driver.Subscription
	closed      bool
	closeErr    error // return value of previous Close call
}

func (ds *driverSubscription) Close() error {
	return ds.si.Close()
}

func (pubsub *PubSub) Conn(ctx context.Context) (*DriverConn, error) {
	return pubsub.conn(ctx)
}

func (pubsub *PubSub) conn(ctx context.Context) (*DriverConn, error) {
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
	// todo: Prefer control connection lifetime and max number.
	pubsub.mu.Unlock()
	ci, err := pubsub.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	pubsub.mu.Lock()
	dc := &DriverConn{
		pubSub:     pubsub,
		createdAt:  nowFunc(),
		returnedAt: nowFunc(),
		ci:         ci,
		inUse:      true,
	}
	pubsub.appendDriver(dc)
	pubsub.mu.Unlock()
	return dc, nil
}

func (pubsub *PubSub) appendDriver(dc *DriverConn) {
	pubsub.connections[dc] = true
	return
}

// SubscribeContext subscribe with context control.
func (pubsub *PubSub) SubscribeContext(ctx context.Context, r *protocol.SubscribeRequest, fn func(ctx context.Context, r *protocol.Message) error) (driver.Subscription, error) {
	return pubsub.subscribe(ctx, r, fn)
}

// Subscribe will open a connection or reuse connection to subscribe
// Pass Metadata to control subscribe options.
func (pubsub *PubSub) Subscribe(r *protocol.SubscribeRequest, fn func(ctx context.Context, r *protocol.Message) error) (driver.Subscription, error) {
	return pubsub.subscribe(context.Background(), r, fn)
}

func (pubsub *PubSub) subscribe(ctx context.Context, r *protocol.SubscribeRequest, fn func(ctx context.Context, r *protocol.Message) error) (driver.Subscription, error) {
	dc, err := pubsub.conn(ctx)
	if err != nil {
		return nil, err
	}
	return dc.ci.Subscribe(ctx, r, fn)
}

func (pubsub *PubSub) PublishContext(ctx context.Context, r *protocol.PublishRequest) error {
	return pubsub.publish(ctx, r)
}

func (pubsub *PubSub) publish(ctx context.Context, r *protocol.PublishRequest) error {
	if pubsub.publisherConn == nil {
		dc, err := pubsub.conn(ctx)
		if err != nil {
			return err
		}
		pubsub.publisherConn = dc
	}
	err := pubsub.publisherConn.Publish(ctx, r)
	if err != nil {
		return err
	}
	return err
}

var errPubSubClosed = errors.New("sql: pubsub is closed")

// withLock runs while holding lk.
func withLock(lk sync.Locker, fn func()) {
	lk.Lock()
	defer lk.Unlock() // in case fn panics
	fn()
}
