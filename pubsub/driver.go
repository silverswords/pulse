package pubsub

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

//from database/sql
var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Open give a conn to MQ to send and receive msg.
func Open(driverName, driverSource string, options interface{}) (Conn, error) {
	driversMu.RLock()
	driveri, ok := drivers[driverName]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("mq: unknown driver %q (forgotten import?)", driverName)
	}
	conn, err := driveri.Dial(driverSource, options)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

type Driver interface {
	Dial(target string, options interface{}) (Conn, error)
}

// Register makes a database driver available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("sql: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("sql: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

func UnregisterAllDrivers() {
	driversMu.Lock()
	defer driversMu.Unlock()
	// For tests.
	drivers = make(map[string]Driver)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	list := make([]string, 0, len(drivers))
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

type Conn interface {
	Pub(topic string, msg []byte) error
	Sub(topic string) (Subscription, error)
}

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to sql.OpenDB, to allow drivers
// to implement their own sql.DB constructors, or returned by
// DriverContext's OpenConnector method, to allow drivers
// access to context and to avoid repeated parsing of driver
// configuration.
type Connector interface {
	// Connect returns a connection to the database.
	// Connect may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The provided context.Context is for dialing purposes only
	// (see net.DialContext) and should not be stored or used for
	// other purposes.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Connect(context.Context) (Conn, error)

	// Driver returns the underlying Driver of the Connector,
	// mainly to maintain compatibility with the Driver method
	// on sql.DB.
	Driver() Driver
}

type Subscription interface {
	Receive() []byte
}
