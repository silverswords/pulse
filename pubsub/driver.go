package pubsub

import (
	"context"
	"sort"
	"sync"
)

//from database/sql
var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

type Driver interface {
	Send(ctx context.Context,msg []byte) error
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
