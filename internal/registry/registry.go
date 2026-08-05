package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/fadhilkurnia/distro/internal/launcher"
)

type Factory func() launcher.Launcher

var (
	mu    sync.Mutex
	items = map[string]Factory{}
)

// Registers a Launcher from all projects inside `sut/` at compile-time.
// Example usage:
//
//	func init() {
//	    registry.Register("ailidani.paxi", func() launcher.Launcher { return &PaxiLauncher{} })
//	}
//
// Note: will panic if a name is registered twice. 
// 	 All projects must have different registry name
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()

	if _, exists := items[name]; exists {
		panic(fmt.Sprintf("registry: %q is already registered", name))
	}
	items[name] = factory
}

// Returns a freshly constructed Launcher for a registered name
func Get(name string) (launcher.Launcher, error) {
	mu.Lock()
	factory, ok := items[name]
	mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("registry: no launcher registered as %q", name)
	}
	return factory(), nil
}

// Returns all registered projects (sorted alphabetically)
func Names() []string {
	mu.Lock()
	defer mu.Unlock()

	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
