package registry

import (
	"fmt"
	"sort"
	"sync"

	"github.com/fadhilkurnia/distro/internal/launcher"
)

type Factory func() launcher.Launcher


// entry bundles everything one protocol registers: how to construct its
// Launcher, and its catalog data (every Variant and Version it supports)
type entry struct {
	factory  Factory
	variants []launcher.Variant
	versions []launcher.Version
}


var (
	mu    sync.Mutex
	items = map[string]entry{}
)


// Registers a Launcher, along with its full catalog of Variants and
// Versions, from all projects inside `sut/` at compile-time.
// Example usage:
//
//	func init() {
//	    registry.Register("ailidani.paxi",
//	        func() launcher.Launcher { return &PaxiLauncher{} },
//	        Variants,
//	        Versions,
//	    )
//	}
//
// Note: will panic if a name is registered twice.
//	All projects must have different registry name
func Register(name string, factory Factory, variants []launcher.Variant, versions []launcher.Version) {
	mu.Lock()
	defer mu.Unlock()
 
	if _, exists := items[name]; exists {
		panic(fmt.Sprintf("registry: %q is already registered", name))
	}
	items[name] = entry{factory: factory, variants: variants, versions: versions}
}


// Returns a freshly constructed Launcher for a registered name
func Get(name string) (launcher.Launcher, error) {
	mu.Lock()
	e, ok := items[name]
	mu.Unlock()
 
	if !ok {
		return nil, fmt.Errorf("registry: no launcher registered as %q", name)
	}
	return e.factory(), nil
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


// A selectable row: a single protocol running as a single Variant at 
// a single Version. This is the flattened shape the TUI renders and 
// lets someone choose from. Everything needed to both display it:
// - name (algorithm being used)
// - language
// - consistency
// - persistency
// - repository
// - human-readable version label
// and construct the corresponding Launcher afterward:
// - Protocol
// - Variant.Name
// - Version.Name
// are available via registry.Get
type CatalogRow struct {
	Protocol string // ex: "ailidani.paxi" (matches the registered name)
	launcher.Variant
	launcher.Version
}
 

// Get list of all registered protocol and its variant + version combinations
// (Sorted alphabetically based on protocol name)
func Catalog() []CatalogRow {
	mu.Lock()
	defer mu.Unlock()
 
	names := make([]string, 0, len(items))
	for name := range items {
		names = append(names, name)
	}
	sort.Strings(names)
 
	var rows []CatalogRow
	for _, name := range names {
		e := items[name]
		for _, variant := range e.variants {
			for _, version := range e.versions {
				rows = append(rows, CatalogRow{
					Protocol: name,
					Variant:  variant,
					Version:  version,
				})
			}
		}
	}
	return rows
}
