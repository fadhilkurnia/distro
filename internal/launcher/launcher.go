package launcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fadhilkurnia/distro/internal/config"
	"github.com/fadhilkurnia/distro/internal/runner"
)

// Checks if a path exists on the local filesystem
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}


// ResolveOverride checks for a "<versionName>-<filename>" override inside
// workdir first, falling back to plain filename if no override exists.
// Returns a path relative to workdir, suitable for passing straight to
// nix.Run (which resolves script paths against the Runner's own workdir)
// or for joining with workdir yourself for local filesystem access.
// 

// Checks for a "<versionName>-<filename>.sh" override inside workdir.
// Falls back to plain <filename>.sh if no override exists.
// Returns a path relative to workdir
func ResolveOverride(workdir, filename, versionName string) string {
	override := fmt.Sprintf("%s-%s", versionName, filename)
	if FileExists(filepath.Join(workdir, override)) {
		return override
	}
	return filename
}


// {Algorithm, Version} pair every protocol's Launcher needs
// Both fields must be explicitly set. There are no silent defaults.
type VariantVersion struct {
	Algorithm string // must match a Variant's Name
	Version   string // must match a Version's Name
}


// Validates that Algorithm and Version are both set and match a real 
// catalog entry within variants/versions, returning clear, specific
// errors otherwise. Every protocol's Build/Start/Stop calls this once at
// the top, via its embedded VariantVersion.
func (vv VariantVersion) Resolve(variants []Variant, versions []Version) (Variant, Version, error) {
	if vv.Algorithm == "" {
		return Variant{}, Version{}, fmt.Errorf("launcher: Algorithm must be set")
	}
	if vv.Version == "" {
		return Variant{}, Version{}, fmt.Errorf("launcher: Version must be set")
	}
 
	variant, err := ResolveVariant(variants, vv.Algorithm)
	if err != nil {
		return Variant{}, Version{}, err
	}
	version, err := ResolveVersion(versions, vv.Version)
	if err != nil {
		return Variant{}, Version{}, err
	}
	return variant, version, nil
}


// Identifies one specific algorithm/mode a protocol can run as.
// Ex: Paxi alone supports paxos, epaxos, dynamo, and others, each with
// its own consistency/persistency characteristics. Every protocol
// maintains its own []Variant catalog (registered alongside its Launcher
// factory in internal/registry), which registry.Catalog() combines
// with every protocol's []Version to produce the full flattened list
type Variant struct {
	Name        string // ex: "paxos", "epaxos"
	Language    string // ex: "Go", "Java"
	Consistency string // ex: "Linearizability", "Eventual"
	Persistency string // ex: "In-Memory", "On-Disk"
	Repository  string // repo URL
}


// ResolveVariant finds the Variant named name within variants, or returns
// an error if no entry matches.
func ResolveVariant(variants []Variant, name string) (Variant, error) {
	for _, v := range variants {
		if v.Name == name {
			return v, nil
		}
	}
	return Variant{}, fmt.Errorf("launcher: no variant named %q", name)
}


// Identifies the version of the repository being benchmarked 
// - Name: human readable identification for a commit
// - Ref: full 40-character commit hash
// Example usage:
// sut/ailidani.paxi/<ref>/server/bin
// sut/ailidani.paxi/scripts/<name>-build.sh
type Version struct {
	Name string
	Ref  string 
}


// Lookup name to specific Version struct.
// Returns error if no entry matches
func ResolveVersion(versions []Version, name string) (Version, error) {
	for _, v := range versions {
		if v.Name == name {
			return v, nil
		}
	}
	return Version{}, fmt.Errorf("launcher: no version named %q", name)
}


// Describes what functions are required for all protocol Launcher
//
// Build/Start/Stop follow a fixed division of responsibility,
// expected of every protocol:
//   - Build: compile the binary, copy it to every node. Nothing runs yet.
//   - Start: generate the config, copy it to every node, launch the
//     process on every node. The only step that starts anything running.
//   - Stop: kill the running process on every node, delete the generated
//     config. The binary is left in place, so a later Start doesn't
//     require rebuilding.
type Launcher interface {
	// Name of the project. Example:
	// - ailidani.paxi
	// - apache.zookeeper
	Name() string

	// Builds the protocol:
	// - Compiles the binary (build.sh)
	// - Copies binary to every node
	// Does not generate config or start anything.
	Build(ctx context.Context, pool *runner.Pool, nodes []config.Node) error


	// Launches the protocol:
	// - Check if binary already exists (will exit if no binary)
	// - Generate config, copies it to every node
	// - Starts the process on every node.
	Start(ctx context.Context, pool *runner.Pool, nodes []config.Node) error

	// Stops the protocol in each node:
	// - Stops start.sh script and kill protocol process PID
	// - Delete configs
	// - Clean up /tmp and state files (if exists)
	// Note: the binary is left in place, so a later Start doesn't
	//       require rebuilding.
	Stop(ctx context.Context, pool *runner.Pool, nodes []config.Node) error
}
