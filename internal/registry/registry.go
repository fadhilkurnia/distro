package registry

import (
	"fmt"
	"sort"

	"github.com/fadhilkurnia/distro/internal/launcher"
)

// Complete information of a project with all available specs and versions
type Project struct {
	Name       string // ex: "ailidani.paxi"
	Repository string // repo URL

	// Constructs a fully-configured Launcher from given spec/version 
	NewLauncher func(spec launcher.Specification, version launcher.Version) launcher.Launcher

	Specifications []launcher.Specification
	Versions       []launcher.Version
}

// A selectable {project, specification, version} combination (by the TUI)
type Instance struct {
	ProjectName    string
	Repository     string
	Specification  launcher.Specification
	Version        launcher.Version
}

var allProjects []Project

// Registers a project (called from a protocol's package's init() function):
//
//	func init() {
//	    registry.AddProject(registry.Project{
//	        Name:        "ailidani.paxi",
//	        Repository:  repoURL,
//	        NewLauncher: func(spec launcher.Specification, version launcher.Version) launcher.Launcher {
//	            return &PaxiLauncher{spec: spec, version: version}
//	        },
//	        Specifications: Specs,
//	        Versions:       Versions,
//	    })
//	}
//
// Panics if Name is already registered. There must be no duplicate project names
func AddProject(p Project) {
	for _, existing := range allProjects {
		if existing.Name == p.Name {
			panic(fmt.Sprintf("registry: %q is already registered", p.Name))
		}
	}
	allProjects = append(allProjects, p)
}

// Returns every registered project's name (sorted alphabetically)
func GetProjects() []string {
	names := make([]string, 0, len(allProjects))
	for _, p := range allProjects {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names
}

// Returns a newly constructed Launcher from a given Instance
func GetLauncher(i Instance) (launcher.Launcher, error) {
	for _, p := range allProjects {
		if p.Name == i.ProjectName {
			return p.NewLauncher(i.Specification, i.Version), nil
		}
	}
	return nil, fmt.Errorf("registry: no project named %q", i.ProjectName)
}

// Returns all ProjectName * []Specifcation * []Version combinations
// (Ordered by ProjectName)
func GetInstances() []Instance {
	names := make([]string, 0, len(allProjects))
	byName := make(map[string]Project, len(allProjects))
	for _, p := range allProjects {
		names = append(names, p.Name)
		byName[p.Name] = p
	}
	sort.Strings(names)

	var instances []Instance
	for _, name := range names {
		p := byName[name]
		for _, spec := range p.Specifications {
			for _, version := range p.Versions {
				instances = append(instances, Instance{
					ProjectName:   p.Name,
					Repository:    p.Repository,
					Specification: spec,
					Version:       version,
				})
			}
		}
	}
	return instances
}
