package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Differentiates replica nodes and client node
type NodeType int
const (
	NodeTypeReplica NodeType = iota
	NodeTypeClient
)

// State of a single node in the consensus protocol
type Node struct {
	// Node ID. Ex: "node1", "node2"
	ID string 

	// IP to access machine from outside the network
	PublicIP string

	// IP to communicate with node from a machine in the same network
	// Note: Used on Cloudlab machine or AWS if 
	// all nodes are in the same internal network
	PrivateIP string

	// Whether nodes run on the same machine as the driver
	// Note: local benchmarks are usually only for testing, 
	// use proper remote machine for real evaluation (ex: Cloudlab)
	Local bool   

	// Replica node or Client node?
	Type NodeType
}

// Credential for SSH to machines
// Note: currently only support plain key-based auth. No agent, no passphrase support
// TODO: consider adding agent support & passphrase
type SSHConfig struct {
	// Absolute path to the SSH private key file
	KeyPath  string

	// Username for SSH key
	Username string
}


// The full set of settings distrobench needs to run
type Config struct {
	Nodes 	[]Node 		// Replica + Client nodes
	SSH 	SSHConfig

	// The file in which the benchmark results are outputted
	OutputFile 	string

	// k6 Latency benchmark defaults
	WarmupDuration 	string  // ex: "60s"
	Duration       	string  // ex: "180s"
	WriteRatio     	float64 // ex: 0.2

	// Add New Peer benchmark defaults
	AddNewPeerTimeout      time.Duration // ex: 60s. Bounds AwaitDataPlaneReady only
	AddNewPeerPollInterval time.Duration // ex: 100ms. Poll interval used by AwaitDataPlaneReady
}

// Return nodes with Type == NodeTypeReplica (in original order)
func ReplicaNodes(nodes []Node) []Node {
	out := make([]Node, 0, len(nodes))
	for _, n := range nodes {
		if n.Type == NodeTypeReplica {
			out = append(out, n)
		}
	}
	return out
}

// Return node with Type == NodeTypeClient
func ClientNode(nodes []Node) Node {
	for _, n := range nodes {
		if n.Type == NodeTypeClient {
			return n
		}
	}
	return Node{}
}

// Converts loaded environment variables into correct data structure then validates them
// NOTE: this function doesn't actually load the .env file. It just checks the datatype
// The .env file must already be loaded by `godotenv.Load()` before this function is called
func Load() (*Config, error) {
	numStr := os.Getenv("NUM_OF_NODES")
	if numStr == "" {
		return nil, fmt.Errorf("config: NUM_OF_NODES is required")
	}

	numNodes, err := strconv.Atoi(numStr)
	if err != nil {
		return nil, fmt.Errorf("config: NUM_OF_NODES must be an integer, got %q: %w", numStr, err)
	}

	if numNodes <= 0 {
		return nil, fmt.Errorf("config: NUM_OF_NODES must be positive, got %d", numNodes)
	}

	nodes := make([]Node, 0, numNodes+1)
	for i := 1; i <= numNodes; i++ {
		privateKey := fmt.Sprintf("PRIVATE_IP%d", i)
		publicKey := fmt.Sprintf("PUBLIC_IP%d", i)

		privateIP := os.Getenv(privateKey)
		if privateIP == "" {
			return nil, fmt.Errorf("config: %s is required (NUM_OF_NODES=%d)", privateKey, numNodes)
		}

		publicIP := os.Getenv(publicKey)
		if publicIP == "" {
			return nil, fmt.Errorf("config: %s is required (NUM_OF_NODES=%d)", publicKey, numNodes)
		}

		nodes = append(nodes, Node{
			ID:        fmt.Sprintf("node%d", i),
			PublicIP:  publicIP,
			PrivateIP: privateIP,
			Local:     publicIP == "127.0.0.1",
			Type:      NodeTypeReplica,
		})
	}

	sshKeyPath := os.Getenv("SSH_KEY")
	if sshKeyPath == "" {
		return nil, fmt.Errorf("config: SSH_KEY is required")
	}

	absKeyPath, err := filepath.Abs(sshKeyPath)
	if err != nil {
		return nil, fmt.Errorf("config: could not resolve SSH_KEY path %q: %w", sshKeyPath, err)
	}

	username := os.Getenv("REMOTE_USERNAME")
	if username == "" {
		return nil, fmt.Errorf("config: REMOTE_USERNAME is required")
	}

	clientPublicIP := os.Getenv("CLIENT_PUBLIC_IP")
	if clientPublicIP == "" {
		return nil, fmt.Errorf("config: CLIENT_PUBLIC_IP is required")
	}
	clientPrivateIP := os.Getenv("CLIENT_PRIVATE_IP")
	if clientPrivateIP == "" {
		return nil, fmt.Errorf("config: CLIENT_PRIVATE_IP is required")
	}
	nodes = append(nodes, Node{
		ID:        "client",
		PublicIP:  clientPublicIP,
		PrivateIP: clientPrivateIP,
		Local:     clientPublicIP == "127.0.0.1",
		Type:      NodeTypeClient,
	})

	// Note: there might be different benchmarks which requires different files.
	// Consider implementing a proper output file
	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		outputFile = "data.local.json"
	}

	warmupDuration := os.Getenv("WARMUP_DURATION")
	if warmupDuration == "" {
		warmupDuration = "60s"
	}

	duration := os.Getenv("DURATION")
	if duration == "" {
		duration = "180s"
	}

	writeRatioStr := os.Getenv("WRITE_RATIO")
	if writeRatioStr == "" {
		writeRatioStr = "0.2"
	}
	writeRatio, err := strconv.ParseFloat(writeRatioStr, 64)
	if err != nil {
		return nil, fmt.Errorf("config: WRITE_RATIO must be a number, got %q: %w", writeRatioStr, err)
	}

	addNewPeerTimeoutStr := os.Getenv("ADD_NEW_PEER_TIMEOUT")
	if addNewPeerTimeoutStr == "" {
		addNewPeerTimeoutStr = "60s"
	}
	addNewPeerTimeout, err := time.ParseDuration(addNewPeerTimeoutStr)
	if err != nil {
		return nil, fmt.Errorf("config: ADD_NEW_PEER_TIMEOUT must be a duration, got %q: %w", addNewPeerTimeoutStr, err)
	}

	addNewPeerPollIntervalStr := os.Getenv("ADD_NEW_PEER_POLL_INTERVAL")
	if addNewPeerPollIntervalStr == "" {
		addNewPeerPollIntervalStr = "100ms"
	}
	addNewPeerPollInterval, err := time.ParseDuration(addNewPeerPollIntervalStr)
	if err != nil {
		return nil, fmt.Errorf("config: ADD_NEW_PEER_POLL_INTERVAL must be a duration, got %q: %w", addNewPeerPollIntervalStr, err)
	}

	cfg := &Config{
		Nodes: nodes,
		SSH: SSHConfig{
			KeyPath:  absKeyPath,
			Username: username,
		},
		OutputFile:     outputFile,

		// Latency Benchmark Defaults
		WarmupDuration: warmupDuration,
		Duration:       duration,
		WriteRatio:     writeRatio,

		// AddNewPeer Benchmark Defaults
		AddNewPeerTimeout:      addNewPeerTimeout,
		AddNewPeerPollInterval: addNewPeerPollInterval,
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Check if the loaded config variables are valid.
// Note: this is kept separate from `Load()` in case the config variables 
// are loaded from a different source than .env
func (c *Config) Validate() error {
	if len(c.Nodes) == 0 {
		return fmt.Errorf("config: at least one node is required")
	}

	clientCount := 0
	replicaCount := 0
	for i, n := range c.Nodes {
		if n.PublicIP == "" {
			return fmt.Errorf("config: nodes[%d] missing PublicIP", i)
		}

		if n.PrivateIP == "" {
			return fmt.Errorf("config: nodes[%d] missing PrivateIP", i)
		}

		switch n.Type {
		case NodeTypeClient:
			clientCount++
		case NodeTypeReplica:
			replicaCount++
		}
	}

	if clientCount != 1 {
		return fmt.Errorf("config: Nodes must contain exactly one client node, found %d", clientCount)
	}
	if replicaCount == 0 {
		return fmt.Errorf("config: at least one replica node is required")
	}

	if c.SSH.KeyPath == "" {
		return fmt.Errorf("config: SSH KeyPath is required")
	}

	if c.SSH.Username == "" {
		return fmt.Errorf("config: SSH Username is required")
	}

	if c.WriteRatio < 0 || c.WriteRatio > 1 {
		return fmt.Errorf("config: WriteRatio must be between 0 and 1, got %v", c.WriteRatio)
	}

	if c.AddNewPeerTimeout <= 0 {
		return fmt.Errorf("config: AddNewPeerTimeout must be positive, got %v", c.AddNewPeerTimeout)
	}

	if c.AddNewPeerPollInterval <= 0 {
		return fmt.Errorf("config: AddNewPeerPollInterval must be positive, got %v", c.AddNewPeerPollInterval)
	}

	return nil
}
