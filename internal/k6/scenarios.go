package k6

import (
	"fmt"
	"strings"

	"github.com/fadhilkurnia/distro/internal/launcher"
)

// Generate the k6 latency benchmark scenarios for a protocol:
// - Warmup
// - Benchmark
// There will be 1 client for every node.
func BuildScenarios(addresses []launcher.NodeAddress) string {
	var b strings.Builder
	for _, a := range addresses {
		addr := fmt.Sprintf("%s:%d", a.PublicIP, a.PublicPort)
		fmt.Fprintf(
			&b,`		%s_warmup: {
				executor: "constant-vus",
				vus: 1,
				duration: __ENV.WARMUP_DURATION,
				env: { ADDR: %q },
				exec: "warmup",
			},
			%s: {
				executor: "constant-vus",
				vus: 1,
				duration: __ENV.DURATION,
				startTime: __ENV.WARMUP_DURATION,
				env: { ADDR: %q },
				exec: "benchmark",
			},
			`, a.NodeID, addr, a.NodeID, addr,
		)
	}
	return b.String()
}
