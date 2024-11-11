# distrobench - Distributed Protocol Benchmarks

`distrobench` aims to measure various **implementations** of distributed
replication/coordination protocols that provide certain consistency models.
For now, we are focusing on linearizability, with protocols like Raft, Paxos,
EPaxos, Viewstamped Replication, ZAB, and ABD for key-value store interface.

With the benchmark, we want to highlight **techniques** that can offer better
performance. We hope that by doing the benchmark, those important approaches can actually be used in production systems, not only ending as research prototypes.

Project structure:
```
distro/
├─ src/                         // benchmarking source code
├─ sut/                         // systems under test
│  ├─ etcd-io.raft/
│  ├─ hashicorp.raft/
│  ├─ databendlabs.openraft/
│  ├─ MicroRaft.MicroRaft/
│  ├─ haraldng.omnipaxos/
│  ├─ ailidani.paxi/
│  ├─ scylladb.raft/
│  ├─ facebook.kuduraft/
│  ├─ psu-csl.replicated-store/
│  ├─ MobilityFirst.gigapaxos/
│  ├─ efficient.epaxos/
│  ├─ PlatformLab.epaxos/
│  ├─ penberg.vsr-rs/
|  ... 
|
├─ README.md

```

## Tested Features
- Common case performance (low-load latency, latency & throughput, memory usage)
- Fault-tolerance when leader crash (latency and throughput over time)
- Correctness

## Workload and System Setup
- We use YCSB as the workload executed by clients of all replicas.
- Run in a single machine, with replica instance run in a Docker container.
- We limit the memory usage of the container by 1GB.
- We use 5 replicas.
- The clients send HTTP requests, if the tested implementations do not provide
  HTTP interface, we will use the supported client library and then expose HTTP 
  interface. We prefer HTTP interface to easily capture the measurement results
  by using Grafana/Promotheus.

> [!CAUTION]
> Feel free to make Pull Requests if you find any part in the benchmark steps 
> that unfair for certain implementations.
