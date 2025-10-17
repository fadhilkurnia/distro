# Definition of Consistency Models for the 21st Century

Consistency models define how distributed systems ensure all participants observe data changes in a coherent way.
Having precise definitions helps us avoid ambiguity and ensures that everyone interprets system behavior the same way.

Historically, these models originated in the context of **shared-memory systems**, where a **“Process”** referred to a program accessing a shared memory module.
However, when applied to 21st century distributed systems, this terminology becomes ambiguous. 
Some interpret “Process” as a **server**, since processes reside in server, while others interpret it as a **client**, since they issue operations to the shared-memory system.

To reconcile these perspectives, we revisit the literal definitions from the original papers that introduce them.
Then, we reinterpret them under a unified, modern system model for 21st century systems.

## Unified System Model
```
 +---------------------+       +---------------------+       +---------------------+
 |     Server 1        |<----->|     Server 2        |<----->|     Server 3        |
 |  +-------------+    |       |  +-------------+    |       |  +-------------+    |
 |  |  Process P1 |    |       |  |  Process P2 |    |       |  |  Process P3 |    |
 |  |  Replica R1 |    |       |  |  Replica R2 |    |       |  |  Replica R3 |    |
 +---------------------+       +---------------------+       +---------------------+
           ^                             ^                              ^        
           |                             |                              |        
          C1                            C2                              C3        
     read/write/rmw               read/write/rmw                 read/write/rmw 
```

| Symbol | Meaning |
|:-------:|:--------|
| **R** | State Replica — stores the state, modled as key–value data. |
| **P** | Process — receives and executes client operations. |
| **C** | Client — issues operations (read, write, read-modify-write) to servers. |
| **O** | Operation — represents an action such as `read`, `write`, or `read-modify-write`. |
| **K, V** | Key and Value — identify and store the data items involved in each operation. |
| | |
| **H** | History - sequence of operations done by Client |
| **S** | Serialization — combination of multiple Client Histories into a single sequence. |

Here are how we translate shared-memory model into our model:
- Operation: historically shared-memory only allow simple read and write operations.
  However, modern system can do more complex read-your write operations (e.g., conditional write).
- Process: historically, process is also the client of shared-memory system.
  In modern system, process still does execution, but we explicitly separate the Client role.
- Client: in modern system, client is not bounded to certain server.
  Client can move freely and contact any server it wants.
  For simplicity, we do not consider client's session, which we can simply be treated as Client too.
- Key and Value: historically the shared-memory model has operations on memory location, we generalize that into Key.

## Linearizability

**Original definition** from the 1990 paper of "Linearizability: A Correctness Condition for Concurrent Objects" (ACM TOPLAS) by Maurice Herlihy and Jeannette Wing:

A history H is **linearizable** if it can be extended (by appending zero or more response events to pending invocations) to a complete history H' such that:
1. **Equivalence to a legal sequential history**
   There exists a legal sequential serialized history S that is equivalent to H'.
   Equivalence means that both histories contain the same set of operations, and each process sees its operations occur in the same order in both H' and S.

2. **Real-time order preservation**
   If operation O<sub>1</sub> completes (its response occurs) before operation O<sub>2</sub> is invoked in the original history H, then O<sub>1</sub> must appear before O<sub>2</sub> in the sequential serialized history S.

**21st Century Update**. Surprisingly, interpreting "process" as "server" or "client" are both fine because of the real-time constraint.

## Sequential Consistency

**Original definition** from the 1979 paper of "How to Make a Multiprocessor Computer That Correctly Executes Multiprocess Programs" by Leslie Lamport:

“The result of any execution is the same as if the operations of all the processors were executed in some sequential order, and the operations of each individual processor appear in this sequence in the order specified by its program.”

**21st Century Update**. Processor can be interpreted as "process" because the definition cares more about execution.

## Causal Consistency

This was originally called Causal Memory, and was discussed by Ahamad et, al in:
- Slow memory: Weakening consistency to enhance concurrency in distributed shared memories (ICDCS, 1990)
- Implementing and Programming Causal Distributed Shared Memory (ICDC, 1991)
- Causal memory: definitions, implementation, and programming (Georgia Tech Technical Report, 1993)

The original definition as is follow.

A memory system is causally consistent if it enforces the following ordering constraints among operations:
- Let us define a potential causality relation, based on Lamport's "happens-before" idea, extended to memory operations:
  1. If in a Process P, an Operation O<sub>1</sub> precedes an operation O<sub>2</sub> in program order, then O<sub>1</sub> causally precedes O<sub>2</sub>.
  2. If a read operation r returns the result of a write operation w, then that write w causally precedes the read r.
  3. Causality is transitive: if O<sub>1</sub> causally precedes O<sub>2</sub> and O<sub>2</sub> causally precedes O<sub>3</sub>, then O<sub>1</sub> causally precedes O<sub>3</sub>.
- The causal consistency gurantee is: All processes must see causally related writes in the same order.
  In other words, if write operation w<sub>1</sub> causally precedes another write w<sub>2</sub>, then every process’s view of writes must reflect w<sub>1</sub> before w<sub>2</sub>.
  However, for concurrent (causally independent) writes—i.e. writes where neither causally precedes the other—different processes may observe them in different orders.

**21st Century Update**. TODO.

## PRAM
The **original definition** of PRAM (Parallel Random-Access Memory) is available in “PRAM: A Scalable Shared Memory” (Lipton & Sandberg, 1988).

**21st Century Update**. TODO.

## Eventual Consistency

**Original definition**. TODO.

**21st Century Update**. TODO.

## Client-centric Consistency
### Read your writes
**Original definition**. TODO.

**21st Century Update**. TODO.

### Monotonic reads
**Original definition**. TODO.

**21st Century Update**. TODO.

### Monotonic writes
**Original definition**. TODO.

**21st Century Update**. TODO.

### Writes follow reads
**Original definition**. TODO.

**21st Century Update**. TODO.

## Other consistency models
- Consistency unit (conit)
- Fork-join consistency
- Consistent prefix
