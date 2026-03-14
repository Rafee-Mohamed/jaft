# Jaft

A Java implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf) for building reliable, replicated state machines.

## Overview

Raft is a consensus algorithm for managing a replicated log across a cluster of nodes, ensuring agreement even in the presence of failures. For more details, see [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) by Diego Ongaro and John Ousterhout.

Jaft is a minimal, embeddable implementation of this protocol in Java. It implements the core algorithm and nothing else - network transport, serialization, and persistent storage are left to the application. This separation keeps the library deterministic, testable, and adaptable to any runtime environment.

The protocol core is modeled as a pure state machine: feed a `Message` in, collect the resulting output. Same state, same input, same output. No threads, no I/O, no side effects. An engine layer drives the state machine in a process/advance loop, and an optional node layer provides a thread-safe event loop for application integration.

Jaft is inspired by [etcd/raft](https://github.com/etcd-io/raft), the most widely deployed Raft implementation in production. While the protocol logic follows the same proven design, Jaft is built from the ground up in idiomatic Java.

## Features

**Core Protocol**

- Leader election and log replication
- Log compaction via snapshots
- Membership changes via joint consensus - safely add, remove, or replace nodes in a running cluster using a two-phase approach that maintains quorum throughout the transition
- Leadership transfer - gracefully hand off leadership to a specific node

**Linearizable Reads**

- Heartbeat-based - the leader confirms it still holds authority by contacting a majority before serving a read, either immediately or on the next periodic heartbeat
- Lease-based - the leader serves reads locally without a network round-trip, relying on bounded clock drift across the cluster
- Reads can be served by both the leader and followers

**Election Modes**

- Direct election - a node that times out immediately requests votes from the cluster
- Dual election - a pre-election round checks if the node can win before starting a real election, preventing unnecessary leader disruptions from partitioned nodes

**Execution Models**

- Sequential - entries are persisted to stable storage before being applied to the state machine
- Pipelined - persistence and application happen concurrently, allowing the leader to write to disk in parallel with its followers for higher throughput

**Operational**

- Flow control for log replication
- Batching of messages and log entries
- Proposal forwarding from followers to leader
- Automatic step-down when the leader loses quorum
- Protection against unbounded log growth when quorum is lost

## Architecture

```
                  +---------------------------+
                  |           Node            |
                  |  thread-safe event loop   |
                  |  async application API    |
                  +-------------+-------------+
                                |
                  +-------------v-------------+
                  |        RaftEngine         |
                  |  process/advance driver   |
                  |  state diff tracking      |
                  +-------------+-------------+
                                |
                  +-------------v-------------+
                  |           Raft            |
                  |  pure state machine       |
                  |  no I/O, no threads       |
                  +---------------------------+
```

**Raft** - the core state machine. Processes messages and buffers output. Elections, replication, membership changes, and reads all happen here.

**RaftEngine** - wraps Raft in a `process(input)` / `advance()` loop. Tracks state diffs so `advance()` only returns what has changed.

**Node** - the application-facing API. A single-threaded event loop that accepts proposals, ticks, and messages from external threads and publishes work items for the application to consume.

## Requirements

- Java 25+

## Project Status

The core implementation is complete. Comprehensive testing is the current milestone.

## Background

Jaft was originally built as the consensus layer for [Axis](https://github.com/Rafee-Mohamed/axis), a strongly consistent distributed key-value store for coordination, locking, and metadata management. It has since been extracted into a standalone library for independent development and use.

This project is part of a broader effort to explore, learn, and engineer production-grade distributed systems from first principles.

## References

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) - Diego Ongaro, John Ousterhout
- [Raft Thesis](https://github.com/ongardie/dissertation/blob/master/stanford.pdf) - Diego Ongaro
- [etcd/raft](https://github.com/etcd-io/raft) - Go reference implementation

## License

TBD
