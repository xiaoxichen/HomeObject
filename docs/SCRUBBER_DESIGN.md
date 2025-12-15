# Cross-Replica Scrubber Design for HomeObject

**Document Version:** 1.0
**Date:** 2025-12-14
**Status:** Final Design
**Target Audience:** Engineering Managers, Technical Leads, Architects

**Related Documents:**
- [SCRUBBER_IMPLEMENTATION.md](./SCRUBBER_IMPLEMENTATION.md) - Implementation guide for engineers
- [SCRUBBER_OPERATIONS.md](./SCRUBBER_OPERATIONS.md) - Deployment and operations guide

---

## Executive Summary

### Problem Statement

HomeObject is a distributed object storage system built on HomeStore with Raft-based replication. While Raft consensus ensures write consistency across replicas, silent data corruption can occur due to hardware failures, software bugs, or bit rot. Without periodic verification, inconsistencies between replicas may go undetected until data is accessed, potentially leading to:

- Undetected data corruption across all replicas
- Replica divergence due to non-deterministic bugs
- Loss of data integrity guarantees
- Inability to identify authoritative data sources during recovery

This design addresses the need for proactive data integrity verification through cross-replica scrubbing.

### Solution Overview

We propose implementing a **cross-replica scrubbing system** that periodically verifies data consistency across all replicas within a Placement Group (PG). The system provides two complementary verification modes:

1. **Shallow Scrub**: Fast metadata-only verification that compares blob indexes across replicas (daily frequency, ~40 sec for 10M blobs)
2. **Deep Scrub**: Comprehensive data verification that computes and compares cryptographic checksums of actual blob data (weekly frequency, ~12 hours for 10M blobs)

The scrubber operates as a background service with configurable resource controls to minimize impact on normal I/O operations. Detected inconsistencies are reported for administrative investigation rather than automatically repaired, prioritizing data safety over automation in the MVP.

### Key Design Decisions

| Decision Area | Choice | Rationale |
|---------------|--------|-----------|
| **Communication** | nuraft_messenger data service | Leverages existing infrastructure, supports bidirectional RPC, UUID-based addressing |
| **Data Flow** | Follower → Leader | Enables independent follower processing, centralized comparison, graceful degradation |
| **Consistency Point** | blob_id ranges | Monotonic ordering within shards, no snapshot mechanism required |
| **Batch Sizes** | Shallow: 500, Deep: 25 | Aligned with B+tree node capacity and Ceph production patterns |
| **Inconsistency Handling** | Report-only, admin investigation | Prioritizes correctness over automation, defers to upper-layer context |
| **Scheduling** | Auto + Manual | Background scheduler with randomization, plus on-demand HTTP API |

### Architecture Highlights

```
┌─────────────────────────────────────────────────────────────┐
│                    Scrub Coordinator (Leader)                │
│  ┌────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Scheduler │→ │ Scrub Engine │→ │ Inconsistency      │  │
│  │  (10 min)  │  │              │  │ Reporter           │  │
│  └────────────┘  └──────────────┘  └────────────────────┘  │
│                         ↓                                    │
└─────────────────────────┼────────────────────────────────────┘
                          ↓ nuraft_messenger data service
        ┌─────────────────┼─────────────────┐
        ↓                 ↓                 ↓
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│  Replica 1    │  │  Replica 2    │  │  Replica 3    │
│  (Leader)     │  │  (Follower)   │  │  (Follower)   │
│               │  │               │  │               │
│ ┌───────────┐ │  │ ┌───────────┐ │  │ ┌───────────┐ │
│ │  Handler  │ │  │ │  Handler  │ │  │ │  Handler  │ │
│ │     ↓     │ │  │ │     ↓     │ │  │ │     ↓     │ │
│ │ IndexTbl  │ │  │ │ IndexTbl  │ │  │ │ IndexTbl  │ │
│ └───────────┘ │  │ └───────────┘ │  │ └───────────┘ │
└───────────────┘  └───────────────┘  └───────────────┘
```

### Scope and Limitations

**In Scope:**
- Detection of blob index inconsistencies (existence, metadata mismatches)
- Detection of data corruption via checksum comparison
- Configurable resource controls (batch size, concurrency, rate limiting)
- Automated scheduling with randomization
- Manual trigger via HTTP API
- Task tracking and reporting

**Out of Scope (MVP):**
- Automatic repair of inconsistencies (admin-triggered manual repair only)
- Load-based throttling (latency or system metrics)
- Time-window scheduling (scrub_begin_hour / scrub_end_hour)
- Pause/resume with checkpoint recovery for shallow scrubs
- Per-PG resource controls (instance-level only)

**Known Limitations:**
- Eventual consistency during active writes may cause transient false positives (mitigated by spot-check phase)
- Post-GC tombstone removal creates ambiguity for "extra blob" detection
- Two-replica deployments cannot use majority voting (leader is assumed authoritative)

---

## 1. Introduction

### 1.1 Background

HomeObject provides distributed object storage with strong consistency guarantees through Raft consensus replication. Each object (blob) is replicated across multiple nodes within a Placement Group (PG). While Raft ensures that committed writes are agreed upon, it does not protect against:

1. **Silent data corruption**: Bit flips in memory or on disk that corrupt data after it has been written
2. **Non-deterministic bugs**: Software defects that cause replicas to diverge despite identical raft logs
3. **Hardware failures**: Disk errors that corrupt data without triggering detectable I/O errors
4. **Metadata drift**: Index inconsistencies due to crashes during non-atomic operations

These issues require periodic verification independent of the replication protocol.

### 1.2 Goals

**Primary Goals:**
1. Detect inconsistencies between replicas in a Placement Group
2. Provide two verification levels: fast metadata checks and thorough data validation
3. Minimize impact on normal I/O operations through configurable resource controls
4. Enable both automated periodic verification and manual on-demand scrubbing
5. Provide clear reporting and observability for operational teams

**Non-Goals:**
1. Automatic repair of detected inconsistencies (deferred to post-MVP)
2. Cross-PG or cluster-wide scrubbing coordination (per-PG scope only)
3. Prevention of corruption (detection only)
4. Real-time verification (background periodic checks only)

### 1.3 Design Principles

1. **Safety over Automation**: Report inconsistencies for admin investigation rather than auto-repair
2. **Minimal Disruption**: Design for background operation with negligible impact on client I/O
3. **Leverage Existing Infrastructure**: Use nuraft_messenger, HomeStore index APIs, existing PG state management
4. **Production-Validated Patterns**: Align with Ceph's proven scrubbing implementation where applicable
5. **Graceful Degradation**: Operate correctly even when some replicas are unavailable
6. **Configurability**: Expose key resource controls as runtime-tunable parameters

---

## 2. Architecture

### 2.1 System Components

The scrubbing system consists of four primary components:

#### 2.1.1 Scrub Scheduler
**Responsibility**: Automated periodic scrub initiation

- Background task running every 10 minutes (configurable)
- Evaluates all PGs to determine scrub eligibility based on time since last scrub, PG state, and resource availability
- Selects PGs using priority algorithm (urgent PGs past max_interval first, then oldest)
- Applies randomization (±50% jitter) to prevent thundering herd

#### 2.1.2 Scrub Engine
**Responsibility**: Scrub execution and coordination

- Runs on PG leader replica only (leadership changes abort in-flight scrubs)
- Orchestrates scrub workflow: set PG state flag, query replicas, compare responses, generate report
- Manages batch processing with configurable sleep intervals
- Handles follower failures gracefully (timeout, partial results)

#### 2.1.3 Scrub Request Handler
**Responsibility**: Respond to scrub requests from leader

- Registered on all replicas (leader + followers)
- Receives requests via nuraft_messenger data service
- Queries local index table for requested blob_id range
- Returns blob list (shallow) or blob checksums (deep)
- Enforces local concurrency limits

#### 2.1.4 Inconsistency Reporter
**Responsibility**: Record and expose scrub findings

- Stores inconsistency details in scrub task state (persisted)
- Updates PG scrub metadata (sets INCONSISTENT flag if issues found)
- Provides HTTP API for querying task status and inconsistency details
- Emits metrics for monitoring

### 2.2 Communication Architecture

#### 2.2.1 Why nuraft_messenger Data Service?

Three communication options were evaluated:

1. **HTTP RPC**: Infeasible due to lack of service discovery (peers known by UUID, not IP:port)
2. **Raft Log Extension**: Incompatible (Raft is unidirectional leader→followers, no response channel)
3. **nuraft_messenger Data Service**: ✅ Selected

The nuraft_messenger library provides a data service layer separate from Raft log replication:

- **Bidirectional RPC**: Request/response pattern via `data_service_request_bidirectional()`
- **UUID-based Addressing**: Uses `peer_id_t` directly, no network discovery needed
- **Already Integrated**: HomeStore uses this for PUSH_DATA and FETCH_DATA operations
- **Production-Proven**: Handles 4MB+ payloads, tested in HomeStore replication

#### 2.2.2 Data Flow Pattern: Follower → Leader

The leader initiates scrub but followers send their data to the leader for comparison. This enables:

- **Independent Pace**: Slow followers don't block fast ones
- **Simpler State Management**: Leader doesn't track per-follower progress
- **Centralized Comparison**: Leader has complete view for majority voting
- **Graceful Degradation**: Can timeout unavailable followers, proceed with partial results

### 2.3 Workflows

#### 2.3.1 Shallow Scrub Workflow

**High-Level Flow:**

```
┌─────────────────────────────────────────────────────────────┐
│                    SHALLOW SCRUB WORKFLOW                    │
└─────────────────────────────────────────────────────────────┘

    [START]
       ↓
   Scheduler selects PG
       ↓
   Leader check
       ↓
   Atomically set SCRUBBING flag
       ↓
   Capture max_blob_id for all shards
       ↓
   For each shard:
     ┌──────────────────────────┐
     │ For batch [id, id+500):  │
     │   - Send RPC to replicas │
     │   - Collect responses    │
     │   - Compare blob_id sets │
     │   - Record discrepancies │
     └──────────────────────────┘
       ↓
   Spot-Check Phase:
     - Wait 100ms (lag)
     - Query specific blobs
     - Filter transient diffs
       ↓
   Update PG metadata
       ↓
   Clear SCRUBBING flag
       ↓
    [END]

Duration: ~40 sec for 10M blobs (20K batches × 2ms)
```

**Key Steps:**
1. Scheduler selects PG for shallow scrub
2. Verify leadership and atomically set SCRUBBING flag
3. Capture PG-level snapshot (max_blob_id for all shards)
4. For each shard, process batches of 500 blobs
5. Compare blob existence across replicas
6. Execute spot-check to filter transient replication lag
7. Update metadata and clear flag

#### 2.3.2 Deep Scrub Workflow

**High-Level Flow:**

```
┌─────────────────────────────────────────────────────────────┐
│                     DEEP SCRUB WORKFLOW                      │
└─────────────────────────────────────────────────────────────┘

    [START]
       ↓
   Scheduler selects PG
       ↓
   Leader check
       ↓
   Atomically set SCRUBBING flag
       ↓
   Capture max_blob_id for all shards
       ↓
   For each shard:
     ┌──────────────────────────────┐
     │ For batch [id, id+25):       │
     │   - Send RPC to replicas     │
     │   - Read blob + compute hash │
     │   - Collect checksums        │
     │   - Compare checksums        │
     │   - Sleep 10ms (throttle)    │
     ├──────────────────────────────┤
     │ Checkpoint progress          │
     │ (persist to metablk)         │
     └──────────────────────────────┘
       ↓
   Spot-Check (if needed)
       ↓
   Update PG metadata
       ↓
   Clear SCRUBBING flag
       ↓
    [END]

Duration: ~12 hours for 10M blobs (400K batches × 110ms)
Resumable: Yes, from last checkpointed shard
```

**Key Differences from Shallow:**
- **Batch Size**: 25 blobs (vs. 500 for shallow)
- **Verification**: Checksums of actual data (vs. existence only)
- **Throttling**: 10ms sleep between batches (vs. no sleep)
- **Checkpointing**: Per-shard persistence for resumability
- **Timeout**: 60 sec per batch (vs. 5 sec)

---

## 3. Design Rationale

### 3.1 Why nuraft_messenger?

**Alternatives Considered:**
- HTTP RPC: Cannot discover peer addresses (only have UUIDs)
- Raft Log Extension: Unidirectional, no response channel

**Selected Approach**: nuraft_messenger data service
- Already integrated in HomeStore for PUSH_DATA/FETCH_DATA
- Supports bidirectional RPC with UUID-based addressing
- Production-proven with 4MB+ payloads

### 3.2 Why Follower → Leader Data Flow?

**Alternatives Considered:**
- Leader → Followers: Leader pushes its data, followers compare locally

**Selected Approach**: Followers send data to leader
- Enables independent processing pace (slow followers don't block fast ones)
- Centralized comparison logic (simpler state management)
- Graceful degradation (can proceed with partial results)

### 3.3 Why Report-Only (No Auto-Repair)?

**Alternatives Considered:**
- Auto-repair with majority voting
- Auto-repair with admin-configured authoritative source

**Selected Approach**: Report inconsistencies for admin investigation
- Safer: prevents automated data loss from bugs in repair logic
- Context-aware: admin can check upper-layer application logs
- Defers to future enhancement when repair logic is battle-tested

### 3.4 Why These Batch Sizes?

**Shallow Scrub (500 blobs):**
- Aligns with B+tree node capacity (~100 entries per 4KB node)
- Index reads are non-blocking
- Optimizes throughput over granularity

**Deep Scrub (25 blobs):**
- Aligned with Ceph production (`osd_scrub_chunk_max = 25`)
- Balances disk I/O, CPU (checksum), and memory (blob data)
- Proven batch size for data verification workloads

---

## 4. Resource Requirements

### 4.1 Team & Timeline

**Team Size**: 2-3 engineers

**Required Skills:**
- Strong C++ (C++17/20)
- Distributed systems experience
- Familiarity with Raft consensus
- HomeStore/nuraft_messenger knowledge (or ability to ramp up quickly)

**Infrastructure:**
- Test cluster: 3-5 nodes for integration testing
- Performance benchmarking environment

**External Dependencies:**
- HomeStore team for API review and guidance
- Platform team for metrics/monitoring integration

### 4.2 Implementation Timeline

**Phase 1: Core Infrastructure** (4-6 weeks)
- nuraft_messenger handler registration and dispatch
- Scrub engine with batch processing
- PG state management (SCRUBBING flag, atomic operations)
- Shallow scrub implementation
- Basic HTTP API (manual trigger, task status query)
- Persistence (PG metadata + task state)

**Phase 2: Scheduling & Resource Control** (2-3 weeks)
- Background scheduler with randomization
- Concurrency limits (instance-level)
- NO_SCRUB flags and HTTP endpoints
- Deep scrub implementation
- Spot-check phase
- Task retention cleanup

**Phase 3: Observability & Operations** (2-3 weeks)
- Metrics emission (Prometheus format)
- Detailed inconsistency reporting API
- PG scrub metadata query endpoints
- Operational documentation
- Integration testing with realistic workloads

**Phase 4: Validation & Rollout** (2-4 weeks)
- Performance benchmarking (impact on client latency)
- Fault injection testing (replica failures, network partitions)
- Long-running stability tests
- Gradual rollout to production clusters

**Total Estimated Timeline**: 10-16 weeks for MVP

---

## 5. Risks & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| **Performance impact on client I/O** | Medium | High | Extensive benchmarking, tunable sleep parameters, conservative defaults (max_concurrent=1) |
| **Leadership change edge cases** | Low | Medium | Comprehensive testing, graceful abort on leadership loss, periodic checks |
| **False positives from replication lag** | Medium | Low | Spot-check phase with 100ms delay, filters transient differences |
| **nuraft_messenger payload size limits** | Low | Medium | Message sizes well within tested limits (~4KB shallow, ~2KB deep) |
| **Checkpoint corruption during crashes** | Low | High | Atomic metablk writes, validation on resume |
| **Timeline slippage** | Medium | Medium | Phased approach allows early value delivery, Phase 1-2 already provide basic scrubbing |

---

## 6. Success Criteria

### 6.1 Functional

- ✅ Detect checksum mismatches between replicas
- ✅ Detect missing/extra blobs between replicas
- ✅ Complete shallow scrub within configured max_interval (7 days default)
- ✅ Complete deep scrub within configured max_interval (30 days default)
- ✅ Survive replica failures gracefully (timeout, partial results)
- ✅ Resume deep scrubs after pod restart

### 6.2 Performance

- ✅ Shallow scrub impact: <1% increase in P99 read latency
- ✅ Deep scrub impact: <5% increase in P99 read latency (with default sleep 10ms)
- ✅ Network overhead: <1 MB/sec per instance during deep scrub

### 6.3 Operational

- ✅ Manual scrub trigger via HTTP API (<1 sec response time)
- ✅ Task status query via HTTP API (<100ms response time)
- ✅ Inconsistency details accessible via API
- ✅ Scrubbing disable/enable takes effect within 1 scheduler cycle (10 min)

---

## 7. Future Enhancements

### 7.1 High Priority

- **Automatic Repair**: Admin-triggered repair workflow with authoritative replica selection
- **Latency-Based Throttling**: Dynamic sleep adjustment based on batch processing time
- **Enhanced Resumability**: Shallow scrub checkpoint recovery

### 7.2 Medium Priority

- **Time-Window Scheduling**: Restrict scrubbing to off-peak hours (scrub_begin_hour / scrub_end_hour)
- **Load-Based Throttling**: Pause scrubbing when system load exceeds threshold
- **Priority Boost for INCONSISTENT PGs**: Re-verify inconsistent PGs more frequently

### 7.3 Low Priority

- **Per-PG Resource Controls**: Fine-grained throttling for individual PGs
- **Cross-PG Coordination**: Cluster-wide scrub scheduling to balance load
- **Incremental Deep Scrub**: Sample-based verification for very large PGs

---

## 8. Conclusion

### 8.1 Design Summary

This document specifies a cross-replica scrubbing system for HomeObject that provides proactive data integrity verification through two complementary modes:

- **Shallow Scrub**: Fast daily metadata verification (index consistency)
- **Deep Scrub**: Thorough weekly data verification (checksum validation)

The design leverages existing infrastructure (nuraft_messenger, HomeStore index APIs, PG state management) to minimize implementation complexity while providing production-grade reliability features:

- Configurable resource controls to minimize client I/O impact
- Automated scheduling with randomization to prevent load spikes
- Two-level persistence for resumability and task tracking
- Comprehensive HTTP API for operational control
- Graceful degradation when replicas are unavailable

### 8.2 Key Strengths

1. **Production-Validated Patterns**: Communication flow, batch sizes, and scheduling logic aligned with Ceph's proven scrubbing implementation
2. **Minimal Infrastructure Changes**: Reuses nuraft_messenger data service, no new communication channels required
3. **Safety First**: Report-only approach prioritizes data integrity over automation
4. **Operational Flexibility**: Both automated and manual trigger modes, ephemeral disable flags for emergency control
5. **Clear MVP Scope**: Focused on detection with well-defined post-MVP enhancements

### 8.3 Open Questions for Review

1. **Message Encoding**: Use Protobuf, FlatBuffers, or custom binary format for scrub messages?
2. **Checksum Algorithm**: Continue with CRC32 or upgrade to SHA256 for deep scrub?
3. **Metrics Backend**: Prometheus format sufficient, or also support OpenTelemetry?
4. **Task ID Generation**: UUID, monotonic counter, or timestamp-based?
5. **Configuration Reload**: Support hot-reload of config parameters, or require restart?

These questions should be resolved during Phase 1 implementation planning.

---

## Appendix: Reference Documents

- **[SCRUBBER_IMPLEMENTATION.md](./SCRUBBER_IMPLEMENTATION.md)**: Detailed implementation guide with data structures, algorithms, and code examples
- **[SCRUBBER_OPERATIONS.md](./SCRUBBER_OPERATIONS.md)**: Deployment, monitoring, and troubleshooting guide
- **scrubber_draft_v2/** (reference only): Original comprehensive design document with technical appendices
