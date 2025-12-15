# Cross-Replica Scrubber Design for HomeObject

**Document Version:** 1.0
**Date:** 2025-12-13
**Status:** Design Review
**Author:** Architecture Team

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

1. **Shallow Scrub**: Fast metadata-only verification that compares blob indexes across replicas (daily frequency)
2. **Deep Scrub**: Comprehensive data verification that computes and compares cryptographic checksums of actual blob data (weekly frequency)

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

### 1.4 Document Structure

- **Section 2**: High-level architecture and component interactions
- **Section 3**: Detailed design for each subsystem
- **Section 4**: API specifications (HTTP and internal messages)
- **Section 5**: Operational guide (configuration, monitoring, troubleshooting)
- **Appendix**: Technical rationale, calculations, and code examples

---

## 2. Architecture

### 2.1 System Components

The scrubbing system consists of four primary components:

#### 2.1.1 Scrub Scheduler
**Responsibility**: Automated periodic scrub initiation

- Background task running every 10 minutes (configurable)
- Evaluates all PGs to determine scrub eligibility based on:
  - Time since last scrub (shallow and deep tracked separately)
  - PG state (skip if in recovery, already scrubbing, etc.)
  - Resource availability (concurrent scrub limits)
- Selects PGs using priority algorithm (urgent PGs past max_interval first, then oldest)
- Applies randomization (±50% jitter) to prevent thundering herd

#### 2.1.2 Scrub Engine
**Responsibility**: Scrub execution and coordination

- Runs on PG leader replica only (leadership changes abort in-flight scrubs)
- Orchestrates scrub workflow:
  1. Set PG SCRUBBING state flag atomically
  2. Query all replicas (including self) for blob indexes via nuraft_messenger
  3. Compare responses to detect inconsistencies
  4. Execute spot-check phase for ambiguous cases
  5. Generate inconsistency report
  6. Update PG scrub metadata (timestamps, state)
- Manages batch processing with configurable sleep intervals
- Handles follower failures gracefully (timeout, partial results)

#### 2.1.3 Scrub Request Handler
**Responsibility**: Respond to scrub requests from leader

- Registered on all replicas (leader + followers)
- Receives requests via nuraft_messenger data service
- Dispatches to HomeObject reactor for thread-safe index access
- Queries local index table for requested blob_id range
- Returns blob list (shallow) or blob checksums (deep)
- Enforces local concurrency limits (reject if overloaded)

#### 2.1.4 Inconsistency Reporter
**Responsibility**: Record and expose scrub findings

- Stores inconsistency details in scrub task state (persisted metablk)
- Updates PG scrub metadata (sets INCONSISTENT flag if issues found)
- Provides HTTP API for querying:
  - Active/completed task status
  - Inconsistency details per blob
  - Historical scrub results (retention: 100 tasks or 7 days)
- Emits metrics for monitoring (scrubs completed, inconsistencies found, etc.)

### 2.2 Communication Architecture

#### 2.2.1 Why nuraft_messenger Data Service?

Three communication options were evaluated:

1. **HTTP RPC**: Infeasible due to lack of service discovery (peers known by UUID, not IP:port)
2. **Raft Log Extension**: Incompatible (Raft is unidirectional leader→followers, no response channel)
3. **nuraft_messenger Data Service**: ✅ Selected

The nuraft_messenger library (`/Users/xiaoxchen/Code/nuraft_mesg`) provides a data service layer separate from Raft log replication:

- **Bidirectional RPC**: Request/response pattern via `data_service_request_bidirectional()`
- **UUID-based Addressing**: Uses `peer_id_t` directly, no network discovery needed
- **Already Integrated**: HomeStore uses this for PUSH_DATA and FETCH_DATA operations
- **Production-Proven**: Handles 4MB+ payloads, tested in HomeStore replication

#### 2.2.2 Data Flow Pattern: Follower → Leader

The leader initiates scrub but followers send their data to the leader for comparison:

```
┌────────────────────────────────────────────────────────────┐
│ SCRUB EXECUTION FLOW                                       │
└────────────────────────────────────────────────────────────┘

Step 1: Leader initiates scrub
  Leader: for each peer_id in pg.get_replication_status().members:
            future = msg_svc.data_service_request_bidirectional(
                       peer_id, "scrub_get_index", request_blob)
            futures.push_back(future)

Step 2: Each replica processes independently
  Replica X Handler:
    - Receive ScrubRequest{pg_id, shard_id, blob_range}
    - Dispatch to reactor thread
    - Query local index: query_blobs_in_shard(...)
    - Return ScrubResponse{blob_ids or checksums}

Step 3: Leader collects and compares
  Leader: auto results = folly::collectAll(futures).get(timeout)
          for each blob_id:
            if replicas disagree on existence/checksum:
              record inconsistency

Step 4: Spot-check ambiguous cases
  Leader: batch questionable blob_ids
          repeat steps 1-3 with specific blob queries
          apply majority logic or flag for admin review
```

**Rationale for Follower → Leader**:
- **Independent Pace**: Slow followers don't block fast ones
- **Simpler State Management**: Leader doesn't track per-follower progress
- **Centralized Comparison**: Leader has complete view for majority voting
- **Graceful Degradation**: Can timeout unavailable followers, proceed with partial results

### 2.3 Data Model

#### 2.3.1 Index Structure

HomeObject uses a B+tree index with the following structure:

```cpp
struct BlobRouteKey {
  shard_id_t shard_id;  // 8 bytes
  blob_id_t blob_id;    // 8 bytes (monotonically increasing per shard)
};

struct BlobRouteValue {
  MultiBlkId pbas;      // Physical block addresses (variable, can differ across replicas)
};
```

**Key Properties**:
- `blob_id` is monotonically increasing within each shard
- Index naturally supports range queries: `[shard_id, start_blob_id]` to `[shard_id, end_blob_id]`
- B+tree node size: 4KB, ~100 entries per node

#### 2.3.2 Blob Header Structure

Each blob's data includes a header with checksum:

```cpp
struct BlobHeader {
  enum class HashAlgorithm : uint8_t {
    NONE = 0, CRC32 = 1, MD5 = 2, SHA1 = 3
  };

  HashAlgorithm hash_algorithm;
  uint8_t hash[32];           // Checksum stored at write time
  shard_id_t shard_id;
  blob_id_t blob_id;
  uint32_t blob_size;
  // ... other metadata
};

enum class BlobState : uint8_t {
  ALIVE = 0,       // Normal blob
  TOMBSTONE = 1,   // Deleted blob (pending GC)
};
```

**Deep Scrub Usage**:
- Read blob header + data from disk
- Recompute checksum using specified algorithm
- Compare against stored `hash` field

#### 2.3.3 PG Scrub Metadata

Stored inline in PG superblock:

```cpp
struct pg_scrub_metadata {
  uint64_t last_scrub_time;         // Unix timestamp (seconds)
  uint64_t last_deep_scrub_time;    // Unix timestamp (seconds)
  ScrubState state;                 // CLEAN | INCONSISTENT
  std::optional<task_id_t> active_task_id;  // Non-zero if scrub running
};
```

**Version Compatibility**: New fields added as optional members, default to zero on read from older versions.

#### 2.3.4 Scrub Task State

Stored as separate metablk (one per task):

```cpp
struct scrub_task_state {
  task_id_t task_id;
  pg_id_t pg_id;
  ScrubType type;                   // SHALLOW | DEEP
  uint64_t start_time;

  // Progress (for resume)
  shard_id_t current_shard;
  blob_id_t current_blob_id;
  uint64_t max_blob_id;             // Upper bound captured at start

  // Findings
  std::vector<InconsistencyRecord> inconsistencies;
  uint64_t blobs_scanned;

  // Status
  TaskStatus status;                // RUNNING | COMPLETED | FAILED | ABORTED
};
```

**Retention Policy**: Keep 100 most recent tasks OR tasks from last 7 days (whichever is more).

### 2.4 State Management

#### 2.4.1 PG State Flags

Scrubbing uses existing `PGStateMask` flags defined in `pg_manager.hpp`:

```cpp
ENUM(PGStateMask, uint32_t,
  SCRUBBING = 0x2,          // Scrub in progress
  BASELINE_RESYNC = 0x4,    // Recovery in progress
  INCONSISTENT = 0x8,       // Inconsistency detected
  REPAIR = 0x10,            // Repair in progress (future)
  // ... other flags
);
```

**State Transitions**:
```
HEALTHY → SCRUBBING: Atomically set when scrub starts (fetch_or)
SCRUBBING → HEALTHY: Clear when scrub completes with no issues
SCRUBBING → (INCONSISTENT | HEALTHY): Set INCONSISTENT if issues found
INCONSISTENT → HEALTHY: Auto-clear on next successful scrub
```

#### 2.4.2 Concurrent Scrub Prevention

Race condition prevention using atomic test-and-set:

```cpp
bool try_start_scrub(pg_id_t pg_id) {
  auto pg = _pg_map.at(pg_id);
  auto old_state = pg->state.fetch_or(
    static_cast<uint64_t>(PGStateMask::SCRUBBING),
    std::memory_order_acquire
  );

  if (old_state & static_cast<uint64_t>(PGStateMask::SCRUBBING)) {
    return false;  // Already scrubbing
  }
  return true;  // Successfully acquired
}
```

This ensures only one scrub per PG at a time across all code paths (manual, auto-scheduled, retry).

**Thread Safety Verification**: All PG state modifications throughout the HomeObject codebase use atomic operations via the `pg_state` struct (defined in `pg_manager.hpp:93-113`). The struct provides `set_state()` (fetch_or), `clear_state()` (fetch_and), and `is_state_set()` (load) methods that wrap atomic operations, ensuring no read-modify-write races can occur.

#### 2.4.3 Instance-Level Resource Limits

Concurrency enforced at two levels:

```cpp
// Global instance-level limits
std::atomic<uint32_t> active_scrub_count_{0};
std::atomic<uint32_t> active_deep_scrub_count_{0};

uint32_t max_concurrent_scrubs_ = 3;        // Total (shallow + deep)
uint32_t max_concurrent_deep_scrubs_ = 1;   // Deep subset
```

**Enforcement Points**:
- **Leader side**: Before initiating scrub (scheduler + manual HTTP)
- **Follower side**: In scrub request handler (reject with RESOURCE_EXHAUSTED if at capacity)

**Rationale for instance-level**: All PGs share disk I/O, CPU, memory, network - per-PG limits don't reduce load.

### 2.5 Thread Safety

#### 2.5.1 Handler Execution Context

nuraft_messenger handlers run in the messaging service thread, NOT the HomeObject reactor thread. Direct access to index_table from handler would be unsafe.

**Solution**: Dispatch to reactor thread using iomanager pattern (verified in HomeStore `FETCH_DATA` handler):

```cpp
void on_scrub_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  // Running in nuraft_mesg service thread

  auto request = decode_scrub_request(rpc_data->request_blob());

  // Dispatch to reactor for safe index access
  iomanager.run_on(iomgr::reactor_regex::random_worker,
    [this, request, rpc_data]() {
      // Now safe to access index_table
      auto result = query_blobs_in_shard(request.pg_id, request.shard_id,
                                         request.start_blob_id, request.batch_size);
      auto response_blob = encode_scrub_response(result);
      rpc_data->send_response(response_blob);

      release_scrub_slot(request.type);
    });
}
```

#### 2.5.2 Index Table Thread Safety

HomeStore `IndexTable` read operations use atomic reference counting for concurrency safety (verified in `index_table.hpp:193-198`). Reads are non-blocking and safe from multiple threads, but scrub still dispatches to reactor for consistency with other HomeObject operations.

### 2.6 Leadership Change Handling

Scrubbing runs exclusively on the PG leader replica. Leadership changes are handled through periodic checks and graceful abort:

**Detection**: Check `is_leader()` before each batch in `can_continue_scrub()`
- Detection latency: ~2ms (shallow) / ~110ms (deep)
- Follows existing HomeObject pattern (same as `put_blob`, `delete_blob`)

**Abort**: Clear SCRUBBING flag, release concurrency slots, mark task as ABORTED

**Recovery**: New leader force-clears stuck SCRUBBING flags left by partitioned old leaders

See Section 3.1.4 for implementation details.

### 2.7 Workflow Overview

#### 2.7.1 Shallow Scrub Workflow

**Flow Diagram**:

```
┌─────────────────────────────────────────────────────────────┐
│                    SHALLOW SCRUB WORKFLOW                    │
└─────────────────────────────────────────────────────────────┘

    [START]
       ↓
   ┌───────────────────────┐
   │ Scheduler selects PG  │
   └───────────┬───────────┘
               ↓
        ┌──────────┐   NO
        │is_leader?├────────→ [ABORT: Not leader]
        └────┬─────┘
             │ YES
             ↓
   ┌─────────────────────┐
   │ try_start_scrub()   │
   │ (atomic SCRUBBING)  │
   └─────────┬───────────┘
             ↓
   ┌──────────────────────┐
   │ Capture max_blob_id  │
   │ for all shards (PG)  │
   └─────────┬────────────┘
             ↓
   ┌──────────────────────┐
   │ For each shard:      │
   │                      │
   │  ┌──────────────────────────────┐
   │  │ For batch [id, id+500):      │
   │  │                              │
   │  │  ┌─────────────────────┐    │
   │  │  │ can_continue_scrub()?├─NO─→ [ABORT: Leadership lost]
   │  │  └──────┬──────────────┘    │
   │  │         │ YES                │
   │  │         ↓                    │
   │  │  ┌──────────────────────┐   │
   │  │  │ Send ScrubRequest to │   │
   │  │  │ all replicas (RPC)   │   │
   │  │  └──────┬───────────────┘   │
   │  │         ↓                    │
   │  │  ┌──────────────────────┐   │
   │  │  │ Collect responses    │   │
   │  │  │ (timeout: 5 sec)     │   │
   │  │  └──────┬───────────────┘   │
   │  │         ↓                    │
   │  │  ┌──────────────────────┐   │
   │  │  │ Compare blob_id sets │   │
   │  │  │ Record discrepancies │   │
   │  │  └──────┬───────────────┘   │
   │  │         ↓                    │
   │  │  Next batch                  │
   │  │                              │
   │  └──────────────────────────────┘
   │  Next shard                     │
   │                                 │
   └─────────────┬───────────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Spot-Check Phase:        │
   │  - Wait 100ms (lag)      │
   │  - Query specific blobs  │
   │  - Filter transient diffs│
   └─────────────┬────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Update PG metadata:      │
   │  - last_scrub_time       │
   │  - state (CLEAN/INCONS)  │
   └─────────────┬────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Clear SCRUBBING flag     │
   │ Persist task state       │
   └─────────────┬────────────┘
                 ↓
              [END]

Duration: ~40 sec for 10M blobs (20K batches × 2ms)
```

**Detailed Steps**:
1. Scheduler selects PG for shallow scrub
2. Check if leader: `if (!repl_dev->is_leader()) return`
3. `try_start_scrub(PG_X)` → atomically set SCRUBBING flag
4. **Capture max_blob_id for all shards upfront** (PG-level snapshot)
5. For each shard in PG:
   - For each batch `[start_id, start_id+500)` up to max_blob_id:
     - Check `can_continue_scrub()` (includes leadership check)
     - Send ScrubRequest to all replicas
     - Collect responses (timeout: 5 sec)
     - Compare blob_id sets across replicas
     - Record discrepancies
     - Sleep 0μs (no throttle for shallow)
6. Batch all discrepancies from step 5
7. Execute spot-check:
   - Wait 100ms for replication lag to settle
   - Query specific blob_ids from all replicas
   - Filter transient differences
   - Flag persistent differences as INCONSISTENT
8. Update pg_scrub_metadata:
   - `last_scrub_time = now`
   - `state = CLEAN` or `INCONSISTENT`
9. Clear SCRUBBING flag
10. Persist task state to metablk

#### 2.7.2 Deep Scrub Workflow

**Flow Diagram**:

```
┌─────────────────────────────────────────────────────────────┐
│                     DEEP SCRUB WORKFLOW                      │
└─────────────────────────────────────────────────────────────┘

    [START]
       ↓
   ┌───────────────────────┐
   │ Scheduler selects PG  │
   └───────────┬───────────┘
               ↓
        ┌──────────┐   NO
        │is_leader?├────────→ [ABORT: Not leader]
        └────┬─────┘
             │ YES
             ↓
   ┌─────────────────────┐
   │ try_start_scrub()   │
   │ (atomic SCRUBBING)  │
   └─────────┬───────────┘
             ↓
   ┌──────────────────────┐
   │ Capture max_blob_id  │
   │ for all shards (PG)  │
   └─────────┬────────────┘
             ↓
   ┌────────────────────────────────────┐
   │ For each shard:                    │
   │                                    │
   │  ┌──────────────────────────────┐  │
   │  │ For batch [id, id+25):       │  │
   │  │                              │  │
   │  │  ┌─────────────────────┐    │  │
   │  │  │ can_continue_scrub()?├─NO─→ [ABORT: Leadership lost]
   │  │  └──────┬──────────────┘    │  │
   │  │         │ YES                │  │
   │  │         ↓                    │  │
   │  │  ┌──────────────────────┐   │  │
   │  │  │ Send DeepScrubRequest│   │  │
   │  │  │ to all replicas (RPC)│   │  │
   │  │  └──────┬───────────────┘   │  │
   │  │         ↓                    │  │
   │  │  ┌──────────────────────┐   │  │
   │  │  │ Each replica:        │   │  │
   │  │  │  - Read blob + data  │   │  │
   │  │  │  - Compute checksum  │   │  │
   │  │  │  - Return checksum   │   │  │
   │  │  └──────┬───────────────┘   │  │
   │  │         ↓                    │  │
   │  │  ┌──────────────────────┐   │  │
   │  │  │ Collect responses    │   │  │
   │  │  │ (timeout: 60 sec)    │   │  │
   │  │  └──────┬───────────────┘   │  │
   │  │         ↓                    │  │
   │  │  ┌──────────────────────┐   │  │
   │  │  │ Compare checksums    │   │  │
   │  │  │ Record mismatches    │   │  │
   │  │  └──────┬───────────────┘   │  │
   │  │         ↓                    │  │
   │  │  ┌──────────────────────┐   │  │
   │  │  │ Sleep 10ms (throttle)│   │  │
   │  │  └──────┬───────────────┘   │  │
   │  │         ↓                    │  │
   │  │  Next batch                  │  │
   │  │                              │  │
   │  └──────────────────────────────┘  │
   │         ↓                          │
   │  ┌──────────────────────┐          │
   │  │ Checkpoint progress: │          │
   │  │ - current_shard      │          │
   │  │ - current_blob_id    │          │
   │  │ (persist to metablk) │          │
   │  └──────┬───────────────┘          │
   │         ↓                          │
   │  Next shard                        │
   │                                    │
   └─────────────┬──────────────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Spot-Check (if needed):  │
   │  - Wait 100ms (lag)      │
   │  - Query specific blobs  │
   │  - Filter transient diffs│
   └─────────────┬────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Update PG metadata:      │
   │  - last_deep_scrub_time  │
   │  - state (CLEAN/INCONS)  │
   └─────────────┬────────────┘
                 ↓
   ┌──────────────────────────┐
   │ Clear SCRUBBING flag     │
   │ Persist final task state │
   └─────────────┬────────────┘
                 ↓
              [END]

Duration: ~12 hours for 10M blobs (400K batches × (100ms + 10ms))
Resumable: Yes, from last checkpointed shard
```

**Detailed Steps**:
1. Scheduler selects PG for deep scrub
2. Check if leader: `if (!repl_dev->is_leader()) return`
3. `try_start_scrub(PG_Y)` → atomically set SCRUBBING flag
4. **Capture max_blob_id for all shards upfront** (PG-level snapshot)
5. For each shard in PG:
   - For each batch `[start_id, start_id+25)` up to max_blob_id:
     - Check `can_continue_scrub()` (includes leadership check)
     - Send DeepScrubRequest to all replicas
     - Each replica:
       - Read BlobHeader + data from disk
       - Recompute checksum (SHA256)
       - Return `{blob_id, checksum, blob_size}`
     - Collect responses (timeout: 60 sec)
     - Compare checksums across replicas
     - Record mismatches
     - Sleep 10ms (throttle)
   - **Checkpoint progress**: Update task metablk with `current_shard`, `current_blob_id`
6. Execute spot-check if needed (same as shallow):
   - Wait 100ms for replication lag to settle
   - Query specific blob_ids from all replicas
   - Filter transient differences
   - Flag persistent differences as INCONSISTENT
7. Update pg_scrub_metadata:
   - `last_deep_scrub_time = now`
   - `state = CLEAN` or `INCONSISTENT`
8. Clear SCRUBBING flag
9. Persist final task state

**Key Differences from Shallow Scrub**:
- **Batch Size**: Shallow 500 blobs vs. Deep 25 blobs
- **Verification**: Shallow checks existence, Deep checks data checksums
- **Throttling**: Shallow no sleep, Deep 10ms sleep between batches
- **Checkpointing**: Shallow none (too fast), Deep per-shard (resumable)
- **Timeout**: Shallow 5 sec, Deep 60 sec (disk I/O + checksum computation)

**Leadership Change Handling**: Both workflows check `is_leader()` before each batch. If leadership lost, scrub aborts gracefully after current batch, clears SCRUBBING flag, marks task as ABORTED.

---

## 3. Detailed Design

### 3.1 Resource Control

Resource control ensures scrubbing has minimal impact on normal client I/O operations.

#### 3.1.1 Batch Sizes

Different batch sizes for shallow vs. deep scrub based on operation characteristics:

```cpp
uint32_t scrub_shallow_batch_size = 500;   // Index-only reads
uint32_t scrub_deep_batch_size = 25;       // Full data reads + checksum
```

**Shallow Scrub (500 blobs)**:
- Only passes blob_ids (8 bytes each) for existence check
- Index reads are non-blocking (no lock contention with client I/O)
- 500 blobs ≈ 5 B+tree leaf nodes (100 entries per 4KB node)
- Response payload: ~4KB per replica (500 × 8 bytes)
- Optimize for throughput over lock duration

**Deep Scrub (25 blobs)**:
- Full blob data read from disk + checksum computation
- Aligned with Ceph production (`osd_scrub_chunk_max = 25`)
- I/O bounded: disk reads dominate
- CPU bounded: SHA256 computation
- Memory bounded: depends on actual blob sizes
- Proven batch size for data verification workloads

See Appendix A.1 for B+tree capacity calculations.

#### 3.1.2 Sleep Between Batches

Configurable delay between batches to yield I/O bandwidth:

```cpp
uint64_t scrub_sleep_shallow_us = 0;        // No sleep (fast, non-blocking reads)
uint64_t scrub_sleep_deep_us = 10000;       // 10ms (10,000 microseconds)
```

**Unit**: Microseconds for fine-grained control
**Maximum**: No hard limit (trust admin, similar to Ceph's `osd_scrub_sleep`)
**Granularity**: Global config (not per-PG)

**Application**:
```cpp
void scrub_batch(ScrubType type, ...) {
  // ... execute batch ...

  uint64_t sleep_us = (type == SHALLOW) ?
    scrub_sleep_shallow_us : scrub_sleep_deep_us;
  if (sleep_us > 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
  }
}
```

#### 3.1.3 Concurrent Scrub Limits

Instance-level concurrency control:

```cpp
uint32_t max_concurrent_scrubs = 3;        // Total scrubs (shallow + deep)
uint32_t max_concurrent_deep_scrubs = 1;   // Deep scrub limit (subset of total)
```

**Enforcement on Leader (Initiator)**:
```cpp
bool try_reserve_scrub_slot(ScrubType type) {
  // Atomically check and increment total scrub count
  uint32_t current = active_scrub_count_.load(std::memory_order_relaxed);
  while (current < max_concurrent_scrubs_) {
    if (active_scrub_count_.compare_exchange_weak(
          current, current + 1,
          std::memory_order_acquire,
          std::memory_order_relaxed)) {
      // Successfully reserved total slot

      // Now check deep limit if needed
      if (type == DEEP) {
        uint32_t deep_current = active_deep_scrub_count_.load(std::memory_order_relaxed);
        while (deep_current < max_concurrent_deep_scrubs_) {
          if (active_deep_scrub_count_.compare_exchange_weak(
                deep_current, deep_current + 1,
                std::memory_order_acquire,
                std::memory_order_relaxed)) {
            return true;  // Successfully reserved both slots
          }
        }

        // Failed to reserve deep slot, release total slot
        active_scrub_count_.fetch_sub(1, std::memory_order_release);
        return false;
      }

      return true;  // Shallow scrub, only needed total slot
    }
  }

  return false;  // At limit, cannot reserve
}
```

**Enforcement on Follower (Handler)**:
```cpp
void on_scrub_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  auto request = decode_scrub_request(rpc_data->request_blob());

  // Check concurrent limit BEFORE processing
  if (!try_reserve_scrub_slot(request.type)) {
    auto error = encode_error("RESOURCE_EXHAUSTED");
    rpc_data->send_response(error);
    return;
  }

  // Dispatch to reactor...
}
```

**Why Instance-Level**: All PGs on a HomeObject instance share physical resources (disk I/O, CPU, memory, network). Per-PG limits provide no meaningful load control.

**Why compare_exchange Loop**: The original check-then-increment pattern had a race condition:
```cpp
// ❌ UNSAFE: Race window between load and fetch_add
if (active_scrub_count_.load() >= max_concurrent_scrubs_) return false;
active_scrub_count_.fetch_add(1);  // Can exceed limit
```

Using `compare_exchange_weak` ensures atomic check-and-increment, strictly enforcing the concurrency limit. The `weak` variant is preferred in loops for better performance on some architectures (may spuriously fail, but loop retries).

#### 3.1.4 Manual Control: NO_SCRUB Flags

Ephemeral flags for operational control:

```cpp
std::atomic<bool> no_scrub_{false};           // Block all scrubbing
std::atomic<bool> no_deep_scrub_{false};      // Block deep scrub only
```

**Checked Before and During Scrub**:
```cpp
bool can_continue_scrub(pg_id_t pg_id, ScrubType type) {
  // Check global flags
  if (no_scrub_.load()) return false;
  if (type == DEEP && no_deep_scrub_.load()) return false;

  // Check PG state
  auto pg = get_pg(pg_id);
  auto state = pg->state.load();

  // Don't scrub if recovering
  if (state & PGStateMask::BASELINE_RESYNC) return false;

  // Check if SCRUBBING flag still set (could be cleared externally)
  if (!(state & PGStateMask::SCRUBBING)) return false;

  // Check leadership (abort if lost leadership mid-scrub)
  auto repl_dev = pg->repl_dev_;
  if (!repl_dev->is_leader()) {
    LOGW("Leadership lost during scrub for pg={}", pg_id);
    return false;
  }

  return true;
}
```

**Behavior**:
- Checked before starting new scrubs
- Checked between batches (pauses ongoing scrubs gracefully)
- Ephemeral: reset on pod restart (conservative default)

See Section 4.1 for HTTP API endpoints.

#### 3.1.5 Block During Recovery

Scrubbing is blocked if PG is in recovery:

```cpp
if (pg->state.load() & PGStateMask::BASELINE_RESYNC) {
  // Don't start scrub during recovery
  return;
}
```

**Mid-Scrub Recovery Handling**:
- Check `can_continue_scrub()` between batches
- If recovery starts mid-scrub: finish current batch, then exit gracefully
- Update task status to ABORTED
- Reschedule based on normal intervals

#### 3.1.6 Timeouts

Per-batch timeouts for replica responses:

```cpp
uint64_t scrub_shallow_timeout_sec = 5;    // Index query + network
uint64_t scrub_deep_timeout_sec = 60;      // Data read + checksum + network
uint64_t scrub_spot_check_timeout_sec = 10; // Retry mechanism
```

**Application**:
```cpp
auto results = folly::collectAll(futures)
  .get(std::chrono::seconds(timeout));

for (auto& result : results) {
  if (result.hasException()) {
    // Replica timeout or failure - record for admin review
    log_replica_failure(peer_id, result.exception());
    continue;
  }
  // Process response...
}
```

**Partial Results**: Leader can proceed with available replicas, flag incomplete scrub in task state.

#### 3.1.7 Deferred to Post-MVP

**Load-Based Throttling**:
- Option A: System load monitoring (Ceph: `osd_scrub_load_threshold`)
- Option B: Latency-based throttling (monitor batch latency, adjust sleep dynamically)
- **Recommendation**: Latency-based preferred (more direct signal than system load)

**Time Windows**:
- Ceph: `osd_scrub_begin_hour`, `osd_scrub_end_hour`
- MVP: Run 24/7, rely on small batches + sleep for minimal impact

**Preemption/Pause**:
- MVP: No support (finish current batch on abort)
- Post-MVP: Checkpointing for both shallow and deep

### 3.2 Scheduling & Initiation

#### 3.2.1 Trigger Mechanisms

**Manual Trigger** (Section 4.1):
- HTTP API: `POST /scrub?pg_id=X&deep=true`
- Immediate execution (subject to resource limits)
- Returns `task_id` for progress tracking

**Auto-Scheduled Trigger**:
- Background scheduler task every 10 minutes (configurable)
- Evaluates all PGs, selects candidates
- Respects resource limits and randomization

#### 3.2.2 Scheduling Intervals

```cpp
// Shallow scrub
uint64_t scrub_min_interval_sec = 86400;          // 1 day
uint64_t scrub_max_interval_sec = 604800;         // 7 days (hard deadline)

// Deep scrub
uint64_t scrub_deep_min_interval_sec = 604800;    // 7 days
uint64_t scrub_deep_max_interval_sec = 2592000;   // 30 days (hard deadline)

// Randomization (avoid thundering herd)
float scrub_interval_randomize_ratio = 0.5;       // ±50% jitter

// Scheduler check frequency
uint64_t scrub_scheduler_interval_sec = 600;      // 10 minutes
```

**Scheduling Logic**:
```cpp
bool should_schedule_scrub(ScrubInfo info, ScrubType type, uint64_t now) {
  uint64_t last_time = (type == SHALLOW) ?
    info.last_scrub_time : info.last_deep_scrub_time;
  uint64_t max_interval = (type == SHALLOW) ?
    scrub_max_interval_sec : scrub_deep_max_interval_sec;
  uint64_t min_interval = (type == SHALLOW) ?
    scrub_min_interval_sec : scrub_deep_min_interval_sec;

  uint64_t time_since = now - last_time;

  // Hard deadline exceeded - MUST scrub
  if (time_since > max_interval) {
    return true;
  }

  // Calculate randomized target time
  float jitter = random_float(-scrub_interval_randomize_ratio,
                               scrub_interval_randomize_ratio);
  uint64_t target_interval = min_interval * (1.0 + jitter);

  // Scrub if past randomized target
  return time_since > target_interval;
}
```

**Randomization Example**:
- `min_interval = 1 day`, `randomize_ratio = 0.5`
- Jitter: ±50% of 1 day = ±12 hours
- Actual schedule: between 12 hours and 36 hours
- Prevents thundering herd when all PGs created simultaneously

#### 3.2.3 PG Selection Algorithm

Priority: urgent PGs (past max_interval) first, then oldest scrub time.

```cpp
std::vector<pg_id_t> select_pgs_for_scrub(ScrubType type) {
  std::vector<std::pair<pg_id_t, ScrubInfo>> candidates;
  uint64_t now = get_current_time();

  // Collect PGs that need scrubbing
  for (auto& [pg_id, info] : scrub_info_map_) {
    if (!can_continue_scrub(pg_id, type)) continue;
    if (should_schedule_scrub(info, type, now)) {
      candidates.push_back({pg_id, info});
    }
  }

  // Sort by: 1) Past max_interval (urgent), 2) Oldest scrub time
  std::sort(candidates.begin(), candidates.end(),
    [type, now](auto& a, auto& b) {
      uint64_t max_interval = (type == SHALLOW) ?
        scrub_max_interval_sec : scrub_deep_max_interval_sec;

      uint64_t time_a = (type == SHALLOW) ?
        a.second.last_scrub_time : a.second.last_deep_scrub_time;
      uint64_t time_b = (type == SHALLOW) ?
        b.second.last_scrub_time : b.second.last_deep_scrub_time;

      bool urgent_a = (now - time_a) > max_interval;
      bool urgent_b = (now - time_b) > max_interval;

      // Urgent PGs first
      if (urgent_a != urgent_b) return urgent_a;

      // Otherwise oldest first
      return time_a < time_b;
    });

  std::vector<pg_id_t> result;
  for (auto& [pg_id, _] : candidates) {
    result.push_back(pg_id);
  }
  return result;
}
```

**No Priority Boost for INCONSISTENT PGs**: They follow normal scheduling. Admin uses manual trigger if urgent re-verification needed.

#### 3.2.4 Scheduler Background Task

```cpp
void scrub_scheduler_task() {
  uint64_t now = get_current_time();

  // Select PGs needing shallow scrub
  auto shallow_pgs = select_pgs_for_scrub(SHALLOW);
  for (auto pg_id : shallow_pgs) {
    if (!try_reserve_scrub_slot(SHALLOW)) break;
    schedule_pg_scrub(pg_id, SHALLOW);
  }

  // Select PGs needing deep scrub
  auto deep_pgs = select_pgs_for_scrub(DEEP);
  for (auto pg_id : deep_pgs) {
    if (!try_reserve_scrub_slot(DEEP)) break;
    schedule_pg_scrub(pg_id, DEEP);
  }
}
```

**Registration**:
```cpp
iomanager.run_on_wait_async(
  iomgr::reactor_regex::random_worker,
  [this]() { scrub_scheduler_task(); },
  std::chrono::seconds(scrub_scheduler_interval_sec)
);
```

**Rationale for 10-min interval**: Very cheap operation (timestamp comparisons), provides responsive scheduling without overhead.

### 3.3 Inconsistency Detection

#### 3.3.1 Two-Phase Approach

**Phase 1: Initial Batch Scrub**

For each batch `[start_blob_id, end_blob_id)`:
1. Leader sends `ScrubRequest{pg_id, shard_id, blob_range}` to all replicas
2. Each replica returns list of ALIVE blob_ids (filter tombstones)
3. Leader compares blob_id sets:
   - Missing blob: replica A has X, replica B doesn't
   - Extra blob: replica B has Y, replica A doesn't
   - Checksum mismatch (deep only): both have Z, different checksums
4. Collect all discrepancies (don't classify yet)

**Phase 2: Spot Check**

After scanning entire PG:
1. Batch all questionable blob_ids
2. **Wait for replication lag to settle** (`scrub_spot_check_delay_ms` = 100ms default)
3. Query ALL replicas for these specific blobs (not just ones with differences)
4. Request includes blob state (ALIVE, TOMBSTONE, NOT_FOUND)

```cpp
SpotCheckRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  std::vector<blob_id_t> blob_ids;  // Questionable blobs
};

SpotCheckResponse {
  std::vector<BlobStatus> blobs;
  // BlobStatus: {blob_id, state: ALIVE|TOMBSTONE|NOT_FOUND}
};
```

5. Filter transient replication lag (blob appeared/disappeared since initial scan)
6. Persistent differences → flag as INCONSISTENT

**Rationale**:
- Simple logic, no complex confidence heuristics
- 100ms delay allows most transient replication lag to resolve (typical lag: 10-50ms)
- Natural lag filtering reduces false positives
- Retry with backoff possible if needed

**Delay Tuning**:
- Too short (< 50ms): High false positive rate (lag not fully resolved)
- Too long (> 500ms): Unnecessarily slows down scrub completion
- Default 100ms balances speed vs accuracy for typical Raft replication latencies

#### 3.3.2 Inconsistency Classification

**Detected Issues**:
1. **Missing Blob**: Some replicas have blob X, others don't
2. **Checksum Mismatch**: All replicas have blob Y, but checksums differ
3. **State Mismatch**: Some replicas mark blob Z as TOMBSTONE, others as ALIVE

**Not Detected** (Known Limitation):
- Post-GC tombstone removal creates ambiguity:
  - Replica A: blob deleted long ago, tombstone GC'd (NOT_FOUND)
  - Replica B: blob still marked TOMBSTONE (not yet GC'd)
  - Appears as inconsistency, but may be valid eventual consistency

#### 3.3.3 Inconsistency Reporting

**InconsistencyRecord Structure**:
```cpp
struct InconsistencyRecord {
  shard_id_t shard_id;
  blob_id_t blob_id;
  InconsistencyType type;  // MISSING | CHECKSUM_MISMATCH | STATE_MISMATCH

  // Per-replica details
  std::map<peer_id_t, ReplicaStatus> replica_states;
  // ReplicaStatus: {exists: bool, checksum: optional<hash>, state: BlobState}
};
```

**Storage**: In `scrub_task_state.inconsistencies` vector, persisted to metablk.

**PG Flag**: Set `PGStateMask::INCONSISTENT` if any inconsistencies found.

**Auto-Clear**: Next successful scrub clears INCONSISTENT flag (self-healing).

### 3.4 Persistence & Recovery

#### 3.4.1 Two-Level Persistence

**Level 1: PG Scrub Metadata** (inline in PG superblock):
```cpp
struct pg_scrub_metadata {
  uint64_t last_scrub_time;         // Unix timestamp
  uint64_t last_deep_scrub_time;    // Unix timestamp
  ScrubState state;                 // CLEAN | INCONSISTENT
  std::optional<task_id_t> active_task_id;
};
```

- **Purpose**: Scheduling decisions, PG state
- **Lifetime**: Permanent (tied to PG lifecycle)
- **Updates**: After each scrub completion

**Level 2: Scrub Task State** (separate metablk per task):
```cpp
struct scrub_task_state {
  task_id_t task_id;
  pg_id_t pg_id;
  ScrubType type;
  uint64_t start_time;

  // Progress tracking
  shard_id_t current_shard;
  blob_id_t current_blob_id;
  uint64_t max_blob_id;

  // Findings
  std::vector<InconsistencyRecord> inconsistencies;
  uint64_t blobs_scanned;

  TaskStatus status;  // RUNNING | COMPLETED | FAILED | ABORTED
};
```

- **Purpose**: Task tracking, detailed findings, resume capability
- **Lifetime**: Retention policy (100 tasks OR 7 days)
- **Updates**: Checkpoint after each shard (deep), final update on completion

#### 3.4.2 Restart Behavior

**Shallow Scrub**:
- Abandon in-progress scrubs (mark as ABORTED)
- Too fast to benefit from resume (~40 sec for 10M blobs)
- Reschedule based on `last_scrub_time` from PG metadata

**Deep Scrub**:
- Resume from last checkpointed shard
- Read `current_shard` and `current_blob_id` from task metablk
- Continue from next shard (discard partial current shard)
- Rationale: Deep scrub is slow (~12 hours for 10M blobs), resume saves significant time

**Checkpoint Frequency**:
- Deep scrub: After each shard completes
- Update `current_shard`, `current_blob_id` in task metablk
- Atomic write ensures crash consistency

#### 3.4.3 Task Retention Policy

```cpp
uint32_t scrub_task_retention_count = 100;   // Keep last N tasks
uint64_t scrub_task_retention_days = 7;      // Keep tasks from last N days
```

**Logic**: Retain task if it meets **EITHER** condition.

**Cleanup**: Periodic background task removes old task metablks.

**Example**:
- Keep 100 most recent tasks (even if older than 7 days)
- Keep all tasks from last 7 days (even if more than 100)

### 3.5 Message Format

#### 3.5.1 Shallow Scrub Messages

**Request**:
```cpp
struct ScrubRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  uint32_t batch_size;        // 500 for shallow
  ScrubType type;             // SHALLOW
};
```

**Response**:
```cpp
struct ScrubResponse {
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  std::vector<blob_id_t> blob_ids;  // ALIVE blobs only
};
```

**Payload Size**: ~4KB for 500 blobs (500 × 8 bytes)

#### 3.5.2 Deep Scrub Messages

**Request**:
```cpp
struct DeepScrubRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  uint32_t batch_size;        // 25 for deep
  ScrubType type;             // DEEP
};
```

**Response**:
```cpp
struct DeepScrubResponse {
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  std::vector<BlobChecksum> checksums;

  // BlobChecksum: {blob_id, checksum: uint8_t[32], blob_size, algorithm}
};
```

**Payload Size**: ~2KB for 25 blobs (25 × ~80 bytes)

#### 3.5.3 Spot Check Messages

**Request**:
```cpp
struct SpotCheckRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  std::vector<blob_id_t> blob_ids;  // Specific blobs to verify
};
```

**Response**:
```cpp
struct SpotCheckResponse {
  shard_id_t shard_id;
  std::vector<BlobStatus> blobs;

  // BlobStatus: {blob_id, state: ALIVE|TOMBSTONE|NOT_FOUND,
  //              optional<checksum>, optional<blob_size>}
};
```

**Batch Limit**: 25 blobs per spot check (same cost as deep scrub)

---

## 4. API Specifications

### 4.1 HTTP REST API

#### 4.1.1 Manual Scrub Trigger

**Endpoint**: `POST /scrub`

**Query Parameters**:
- `pg_id` (required): PG identifier
- `deep` (optional): `true` for deep scrub, `false` for shallow (default: `false`)

**Request Body**: None

**Response**:
```json
{
  "status": "success",
  "task_id": "12345",
  "pg_id": 42,
  "type": "shallow",
  "message": "Scrub task initiated"
}
```

**Error Responses**:
- `400 Bad Request`: Invalid pg_id
- `404 Not Found`: PG does not exist
- `409 Conflict`: PG already scrubbing
- `503 Service Unavailable`: Resource limits exceeded (max concurrent scrubs)

**Example**:
```bash
curl -X POST "http://localhost:8080/scrub?pg_id=42&deep=true"
```

#### 4.1.2 Query Task Status

**Endpoint**: `GET /scrub/task/{task_id}`

**Path Parameters**:
- `task_id` (required): Task identifier returned from scrub initiation

**Response**:
```json
{
  "task_id": "12345",
  "pg_id": 42,
  "type": "deep",
  "status": "running",
  "progress": {
    "current_shard": 3,
    "total_shards": 10,
    "blobs_scanned": 1500000,
    "percent_complete": 30
  },
  "start_time": "2025-12-13T10:00:00Z",
  "inconsistencies_found": 0
}
```

**Task Status Values**:
- `running`: Scrub in progress
- `completed`: Scrub finished successfully
- `failed`: Scrub encountered unrecoverable error
- `aborted`: Scrub canceled (recovery started, pod restarted, etc.)

**Error Responses**:
- `404 Not Found`: Task ID does not exist or expired (retention policy)

**Example**:
```bash
curl "http://localhost:8080/scrub/task/12345"
```

#### 4.1.3 Query Inconsistency Details

**Endpoint**: `GET /scrub/task/{task_id}/inconsistencies`

**Response**:
```json
{
  "task_id": "12345",
  "pg_id": 42,
  "total_inconsistencies": 2,
  "inconsistencies": [
    {
      "shard_id": 5,
      "blob_id": 10023,
      "type": "checksum_mismatch",
      "replicas": {
        "replica-uuid-1": {
          "checksum": "abc123...",
          "blob_size": 4096
        },
        "replica-uuid-2": {
          "checksum": "def456...",
          "blob_size": 4096
        }
      }
    },
    {
      "shard_id": 7,
      "blob_id": 50234,
      "type": "missing_blob",
      "replicas": {
        "replica-uuid-1": {"exists": true},
        "replica-uuid-2": {"exists": false}
      }
    }
  ]
}
```

#### 4.1.4 Disable Scrubbing

**Endpoint**: `POST /scrub/disable`

**Query Parameters**:
- `deep_only` (optional): `true` to disable only deep scrubs, `false` to disable all (default: `false`)

**Response**:
```json
{
  "status": "success",
  "message": "Scrubbing disabled",
  "no_scrub": true,
  "no_deep_scrub": false
}
```

**Effect**:
- Sets `no_scrub_` flag (or `no_deep_scrub_` if `deep_only=true`)
- Blocks new scrub initiation (scheduler and manual)
- Pauses ongoing scrubs between batches

**Example**:
```bash
# Disable all scrubbing
curl -X POST "http://localhost:8080/scrub/disable"

# Disable only deep scrubs
curl -X POST "http://localhost:8080/scrub/disable?deep_only=true"
```

#### 4.1.5 Enable Scrubbing

**Endpoint**: `DELETE /scrub/disable`

**Query Parameters**:
- `deep_only` (optional): `true` to enable only deep scrubs, `false` to enable all (default: `false`)

**Response**:
```json
{
  "status": "success",
  "message": "Scrubbing enabled",
  "no_scrub": false,
  "no_deep_scrub": false
}
```

**Example**:
```bash
# Enable all scrubbing
curl -X DELETE "http://localhost:8080/scrub/disable"
```

#### 4.1.6 Query Scrub Status

**Endpoint**: `GET /scrub/status`

**Response**:
```json
{
  "scrubbing_enabled": true,
  "deep_scrub_enabled": true,
  "active_scrubs": 2,
  "active_deep_scrubs": 1,
  "max_concurrent_scrubs": 3,
  "max_concurrent_deep_scrubs": 1,
  "config": {
    "shallow_batch_size": 500,
    "deep_batch_size": 25,
    "shallow_sleep_us": 0,
    "deep_sleep_us": 10000,
    "shallow_timeout_sec": 5,
    "deep_timeout_sec": 60
  }
}
```

#### 4.1.7 Query PG Scrub Metadata

**Endpoint**: `GET /pg/{pg_id}/scrub`

**Response**:
```json
{
  "pg_id": 42,
  "state": "clean",
  "last_scrub_time": "2025-12-12T10:00:00Z",
  "last_deep_scrub_time": "2025-12-10T08:00:00Z",
  "active_task_id": null,
  "next_scrub_due": "2025-12-13T15:30:00Z",
  "next_deep_scrub_due": "2025-12-17T12:00:00Z"
}
```

**State Values**:
- `clean`: No known inconsistencies
- `inconsistent`: Inconsistencies detected in last scrub
- `scrubbing`: Scrub currently in progress

### 4.2 Internal nuraft_messenger APIs

#### 4.2.1 Handler Registration

During PG creation, register scrub handlers:

```cpp
void HSHomeObject::register_scrub_handlers(pg_id_t pg_id) {
  auto group_id = pg_to_group_id(pg_id);
  auto msg_svc = repl_dev->group_msg_service();

  // Shallow scrub handler
  msg_svc->bind_data_service_request(
    "scrub_get_index",
    group_id,
    std::bind(&HSHomeObject::on_scrub_index_request, this, _1)
  );

  // Deep scrub handler
  msg_svc->bind_data_service_request(
    "scrub_get_checksums",
    group_id,
    std::bind(&HSHomeObject::on_scrub_checksum_request, this, _1)
  );

  // Spot check handler
  msg_svc->bind_data_service_request(
    "scrub_spot_check",
    group_id,
    std::bind(&HSHomeObject::on_scrub_spot_check_request, this, _1)
  );
}
```

#### 4.2.2 Request Dispatch (Leader)

Leader sends scrub request to all replicas:

```cpp
folly::Future<std::vector<ScrubResponse>>
HSHomeObject::dispatch_scrub_request(pg_id_t pg_id, ScrubRequest req) {
  auto repl_dev = get_repl_dev(pg_id);
  auto peers = repl_dev->get_replication_status().members;
  auto msg_svc = repl_dev->group_msg_service();

  std::vector<folly::SemiFuture<GenericClientResponse>> futures;

  for (auto& peer : peers) {
    auto request_blob = encode_scrub_request(req);
    auto future = msg_svc->data_service_request_bidirectional(
      peer.id,
      "scrub_get_index",
      request_blob
    );
    futures.push_back(std::move(future));
  }

  return folly::collectAll(std::move(futures))
    .via(folly::getCPUExecutor())
    .thenValue([this](std::vector<folly::Try<GenericClientResponse>>&& results) {
      std::vector<ScrubResponse> responses;
      for (auto& result : results) {
        if (result.hasValue()) {
          responses.push_back(decode_scrub_response(result.value().response_blob()));
        }
      }
      return responses;
    });
}
```

#### 4.2.3 Request Handler (Follower)

Follower processes scrub request:

```cpp
void HSHomeObject::on_scrub_index_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  // Running in nuraft_mesg service thread

  auto request = decode_scrub_request(rpc_data->request_blob());

  // Check concurrency limits
  if (!try_reserve_scrub_slot(request.type)) {
    auto error = encode_error("RESOURCE_EXHAUSTED");
    rpc_data->send_response(error);
    return;
  }

  // Dispatch to reactor thread for safe index access
  iomanager.run_on(iomgr::reactor_regex::random_worker,
    [this, request, rpc_data]() {
      try {
        // Query index table
        auto result = query_blobs_in_shard(
          request.pg_id,
          request.shard_id,
          request.start_blob_id,
          request.batch_size
        );

        // Filter ALIVE blobs only
        std::vector<blob_id_t> alive_blobs;
        for (auto& blob : result.value()) {
          if (blob.state == BlobState::ALIVE) {
            alive_blobs.push_back(blob.blob_id);
          }
        }

        // Send response
        auto response_blob = encode_scrub_response(alive_blobs);
        rpc_data->send_response(response_blob);

      } catch (std::exception& e) {
        auto error = encode_error(e.what());
        rpc_data->send_response(error);
      }

      // Release concurrency slot
      release_scrub_slot(request.type);
    });
}
```

### 4.3 Configuration Parameters

All configuration parameters with defaults:

```cpp
// Batch sizes
uint32_t scrub_shallow_batch_size = 500;
uint32_t scrub_deep_batch_size = 25;

// Sleep between batches (microseconds)
uint64_t scrub_sleep_shallow_us = 0;
uint64_t scrub_sleep_deep_us = 10000;

// Timeouts (seconds)
uint64_t scrub_shallow_timeout_sec = 5;
uint64_t scrub_deep_timeout_sec = 60;
uint64_t scrub_spot_check_timeout_sec = 10;

// Concurrency limits
uint32_t max_concurrent_scrubs = 3;
uint32_t max_concurrent_deep_scrubs = 1;

// Scheduling intervals (seconds)
uint64_t scrub_min_interval_sec = 86400;           // 1 day
uint64_t scrub_max_interval_sec = 604800;          // 7 days
uint64_t scrub_deep_min_interval_sec = 604800;     // 7 days
uint64_t scrub_deep_max_interval_sec = 2592000;    // 30 days
float scrub_interval_randomize_ratio = 0.5;        // ±50%
uint64_t scrub_scheduler_interval_sec = 600;       // 10 minutes

// Task retention
uint32_t scrub_task_retention_count = 100;
uint64_t scrub_task_retention_days = 7;

// Spot check
uint32_t scrub_spot_check_batch_size = 25;
uint64_t scrub_spot_check_delay_ms = 100;      // Wait before spot check to allow replication lag to settle
```

**Runtime Configurability**: All parameters should be exposed via configuration file and/or environment variables. Changes require restart (no hot-reload in MVP).

---

## 5. Operational Guide

### 5.1 Deployment

#### 5.1.1 Prerequisites

- HomeObject cluster with Raft replication enabled
- nuraft_messenger library integrated
- Sufficient disk I/O headroom for deep scrubs (recommend <70% sustained utilization)

#### 5.1.2 Initial Configuration

Recommended production settings:

```yaml
# config.yaml
scrubber:
  # Conservative defaults for production
  max_concurrent_scrubs: 1              # Start low, increase if impact acceptable
  max_concurrent_deep_scrubs: 1

  # Shallow scrub (daily)
  scrub_min_interval_sec: 86400         # 1 day
  scrub_max_interval_sec: 604800        # 7 days
  shallow_batch_size: 500
  shallow_sleep_us: 0                   # No throttle for shallow

  # Deep scrub (weekly)
  scrub_deep_min_interval_sec: 604800   # 7 days
  scrub_deep_max_interval_sec: 2592000  # 30 days
  deep_batch_size: 25
  deep_sleep_us: 10000                  # 10ms between batches

  # Scheduler
  scheduler_interval_sec: 600           # Check every 10 minutes
  interval_randomize_ratio: 0.5         # ±50% jitter
```

#### 5.1.3 Rollout Strategy

1. **Deploy with scrubbing disabled**:
   ```bash
   curl -X POST "http://localhost:8080/scrub/disable"
   ```

2. **Enable shallow scrubs first** (low impact):
   ```bash
   curl -X DELETE "http://localhost:8080/scrub/disable"
   curl -X POST "http://localhost:8080/scrub/disable?deep_only=true"
   ```

3. **Monitor for 1 week**:
   - Check client latency impact
   - Verify scrub completion rates
   - Adjust `scrub_sleep_shallow_us` if needed

4. **Enable deep scrubs**:
   ```bash
   curl -X DELETE "http://localhost:8080/scrub/disable?deep_only=true"
   ```

5. **Monitor for 1 month**, adjust `deep_sleep_us` based on I/O impact

### 5.2 Monitoring

#### 5.2.1 Key Metrics

**Scrub Activity**:
- `homeobject_scrubs_active{type="shallow|deep"}`: Current active scrubs
- `homeobject_scrubs_completed_total{type="shallow|deep",status="success|failed|aborted"}`: Completed scrub count
- `homeobject_scrubs_duration_seconds{type="shallow|deep"}`: Scrub duration histogram
- `homeobject_scrub_batches_total{type="shallow|deep"}`: Batches processed
- `homeobject_scrub_blobs_scanned_total{type="shallow|deep"}`: Blobs verified

**Inconsistencies**:
- `homeobject_scrub_inconsistencies_found_total{type="missing|checksum_mismatch|state_mismatch"}`: Inconsistency count
- `homeobject_pgs_inconsistent`: Number of PGs in INCONSISTENT state

**Resource Usage**:
- `homeobject_scrub_batch_latency_seconds{type="shallow|deep"}`: Batch processing time
- `homeobject_scrub_replica_timeouts_total{type="shallow|deep"}`: Replica timeout count

**Configuration**:
- `homeobject_scrub_enabled{type="shallow|deep"}`: 1 if enabled, 0 if disabled
- `homeobject_scrub_max_concurrent{type="shallow|deep"}`: Configured limits

#### 5.2.2 Alerts

**Critical**:
```yaml
- alert: ScrubInconsistencyDetected
  expr: increase(homeobject_scrub_inconsistencies_found_total[5m]) > 0
  severity: critical
  summary: "Data inconsistency detected in PG {{ $labels.pg_id }}"
```

**Warning**:
```yaml
- alert: ScrubsNotCompleting
  expr: time() - homeobject_pg_last_scrub_time > 864000  # 10 days
  severity: warning
  summary: "PG {{ $labels.pg_id }} not scrubbed in 10+ days"

- alert: DeepScrubOverdue
  expr: time() - homeobject_pg_last_deep_scrub_time > 3456000  # 40 days
  severity: warning
  summary: "PG {{ $labels.pg_id }} deep scrub overdue (40+ days)"
```

### 5.3 Troubleshooting

#### 5.3.1 Scrubs Not Running

**Symptom**: PGs past `max_interval`, but scrubs not scheduled

**Checks**:
1. Verify scrubbing enabled:
   ```bash
   curl "http://localhost:8080/scrub/status"
   ```

2. Check concurrency limits:
   ```bash
   # If active_scrubs == max_concurrent_scrubs, increase limit or wait
   ```

3. Check PG state:
   ```bash
   curl "http://localhost:8080/pg/{pg_id}"
   # state should not include "baseline_resync"
   ```

4. Check logs for scheduler errors

**Resolution**:
- Enable scrubbing if disabled
- Increase `max_concurrent_scrubs` if at capacity
- Wait for recovery to complete if PG in `BASELINE_RESYNC`

#### 5.3.2 High Client Latency During Scrub

**Symptom**: P99 read/write latency increases during deep scrubs

**Tuning**:
1. Increase sleep interval:
   ```yaml
   deep_sleep_us: 20000  # Increase from 10ms to 20ms
   ```

2. Reduce batch size:
   ```yaml
   deep_batch_size: 10  # Reduce from 25 to 10
   ```

3. Reduce concurrency:
   ```yaml
   max_concurrent_deep_scrubs: 1  # Already at minimum
   max_concurrent_scrubs: 1       # Reduce total if multiple PGs on node
   ```

4. Temporarily disable deep scrubs during peak hours (manual control):
   ```bash
   curl -X POST "http://localhost:8080/scrub/disable?deep_only=true"
   # Re-enable after peak
   curl -X DELETE "http://localhost:8080/scrub/disable?deep_only=true"
   ```

**Future**: Time-window scheduling (post-MVP)

#### 5.3.3 Inconsistency Detected

**Symptom**: `homeobject_scrub_inconsistencies_found_total` increases

**Investigation**:
1. Query inconsistency details:
   ```bash
   curl "http://localhost:8080/scrub/task/{task_id}/inconsistencies"
   ```

2. Identify affected blobs and replicas

3. Check upper-layer logs:
   - Application write logs for affected blob_ids
   - Timestamps of writes vs. scrub detection
   - Any crash/restart events near write time

4. Analyze replica states:
   - Checksum mismatch → likely bit rot or write bug
   - Missing blob → replication failure or delete bug
   - State mismatch → GC timing or tombstone handling

**Resolution**:
- Admin determines authoritative replica based on context
- Trigger manual repair (post-MVP feature)
- If widespread: file bug report with HomeObject team

#### 5.3.4 Scrub Task Stuck

**Symptom**: Task shows `running` for extended period (>24 hours)

**Checks**:
1. Query task progress:
   ```bash
   curl "http://localhost:8080/scrub/task/{task_id}"
   ```

2. Check if progress is advancing (`blobs_scanned` increasing)

3. Check for replica timeouts in logs

**Resolution**:
- If progress advancing slowly: deep scrub on large PG is normal (can take 12+ hours for 10M blobs)
- If stuck (no progress): likely replica unavailable or network partition
  - Check replica health
  - Task will eventually timeout and abort
- If persistent: file bug report

### 5.4 Capacity Planning

#### 5.4.1 Scrub Duration Estimates

**Shallow Scrub**:
```
Duration = (total_blobs / batch_size) × (batch_latency + sleep_interval)
         = (total_blobs / 500) × (2ms + 0ms)
         = total_blobs × 0.004ms

Example: 10M blobs → ~40 seconds
```

**Deep Scrub**:
```
Duration = (total_blobs / batch_size) × (batch_latency + sleep_interval)
         = (total_blobs / 25) × (100ms + 10ms)
         = total_blobs × 4.4ms

Example: 10M blobs → ~12 hours
```

**Network Bandwidth**:
- Shallow: 500 blobs × 8 bytes × 3 replicas = ~12KB per batch
- Deep: 25 blobs × 80 bytes × 3 replicas = ~6KB per batch

#### 5.4.2 Recommended Intervals by PG Size

| PG Size (blobs) | Shallow Min | Shallow Max | Deep Min | Deep Max |
|-----------------|-------------|-------------|----------|----------|
| < 1M            | 1 day       | 3 days      | 3 days   | 7 days   |
| 1M - 10M        | 1 day       | 7 days      | 7 days   | 14 days  |
| 10M - 100M      | 1 day       | 7 days      | 7 days   | 30 days  |
| > 100M          | 1 day       | 7 days      | 14 days  | 60 days  |

Adjust based on available I/O headroom and data integrity requirements.

---

## 6. Conclusion

### 6.1 Design Summary

This document specifies a cross-replica scrubbing system for HomeObject that provides proactive data integrity verification through two complementary modes:

- **Shallow Scrub**: Fast daily metadata verification (index consistency)
- **Deep Scrub**: Thorough weekly data verification (checksum validation)

The design leverages existing infrastructure (nuraft_messenger, HomeStore index APIs, PG state management) to minimize implementation complexity while providing production-grade reliability features:

- Configurable resource controls to minimize client I/O impact
- Automated scheduling with randomization to prevent load spikes
- Two-level persistence for resumability and task tracking
- Comprehensive HTTP API for operational control
- Graceful degradation when replicas are unavailable

### 6.2 Key Strengths

1. **Production-Validated Patterns**: Communication flow, batch sizes, and scheduling logic aligned with Ceph's proven scrubbing implementation
2. **Minimal Infrastructure Changes**: Reuses nuraft_messenger data service, no new communication channels required
3. **Thread Safety**: Verified dispatcher pattern ensures safe concurrent access to index tables
4. **Graceful Resource Control**: Multiple layers of throttling (batch size, sleep, concurrency limits, manual flags)
5. **Operational Flexibility**: Both automated and manual trigger modes, ephemeral disable flags for emergency control

### 6.3 Known Limitations

1. **No Automatic Repair**: MVP detects inconsistencies but requires admin investigation and manual repair
2. **Post-GC Ambiguity**: Tombstone removal can create false positives for "missing blob" detection
3. **Two-Replica Quorum**: Cannot use majority voting; leader assumed authoritative (admin reviews conflicts)
4. **Eventual Consistency Noise**: Active writes may cause transient false positives (mitigated by spot-check phase)
5. **No Load-Based Throttling**: MVP uses fixed sleep intervals, not dynamic adjustment based on system load

These limitations are acceptable for MVP and can be addressed in future iterations.

### 6.4 Implementation Phases

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

### 6.5 Future Enhancements (Post-MVP)

**High Priority**:
- **Automatic Repair**: Admin-triggered repair workflow with authoritative replica selection
- **Latency-Based Throttling**: Dynamic sleep adjustment based on batch processing time
- **Enhanced Resumability**: Shallow scrub checkpoint recovery

**Medium Priority**:
- **Time-Window Scheduling**: Restrict scrubbing to off-peak hours (scrub_begin_hour / scrub_end_hour)
- **Load-Based Throttling**: Pause scrubbing when system load exceeds threshold
- **Priority Boost for INCONSISTENT PGs**: Re-verify inconsistent PGs more frequently

**Low Priority**:
- **Per-PG Resource Controls**: Fine-grained throttling for individual PGs
- **Cross-PG Coordination**: Cluster-wide scrub scheduling to balance load
- **Incremental Deep Scrub**: Sample-based verification for very large PGs

### 6.6 Success Criteria

**Functional**:
- ✅ Detect checksum mismatches between replicas
- ✅ Detect missing/extra blobs between replicas
- ✅ Complete shallow scrub within configured max_interval (7 days default)
- ✅ Complete deep scrub within configured max_interval (30 days default)
- ✅ Survive replica failures gracefully (timeout, partial results)
- ✅ Resume deep scrubs after pod restart

**Performance**:
- ✅ Shallow scrub impact: <1% increase in P99 read latency
- ✅ Deep scrub impact: <5% increase in P99 read latency (with default sleep 10ms)
- ✅ Network overhead: <1 MB/sec per instance during deep scrub

**Operational**:
- ✅ Manual scrub trigger via HTTP API (<1 sec response time)
- ✅ Task status query via HTTP API (<100ms response time)
- ✅ Inconsistency details accessible via API
- ✅ Scrubbing disable/enable takes effect within 1 scheduler cycle (10 min)

### 6.7 Open Questions for Review

1. **Message Encoding**: Use Protobuf, FlatBuffers, or custom binary format for scrub messages?
2. **Checksum Algorithm**: Continue with CRC32 or upgrade to SHA256 for deep scrub?
3. **Metrics Backend**: Prometheus format sufficient, or also support OpenTelemetry?
4. **Task ID Generation**: UUID, monotonic counter, or timestamp-based?
5. **Configuration Reload**: Support hot-reload of config parameters, or require restart?

These questions should be resolved during Phase 1 implementation planning.

---

## Appendix

See `scrubber_design_appendix.md` for:
- **Appendix A**: Detailed technical rationale (B+tree calculations, batch size analysis, thread safety verification)
- **Appendix B**: Ceph comparison and validation
- **Appendix C**: Alternative approaches considered and rejected
- **Appendix D**: Code examples and implementation patterns

