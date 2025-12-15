# Cross-Replica Scrubber Implementation Guide

**Document Version:** 1.0
**Date:** 2025-12-14
**Status:** Final Implementation Guide
**Target Audience:** Software Engineers

**Related Documents:**
- [SCRUBBER_DESIGN.md](./SCRUBBER_DESIGN.md) - Architecture and design rationale
- [SCRUBBER_OPERATIONS.md](./SCRUBBER_OPERATIONS.md) - Deployment and operations guide

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Implementation Sequence](#2-implementation-sequence)
3. [Data Structures](#3-data-structures)
4. [Thread Safety](#4-thread-safety)
5. [Resource Control](#5-resource-control)
6. [Scheduling & Initiation](#6-scheduling--initiation)
7. [Inconsistency Detection](#7-inconsistency-detection)
8. [Persistence & Recovery](#8-persistence--recovery)
9. [Message Formats & APIs](#9-message-formats--apis)
10. [Configuration](#10-configuration)

---

## 1. Introduction

### 1.1 Purpose

This document provides detailed implementation guidance for the cross-replica scrubber feature in HomeObject. It is intended for engineers who will be writing the code, reviewing pull requests, or debugging scrubber-related issues.

### 1.2 Prerequisites

Before implementing the scrubber, you should be familiar with:

- **C++17/20**: Modern C++ features including std::optional, structured bindings, lambdas
- **nuraft_messenger**: Bidirectional RPC, data service APIs
- **HomeStore**: Index table APIs, metablk persistence, iomanager reactor pattern
- **Raft Consensus**: Leadership election, state machine replication
- **HomeObject Architecture**: PG management, shard structure, blob lifecycle

### 1.3 Key Design Constraints

1. **Leader-Only Execution**: Scrubbing runs exclusively on the PG leader replica
2. **Atomic State Management**: All PG state flags use atomic operations (no mutexes)
3. **Thread Safety**: nuraft_messenger handlers dispatch to reactor threads for index access
4. **Follower → Leader Data Flow**: Followers send their data to leader for comparison
5. **Report-Only MVP**: Detect inconsistencies but don't auto-repair

---

## 2. Implementation Sequence

### 2.1 Build Order

Recommended order to minimize integration issues and enable incremental testing:

#### Phase 1: Foundation (Week 1-2)
1. **Data Structures** (Section 3)
   - Define `pg_scrub_metadata`, `scrub_task_state`, message structs
   - Add serialization/deserialization helpers
   - Unit tests for struct packing/unpacking

2. **PG State Management** (Section 4.1)
   - Add SCRUBBING, INCONSISTENT flags to `PGStateMask`
   - Implement `try_start_scrub()`, `can_continue_scrub()`
   - Unit tests for atomic state transitions

3. **nuraft_messenger Handlers** (Section 9.2)
   - Register scrub handlers during PG creation
   - Implement request/response encoding/decoding
   - Mock handler tests (no real index queries yet)

#### Phase 2: Shallow Scrub (Week 3-4)
4. **Index Query Integration**
   - Implement `query_blobs_in_shard()` wrapper around HomeStore IndexTable
   - Reactor dispatch pattern in handlers
   - Integration test with real index table

5. **Scrub Engine Core**
   - Implement batch processing loop
   - Leader RPC dispatch to all replicas
   - Response collection with folly::collectAll
   - Basic inconsistency detection (blob existence comparison)

6. **Manual Trigger API** (Section 9.1)
   - HTTP POST /scrub endpoint
   - Task ID generation and tracking
   - GET /scrub/task/{id} status query

#### Phase 3: Scheduling & Resource Control (Week 5-6)
7. **Resource Limits** (Section 5)
   - Implement `try_reserve_scrub_slot()` with atomic CAS loop
   - Add concurrency counters (active_scrub_count, active_deep_scrub_count)
   - Test race conditions with concurrent requests

8. **Scheduler** (Section 6)
   - Background task registration with iomanager
   - PG selection algorithm (urgent + oldest logic)
   - Randomization (±50% jitter)

9. **Configuration** (Section 10)
   - Add scrub config parameters to HomeObject config
   - Runtime config loading
   - Validation (e.g., max_interval > min_interval)

#### Phase 4: Deep Scrub & Persistence (Week 7-8)
10. **Deep Scrub**
    - Blob data read + checksum computation
    - Checkpointing after each shard
    - Resume logic on restart

11. **Persistence** (Section 8)
    - Metablk serialization for task state
    - PG metadata updates (last_scrub_time, state)
    - Task retention cleanup

12. **Spot-Check Phase** (Section 7.2)
    - 100ms delay + retry mechanism
    - Transient difference filtering

#### Phase 5: Observability & Testing (Week 9-10)
13. **Metrics**
    - Prometheus metric emission
    - Scrub counters, latency histograms

14. **Full HTTP API** (Section 9.1)
    - All query endpoints
    - Disable/enable endpoints
    - Error handling

15. **Integration Tests**
    - Multi-PG scrub scenarios
    - Leadership change during scrub
    - Recovery during scrub
    - Inconsistency injection tests

### 2.2 Component Dependencies

```
┌─────────────────────────────────────────────────────────────┐
│                    COMPONENT DEPENDENCY GRAPH                │
└─────────────────────────────────────────────────────────────┘

  Data Structures
       ↓
  PG State Management ←──────────────┐
       ↓                              │
  nuraft_messenger Handlers           │
       ↓                              │
  Index Query Integration             │
       ↓                              │
  Scrub Engine Core ──────────────────┤
       ↓                              │
  Manual Trigger API                  │
       ↓                              │
  Resource Limits ────────────────────┤
       ↓                              │
  Scheduler                           │
       ↓                              │
  Configuration                       │
       ↓                              │
  Deep Scrub ─────────────────────────┤
       ↓                              │
  Persistence ─────────────────────────┤
       ↓
  Spot-Check Phase
       ↓
  Metrics
       ↓
  Full HTTP API
       ↓
  Integration Tests
```

### 2.3 Testing Strategy

**Unit Tests** (per component):
- Data structure serialization round-trip
- Atomic state transitions (race condition scenarios)
- Message encoding/decoding
- Scheduling algorithm (time-based selection, randomization)
- Resource limit enforcement (CAS loop correctness)

**Integration Tests** (cross-component):
- Shallow scrub end-to-end (manual trigger → completion)
- Deep scrub with checkpointing
- Leadership change abort
- Concurrent scrub limit enforcement
- Inconsistency detection and reporting

**Fault Injection Tests**:
- Follower timeout scenarios
- Network partition during scrub
- Pod restart during deep scrub (resume test)
- Inconsistency scenarios (inject mismatched blobs)

**Performance Tests**:
- Client latency impact (P99 during shallow/deep scrub)
- Scrub duration scaling (1M, 10M, 100M blobs)
- Network bandwidth usage

---

## 3. Data Structures

### 3.1 Index Structure (Existing)

HomeObject uses a B+tree index for blob routing. You'll query this in scrub handlers.

**Location**: `src/lib/homestore_backend/index_kv.hpp`

```cpp
struct BlobRouteKey {
  shard_id_t shard_id;  // 8 bytes
  blob_id_t blob_id;    // 8 bytes (monotonically increasing per shard)

  // Comparison operators for B+tree ordering
  auto operator<=>(const BlobRouteKey&) const = default;
};

struct BlobRouteValue {
  MultiBlkId pbas;      // Physical block addresses (variable size)
  // NOTE: pbas can differ across replicas (different physical allocation)
  // Only logical blob_id matters for consistency check
};
```

**Key Properties for Scrubbing**:
- `blob_id` is monotonically increasing within each shard
- Natural support for range queries: `[shard_id, start_blob_id]` to `[shard_id, end_blob_id]`
- B+tree node size: 4KB, ~100 entries per node
- Shallow scrub batch of 500 blobs ≈ 5 leaf nodes

**Query API** (you'll use this in handlers):
```cpp
// In src/lib/homestore_backend/index_kv.hpp
auto query_blobs_in_shard(shard_id_t shard,
                          blob_id_t start_id,
                          uint32_t batch_size)
  -> std::vector<BlobInfo>;
```

### 3.2 Blob Header Structure (Existing)

Each blob has metadata including checksum. You'll use this for deep scrub.

**Location**: `src/lib/homestore_backend/hs_blob.hpp`

```cpp
struct BlobHeader {
  enum class HashAlgorithm : uint8_t {
    NONE = 0, CRC32 = 1, MD5 = 2, SHA1 = 3, SHA256 = 4
  };

  HashAlgorithm hash_algorithm;
  uint8_t hash[32];           // Checksum stored at write time
  shard_id_t shard_id;
  blob_id_t blob_id;
  uint32_t blob_size;
  uint64_t timestamp;         // Write time
  // ... other metadata
};

enum class BlobState : uint8_t {
  ALIVE = 0,       // Normal blob
  TOMBSTONE = 1,   // Deleted blob (pending GC)
};
```

**Deep Scrub Usage**:
1. Read blob header + data from disk via HomeStore
2. Recompute checksum using `hash_algorithm`
3. Compare against stored `hash` field
4. Return checksum to leader for comparison

### 3.3 PG Scrub Metadata (NEW)

Add to PG superblock structure. This persists scrub state across restarts.

**Location**: `src/include/homeobject/pg_manager.hpp` (modify existing `pg_info` struct)

```cpp
struct pg_scrub_metadata {
  uint64_t last_scrub_time{0};         // Unix timestamp (seconds)
  uint64_t last_deep_scrub_time{0};    // Unix timestamp (seconds)

  enum class ScrubState : uint8_t {
    CLEAN = 0,        // No known inconsistencies
    INCONSISTENT = 1  // Last scrub found issues
  };
  ScrubState state{ScrubState::CLEAN};

  std::optional<uint64_t> active_task_id;  // Non-zero if scrub running

  // Serialization for metablk persistence
  friend void serialize(serializer& s, const pg_scrub_metadata& m) {
    s(m.last_scrub_time, m.last_deep_scrub_time,
      m.state, m.active_task_id);
  }
};

// Add to existing pg_info struct:
struct pg_info {
  pg_id_t id;
  // ... existing fields ...
  pg_scrub_metadata scrub_meta;  // ADD THIS
};
```

**Version Compatibility**:
- New field defaults to zero on read from older versions
- Serialization handles optional fields gracefully

### 3.4 Scrub Task State (NEW)

Separate metablk per task for detailed tracking and resume capability.

**Location**: `src/lib/homestore_backend/scrub_manager.hpp` (new file)

```cpp
struct scrub_task_state {
  uint64_t task_id;           // Unique task identifier
  pg_id_t pg_id;

  enum class ScrubType : uint8_t {
    SHALLOW = 0,
    DEEP = 1
  };
  ScrubType type;

  uint64_t start_time;        // Unix timestamp
  uint64_t end_time{0};       // Unix timestamp (0 if running)

  // Progress tracking (for resume)
  shard_id_t current_shard{0};
  blob_id_t current_blob_id{0};
  uint64_t max_blob_id{0};    // Upper bound captured at start

  // Statistics
  uint64_t blobs_scanned{0};
  uint64_t batches_processed{0};

  // Findings
  std::vector<InconsistencyRecord> inconsistencies;

  enum class TaskStatus : uint8_t {
    RUNNING = 0,
    COMPLETED = 1,
    FAILED = 2,
    ABORTED = 3     // Leadership lost, recovery started, etc.
  };
  TaskStatus status{TaskStatus::RUNNING};

  std::string error_message;  // If status == FAILED

  // Serialization
  friend void serialize(serializer& s, scrub_task_state& t) {
    s(t.task_id, t.pg_id, t.type, t.start_time, t.end_time,
      t.current_shard, t.current_blob_id, t.max_blob_id,
      t.blobs_scanned, t.batches_processed,
      t.inconsistencies, t.status, t.error_message);
  }
};
```

**Metablk Key Format**:
```cpp
// Use task_id as metablk name for easy lookup
std::string metablk_name = fmt::format("scrub_task_{}", task_id);
```

**Retention Policy** (implemented in cleanup task):
```cpp
uint32_t scrub_task_retention_count = 100;   // Keep last N tasks
uint64_t scrub_task_retention_days = 7;      // Keep tasks from last N days

// Retain if EITHER condition is met
bool should_retain(const scrub_task_state& task, uint64_t now) {
  uint64_t age_days = (now - task.end_time) / 86400;
  return (task_index < scrub_task_retention_count) ||
         (age_days < scrub_task_retention_days);
}
```

### 3.5 Inconsistency Record (NEW)

Detailed record of each detected inconsistency.

**Location**: Same file as `scrub_task_state`

```cpp
struct InconsistencyRecord {
  shard_id_t shard_id;
  blob_id_t blob_id;

  enum class InconsistencyType : uint8_t {
    MISSING_BLOB = 0,       // Some replicas have blob, others don't
    EXTRA_BLOB = 1,         // Replica has blob not in others (deprecated - same as MISSING)
    CHECKSUM_MISMATCH = 2,  // All replicas have blob, checksums differ
    STATE_MISMATCH = 3      // Some mark as TOMBSTONE, others as ALIVE
  };
  InconsistencyType type;

  // Per-replica status
  struct ReplicaStatus {
    bool exists{false};
    BlobState state{BlobState::ALIVE};
    std::optional<std::array<uint8_t, 32>> checksum;  // For deep scrub
    std::optional<uint32_t> blob_size;

    friend void serialize(serializer& s, ReplicaStatus& r) {
      s(r.exists, r.state, r.checksum, r.blob_size);
    }
  };

  std::map<peer_id_t, ReplicaStatus> replica_states;

  uint64_t detected_time;  // Unix timestamp

  friend void serialize(serializer& s, InconsistencyRecord& r) {
    s(r.shard_id, r.blob_id, r.type, r.replica_states, r.detected_time);
  }
};
```

---

## 4. Thread Safety

### 4.1 The Core Problem

nuraft_messenger handlers execute in the **messaging service thread**, NOT the HomeObject reactor thread. Direct access to `index_table` from a handler would be unsafe and cause data races.

**Verification** (from HomeStore codebase):
- Location: `nuraft_mesg/src/lib/service.cpp:handle_data_service_request()`
- Handlers run in `messaging_service` thread pool
- HomeObject index_table is NOT thread-safe for concurrent access

### 4.2 Solution: Reactor Dispatch Pattern

Dispatch index access to a HomeObject reactor thread using iomanager.

**Pattern** (verified in HomeStore FETCH_DATA handler):

```cpp
void HSHomeObject::on_scrub_index_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  // ⚠️ RUNNING IN nuraft_mesg SERVICE THREAD - DO NOT ACCESS index_table

  auto request = decode_scrub_request(rpc_data->request_blob());

  // Check concurrency limits in service thread (safe - uses atomics)
  if (!try_reserve_scrub_slot(request.type)) {
    auto error = encode_error_response("RESOURCE_EXHAUSTED");
    rpc_data->send_response(error);
    return;
  }

  // Dispatch to reactor thread for safe index access
  iomanager.run_on(
    iomgr::reactor_regex::random_worker,  // Pick any worker reactor
    [this, request, rpc_data]() {
      // ✅ NOW SAFE - Running in HomeObject reactor thread

      try {
        // Query index table (thread-safe within reactor)
        auto result = query_blobs_in_shard(
          request.pg_id,
          request.shard_id,
          request.start_blob_id,
          request.batch_size
        );

        // Filter ALIVE blobs only (skip TOMBSTONE)
        std::vector<blob_id_t> alive_blobs;
        for (auto& blob : result) {
          if (blob.state == BlobState::ALIVE) {
            alive_blobs.push_back(blob.blob_id);
          }
        }

        // Encode and send response
        auto response_blob = encode_scrub_response(alive_blobs);
        rpc_data->send_response(response_blob);

      } catch (const std::exception& e) {
        LOGE("Scrub index query failed: {}", e.what());
        auto error = encode_error_response(e.what());
        rpc_data->send_response(error);
      }

      // Release concurrency slot (safe - uses atomics)
      release_scrub_slot(request.type);
    }
  );
}
```

**Key Points**:
1. **No index_table access in service thread**: Only decode request, check limits
2. **Dispatch to reactor**: Use `iomanager.run_on()` for index operations
3. **Capture rpc_data**: Keep RPC context alive via lambda capture
4. **Error handling**: Catch exceptions in reactor thread, send error response

### 4.3 Index Table Thread Safety (HomeStore Detail)

HomeStore `IndexTable` read operations are thread-safe via atomic reference counting:

**Verification** (from HomeStore codebase):
- Location: `HomeStore/src/include/homestore/index_table.hpp:193-198`
- Query operations use `shared_ptr` with atomic refcount
- Multiple concurrent readers are safe
- No explicit locking needed

**However**: We still dispatch to reactor for consistency with other HomeObject operations (`put_blob`, `delete_blob`, etc.), which ALL use reactor pattern.

### 4.4 Atomic State Management

All PG state modifications use atomics (NO mutexes).

**Pattern**: `fetch_or`, `fetch_and`, `compare_exchange` for state flags.

```cpp
// Example: Set SCRUBBING flag atomically
bool try_start_scrub(pg_id_t pg_id) {
  auto pg = _pg_map.at(pg_id);

  // Atomic test-and-set
  auto old_state = pg->state.fetch_or(
    static_cast<uint64_t>(PGStateMask::SCRUBBING),
    std::memory_order_acquire  // Ensures visibility of state change
  );

  // Check if already scrubbing
  if (old_state & static_cast<uint64_t>(PGStateMask::SCRUBBING)) {
    return false;  // Already scrubbing
  }

  return true;  // Successfully acquired scrubbing lock
}

// Example: Clear SCRUBBING flag atomically
void finish_scrub(pg_id_t pg_id) {
  auto pg = _pg_map.at(pg_id);

  pg->state.fetch_and(
    ~static_cast<uint64_t>(PGStateMask::SCRUBBING),
    std::memory_order_release  // Ensures all scrub updates are visible
  );
}

// Example: Check state atomically
bool is_scrubbing(pg_id_t pg_id) {
  auto pg = _pg_map.at(pg_id);
  auto state = pg->state.load(std::memory_order_acquire);
  return state & static_cast<uint64_t>(PGStateMask::SCRUBBING);
}
```

**Memory Ordering**:
- `memory_order_acquire`: Use when reading state (ensures you see all previous writes)
- `memory_order_release`: Use when writing state (ensures all your writes are visible)
- `memory_order_relaxed`: Use for non-synchronizing atomics (counters)

**Verification** (from HomeObject codebase):
- Location: `src/include/homeobject/pg_manager.hpp:93-113`
- `pg_state` struct provides `set_state()`, `clear_state()`, `is_state_set()` methods
- All wrap atomic operations

---

## 5. Resource Control

Resource control ensures scrubbing has minimal impact on normal client I/O operations. Multiple layers of throttling work together.

### 5.1 Batch Sizes

Different batch sizes optimize for different operation characteristics.

**Configuration**:
```cpp
// In scrub_manager.hpp or config
uint32_t scrub_shallow_batch_size_ = 500;   // Index-only reads
uint32_t scrub_deep_batch_size_ = 25;       // Full data reads + checksum
```

**Shallow Scrub (500 blobs)**:
- **Rationale**: Index reads are cheap, non-blocking
- **Calculation**: 500 blobs ≈ 5 B+tree leaf nodes (100 entries per 4KB node)
- **Network payload**: ~4KB per replica (500 × 8 bytes for blob_ids)
- **Optimization**: Maximize throughput, minimize round trips

**Deep Scrub (25 blobs)**:
- **Rationale**: Aligned with Ceph production (`osd_scrub_chunk_max = 25`)
- **Constraints**:
  - I/O bounded: disk reads dominate
  - CPU bounded: SHA256 computation
  - Memory bounded: depends on actual blob sizes
- **Network payload**: ~2KB per replica (25 × ~80 bytes for checksum records)
- **Proven**: Battle-tested batch size for data verification workloads

### 5.2 Sleep Between Batches

Configurable delay to yield I/O bandwidth to client operations.

**Configuration**:
```cpp
uint64_t scrub_sleep_shallow_us_ = 0;        // Microseconds (no sleep)
uint64_t scrub_sleep_deep_us_ = 10000;       // Microseconds (10ms)
```

**Why microseconds?**
- Fine-grained control (1ms = 1000μs)
- No hard limit (trust admin to configure appropriately)
- Same granularity as Ceph's `osd_scrub_sleep`

**Implementation**:
```cpp
void scrub_batch(pg_id_t pg_id, ScrubType type, /* ... */) {
  // ... execute batch (RPC, compare, record) ...

  // Throttle between batches
  uint64_t sleep_us = (type == ScrubType::SHALLOW) ?
    scrub_sleep_shallow_us_ : scrub_sleep_deep_us_;

  if (sleep_us > 0) {
    std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
  }

  // Continue to next batch...
}
```

**Tuning Guidelines**:
- Start conservative: 10ms for deep, 0 for shallow
- Monitor client P99 latency during scrub
- Increase sleep if latency impact > 5%
- Decrease sleep if scrub completion time too slow

### 5.3 Concurrent Scrub Limits

Instance-level concurrency control (NOT per-PG).

**Why instance-level?**
All PGs on a HomeObject instance share physical resources:
- Disk I/O bandwidth
- CPU cycles (checksum computation)
- Memory (blob data buffers)
- Network bandwidth (RPC payloads)

Per-PG limits provide no meaningful load control.

**Configuration**:
```cpp
// In scrub_manager.hpp
std::atomic<uint32_t> active_scrub_count_{0};
std::atomic<uint32_t> active_deep_scrub_count_{0};

uint32_t max_concurrent_scrubs_ = 3;        // Total (shallow + deep)
uint32_t max_concurrent_deep_scrubs_ = 1;   // Deep subset
```

**Enforcement on Leader (Initiator)**:

```cpp
bool try_reserve_scrub_slot(ScrubType type) {
  // Phase 1: Reserve total scrub slot
  uint32_t current = active_scrub_count_.load(std::memory_order_relaxed);

  while (current < max_concurrent_scrubs_) {
    // Atomic check-and-increment
    if (active_scrub_count_.compare_exchange_weak(
          current, current + 1,
          std::memory_order_acquire,  // Success: synchronize
          std::memory_order_relaxed   // Failure: retry
        )) {
      // Successfully reserved total slot

      // Phase 2: If deep scrub, reserve deep slot
      if (type == ScrubType::DEEP) {
        uint32_t deep_current = active_deep_scrub_count_.load(std::memory_order_relaxed);

        while (deep_current < max_concurrent_deep_scrubs_) {
          if (active_deep_scrub_count_.compare_exchange_weak(
                deep_current, deep_current + 1,
                std::memory_order_acquire,
                std::memory_order_relaxed)) {
            return true;  // Successfully reserved both slots
          }
        }

        // Failed to reserve deep slot - release total slot
        active_scrub_count_.fetch_sub(1, std::memory_order_release);
        return false;
      }

      return true;  // Shallow scrub - only needed total slot
    }
  }

  return false;  // At limit, cannot reserve
}

void release_scrub_slot(ScrubType type) {
  active_scrub_count_.fetch_sub(1, std::memory_order_release);
  if (type == ScrubType::DEEP) {
    active_deep_scrub_count_.fetch_sub(1, std::memory_order_release);
  }
}
```

**Why compare_exchange_weak loop?**

The original check-then-increment pattern has a race condition:
```cpp
// ❌ UNSAFE - Race window between load and fetch_add
if (active_scrub_count_.load() >= max_concurrent_scrubs_) {
  return false;
}
active_scrub_count_.fetch_add(1);  // Can exceed limit!
```

Using `compare_exchange_weak` ensures atomic check-and-increment:
1. Load current value
2. Check if < limit
3. Atomically: if still equal to loaded value, increment
4. If spurious failure (weak variant), loop retries

**Enforcement on Follower (Handler)**:

Followers also check limits to prevent overload from multiple leaders:

```cpp
void on_scrub_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  auto request = decode_scrub_request(rpc_data->request_blob());

  // Check local concurrent limit BEFORE dispatching
  if (!try_reserve_scrub_slot(request.type)) {
    auto error = encode_error_response("RESOURCE_EXHAUSTED");
    rpc_data->send_response(error);
    return;
  }

  // Dispatch to reactor (slot will be released in reactor callback)
  iomanager.run_on(/* ... */);
}
```

### 5.4 Manual Control: NO_SCRUB Flags

Ephemeral flags for operational control (e.g., during incidents).

**Configuration**:
```cpp
// In scrub_manager.hpp
std::atomic<bool> no_scrub_{false};           // Block ALL scrubbing
std::atomic<bool> no_deep_scrub_{false};      // Block deep scrub only
```

**Checked Before and During Scrub**:

```cpp
bool can_continue_scrub(pg_id_t pg_id, ScrubType type) {
  // Check 1: Global manual disable flags
  if (no_scrub_.load(std::memory_order_acquire)) {
    return false;
  }
  if (type == ScrubType::DEEP && no_deep_scrub_.load(std::memory_order_acquire)) {
    return false;
  }

  // Check 2: PG state
  auto pg = get_pg(pg_id);
  auto state = pg->state.load(std::memory_order_acquire);

  // Don't scrub if recovering
  if (state & static_cast<uint64_t>(PGStateMask::BASELINE_RESYNC)) {
    return false;
  }

  // Check if SCRUBBING flag still set (could be cleared externally)
  if (!(state & static_cast<uint64_t>(PGStateMask::SCRUBBING))) {
    return false;
  }

  // Check 3: Leadership (abort if lost leadership mid-scrub)
  auto repl_dev = pg->repl_dev_;
  if (!repl_dev->is_leader()) {
    LOGW("Leadership lost during scrub for pg={}", pg_id);
    return false;
  }

  return true;
}
```

**Usage in Scrub Loop**:
```cpp
void execute_scrub(pg_id_t pg_id, ScrubType type) {
  // ... initial setup ...

  for (auto shard_id : pg_shards) {
    blob_id_t current_id = 0;

    while (current_id < max_blob_id) {
      // Check before EVERY batch
      if (!can_continue_scrub(pg_id, type)) {
        LOGW("Scrub aborted for pg={}, type={}", pg_id, type);
        mark_task_aborted(task_id);
        return;
      }

      // Execute batch...
      scrub_batch(pg_id, shard_id, current_id, batch_size);
      current_id += batch_size;
    }
  }
}
```

**HTTP API** (see Section 9.1 for full API):
```cpp
// POST /scrub/disable?deep_only=true
void disable_scrubbing(bool deep_only) {
  if (deep_only) {
    no_deep_scrub_.store(true, std::memory_order_release);
  } else {
    no_scrub_.store(true, std::memory_order_release);
  }
}

// DELETE /scrub/disable
void enable_scrubbing(bool deep_only) {
  if (deep_only) {
    no_deep_scrub_.store(false, std::memory_order_release);
  } else {
    no_scrub_.store(false, std::memory_order_release);
  }
}
```

**Behavior**:
- Checked before starting new scrubs (scheduler + manual trigger)
- Checked between batches (pauses ongoing scrubs gracefully)
- **Ephemeral**: Flags reset to false on pod restart (conservative default)
- **Non-persistent**: Intentionally not saved to metablk

### 5.5 Timeouts

Per-batch timeouts for replica responses. Prevents scrub from hanging on slow/failed replicas.

**Configuration**:
```cpp
uint64_t scrub_shallow_timeout_sec_ = 5;      // Index query + network
uint64_t scrub_deep_timeout_sec_ = 60;        // Data read + checksum + network
uint64_t scrub_spot_check_timeout_sec_ = 10;  // Retry mechanism
```

**Implementation with folly::collectAll**:

```cpp
folly::Future<std::vector<ScrubResponse>>
dispatch_scrub_request(pg_id_t pg_id, ScrubRequest req, ScrubType type) {
  auto repl_dev = get_repl_dev(pg_id);
  auto peers = repl_dev->get_replication_status().members;
  auto msg_svc = repl_dev->group_msg_service();

  std::vector<folly::SemiFuture<GenericClientResponse>> futures;

  // Send request to all replicas
  for (auto& peer : peers) {
    auto request_blob = encode_scrub_request(req);
    auto future = msg_svc->data_service_request_bidirectional(
      peer.id,
      "scrub_get_index",
      request_blob
    );
    futures.push_back(std::move(future));
  }

  // Collect with timeout
  uint64_t timeout_sec = (type == ScrubType::SHALLOW) ?
    scrub_shallow_timeout_sec_ : scrub_deep_timeout_sec_;

  return folly::collectAll(std::move(futures))
    .via(folly::getCPUExecutor())
    .within(std::chrono::seconds(timeout_sec))  // Timeout here
    .thenValue([this, pg_id](std::vector<folly::Try<GenericClientResponse>>&& results) {
      std::vector<ScrubResponse> responses;

      for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].hasValue()) {
          // Success - decode response
          responses.push_back(decode_scrub_response(results[i].value().response_blob()));
        } else if (results[i].hasException()) {
          // Timeout or RPC failure
          try {
            results[i].throwUnlessValue();
          } catch (const std::exception& e) {
            LOGW("Replica {} failed for pg={}: {}", i, pg_id, e.what());
            // Record failure for admin review, but continue
            record_replica_failure(pg_id, i, e.what());
          }
        }
      }

      return responses;
    })
    .thenError([](folly::exception_wrapper&& ew) {
      // Global timeout (all replicas failed)
      LOGE("Scrub request timed out: {}", ew.what());
      throw;
    });
}
```

**Partial Results Handling**:

Leader can proceed with available replicas and flag incomplete scrub:

```cpp
void process_batch_responses(const std::vector<ScrubResponse>& responses,
                              size_t expected_replica_count,
                              scrub_task_state& task) {
  if (responses.size() < expected_replica_count) {
    LOGW("Incomplete scrub: {}/{} replicas responded",
         responses.size(), expected_replica_count);

    // Mark task as incomplete
    task.flags |= TaskFlags::INCOMPLETE_REPLICAS;
  }

  // Continue comparison with available responses...
}
```

### 5.6 Block During Recovery

Scrubbing is blocked if PG is in recovery to avoid interference.

**Check in Scheduler**:
```cpp
void select_pgs_for_scrub(ScrubType type, std::vector<pg_id_t>& out) {
  for (auto& [pg_id, pg_info] : pg_map_) {
    auto state = pg_info.state.load(std::memory_order_acquire);

    // Skip if recovering
    if (state & static_cast<uint64_t>(PGStateMask::BASELINE_RESYNC)) {
      continue;  // Don't schedule scrub during recovery
    }

    // ... other checks ...
    out.push_back(pg_id);
  }
}
```

**Mid-Scrub Recovery Handling**:

If recovery starts mid-scrub, `can_continue_scrub()` detects it between batches:

```cpp
// In scrub loop
if (!can_continue_scrub(pg_id, type)) {
  // Recovery started - abort gracefully
  clear_scrubbing_flag(pg_id);
  task_state.status = TaskStatus::ABORTED;
  task_state.error_message = "PG entered recovery";
  persist_task_state(task_state);
  release_scrub_slot(type);
  return;
}
```

Scrub will be rescheduled based on normal intervals after recovery completes.

---

## 6. Scheduling & Initiation

### 6.1 Trigger Mechanisms

Scrubbing can be initiated in two ways: automatically via scheduler, or manually via HTTP API.

#### 6.1.1 Manual Trigger

HTTP API (see Section 9.1 for full API specification):

```cpp
// POST /scrub?pg_id=X&deep=true
void manual_scrub_trigger(pg_id_t pg_id, bool deep) {
  // Check if leader
  auto repl_dev = get_repl_dev(pg_id);
  if (!repl_dev->is_leader()) {
    throw std::runtime_error("Not leader for PG");
  }

  // Check resource limits
  ScrubType type = deep ? ScrubType::DEEP : ScrubType::SHALLOW;
  if (!try_reserve_scrub_slot(type)) {
    throw std::runtime_error("Resource limits exceeded");
  }

  // Generate task ID
  uint64_t task_id = generate_task_id();

  // Launch scrub asynchronously
  iomanager.run_on_wait_async(
    iomgr::reactor_regex::random_worker,
    [this, pg_id, type, task_id]() {
      execute_scrub(pg_id, type, task_id);
    }
  );

  // Return task_id to caller
  return task_id;
}
```

**Immediate execution**: Subject to resource limits only, bypasses scheduling intervals.

#### 6.1.2 Auto-Scheduled Trigger

Background scheduler runs every 10 minutes (configurable):

```cpp
void scrub_scheduler_task() {
  uint64_t now = get_current_time();

  // Select PGs needing shallow scrub
  auto shallow_pgs = select_pgs_for_scrub(ScrubType::SHALLOW, now);
  for (auto pg_id : shallow_pgs) {
    if (!try_reserve_scrub_slot(ScrubType::SHALLOW)) {
      break;  // Hit concurrency limit
    }
    schedule_pg_scrub(pg_id, ScrubType::SHALLOW);
  }

  // Select PGs needing deep scrub
  auto deep_pgs = select_pgs_for_scrub(ScrubType::DEEP, now);
  for (auto pg_id : deep_pgs) {
    if (!try_reserve_scrub_slot(ScrubType::DEEP)) {
      break;  // Hit concurrency limit
    }
    schedule_pg_scrub(pg_id, ScrubType::DEEP);
  }
}
```

**Respects**:
- Resource limits (concurrent scrub counts)
- NO_SCRUB flags
- PG state (skip if recovering)
- Leadership (only schedule on leader PGs)

### 6.2 Scheduling Intervals

Configuration parameters control when scrubs are scheduled:

```cpp
// Shallow scrub intervals
uint64_t scrub_min_interval_sec_ = 86400;         // 1 day
uint64_t scrub_max_interval_sec_ = 604800;        // 7 days (hard deadline)

// Deep scrub intervals
uint64_t scrub_deep_min_interval_sec_ = 604800;   // 7 days
uint64_t scrub_deep_max_interval_sec_ = 2592000;  // 30 days (hard deadline)

// Randomization to prevent thundering herd
float scrub_interval_randomize_ratio_ = 0.5;      // ±50% jitter

// Scheduler check frequency
uint64_t scrub_scheduler_interval_sec_ = 600;     // 10 minutes
```

**Scheduling Logic**:

```cpp
bool should_schedule_scrub(const pg_scrub_metadata& scrub_meta,
                           ScrubType type,
                           uint64_t now) {
  uint64_t last_time = (type == ScrubType::SHALLOW) ?
    scrub_meta.last_scrub_time : scrub_meta.last_deep_scrub_time;

  uint64_t max_interval = (type == ScrubType::SHALLOW) ?
    scrub_max_interval_sec_ : scrub_deep_max_interval_sec_;

  uint64_t min_interval = (type == ScrubType::SHALLOW) ?
    scrub_min_interval_sec_ : scrub_deep_min_interval_sec_;

  uint64_t time_since = now - last_time;

  // Hard deadline exceeded - MUST scrub
  if (time_since > max_interval) {
    return true;
  }

  // Calculate randomized target interval
  float jitter = random_float(
    -scrub_interval_randomize_ratio_,
     scrub_interval_randomize_ratio_
  );
  uint64_t target_interval = min_interval * (1.0 + jitter);

  // Scrub if past randomized target
  return time_since > target_interval;
}
```

**Randomization Example**:
- `min_interval = 1 day` (86400 sec)
- `randomize_ratio = 0.5` (±50%)
- **Jitter range**: ±43200 sec (±12 hours)
- **Actual schedule**: Between 12 hours and 36 hours after last scrub
- **Purpose**: Prevents all PGs created at the same time from scrubbing simultaneously

**Why Two Intervals?**
- `min_interval`: Target frequency (with randomization)
- `max_interval`: Hard deadline (never exceed this)
- Allows flexibility while guaranteeing scrub happens within acceptable timeframe

### 6.3 PG Selection Algorithm

Prioritize urgent PGs (past max_interval) first, then oldest scrub time.

```cpp
std::vector<pg_id_t> select_pgs_for_scrub(ScrubType type, uint64_t now) {
  std::vector<std::pair<pg_id_t, pg_scrub_metadata>> candidates;

  // Collect eligible PGs
  for (auto& [pg_id, pg_info] : pg_map_) {
    // Skip if not leader
    if (!pg_info.repl_dev_->is_leader()) {
      continue;
    }

    // Skip if manual disable flags set
    if (!can_continue_scrub(pg_id, type)) {
      continue;
    }

    // Skip if already scrubbing
    auto state = pg_info.state.load(std::memory_order_acquire);
    if (state & static_cast<uint64_t>(PGStateMask::SCRUBBING)) {
      continue;
    }

    // Skip if in recovery
    if (state & static_cast<uint64_t>(PGStateMask::BASELINE_RESYNC)) {
      continue;
    }

    // Check if scrub needed based on intervals
    if (should_schedule_scrub(pg_info.scrub_meta, type, now)) {
      candidates.push_back({pg_id, pg_info.scrub_meta});
    }
  }

  // Sort by priority
  std::sort(candidates.begin(), candidates.end(),
    [type, now, this](const auto& a, const auto& b) {
      uint64_t max_interval = (type == ScrubType::SHALLOW) ?
        scrub_max_interval_sec_ : scrub_deep_max_interval_sec_;

      uint64_t time_a = (type == ScrubType::SHALLOW) ?
        a.second.last_scrub_time : a.second.last_deep_scrub_time;
      uint64_t time_b = (type == ScrubType::SHALLOW) ?
        b.second.last_scrub_time : b.second.last_deep_scrub_time;

      uint64_t age_a = now - time_a;
      uint64_t age_b = now - time_b;

      bool urgent_a = (age_a > max_interval);
      bool urgent_b = (age_b > max_interval);

      // Urgent PGs first
      if (urgent_a != urgent_b) {
        return urgent_a;  // true sorts before false
      }

      // Otherwise oldest first
      return time_a < time_b;
    });

  // Extract PG IDs
  std::vector<pg_id_t> result;
  for (auto& [pg_id, _] : candidates) {
    result.push_back(pg_id);
  }

  return result;
}
```

**Priority Rules**:
1. **Urgent PGs first**: Past `max_interval` (hard deadline exceeded)
2. **Within urgent or non-urgent**: Oldest `last_scrub_time` first
3. **No INCONSISTENT boost**: Inconsistent PGs follow normal scheduling (admin can manually trigger if urgent)

**Why No Special Priority for INCONSISTENT PGs?**
- Inconsistency may be transient (replication lag)
- Next scheduled scrub will re-verify
- Admin can manually trigger if immediate re-verification needed
- Avoids scrub storms when transient issues affect many PGs

### 6.4 Scheduler Background Task

Register scheduler task during HomeObject initialization:

```cpp
void HSHomeObject::init_scrub_scheduler() {
  // Schedule recurring task
  iomanager.run_on_wait_async(
    iomgr::reactor_regex::random_worker,
    [this]() {
      scrub_scheduler_task();
    },
    std::chrono::seconds(scrub_scheduler_interval_sec_)
  );

  LOGI("Scrub scheduler initialized: check_interval={}s",
       scrub_scheduler_interval_sec_);
}
```

**Task Frequency**: Every 10 minutes (600 seconds)

**Why 10 Minutes?**
- **Cheap operation**: Timestamp comparisons only, no I/O
- **Responsive**: Detects scrub-due PGs within reasonable time
- **Low overhead**: ~0.001% CPU usage
- **Flexible**: Allows `min_interval` as low as 30 minutes while still being responsive

**Scheduler Shutdown**:

Gracefully stop scheduler on HomeObject shutdown:

```cpp
void HSHomeObject::shutdown_scrub_scheduler() {
  // Cancel recurring task
  iomanager.cancel_timer(scheduler_timer_handle_);

  // Wait for in-flight scrubs to complete or timeout
  wait_for_active_scrubs(std::chrono::seconds(30));

  LOGI("Scrub scheduler stopped");
}
```

### 6.5 Task ID Generation

Generate unique task IDs for tracking:

```cpp
// Option 1: Monotonic counter (simple, efficient)
std::atomic<uint64_t> next_task_id_{1};

uint64_t generate_task_id() {
  return next_task_id_.fetch_add(1, std::memory_order_relaxed);
}
```

**Pros**:
- Simple, fast
- No collisions
- Sequential ordering

**Cons**:
- Requires persistent counter state (add to metablk)
- Reset on instance restart (acceptable - task IDs are ephemeral)

```cpp
// Option 2: Timestamp-based (no state required)
uint64_t generate_task_id() {
  auto now = std::chrono::system_clock::now();
  auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
    now.time_since_epoch()
  ).count();
  return static_cast<uint64_t>(micros);
}
```

**Pros**:
- No persistent state
- Naturally time-ordered
- Human-readable (can decode to timestamp)

**Cons**:
- Collision risk if two scrubs start in same microsecond (very low probability)

**Recommendation**: Use monotonic counter (Option 1) for MVP, add UUID support post-MVP if needed.

### 6.6 Scrub Execution Entry Point

Common entry point for both manual and auto-scheduled scrubs:

```cpp
void schedule_pg_scrub(pg_id_t pg_id, ScrubType type) {
  // Atomically set SCRUBBING flag
  if (!try_start_scrub(pg_id)) {
    LOGW("PG {} already scrubbing, skipping", pg_id);
    release_scrub_slot(type);  // Release reserved slot
    return;
  }

  // Generate task ID
  uint64_t task_id = generate_task_id();

  // Create task state
  scrub_task_state task;
  task.task_id = task_id;
  task.pg_id = pg_id;
  task.type = type;
  task.start_time = get_current_time();
  task.status = TaskStatus::RUNNING;

  // Persist initial task state
  persist_task_state(task);

  // Update PG metadata
  auto pg = get_pg(pg_id);
  pg->scrub_meta.active_task_id = task_id;
  persist_pg_scrub_metadata(pg_id);

  // Launch scrub in background
  iomanager.run_on_wait_async(
    iomgr::reactor_regex::random_worker,
    [this, pg_id, type, task_id]() {
      try {
        execute_scrub_impl(pg_id, type, task_id);
      } catch (const std::exception& e) {
        LOGE("Scrub failed for pg={}: {}", pg_id, e.what());
        handle_scrub_failure(pg_id, task_id, e.what());
      }
    }
  );

  LOGI("Scrub task {} initiated for pg={}, type={}",
       task_id, pg_id, type == ScrubType::SHALLOW ? "shallow" : "deep");
}
```

**Key Steps**:
1. Atomically set SCRUBBING flag (prevents concurrent scrubs)
2. Generate unique task ID
3. Create and persist initial task state
4. Update PG metadata with active_task_id
5. Launch async scrub execution
6. Log initiation

**Error Handling**:
- If `try_start_scrub()` fails: PG already scrubbing, release slot and return
- If exception during execution: Caught in lambda, calls `handle_scrub_failure()`

---

## 7. Inconsistency Detection

### 7.1 Two-Phase Approach

Inconsistency detection uses a two-phase approach to filter transient replication lag from persistent inconsistencies.

#### Phase 1: Initial Batch Scrub

Scan entire PG and collect all potential inconsistencies:

```cpp
void execute_scrub_impl(pg_id_t pg_id, ScrubType type, uint64_t task_id) {
  auto task = load_task_state(task_id);
  std::vector<blob_id_t> questionable_blobs;  // Collect for spot-check

  // Iterate through all shards in PG
  for (auto shard_id : get_pg_shards(pg_id)) {
    blob_id_t current_blob_id = 0;
    blob_id_t max_blob_id = get_max_blob_id(shard_id);

    uint32_t batch_size = (type == ScrubType::SHALLOW) ?
      scrub_shallow_batch_size_ : scrub_deep_batch_size_;

    // Process batches
    while (current_blob_id < max_blob_id) {
      // Check if we should continue
      if (!can_continue_scrub(pg_id, type)) {
        mark_task_aborted(task_id);
        return;
      }

      // Query all replicas for this batch
      ScrubRequest req;
      req.pg_id = pg_id;
      req.shard_id = shard_id;
      req.start_blob_id = current_blob_id;
      req.batch_size = batch_size;
      req.type = type;

      auto responses = dispatch_scrub_request(pg_id, req, type).get();

      // Compare responses
      auto differences = compare_scrub_responses(responses, req);

      // Collect questionable blobs for spot-check
      for (auto& diff : differences) {
        questionable_blobs.push_back(diff.blob_id);
      }

      // Update progress
      task.blobs_scanned += batch_size;
      task.batches_processed++;

      // Throttle
      uint64_t sleep_us = (type == ScrubType::SHALLOW) ?
        scrub_sleep_shallow_us_ : scrub_deep_batch_size_;
      if (sleep_us > 0) {
        std::this_thread::sleep_for(std::chrono::microseconds(sleep_us));
      }

      current_blob_id += batch_size;
    }

    // Checkpoint after each shard (deep scrub only)
    if (type == ScrubType::DEEP) {
      task.current_shard = shard_id;
      task.current_blob_id = current_blob_id;
      persist_task_state(task);
    }
  }

  // Phase 2: Spot-check questionable blobs
  if (!questionable_blobs.empty()) {
    spot_check_blobs(pg_id, questionable_blobs, task);
  }

  // Finalize task
  finalize_scrub_task(pg_id, task);
}
```

#### Phase 2: Spot-Check

Re-verify questionable blobs after a delay to filter transient replication lag:

```cpp
void spot_check_blobs(pg_id_t pg_id,
                      const std::vector<blob_id_t>& questionable_blobs,
                      scrub_task_state& task) {
  // Wait for replication lag to settle
  std::this_thread::sleep_for(
    std::chrono::milliseconds(scrub_spot_check_delay_ms_)
  );

  // Group by shard
  std::map<shard_id_t, std::vector<blob_id_t>> by_shard;
  for (auto blob_id : questionable_blobs) {
    auto shard_id = extract_shard_id(blob_id);  // Encoded in blob_id
    by_shard[shard_id].push_back(blob_id);
  }

  // Spot-check each shard
  for (auto& [shard_id, blob_ids] : by_shard) {
    // Batch spot-check requests (max 25 blobs per request)
    for (size_t i = 0; i < blob_ids.size(); i += scrub_spot_check_batch_size_) {
      std::vector<blob_id_t> batch(
        blob_ids.begin() + i,
        blob_ids.begin() + std::min(i + scrub_spot_check_batch_size_,
                                     blob_ids.size())
      );

      SpotCheckRequest req;
      req.pg_id = pg_id;
      req.shard_id = shard_id;
      req.blob_ids = batch;

      auto responses = dispatch_spot_check_request(pg_id, req).get();

      // Analyze spot-check results
      analyze_spot_check_results(responses, req, task);
    }
  }
}
```

**Delay Tuning**:
- **Default**: 100ms
- **Too short** (< 50ms): High false positive rate (lag not fully resolved)
- **Too long** (> 500ms): Unnecessarily slows scrub completion
- **Rationale**: Typical Raft replication latency is 10-50ms; 100ms provides 2-5x margin

### 7.2 Response Comparison

Compare responses from all replicas to detect inconsistencies:

```cpp
struct BatchDifference {
  blob_id_t blob_id;
  std::map<peer_id_t, BlobStatus> replica_states;
  // BlobStatus: {exists, state (ALIVE/TOMBSTONE), optional<checksum>}
};

std::vector<BatchDifference>
compare_scrub_responses(const std::vector<ScrubResponse>& responses,
                        const ScrubRequest& req) {
  std::vector<BatchDifference> differences;

  // Build set of all blob_ids mentioned by any replica
  std::set<blob_id_t> all_blob_ids;
  for (auto& response : responses) {
    for (auto blob_id : response.blob_ids) {
      all_blob_ids.insert(blob_id);
    }
  }

  // Check each blob_id
  for (auto blob_id : all_blob_ids) {
    std::map<peer_id_t, BlobStatus> replica_states;
    bool inconsistent = false;

    // Collect status from each replica
    for (size_t i = 0; i < responses.size(); ++i) {
      auto& response = responses[i];

      BlobStatus status;
      auto it = std::find(response.blob_ids.begin(),
                          response.blob_ids.end(),
                          blob_id);

      if (it != response.blob_ids.end()) {
        status.exists = true;
        status.state = BlobState::ALIVE;  // Response only includes ALIVE blobs

        // For deep scrub, include checksum
        if (req.type == ScrubType::DEEP) {
          status.checksum = response.checksums[std::distance(
            response.blob_ids.begin(), it
          )];
        }
      } else {
        status.exists = false;
      }

      replica_states[response.peer_id] = status;
    }

    // Check for disagreement
    bool first = true;
    bool first_exists;
    std::optional<std::array<uint8_t, 32>> first_checksum;

    for (auto& [peer_id, status] : replica_states) {
      if (first) {
        first_exists = status.exists;
        first_checksum = status.checksum;
        first = false;
      } else {
        // Check existence mismatch
        if (status.exists != first_exists) {
          inconsistent = true;
          break;
        }

        // Check checksum mismatch (deep scrub only)
        if (req.type == ScrubType::DEEP &&
            status.exists &&
            status.checksum != first_checksum) {
          inconsistent = true;
          break;
        }
      }
    }

    if (inconsistent) {
      BatchDifference diff;
      diff.blob_id = blob_id;
      diff.replica_states = replica_states;
      differences.push_back(diff);
    }
  }

  return differences;
}
```

### 7.3 Spot-Check Analysis

Analyze spot-check results to classify persistent vs. transient inconsistencies:

```cpp
void analyze_spot_check_results(const std::vector<SpotCheckResponse>& responses,
                                 const SpotCheckRequest& req,
                                 scrub_task_state& task) {
  // Group by blob_id
  std::map<blob_id_t, std::map<peer_id_t, BlobStatus>> by_blob;

  for (auto& response : responses) {
    for (auto& blob_status : response.blobs) {
      by_blob[blob_status.blob_id][response.peer_id] = blob_status;
    }
  }

  // Analyze each blob
  for (auto& [blob_id, replica_states] : by_blob) {
    // Check if still inconsistent
    bool still_inconsistent = false;
    InconsistencyType type;

    // Check for existence mismatches
    bool first = true;
    bool first_exists;
    BlobState first_state;
    std::optional<std::array<uint8_t, 32>> first_checksum;

    for (auto& [peer_id, status] : replica_states) {
      if (first) {
        first_exists = status.exists;
        first_state = status.state;
        first_checksum = status.checksum;
        first = false;
      } else {
        // Existence mismatch
        if (status.exists != first_exists) {
          still_inconsistent = true;
          type = InconsistencyType::MISSING_BLOB;
          break;
        }

        // State mismatch (ALIVE vs TOMBSTONE)
        if (status.exists && status.state != first_state) {
          still_inconsistent = true;
          type = InconsistencyType::STATE_MISMATCH;
          break;
        }

        // Checksum mismatch (deep scrub only)
        if (status.exists &&
            status.checksum.has_value() &&
            status.checksum != first_checksum) {
          still_inconsistent = true;
          type = InconsistencyType::CHECKSUM_MISMATCH;
          break;
        }
      }
    }

    // Record persistent inconsistency
    if (still_inconsistent) {
      InconsistencyRecord record;
      record.shard_id = req.shard_id;
      record.blob_id = blob_id;
      record.type = type;
      record.replica_states = replica_states;
      record.detected_time = get_current_time();

      task.inconsistencies.push_back(record);

      LOGW("Persistent inconsistency detected: pg={}, shard={}, blob={}, type={}",
           req.pg_id, req.shard_id, blob_id, static_cast<int>(type));
    } else {
      LOGD("Transient difference resolved: pg={}, shard={}, blob={}",
           req.pg_id, req.shard_id, blob_id);
    }
  }
}
```

### 7.4 Inconsistency Classification

Three types of persistent inconsistencies are detected:

#### 7.4.1 Missing Blob

**Condition**: Some replicas have blob, others don't (after spot-check confirms)

**Example**:
```
Replica A: blob_id=12345 exists=true
Replica B: blob_id=12345 exists=true
Replica C: blob_id=12345 exists=false  ← Missing
```

**Possible Causes**:
- Replication failure (Raft log entry lost)
- Partial write (leader crashed mid-replication)
- Storage corruption on Replica C

#### 7.4.2 Checksum Mismatch

**Condition**: All replicas have blob, but checksums differ

**Example**:
```
Replica A: blob_id=67890 checksum=0xABCD1234...
Replica B: blob_id=67890 checksum=0xABCD1234...
Replica C: blob_id=67890 checksum=0xDEADBEEF...  ← Different
```

**Possible Causes**:
- Silent data corruption (bit rot)
- Disk error during write
- Non-deterministic bug in write path

#### 7.4.3 State Mismatch

**Condition**: Some replicas mark blob as TOMBSTONE, others as ALIVE

**Example**:
```
Replica A: blob_id=11111 state=ALIVE
Replica B: blob_id=11111 state=ALIVE
Replica C: blob_id=11111 state=TOMBSTONE  ← Different
```

**Possible Causes**:
- Delete operation failed on some replicas
- GC timing difference (post-MVP: may be valid eventual consistency)

### 7.5 Known Limitations

#### Post-GC Tombstone Ambiguity

After GC removes tombstones, there's ambiguity:

```
Replica A: blob NOT_FOUND (tombstone GC'd long ago)
Replica B: blob TOMBSTONE (not yet GC'd)
```

**Current Behavior**: Flagged as STATE_MISMATCH
**Correct Behavior**: May be valid eventual consistency
**Mitigation**: Admin reviews context (logs, timing)
**Post-MVP**: Add GC timestamp to differentiate

#### Two-Replica Quorum

With only 2 replicas, majority voting is impossible:

```
Replica A: blob checksum=0xAAAA
Replica B: blob checksum=0xBBBB
```

**Current Behavior**: Flag as CHECKSUM_MISMATCH, no automatic decision
**Correct Behavior**: Admin reviews logs to determine authoritative replica
**Post-MVP**: Add admin-configured authoritative source

### 7.6 Reporting

Store inconsistencies in task state for API queries:

```cpp
void finalize_scrub_task(pg_id_t pg_id, scrub_task_state& task) {
  task.end_time = get_current_time();
  task.status = TaskStatus::COMPLETED;

  // Update PG state
  auto pg = get_pg(pg_id);

  if (task.type == ScrubType::SHALLOW) {
    pg->scrub_meta.last_scrub_time = task.end_time;
  } else {
    pg->scrub_meta.last_deep_scrub_time = task.end_time;
  }

  // Set INCONSISTENT flag if issues found
  if (!task.inconsistencies.empty()) {
    pg->scrub_meta.state = pg_scrub_metadata::ScrubState::INCONSISTENT;
    pg->state.fetch_or(
      static_cast<uint64_t>(PGStateMask::INCONSISTENT),
      std::memory_order_release
    );

    LOGW("Scrub found {} inconsistencies for pg={}",
         task.inconsistencies.size(), pg_id);
  } else {
    pg->scrub_meta.state = pg_scrub_metadata::ScrubState::CLEAN;

    // Clear INCONSISTENT flag (auto-heal)
    pg->state.fetch_and(
      ~static_cast<uint64_t>(PGStateMask::INCONSISTENT),
      std::memory_order_release
    );

    LOGI("Scrub completed successfully for pg={}, no inconsistencies", pg_id);
  }

  // Clear active task ID
  pg->scrub_meta.active_task_id = std::nullopt;

  // Persist updates
  persist_pg_scrub_metadata(pg_id);
  persist_task_state(task);

  // Clear SCRUBBING flag
  pg->state.fetch_and(
    ~static_cast<uint64_t>(PGStateMask::SCRUBBING),
    std::memory_order_release
  );

  // Release concurrency slot
  release_scrub_slot(task.type);
}
```

**Auto-Heal**: If next scrub finds no inconsistencies, INCONSISTENT flag is automatically cleared. This handles transient issues that self-resolve.

---

## 8. Persistence & Recovery

### 8.1 Two-Level Persistence

Scrub state is persisted at two levels with different purposes and lifetimes.

#### 8.1.1 Level 1: PG Scrub Metadata

Stored inline in PG superblock (same metablk as PG info).

**Purpose**: Scheduling decisions, PG state flags
**Lifetime**: Permanent (tied to PG lifecycle)
**Updates**: After each scrub completion

```cpp
// Part of pg_info struct (Section 3.3)
struct pg_scrub_metadata {
  uint64_t last_scrub_time{0};
  uint64_t last_deep_scrub_time{0};
  ScrubState state{ScrubState::CLEAN};
  std::optional<uint64_t> active_task_id;
};
```

**Persist After Scrub Completion**:

```cpp
void persist_pg_scrub_metadata(pg_id_t pg_id) {
  auto pg = get_pg(pg_id);

  // Serialize pg_info (includes scrub_meta)
  auto pg_sb = homestore::superblk<pg_info>(
    fmt::format("pg_{}", pg_id)
  );

  pg_sb.write(pg->info);  // Atomic write
  pg_sb.commit();

  LOGD("Persisted PG scrub metadata: pg={}, last_scrub={}, last_deep_scrub={}",
       pg_id,
       pg->scrub_meta.last_scrub_time,
       pg->scrub_meta.last_deep_scrub_time);
}
```

**Load on PG Initialization**:

```cpp
void load_pg_scrub_metadata(pg_id_t pg_id) {
  auto pg_sb = homestore::superblk<pg_info>(
    fmt::format("pg_{}", pg_id)
  );

  if (pg_sb.exists()) {
    pg_info info;
    pg_sb.read(info);

    auto pg = get_pg(pg_id);
    pg->scrub_meta = info.scrub_meta;

    LOGI("Loaded PG scrub metadata: pg={}, last_scrub={}, state={}",
         pg_id,
         pg->scrub_meta.last_scrub_time,
         static_cast<int>(pg->scrub_meta.state));
  }
}
```

#### 8.1.2 Level 2: Scrub Task State

Stored as separate metablk per task.

**Purpose**: Task tracking, detailed findings, resume capability
**Lifetime**: Retention policy (100 tasks OR 7 days)
**Updates**:
- Initial: On task creation
- Periodic: Checkpoint after each shard (deep scrub only)
- Final: On task completion/failure/abort

```cpp
// Metablk key format
std::string get_task_metablk_name(uint64_t task_id) {
  return fmt::format("scrub_task_{}", task_id);
}
```

**Persist Task State**:

```cpp
void persist_task_state(const scrub_task_state& task) {
  auto task_sb = homestore::superblk<scrub_task_state>(
    get_task_metablk_name(task.task_id)
  );

  task_sb.write(task);
  task_sb.commit();

  LOGD("Persisted task state: task_id={}, status={}, blobs_scanned={}",
       task.task_id,
       static_cast<int>(task.status),
       task.blobs_scanned);
}
```

**Load Task State**:

```cpp
std::optional<scrub_task_state> load_task_state(uint64_t task_id) {
  auto task_sb = homestore::superblk<scrub_task_state>(
    get_task_metablk_name(task_id)
  );

  if (!task_sb.exists()) {
    return std::nullopt;
  }

  scrub_task_state task;
  task_sb.read(task);
  return task;
}
```

**Delete Task State** (for cleanup):

```cpp
void delete_task_state(uint64_t task_id) {
  auto task_sb = homestore::superblk<scrub_task_state>(
    get_task_metablk_name(task_id)
  );

  if (task_sb.exists()) {
    task_sb.destroy();
    LOGD("Deleted task state: task_id={}", task_id);
  }
}
```

### 8.2 Restart Behavior

Different strategies for shallow vs. deep scrub based on duration and resume value.

#### 8.2.1 Shallow Scrub: Abandon on Restart

**Rationale**: Too fast to benefit from resume (~40 sec for 10M blobs)

```cpp
void recover_shallow_scrub_tasks() {
  // Enumerate all PGs
  for (auto& [pg_id, pg_info] : pg_map_) {
    // Check if shallow scrub was running
    if (pg_info.scrub_meta.active_task_id.has_value()) {
      uint64_t task_id = pg_info.scrub_meta.active_task_id.value();
      auto task = load_task_state(task_id);

      if (task.has_value() &&
          task->type == ScrubType::SHALLOW &&
          task->status == TaskStatus::RUNNING) {
        // Mark as aborted
        task->status = TaskStatus::ABORTED;
        task->error_message = "Pod restarted during shallow scrub";
        task->end_time = get_current_time();
        persist_task_state(task.value());

        // Clear active task ID
        pg_info.scrub_meta.active_task_id = std::nullopt;
        persist_pg_scrub_metadata(pg_id);

        // Clear SCRUBBING flag
        pg_info.state.fetch_and(
          ~static_cast<uint64_t>(PGStateMask::SCRUBBING),
          std::memory_order_release
        );

        LOGI("Aborted shallow scrub task {} for pg={} after restart",
             task_id, pg_id);
      }
    }
  }

  // Reschedule based on last_scrub_time (scheduler will pick up)
}
```

**Next Scrub**: Scheduled normally based on `last_scrub_time` from PG metadata

#### 8.2.2 Deep Scrub: Resume from Last Checkpoint

**Rationale**: Deep scrub is slow (~12 hours for 10M blobs), resume saves significant time

```cpp
void recover_deep_scrub_tasks() {
  for (auto& [pg_id, pg_info] : pg_map_) {
    if (pg_info.scrub_meta.active_task_id.has_value()) {
      uint64_t task_id = pg_info.scrub_meta.active_task_id.value();
      auto task = load_task_state(task_id);

      if (task.has_value() &&
          task->type == ScrubType::DEEP &&
          task->status == TaskStatus::RUNNING) {

        // Check if still leader
        if (!pg_info.repl_dev_->is_leader()) {
          // No longer leader - abort
          task->status = TaskStatus::ABORTED;
          task->error_message = "Leadership lost during restart";
          task->end_time = get_current_time();
          persist_task_state(task.value());

          pg_info.scrub_meta.active_task_id = std::nullopt;
          persist_pg_scrub_metadata(pg_id);

          pg_info.state.fetch_and(
            ~static_cast<uint64_t>(PGStateMask::SCRUBBING),
            std::memory_order_release
          );

          LOGI("Aborted deep scrub task {} for pg={} - no longer leader",
               task_id, pg_id);
          continue;
        }

        // Resume from checkpoint
        LOGI("Resuming deep scrub task {} for pg={} from shard={}, blob={}",
             task_id, pg_id, task->current_shard, task->current_blob_id);

        // Reserve concurrency slot
        if (!try_reserve_scrub_slot(ScrubType::DEEP)) {
          LOGW("Cannot resume deep scrub task {} - resource limits exceeded",
               task_id);
          // Will be retried by scheduler
          continue;
        }

        // Launch resume
        iomanager.run_on_wait_async(
          iomgr::reactor_regex::random_worker,
          [this, pg_id, task_id, task_val = task.value()]() mutable {
            try {
              resume_deep_scrub(pg_id, task_val);
            } catch (const std::exception& e) {
              LOGE("Resume failed for task {}: {}", task_id, e.what());
              handle_scrub_failure(pg_id, task_id, e.what());
            }
          }
        );
      }
    }
  }
}
```

**Resume Implementation**:

```cpp
void resume_deep_scrub(pg_id_t pg_id, scrub_task_state task) {
  LOGI("Resuming deep scrub: pg={}, from shard={}, blob={}",
       pg_id, task.current_shard, task.current_blob_id);

  // Get PG shards
  auto shards = get_pg_shards(pg_id);

  // Find current shard position
  auto shard_it = std::find(shards.begin(), shards.end(), task.current_shard);

  if (shard_it == shards.end()) {
    // Shard no longer exists - start from beginning
    LOGW("Checkpoint shard {} not found, restarting from beginning",
         task.current_shard);
    task.current_shard = shards.front();
    task.current_blob_id = 0;
    shard_it = shards.begin();
  }

  // Continue from next shard (discard partial current shard)
  ++shard_it;

  // Process remaining shards
  for (; shard_it != shards.end(); ++shard_it) {
    auto shard_id = *shard_it;
    blob_id_t current_blob_id = 0;  // Start from beginning of shard
    blob_id_t max_blob_id = get_max_blob_id(shard_id);

    // Same batch processing as execute_scrub_impl...
    while (current_blob_id < max_blob_id) {
      if (!can_continue_scrub(pg_id, ScrubType::DEEP)) {
        mark_task_aborted(task.task_id);
        return;
      }

      // Process batch...
      // (same code as Section 7.1)

      current_blob_id += scrub_deep_batch_size_;
    }

    // Checkpoint after shard
    task.current_shard = shard_id;
    task.current_blob_id = current_blob_id;
    persist_task_state(task);
  }

  // Spot-check and finalize (same as normal completion)
  // ...
}
```

**Why Discard Partial Shard?**
- Simplifies resume logic (no mid-shard checkpoint)
- Wasted work is minimal (one shard out of many)
- Ensures complete coverage of each shard

### 8.3 Checkpoint Frequency

Different checkpointing strategies optimize for each scrub type.

#### Shallow Scrub: No Checkpoints

```cpp
// In execute_scrub_impl, after each shard:
if (type == ScrubType::SHALLOW) {
  // No checkpoint - too fast
} else {
  // Checkpoint for deep scrub
  task.current_shard = shard_id;
  task.current_blob_id = current_blob_id;
  persist_task_state(task);
}
```

**Rationale**: Shallow scrub completes in ~40 seconds, checkpointing overhead not justified

#### Deep Scrub: Per-Shard Checkpoints

```cpp
// After each shard completes
if (type == ScrubType::DEEP) {
  task.current_shard = shard_id;
  task.current_blob_id = max_blob_id;  // Completed this shard
  persist_task_state(task);

  LOGD("Checkpointed deep scrub: task_id={}, shard={}, progress={}/{}",
       task.task_id, shard_id,
       std::distance(shards.begin(), current_shard_it),
       shards.size());
}
```

**Checkpoint Interval Calculation**:
```
Shards per PG: 10-100 typical
Time per shard: ~7-70 minutes for 10M blobs
Checkpoint frequency: Every 7-70 minutes
Metablk write cost: ~1ms (negligible)
```

**Atomic Write Guarantee**: HomeStore metablk writes are atomic, crash during write leaves old state intact.

### 8.4 Task Retention & Cleanup

Periodic cleanup task removes old task metablks.

#### 8.4.1 Retention Policy

```cpp
// Configuration (Section 10)
uint32_t scrub_task_retention_count_ = 100;  // Keep last N tasks
uint64_t scrub_task_retention_days_ = 7;     // Keep tasks from last N days

// Retain if EITHER condition is met
bool should_retain_task(const scrub_task_state& task, uint64_t now) {
  // Condition 1: Within task count limit (most recent 100)
  // Condition 2: Within time limit (last 7 days)

  if (task.end_time == 0) {
    return true;  // Keep running tasks
  }

  uint64_t age_seconds = now - task.end_time;
  uint64_t age_days = age_seconds / 86400;

  return (age_days < scrub_task_retention_days_);
  // Note: task_index check done during enumeration
}
```

#### 8.4.2 Cleanup Task

```cpp
void cleanup_old_scrub_tasks() {
  LOGI("Starting scrub task cleanup");

  // Enumerate all task metablks
  std::vector<std::pair<uint64_t, scrub_task_state>> all_tasks;

  homestore::MetaBlkMgr::instance()->for_each_metablk(
    [&all_tasks](const std::string& name, const auto& blk) {
      if (name.starts_with("scrub_task_")) {
        uint64_t task_id = std::stoull(name.substr(11));  // Extract ID
        scrub_task_state task;
        blk.read(task);
        all_tasks.push_back({task_id, task});
      }
    }
  );

  // Sort by end_time (oldest first)
  std::sort(all_tasks.begin(), all_tasks.end(),
    [](const auto& a, const auto& b) {
      return a.second.end_time < b.second.end_time;
    });

  uint64_t now = get_current_time();
  size_t retained = 0;
  size_t deleted = 0;

  // Keep most recent N tasks
  size_t keep_count = std::min(
    all_tasks.size(),
    static_cast<size_t>(scrub_task_retention_count_)
  );

  for (size_t i = 0; i < all_tasks.size(); ++i) {
    auto& [task_id, task] = all_tasks[i];
    bool in_recent_n = (all_tasks.size() - i <= keep_count);
    bool within_time_limit = should_retain_task(task, now);

    if (in_recent_n || within_time_limit) {
      retained++;
    } else {
      delete_task_state(task_id);
      deleted++;
    }
  }

  LOGI("Scrub task cleanup complete: retained={}, deleted={}",
       retained, deleted);
}
```

#### 8.4.3 Cleanup Scheduling

```cpp
void init_task_cleanup_scheduler() {
  // Run daily
  iomanager.run_on_wait_async(
    iomgr::reactor_regex::random_worker,
    [this]() { cleanup_old_scrub_tasks(); },
    std::chrono::hours(24)
  );

  LOGI("Task cleanup scheduler initialized: interval=24h");
}
```

**Frequency**: Daily (low overhead, infrequent enough to not impact performance)

### 8.5 Crash Consistency

HomeStore metablk operations provide crash consistency guarantees.

#### Atomic Write Guarantees

```cpp
// Metablk write is atomic
task_sb.write(task);   // Prepare new state
task_sb.commit();      // Atomic switch to new state

// Crash scenarios:
// 1. Crash before commit: Old state remains
// 2. Crash during commit: Atomic (either old or new, never partial)
// 3. Crash after commit: New state persisted
```

**Verification** (from HomeStore documentation):
- Location: `HomeStore/src/lib/meta/meta_blk_mgr.cpp`
- Metablk uses double-buffering with atomic pointer swap
- Crash-safe via journal + checksum validation

#### Recovery Validation

```cpp
void validate_task_state_on_load(const scrub_task_state& task) {
  // Sanity checks
  if (task.task_id == 0) {
    throw std::runtime_error("Invalid task_id");
  }

  if (task.end_time > 0 && task.end_time < task.start_time) {
    throw std::runtime_error("end_time before start_time");
  }

  if (task.status == TaskStatus::RUNNING && task.end_time != 0) {
    throw std::runtime_error("RUNNING task has end_time");
  }

  // Checksum validation (done by HomeStore automatically)
}
```

**Corrupt Metablk Handling**:
- HomeStore detects corruption via checksum
- Returns error on read
- Scrubber treats as task not found, reschedules based on PG metadata

---

## 9. Message Formats & APIs

### 9.1 HTTP REST API

The scrubber exposes an HTTP REST API for manual control and monitoring.

#### 9.1.1 Manual Scrub Trigger

**Endpoint**: `POST /scrub`

**Query Parameters**:
- `pg_id` (required): PG identifier
- `deep` (optional): `true` for deep scrub, `false` for shallow (default: `false`)

**Request Body**: None

**Response** (200 OK):
```json
{
  "status": "success",
  "task_id": 12345,
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

**Implementation**:
```cpp
void handle_scrub_trigger(const HttpRequest& req, HttpResponse& resp) {
  // Parse parameters
  auto pg_id = req.get_query_param<pg_id_t>("pg_id");
  bool deep = req.get_query_param<bool>("deep", false);

  if (!pg_id.has_value()) {
    resp.set_status(400);
    resp.set_body(R"({"status":"error","message":"Missing pg_id"})");
    return;
  }

  try {
    uint64_t task_id = manual_scrub_trigger(pg_id.value(), deep);

    nlohmann::json response = {
      {"status", "success"},
      {"task_id", task_id},
      {"pg_id", pg_id.value()},
      {"type", deep ? "deep" : "shallow"},
      {"message", "Scrub task initiated"}
    };

    resp.set_status(200);
    resp.set_body(response.dump());

  } catch (const std::exception& e) {
    resp.set_status(503);
    nlohmann::json error = {
      {"status", "error"},
      {"message", e.what()}
    };
    resp.set_body(error.dump());
  }
}
```

**cURL Example**:
```bash
# Trigger shallow scrub
curl -X POST "http://localhost:8080/scrub?pg_id=42"

# Trigger deep scrub
curl -X POST "http://localhost:8080/scrub?pg_id=42&deep=true"
```

#### 9.1.2 Query Task Status

**Endpoint**: `GET /scrub/task/{task_id}`

**Response** (200 OK):
```json
{
  "task_id": 12345,
  "pg_id": 42,
  "type": "deep",
  "status": "running",
  "progress": {
    "current_shard": 3,
    "total_shards": 10,
    "blobs_scanned": 1500000,
    "percent_complete": 30
  },
  "start_time": "2025-12-14T10:00:00Z",
  "inconsistencies_found": 0
}
```

**Status Values**:
- `running`: Scrub in progress
- `completed`: Scrub finished successfully
- `failed`: Scrub encountered unrecoverable error
- `aborted`: Scrub canceled (recovery started, pod restarted, etc.)

**Implementation**:
```cpp
void handle_task_status(const HttpRequest& req, HttpResponse& resp) {
  uint64_t task_id = req.get_path_param<uint64_t>("task_id");

  auto task = load_task_state(task_id);
  if (!task.has_value()) {
    resp.set_status(404);
    resp.set_body(R"({"status":"error","message":"Task not found"})");
    return;
  }

  auto pg = get_pg(task->pg_id);
  auto shards = get_pg_shards(task->pg_id);

  nlohmann::json response = {
    {"task_id", task->task_id},
    {"pg_id", task->pg_id},
    {"type", task->type == ScrubType::SHALLOW ? "shallow" : "deep"},
    {"status", task_status_to_string(task->status)},
    {"start_time", unix_to_iso8601(task->start_time)},
    {"inconsistencies_found", task->inconsistencies.size()}
  };

  if (task->status == TaskStatus::RUNNING) {
    // Calculate progress
    size_t total_shards = shards.size();
    size_t current_shard_index = 0;
    for (size_t i = 0; i < shards.size(); ++i) {
      if (shards[i] == task->current_shard) {
        current_shard_index = i;
        break;
      }
    }

    uint32_t percent = (current_shard_index * 100) / total_shards;

    response["progress"] = {
      {"current_shard", task->current_shard},
      {"total_shards", total_shards},
      {"blobs_scanned", task->blobs_scanned},
      {"percent_complete", percent}
    };
  }

  if (task->end_time > 0) {
    response["end_time"] = unix_to_iso8601(task->end_time);
    response["duration_seconds"] = task->end_time - task->start_time;
  }

  resp.set_status(200);
  resp.set_body(response.dump());
}
```

#### 9.1.3 Query Inconsistency Details

**Endpoint**: `GET /scrub/task/{task_id}/inconsistencies`

**Response** (200 OK):
```json
{
  "task_id": 12345,
  "pg_id": 42,
  "total_inconsistencies": 2,
  "inconsistencies": [
    {
      "shard_id": 5,
      "blob_id": 10023,
      "type": "checksum_mismatch",
      "detected_time": "2025-12-14T12:30:00Z",
      "replicas": {
        "uuid-replica-1": {
          "exists": true,
          "checksum": "abc123...",
          "blob_size": 4096
        },
        "uuid-replica-2": {
          "exists": true,
          "checksum": "def456...",
          "blob_size": 4096
        }
      }
    },
    {
      "shard_id": 7,
      "blob_id": 50234,
      "type": "missing_blob",
      "detected_time": "2025-12-14T12:35:00Z",
      "replicas": {
        "uuid-replica-1": {"exists": true},
        "uuid-replica-2": {"exists": false}
      }
    }
  ]
}
```

**Implementation**:
```cpp
void handle_inconsistency_details(const HttpRequest& req, HttpResponse& resp) {
  uint64_t task_id = req.get_path_param<uint64_t>("task_id");

  auto task = load_task_state(task_id);
  if (!task.has_value()) {
    resp.set_status(404);
    resp.set_body(R"({"status":"error","message":"Task not found"})");
    return;
  }

  nlohmann::json response = {
    {"task_id", task->task_id},
    {"pg_id", task->pg_id},
    {"total_inconsistencies", task->inconsistencies.size()},
    {"inconsistencies", nlohmann::json::array()}
  };

  for (auto& inc : task->inconsistencies) {
    nlohmann::json inc_json = {
      {"shard_id", inc.shard_id},
      {"blob_id", inc.blob_id},
      {"type", inconsistency_type_to_string(inc.type)},
      {"detected_time", unix_to_iso8601(inc.detected_time)},
      {"replicas", nlohmann::json::object()}
    };

    for (auto& [peer_id, status] : inc.replica_states) {
      nlohmann::json replica_json = {{"exists", status.exists}};

      if (status.exists) {
        replica_json["state"] = blob_state_to_string(status.state);
        if (status.blob_size.has_value()) {
          replica_json["blob_size"] = status.blob_size.value();
        }
        if (status.checksum.has_value()) {
          replica_json["checksum"] = checksum_to_hex(status.checksum.value());
        }
      }

      inc_json["replicas"][peer_id.to_string()] = replica_json;
    }

    response["inconsistencies"].push_back(inc_json);
  }

  resp.set_status(200);
  resp.set_body(response.dump());
}
```

#### 9.1.4 Disable/Enable Scrubbing

**Disable Endpoint**: `POST /scrub/disable`

**Query Parameters**:
- `deep_only` (optional): `true` to disable only deep scrubs, `false` to disable all (default: `false`)

**Response** (200 OK):
```json
{
  "status": "success",
  "message": "Scrubbing disabled",
  "no_scrub": true,
  "no_deep_scrub": false
}
```

**Enable Endpoint**: `DELETE /scrub/disable`

**Query Parameters**:
- `deep_only` (optional): `true` to enable only deep scrubs, `false` to enable all (default: `false`)

**Implementation**:
```cpp
void handle_disable_scrubbing(const HttpRequest& req, HttpResponse& resp) {
  bool deep_only = req.get_query_param<bool>("deep_only", false);

  if (deep_only) {
    no_deep_scrub_.store(true, std::memory_order_release);
  } else {
    no_scrub_.store(true, std::memory_order_release);
  }

  nlohmann::json response = {
    {"status", "success"},
    {"message", deep_only ? "Deep scrubbing disabled" : "Scrubbing disabled"},
    {"no_scrub", no_scrub_.load(std::memory_order_acquire)},
    {"no_deep_scrub", no_deep_scrub_.load(std::memory_order_acquire)}
  };

  resp.set_status(200);
  resp.set_body(response.dump());
}

void handle_enable_scrubbing(const HttpRequest& req, HttpResponse& resp) {
  bool deep_only = req.get_query_param<bool>("deep_only", false);

  if (deep_only) {
    no_deep_scrub_.store(false, std::memory_order_release);
  } else {
    no_scrub_.store(false, std::memory_order_release);
  }

  nlohmann::json response = {
    {"status", "success"},
    {"message", deep_only ? "Deep scrubbing enabled" : "Scrubbing enabled"},
    {"no_scrub", no_scrub_.load(std::memory_order_acquire)},
    {"no_deep_scrub", no_deep_scrub_.load(std::memory_order_acquire)}
  };

  resp.set_status(200);
  resp.set_body(response.dump());
}
```

**cURL Examples**:
```bash
# Disable all scrubbing
curl -X POST "http://localhost:8080/scrub/disable"

# Disable only deep scrubs
curl -X POST "http://localhost:8080/scrub/disable?deep_only=true"

# Enable all scrubbing
curl -X DELETE "http://localhost:8080/scrub/disable"
```

#### 9.1.5 Query Global Scrub Status

**Endpoint**: `GET /scrub/status`

**Response** (200 OK):
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

**Implementation**:
```cpp
void handle_scrub_status(const HttpRequest& req, HttpResponse& resp) {
  nlohmann::json response = {
    {"scrubbing_enabled", !no_scrub_.load(std::memory_order_acquire)},
    {"deep_scrub_enabled", !no_deep_scrub_.load(std::memory_order_acquire)},
    {"active_scrubs", active_scrub_count_.load(std::memory_order_relaxed)},
    {"active_deep_scrubs", active_deep_scrub_count_.load(std::memory_order_relaxed)},
    {"max_concurrent_scrubs", max_concurrent_scrubs_},
    {"max_concurrent_deep_scrubs", max_concurrent_deep_scrubs_},
    {"config", {
      {"shallow_batch_size", scrub_shallow_batch_size_},
      {"deep_batch_size", scrub_deep_batch_size_},
      {"shallow_sleep_us", scrub_sleep_shallow_us_},
      {"deep_sleep_us", scrub_sleep_deep_us_},
      {"shallow_timeout_sec", scrub_shallow_timeout_sec_},
      {"deep_timeout_sec", scrub_deep_timeout_sec_}
    }}
  };

  resp.set_status(200);
  resp.set_body(response.dump());
}
```

#### 9.1.6 Query PG Scrub Metadata

**Endpoint**: `GET /pg/{pg_id}/scrub`

**Response** (200 OK):
```json
{
  "pg_id": 42,
  "state": "clean",
  "last_scrub_time": "2025-12-13T10:00:00Z",
  "last_deep_scrub_time": "2025-12-10T08:00:00Z",
  "active_task_id": null,
  "next_scrub_due": "2025-12-14T15:30:00Z",
  "next_deep_scrub_due": "2025-12-17T12:00:00Z"
}
```

**State Values**:
- `clean`: No known inconsistencies
- `inconsistent`: Inconsistencies detected in last scrub
- `scrubbing`: Scrub currently in progress

**Implementation**:
```cpp
void handle_pg_scrub_metadata(const HttpRequest& req, HttpResponse& resp) {
  pg_id_t pg_id = req.get_path_param<pg_id_t>("pg_id");

  auto pg = get_pg(pg_id);
  if (!pg) {
    resp.set_status(404);
    resp.set_body(R"({"status":"error","message":"PG not found"})");
    return;
  }

  auto state = pg->state.load(std::memory_order_acquire);
  std::string state_str = "clean";
  if (state & static_cast<uint64_t>(PGStateMask::SCRUBBING)) {
    state_str = "scrubbing";
  } else if (state & static_cast<uint64_t>(PGStateMask::INCONSISTENT)) {
    state_str = "inconsistent";
  }

  // Calculate next scrub due times
  uint64_t now = get_current_time();
  uint64_t next_shallow = pg->scrub_meta.last_scrub_time +
                          scrub_max_interval_sec_;
  uint64_t next_deep = pg->scrub_meta.last_deep_scrub_time +
                       scrub_deep_max_interval_sec_;

  nlohmann::json response = {
    {"pg_id", pg_id},
    {"state", state_str},
    {"last_scrub_time", unix_to_iso8601(pg->scrub_meta.last_scrub_time)},
    {"last_deep_scrub_time", unix_to_iso8601(pg->scrub_meta.last_deep_scrub_time)},
    {"active_task_id", pg->scrub_meta.active_task_id.has_value() ?
                       nlohmann::json(pg->scrub_meta.active_task_id.value()) :
                       nlohmann::json(nullptr)},
    {"next_scrub_due", unix_to_iso8601(next_shallow)},
    {"next_deep_scrub_due", unix_to_iso8601(next_deep)}
  };

  resp.set_status(200);
  resp.set_body(response.dump());
}
```

### 9.2 Internal nuraft_messenger APIs

Communication between leader and followers uses nuraft_messenger data service.

#### 9.2.1 Handler Registration

Register scrub handlers during PG creation:

```cpp
void HSHomeObject::register_scrub_handlers(pg_id_t pg_id) {
  auto group_id = pg_to_group_id(pg_id);
  auto repl_dev = get_repl_dev(pg_id);
  auto msg_svc = repl_dev->group_msg_service();

  // Shallow scrub handler
  msg_svc->bind_data_service_request(
    "scrub_get_index",
    group_id,
    [this](intrusive_ptr<GenericRpcData>& rpc_data) {
      on_scrub_index_request(rpc_data);
    }
  );

  // Deep scrub handler
  msg_svc->bind_data_service_request(
    "scrub_get_checksums",
    group_id,
    [this](intrusive_ptr<GenericRpcData>& rpc_data) {
      on_scrub_checksum_request(rpc_data);
    }
  );

  // Spot check handler
  msg_svc->bind_data_service_request(
    "scrub_spot_check",
    group_id,
    [this](intrusive_ptr<GenericRpcData>& rpc_data) {
      on_scrub_spot_check_request(rpc_data);
    }
  );

  LOGD("Registered scrub handlers for pg={}", pg_id);
}
```

#### 9.2.2 Message Encoding/Decoding

Use simple binary serialization for efficiency:

```cpp
// Shallow scrub request
struct ScrubRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  uint32_t batch_size;
  ScrubType type;

  // Serialize to binary blob
  sisl::byte_array serialize() const {
    sisl::byte_array blob(sizeof(ScrubRequest));
    std::memcpy(blob.bytes(), this, sizeof(ScrubRequest));
    return blob;
  }

  // Deserialize from binary blob
  static ScrubRequest deserialize(const sisl::byte_view& blob) {
    ScrubRequest req;
    std::memcpy(&req, blob.bytes(), sizeof(ScrubRequest));
    return req;
  }
};

// Shallow scrub response
struct ScrubResponse {
  peer_id_t peer_id;
  shard_id_t shard_id;
  blob_id_t start_blob_id;
  std::vector<blob_id_t> blob_ids;  // ALIVE blobs only

  sisl::byte_array serialize() const {
    size_t size = sizeof(peer_id_t) + sizeof(shard_id_t) +
                  sizeof(blob_id_t) + sizeof(uint32_t) +
                  (blob_ids.size() * sizeof(blob_id_t));

    sisl::byte_array blob(size);
    uint8_t* ptr = blob.bytes();

    std::memcpy(ptr, &peer_id, sizeof(peer_id_t));
    ptr += sizeof(peer_id_t);

    std::memcpy(ptr, &shard_id, sizeof(shard_id_t));
    ptr += sizeof(shard_id_t);

    std::memcpy(ptr, &start_blob_id, sizeof(blob_id_t));
    ptr += sizeof(blob_id_t);

    uint32_t count = blob_ids.size();
    std::memcpy(ptr, &count, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    std::memcpy(ptr, blob_ids.data(), count * sizeof(blob_id_t));

    return blob;
  }

  static ScrubResponse deserialize(const sisl::byte_view& blob) {
    ScrubResponse resp;
    const uint8_t* ptr = blob.bytes();

    std::memcpy(&resp.peer_id, ptr, sizeof(peer_id_t));
    ptr += sizeof(peer_id_t);

    std::memcpy(&resp.shard_id, ptr, sizeof(shard_id_t));
    ptr += sizeof(shard_id_t);

    std::memcpy(&resp.start_blob_id, ptr, sizeof(blob_id_t));
    ptr += sizeof(blob_id_t);

    uint32_t count;
    std::memcpy(&count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    resp.blob_ids.resize(count);
    std::memcpy(resp.blob_ids.data(), ptr, count * sizeof(blob_id_t));

    return resp;
  }
};
```

**Payload Sizes**:
- Shallow scrub request: 28 bytes (fixed)
- Shallow scrub response: 20 bytes + (N × 8 bytes) ≈ 4KB for 500 blobs
- Deep scrub response: 20 bytes + (N × 80 bytes) ≈ 2KB for 25 blobs

#### 9.2.3 Leader Request Dispatch

Dispatch scrub request to all replicas (Section 5.5 for full code with timeout handling).

**Simplified Version**:
```cpp
folly::Future<std::vector<ScrubResponse>>
dispatch_scrub_request(pg_id_t pg_id, const ScrubRequest& req) {
  auto repl_dev = get_repl_dev(pg_id);
  auto peers = repl_dev->get_replication_status().members;
  auto msg_svc = repl_dev->group_msg_service();

  std::vector<folly::SemiFuture<GenericClientResponse>> futures;

  for (auto& peer : peers) {
    auto request_blob = req.serialize();
    auto future = msg_svc->data_service_request_bidirectional(
      peer.id,
      "scrub_get_index",
      request_blob
    );
    futures.push_back(std::move(future));
  }

  return folly::collectAll(std::move(futures))
    .via(folly::getCPUExecutor())
    .thenValue([](std::vector<folly::Try<GenericClientResponse>>&& results) {
      std::vector<ScrubResponse> responses;
      for (auto& result : results) {
        if (result.hasValue()) {
          responses.push_back(
            ScrubResponse::deserialize(result.value().response_blob())
          );
        }
      }
      return responses;
    });
}
```

#### 9.2.4 Follower Request Handler

Handle scrub request from leader (Section 4.2 for full code with reactor dispatch).

**Key Pattern**:
1. Decode request in service thread
2. Check concurrency limits (atomic operations safe)
3. Dispatch to reactor for index access
4. Query index table
5. Encode and send response
6. Release concurrency slot

---

## 10. Configuration

### 10.1 Configuration Parameters

All scrub configuration parameters with defaults and descriptions.

#### 10.1.1 Batch Sizes

```cpp
// Shallow scrub batch size
uint32_t scrub_shallow_batch_size = 500;

// Deep scrub batch size
uint32_t scrub_deep_batch_size = 25;
```

**Description**:
- `scrub_shallow_batch_size`: Number of blobs to query per batch for shallow scrub (index-only reads)
- `scrub_deep_batch_size`: Number of blobs to verify per batch for deep scrub (data reads + checksum)

**Tuning Guidelines**:
- **Shallow**: Increase for faster completion (up to 1000), decrease if network bandwidth limited
- **Deep**: Keep at 25 (Ceph-validated), decrease if I/O impact too high

**Valid Range**:
- Shallow: 100 - 2000
- Deep: 10 - 100

#### 10.1.2 Sleep Intervals

```cpp
// Sleep between shallow scrub batches (microseconds)
uint64_t scrub_sleep_shallow_us = 0;

// Sleep between deep scrub batches (microseconds)
uint64_t scrub_sleep_deep_us = 10000;  // 10ms
```

**Description**:
- `scrub_sleep_shallow_us`: Delay between shallow scrub batches to yield I/O bandwidth
- `scrub_sleep_deep_us`: Delay between deep scrub batches to yield I/O bandwidth

**Tuning Guidelines**:
- **Increase if**: Client P99 latency increases > 5% during scrub
- **Decrease if**: Scrub completion time too slow (not meeting max_interval)
- **Monitor**: Client latency impact, scrub duration metrics

**Valid Range**: 0 - 1000000 (0 - 1 second)

#### 10.1.3 Timeouts

```cpp
// Timeout for shallow scrub RPC (seconds)
uint64_t scrub_shallow_timeout_sec = 5;

// Timeout for deep scrub RPC (seconds)
uint64_t scrub_deep_timeout_sec = 60;

// Timeout for spot-check RPC (seconds)
uint64_t scrub_spot_check_timeout_sec = 10;
```

**Description**:
- `scrub_shallow_timeout_sec`: Max time to wait for replica response (index query)
- `scrub_deep_timeout_sec`: Max time to wait for replica response (data read + checksum)
- `scrub_spot_check_timeout_sec`: Max time to wait for spot-check response

**Tuning Guidelines**:
- **Increase if**: Frequent timeout errors in logs, large blobs
- **Decrease if**: Want faster failure detection

**Valid Range**: 1 - 300 (1 sec - 5 min)

#### 10.1.4 Concurrency Limits

```cpp
// Maximum concurrent scrubs (shallow + deep)
uint32_t max_concurrent_scrubs = 3;

// Maximum concurrent deep scrubs (subset of total)
uint32_t max_concurrent_deep_scrubs = 1;
```

**Description**:
- `max_concurrent_scrubs`: Total scrubs allowed simultaneously on this instance
- `max_concurrent_deep_scrubs`: Deep scrubs allowed simultaneously (subset of total)

**Tuning Guidelines**:
- **Conservative start**: 1 total, 1 deep
- **Increase if**: Scrubs not completing within max_interval AND I/O headroom available
- **Decrease if**: Client latency impact too high

**Valid Range**: 1 - 10

**Constraint**: `max_concurrent_deep_scrubs <= max_concurrent_scrubs`

#### 10.1.5 Scheduling Intervals

```cpp
// Shallow scrub intervals (seconds)
uint64_t scrub_min_interval_sec = 86400;        // 1 day
uint64_t scrub_max_interval_sec = 604800;       // 7 days

// Deep scrub intervals (seconds)
uint64_t scrub_deep_min_interval_sec = 604800;  // 7 days
uint64_t scrub_deep_max_interval_sec = 2592000; // 30 days

// Randomization factor (0.0 - 1.0)
float scrub_interval_randomize_ratio = 0.5;     // ±50%

// Scheduler check frequency (seconds)
uint64_t scrub_scheduler_interval_sec = 600;    // 10 minutes
```

**Description**:
- `scrub_min_interval_sec`: Target time between shallow scrubs (with randomization)
- `scrub_max_interval_sec`: Hard deadline for shallow scrubs (never exceed this)
- `scrub_deep_min_interval_sec`: Target time between deep scrubs
- `scrub_deep_max_interval_sec`: Hard deadline for deep scrubs
- `scrub_interval_randomize_ratio`: Jitter to prevent thundering herd (±50% of min_interval)
- `scrub_scheduler_interval_sec`: How often scheduler checks for scrub-due PGs

**Tuning Guidelines**:
- **Increase intervals if**: Scrub impact too high, cluster under heavy load
- **Decrease intervals if**: Need more frequent verification, scrub duration << interval
- **Randomization**: Keep at 0.5 to spread load

**Valid Range**:
- min_interval: 3600 - 604800 (1 hour - 7 days)
- max_interval: min_interval - 2592000 (min - 30 days)
- randomize_ratio: 0.0 - 1.0
- scheduler_interval: 60 - 3600 (1 min - 1 hour)

**Constraint**: `max_interval > min_interval`

#### 10.1.6 Task Retention

```cpp
// Keep last N scrub tasks
uint32_t scrub_task_retention_count = 100;

// Keep tasks from last N days
uint64_t scrub_task_retention_days = 7;
```

**Description**:
- `scrub_task_retention_count`: Keep most recent N tasks (regardless of age)
- `scrub_task_retention_days`: Keep all tasks from last N days (regardless of count)

**Retention Logic**: Task is retained if **EITHER** condition is met

**Tuning Guidelines**:
- **Increase count if**: Need longer task history for debugging
- **Increase days if**: Need to retain old inconsistency records
- **Decrease if**: Metablk storage space limited

**Valid Range**:
- count: 10 - 1000
- days: 1 - 90

#### 10.1.7 Spot-Check

```cpp
// Delay before spot-check (milliseconds)
uint64_t scrub_spot_check_delay_ms = 100;

// Batch size for spot-check requests
uint32_t scrub_spot_check_batch_size = 25;
```

**Description**:
- `scrub_spot_check_delay_ms`: Wait time to allow replication lag to settle
- `scrub_spot_check_batch_size`: Number of blobs to verify per spot-check batch

**Tuning Guidelines**:
- **Increase delay if**: High false positive rate (many transient differences)
- **Decrease delay if**: Acceptable false positive rate, want faster completion

**Valid Range**:
- delay: 50 - 500 (50ms - 500ms)
- batch_size: 10 - 100

### 10.2 Configuration File Format

Example YAML configuration:

```yaml
# HomeObject configuration
homeobject:
  # ... other config ...

  scrubber:
    # Batch sizes
    shallow_batch_size: 500
    deep_batch_size: 25

    # Sleep intervals (microseconds)
    sleep_shallow_us: 0
    sleep_deep_us: 10000

    # Timeouts (seconds)
    timeout_shallow_sec: 5
    timeout_deep_sec: 60
    timeout_spot_check_sec: 10

    # Concurrency limits
    max_concurrent_scrubs: 3
    max_concurrent_deep_scrubs: 1

    # Scheduling intervals (seconds)
    scrub_min_interval_sec: 86400       # 1 day
    scrub_max_interval_sec: 604800      # 7 days
    deep_min_interval_sec: 604800       # 7 days
    deep_max_interval_sec: 2592000      # 30 days
    interval_randomize_ratio: 0.5       # ±50%
    scheduler_interval_sec: 600         # 10 minutes

    # Task retention
    task_retention_count: 100
    task_retention_days: 7

    # Spot-check
    spot_check_delay_ms: 100
    spot_check_batch_size: 25
```

### 10.3 Configuration Loading

Load configuration on HomeObject initialization:

```cpp
struct ScrubConfig {
  // Batch sizes
  uint32_t shallow_batch_size{500};
  uint32_t deep_batch_size{25};

  // Sleep intervals (microseconds)
  uint64_t sleep_shallow_us{0};
  uint64_t sleep_deep_us{10000};

  // Timeouts (seconds)
  uint64_t timeout_shallow_sec{5};
  uint64_t timeout_deep_sec{60};
  uint64_t timeout_spot_check_sec{10};

  // Concurrency limits
  uint32_t max_concurrent_scrubs{3};
  uint32_t max_concurrent_deep_scrubs{1};

  // Scheduling intervals (seconds)
  uint64_t scrub_min_interval_sec{86400};
  uint64_t scrub_max_interval_sec{604800};
  uint64_t deep_min_interval_sec{604800};
  uint64_t deep_max_interval_sec{2592000};
  float interval_randomize_ratio{0.5f};
  uint64_t scheduler_interval_sec{600};

  // Task retention
  uint32_t task_retention_count{100};
  uint64_t task_retention_days{7};

  // Spot-check
  uint64_t spot_check_delay_ms{100};
  uint32_t spot_check_batch_size{25};

  // Validation
  void validate() const {
    if (max_concurrent_deep_scrubs > max_concurrent_scrubs) {
      throw std::invalid_argument(
        "max_concurrent_deep_scrubs must be <= max_concurrent_scrubs"
      );
    }

    if (scrub_max_interval_sec <= scrub_min_interval_sec) {
      throw std::invalid_argument(
        "scrub_max_interval_sec must be > scrub_min_interval_sec"
      );
    }

    if (deep_max_interval_sec <= deep_min_interval_sec) {
      throw std::invalid_argument(
        "deep_max_interval_sec must be > deep_min_interval_sec"
      );
    }

    if (interval_randomize_ratio < 0.0f || interval_randomize_ratio > 1.0f) {
      throw std::invalid_argument(
        "interval_randomize_ratio must be in range [0.0, 1.0]"
      );
    }
  }
};

void HSHomeObject::load_scrub_config(const nlohmann::json& config) {
  if (config.contains("scrubber")) {
    auto scrub_cfg = config["scrubber"];

    // Load with defaults
    scrub_config_.shallow_batch_size =
      scrub_cfg.value("shallow_batch_size", 500);
    scrub_config_.deep_batch_size =
      scrub_cfg.value("deep_batch_size", 25);

    scrub_config_.sleep_shallow_us =
      scrub_cfg.value("sleep_shallow_us", 0);
    scrub_config_.sleep_deep_us =
      scrub_cfg.value("sleep_deep_us", 10000);

    scrub_config_.timeout_shallow_sec =
      scrub_cfg.value("timeout_shallow_sec", 5);
    scrub_config_.timeout_deep_sec =
      scrub_cfg.value("timeout_deep_sec", 60);
    scrub_config_.timeout_spot_check_sec =
      scrub_cfg.value("timeout_spot_check_sec", 10);

    scrub_config_.max_concurrent_scrubs =
      scrub_cfg.value("max_concurrent_scrubs", 3);
    scrub_config_.max_concurrent_deep_scrubs =
      scrub_cfg.value("max_concurrent_deep_scrubs", 1);

    scrub_config_.scrub_min_interval_sec =
      scrub_cfg.value("scrub_min_interval_sec", 86400);
    scrub_config_.scrub_max_interval_sec =
      scrub_cfg.value("scrub_max_interval_sec", 604800);
    scrub_config_.deep_min_interval_sec =
      scrub_cfg.value("deep_min_interval_sec", 604800);
    scrub_config_.deep_max_interval_sec =
      scrub_cfg.value("deep_max_interval_sec", 2592000);
    scrub_config_.interval_randomize_ratio =
      scrub_cfg.value("interval_randomize_ratio", 0.5f);
    scrub_config_.scheduler_interval_sec =
      scrub_cfg.value("scheduler_interval_sec", 600);

    scrub_config_.task_retention_count =
      scrub_cfg.value("task_retention_count", 100);
    scrub_config_.task_retention_days =
      scrub_cfg.value("task_retention_days", 7);

    scrub_config_.spot_check_delay_ms =
      scrub_cfg.value("spot_check_delay_ms", 100);
    scrub_config_.spot_check_batch_size =
      scrub_cfg.value("spot_check_batch_size", 25);

    // Validate
    scrub_config_.validate();

    LOGI("Loaded scrub configuration: shallow_batch={}, deep_batch={}, "
         "max_concurrent={}, scheduler_interval={}s",
         scrub_config_.shallow_batch_size,
         scrub_config_.deep_batch_size,
         scrub_config_.max_concurrent_scrubs,
         scrub_config_.scheduler_interval_sec);
  } else {
    LOGI("No scrubber config found, using defaults");
  }
}
```

### 10.4 Runtime Configuration Changes

**MVP**: Requires restart (no hot-reload)

**Rationale**: Simplifies implementation, configuration changes are infrequent

**Post-MVP Enhancement**: Add hot-reload support:
```cpp
void reload_scrub_config(const ScrubConfig& new_config) {
  new_config.validate();

  // Apply non-disruptive changes immediately
  scrub_shallow_batch_size_ = new_config.shallow_batch_size;
  scrub_deep_batch_size_ = new_config.deep_batch_size;
  scrub_sleep_shallow_us_ = new_config.sleep_shallow_us;
  scrub_sleep_deep_us_ = new_config.sleep_deep_us;

  // Update atomics
  max_concurrent_scrubs_.store(new_config.max_concurrent_scrubs,
                                std::memory_order_release);
  max_concurrent_deep_scrubs_.store(new_config.max_concurrent_deep_scrubs,
                                     std::memory_order_release);

  // Reschedule background tasks if intervals changed
  if (new_config.scheduler_interval_sec != scrub_config_.scheduler_interval_sec) {
    restart_scrub_scheduler(new_config.scheduler_interval_sec);
  }

  scrub_config_ = new_config;

  LOGI("Reloaded scrub configuration");
}
```

### 10.5 Configuration Best Practices

#### 10.5.1 Production Recommendations

**Conservative Start**:
```yaml
scrubber:
  max_concurrent_scrubs: 1
  max_concurrent_deep_scrubs: 1
  sleep_deep_us: 10000              # 10ms
  scrub_min_interval_sec: 86400     # 1 day
  scrub_max_interval_sec: 259200    # 3 days
  deep_min_interval_sec: 604800     # 7 days
  deep_max_interval_sec: 1209600    # 14 days
```

**After Monitoring** (if impact < 1%):
```yaml
scrubber:
  max_concurrent_scrubs: 3
  max_concurrent_deep_scrubs: 1
  sleep_deep_us: 5000               # 5ms
```

#### 10.5.2 Tuning for Different Workloads

**Read-Heavy Workload** (scrub has minimal impact):
```yaml
scrubber:
  max_concurrent_scrubs: 5
  sleep_deep_us: 0                  # No throttle
  scrub_min_interval_sec: 43200     # 12 hours (more frequent)
```

**Write-Heavy Workload** (scrub competes for I/O):
```yaml
scrubber:
  max_concurrent_scrubs: 1
  sleep_deep_us: 20000              # 20ms (more throttle)
  deep_min_interval_sec: 1209600    # 14 days (less frequent)
```

**Large Blobs** (>1MB average):
```yaml
scrubber:
  deep_batch_size: 10               # Smaller batches
  timeout_deep_sec: 120             # Longer timeout
  sleep_deep_us: 50000              # 50ms (more throttle)
```

---

**End of SCRUBBER_IMPLEMENTATION.md**

This implementation guide provides complete, production-ready specifications for implementing the cross-replica scrubber feature in HomeObject. For architecture and design rationale, see [SCRUBBER_DESIGN.md](./SCRUBBER_DESIGN.md). For deployment and operations, see [SCRUBBER_OPERATIONS.md](./SCRUBBER_OPERATIONS.md).
