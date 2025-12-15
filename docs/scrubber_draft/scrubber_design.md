# Cross-Replica Scrubber Design

## Overview
Design document for implementing cross-replica scrubbing in HomeObject, inspired by Ceph's scrubbing mechanism.

## Goals
- Detect inconsistencies between replicas in a Placement Group (PG)
- Provide two scrub types:
  - **Shallow Scrub**: Compare index metadata only (fast, cheap)
  - **Deep Scrub**: Compare actual blob data checksums (slow, thorough)
- Maintain data integrity without impacting normal operations significantly

## Current Architecture Analysis

### What We Have
1. **Blob Header with Checksums** (hs_homeobject.hpp:386-458)
   - `BlobHeader` supports CRC32, MD5, SHA1 (only CRC32 currently implemented)
   - `verify_blob()` function already validates blob headers and checksums (hs_blob_manager.cpp:631-676)

2. **Replication Infrastructure**
   - `ReplDev` with group_id for replica coordination
   - `get_peer_info()` to get peer replication status
   - NuRaft handles all replica communication

3. **Index Iteration**
   - `query_blobs_in_shard()` can iterate all blobs in a shard
   - `get_shard_blobs()` returns all blob info for a shard

4. **PG State Management**
   - `PGStateMask` already defines: SCRUBBING, INCONSISTENT, REPAIR states (pg_manager.hpp:18-19)

5. **HTTP Endpoints**
   - HttpManager with Pistache-based REST API
   - Already has endpoints for PG/shard introspection

6. **GC Manager Pattern**
   - Periodic background task scheduling
   - Rate limiting for I/O control
   - Metrics tracking

### What Snapshot/Baseline Resync Does (Cannot Be Reused)
- Snapshot mechanism (`create_snapshot()`, `apply_snapshot()`, `read/write_snapshot_obj()`) is **NuRaft-controlled**
- Used for baseline resync when a new replica joins
- Communication is wrapped by NuRaft, not directly callable
- **We cannot reuse this for scrubbing** - need different mechanism

## Key Architectural Questions

### Q1: Cross-Replica Communication? ✅ ANALYZED

**Problem**: How does the scrub coordinator (leader) query followers for their index/checksum data?

**❌ Option A: HTTP-based RPC - REJECTED**

Reasoning:
- HomeObject nodes only know peer UUIDs (`peer_id_t`), not network addresses
- In Kubernetes/containerized deployments, pod IPs are ephemeral and change on recreation
- No service discovery mechanism exists to map UUID → IP:port
- Even with service mesh, adds unnecessary complexity
- **Verdict: Not viable without major infrastructure changes**

**❌ Option B: Extend Raft Replication Protocol - REJECTED**

Reasoning:
- Raft is fundamentally **unidirectional** (Leader → Followers)
- Leader proposes via `async_alloc_write()` → Followers receive in `on_commit()`
- Followers have no way to send data back to leader:
  - Can't propose to raft log (not leader)
  - Can't use rejection (returns bool, blocks state machine)
  - No built-in query/response mechanism in Raft protocol
- `on_fetch_data()` exists but is for log replay during catch-up, not general queries
- Writing SCRUB_REQUEST to raft log would commit to all replicas, no response channel
- **Verdict: Raft protocol not designed for bidirectional query/response**

**✅ Option C: nuraft_messenger Data Service - PROPOSED**

**Infrastructure**: `nuraft_mesg` library (located at `/Users/xiaoxchen/Code/nuraft_mesg`)

**Key Components:**
1. **`repl_service_ctx`** (mesg_state_mgr.hpp:48-73)
   - Available via `ReplDev::group_msg_service()` (needs verification)
   - Provides data service APIs separate from raft log replication

2. **Bidirectional Request API** (mesg_state_mgr.hpp:66-68):
   ```cpp
   AsyncResult<sisl::GenericClientResponse>
   data_service_request_bidirectional(destination_t const& dest,
                                      std::string const& request_name,
                                      io_blob_list_t const& cli_buf);
   ```

3. **Destination Types** (common.hpp:43-44):
   ```cpp
   ENUM(role_regex, uint8_t, LEADER, FOLLOWER, ALL, ANY);
   using destination_t = std::variant<peer_id_t, role_regex, svr_id_t>;
   ```
   - Can send to: specific peer UUID, LEADER, ALL, ANY follower
   - **Question**: Can we add support for "specific follower by UUID"?

4. **Request Handler Registration**:
   ```cpp
   bind_data_service_request(std::string const& request_name,
                             group_id_t const& group_id,
                             data_service_request_handler_t const&);
   ```

**How It Solves Our Problems:**
- ✅ Uses UUID (`peer_id_t`) for addressing, no network addresses needed
- ✅ Bidirectional: request → response pattern
- ✅ Separate from raft log, doesn't block state machine
- ✅ Already integrated infrastructure, minimal new dependencies
- ✅ Supports sending to LEADER or ALL members

**Scrub Flow Direction Decision: Follower → Leader**

**Options Considered:**
- A) Leader → Follower: Leader pushes its index to followers, followers compare and report
- B) Follower → Leader: Leader requests indexes from followers, leader compares centrally

**Decision**: ✅ **Follower → Leader**

**Reasoning:**
1. **Independent Pace**: Followers process at their own speed without coordination
   - Fast followers respond quickly
   - Slow followers take longer without blocking others
   - No synchronization barriers needed between followers

2. **Simpler State Management**:
   - Leader doesn't track per-follower progress
   - No need to persist "sent to follower X" state
   - If leader crashes, scrub simply fails (no partial state recovery)

3. **Majority Voting Capability**:
   - Leader has complete view of all replica indexes
   - Can detect if leader itself is corrupted (not just followers)
   - Enables quorum-based inconsistency detection

4. **Graceful Degradation**:
   - Leader can timeout slow/unavailable followers
   - Can proceed with partial results (compare available replicas)
   - No need to cancel/cleanup pending requests

**Proposed Scrub Flow:**
```
Leader (Scrub Coordinator):
1. Send scrub request to all peers (iterate each peer individually)
2. Collect responses with timeout via folly::collectAll()
3. Compare all indexes (including leader's own)
4. Detect inconsistencies using majority voting
5. Generate scrub report

Each Follower (including leader):
1. Registered handler receives "scrub_get_index" request
2. Compute local index/checksums independently
3. Send response back to requester
```

**Decision**: ✅ **Option C VALIDATED** - nuraft_messenger data service with Follower→Leader pattern.

---

## Validation Results: Data Service Usage Analysis

After studying HomeStore's production usage (raft_repl_dev.cpp) and nuraft_mesg unit tests (data_service_tests.cpp), all open questions are now answered:

### ✅ Confirmed Capabilities

**1. Access Pattern - SOLVED**
- `ReplDev::group_msg_service()` **already exists** in HomeStore (raft_repl_dev.cpp:1786)
- Returns `repl_service_ctx*` for data service operations
- No interface changes needed in HomeObject

**2. Production Usage - VERIFIED**
- HomeStore already uses this mechanism for `PUSH_DATA` and `FETCH_DATA` operations
- Handlers registered in `RaftReplDev::bind_data_service()` (lines 96-126)
- Proven production-ready for replica communication

**3. Response Collection Pattern - CLARIFIED**
- Each `data_service_request_bidirectional()` returns **single** `AsyncResult<GenericClientResponse>`
- To query multiple replicas: iterate peers and collect via `folly::collectAll()`
- Example pattern from tests (data_service_tests.cpp:119-128):
  ```cpp
  std::vector<folly::SemiFuture<GenericClientResponse>> futures;
  for (auto& peer : peers) {
      futures.push_back(
          msg_svc->data_service_request_bidirectional(peer.id, "REQUEST", blob));
  }
  auto all = folly::collectAll(std::move(futures)).get();
  ```

**4. Destination Support - CONFIRMED**
- ✅ Specific peer by UUID: `data_service_request_bidirectional(peer_id_t, ...)`
- ✅ All peers: `data_service_request_bidirectional(role_regex::ALL, ...)`
- ✅ Leader only: `data_service_request_bidirectional(role_regex::LEADER, ...)`
- **For scrubbing**: iterate peers individually for separate responses

**5. Payload Size - TESTED**
- Test successfully sends **4MB payloads** (data_service_tests.cpp)
- Sufficient for index data (even large shards)
- Pagination available if needed via `start_blob_id` parameter

**6. Response Handling - UNDERSTOOD**
- Response accessed via `result.value().response_blob()`
- Supports timeout via folly futures: `.get(std::chrono::seconds(30))`
- Can handle failures gracefully with `result.hasError()`

**7. Handler Registration - ESTABLISHED**
- Register during PG creation, similar to PUSH_DATA/FETCH_DATA
- Global handler pattern (dispatches by group_id):
  ```cpp
  msg_mgr.bind_data_service_request(
      "scrub_get_index", group_id,
      std::bind(&HSHomeObject::on_scrub_index_request, this, _1));
  ```

**8. Thread Context - IDENTIFIED**
- Handler runs in message service thread
- Must dispatch to appropriate iomgr reactor for HomeObject operations
- Use `iomanager.run_on()` pattern for thread safety

### Q2: Consistency Point? ✅ DECIDED

**Problem**: Scrub should happen at a consistent point across replicas to avoid false inconsistencies from replication lag.

**Initial Approach (LSN-based) - REJECTED:**
- Scrub at specific LSN (e.g., LSN=1000)
- **Fatal Flaw**: Cannot rewind to past LSN
  - Follower at LSN 1005 can't reconstruct state at LSN 1000
  - Leader at LSN 1010 can't compare against its LSN 1000 state
  - No snapshot mechanism available (NuRaft-controlled, not reusable)

**Decision**: ✅ **Use blob_id ranges instead of LSN**

**Key Insights:**
1. **blob_id is monotonically increasing** within each shard (PG-level sequence)
2. **Index structure is `BlobRoute{shard_id, blob_id}`** - naturally supports range queries
3. **Shard-by-shard scrubbing** avoids interleaved blob_id complexity

**Scrub Task Structure:**
```
PG Scrub Task Metadata:
  - scrub_lsn: <LSN at task start>        // for audit/timestamp
  - max_blob_id: <Y>                       // upper bound for all batches
  - timestamp: <when scrub started>

Execution:
  For each shard in PG:
    For each batch [x, y) where y = max_blob_id:
      ScrubRequest {
        pg_id: X
        shard_id: S
        blob_range: [x, y)
      }

      ScrubResponse {
        shard_id: S
        blob_range: [x, y)
        entries: [<blob_id, ...metadata TBD...>]
      }
```

**Inconsistency Detection Heuristic: TBD**

**Eventual Consistency Ambiguity Cases:**

1. **Missing Blob (Follower missing, Leader has)**:
   - Sealed shard + missing blob x1 → likely real inconsistency
   - Open shard + missing x1, but has x5 (x5 > x1) → likely real inconsistency
   - Open shard + missing x1, no higher blobs → might be replication lag

2. **Extra Blob (Follower has, Leader doesn't)**:
   - delete_blob can happen on both sealed and open shards
   - Leader deleted Y1 → marked as tombstone
   - Delete not yet replicated to follower → follower still has live Y1

   **Leader's Analysis Options:**
   - Y1 is **tombstone** in leader's index → eventual consistency (delete in-flight)
   - Y1 **not in index at all** (tombstone GC'd) → **ambiguous**:
     - Could be eventual consistency (delete very delayed, GC already ran)
     - Could be real inconsistency (follower has corrupt extra blob)

**Complexity:**
- Both "missing blob" and "extra blob" have eventual consistency ambiguity
- GC complicates detection (tombstone removal loses history)
- Need comprehensive heuristic design

**Decision**: Mark Inconsistency Detection Heuristic as **TBD** - address during Q3 (Inconsistency Handling)

**Why blob_id Range Approach Still Works:**
- ✅ No need to rewind/snapshot at past LSN
- ✅ Monotonic blob_id provides natural ordering within shard
- ✅ Index structure supports efficient range queries
- ✅ Enables collecting data for inconsistency analysis (even if heuristic is complex)

**Open Detail:**
- Exact response payload format: **TBD** (not critical for architectural decision)
  - Need blob existence, possibly metadata (size, tombstone status)
  - NOT physical block addresses (MultiBlkId) - can differ between replicas

### Q3: Handling Inconsistencies? ✅ DECIDED (Partial)

**Context from Q2:**
- Scrub compares ALIVE blob indexes across replicas using blob_id ranges
- Ambiguity sources: replication lag, delete in-flight, post-GC tombstone removal

---

#### Q3.1: Inconsistency Classification ✅ DECIDED

**Problem**: How to distinguish real inconsistency from replication lag noise?

**Decision**: ✅ **Two-phase approach: Batch Scrub + Spot Check**

**Phase 1: Initial Batch Scrub (per shard, per blob_id range)**
```
ScrubRequest {
  pg_id: X
  shard_id: S
  blob_range: [1000, 2000)
}

ScrubResponse {
  blob_ids: [1001, 1003, 1005, ...]  // ALIVE blobs only (filter tombstones)
}
```

- Leader compares ALIVE blob sets across all replicas
- Collect differences: missing blobs, extra blobs
- **Do not classify yet** - proceed to spot check

**Phase 2: Spot Check (end of PG scan)**
- Batch all questionable blob_ids from entire PG scan
- Query **ALL replicas** for these specific blobs (not just one with difference)

```
SpotCheckRequest {
  pg_id: X
  shard_id: S
  blob_ids: [1234, 1567, ...]  // questionable blobs
}

SpotCheckResponse {
  blobs: [
    {blob_id: 1234, state: ALIVE},
    {blob_id: 1567, state: TOMBSTONE},
    {blob_id: 9999, state: NOT_FOUND}
  ]
}
```

- **Purpose**: Filter out transient replication lag (blob appeared/disappeared since initial scan)
- Use majority/quorum to determine truth (details TBD - corner cases deferred)
- Retry with backoff if needed (config TBD)

**Result**: After spot check, confirmed inconsistencies that persist across retries

**Advantages**:
- ✅ Simple logic - no complex confidence level heuristics
- ✅ Natural lag filtering - most replication delays resolve quickly
- ✅ Defers complexity to spot check phase where needed

---

#### Q3.2: Source of Truth ✅ DECIDED

**Decision**: No automatic authoritative replica selection

**Approach:**
- Scrub detects and reports inconsistencies (differences between replicas)
- Flag PG as `INCONSISTENT`
- **Admin investigates** using upper layer information:
  - Application logs
  - Metrics/monitoring
  - Business logic context
  - Timestamp analysis
- Admin determines which replica is correct
- Admin triggers manual repair with correct source

**Rationale:**
- Automatic auth selection (leader-first, majority voting, etc.) can be wrong in edge cases
- Upper layer context (application semantics, timestamps, logs) provides better signal
- Manual investigation ensures correctness over speed
- MVP prioritizes data integrity over automation

**Implementation:**
- Scrub reports all replica states for inconsistent blobs
- Store detailed inconsistency records in scrub task state
- HTTP API to query inconsistency details
- Manual repair triggered after admin decision

---

#### Q3.3: Repair Strategy - PARKED

**Quick Decision**: Manual repair only (admin-triggered)
- No auto-repair in MVP
- Admin investigates spot check results and decides action

---

#### Q3.4: PG State Management ✅ DECIDED

**Problem**: What state should PG be in during/after scrub?

**During Scrub:**
- ✅ Set `PGStateMask::SCRUBBING` flag (already defined in code)
- ✅ **Allow all I/O** (reads and writes)
- Reasoning: Scrub is read-only background task, non-blocking

**After Inconsistency Detected:**
- ✅ Set `PGStateMask::INCONSISTENT` flag
- ✅ **Allow all I/O** (continue serving)
- Reasoning:
  - Don't know which replica is wrong yet (needs investigation)
  - Blocking could be catastrophic if many PGs inconsistent
  - Better to serve potentially stale data than be unavailable

**Persistence:**
- ✅ Use **separate scrub metablk** (not PG superblock - avoid polluting PG struct)
- Store per PG:
  ```
  struct scrub_info_superblk {
    pg_id_t pg_id;
    int64_t last_scrub_lsn;
    uint64_t last_scrub_time;
    uint64_t last_scrub_max_blob_id;
    ScrubState state;  // CLEAN, INCONSISTENT
  }
  ```
- Detailed blob-level findings go to report (Q3.5), not metablk

**Auto-clear INCONSISTENT:**
- ✅ Next successful scrub auto-clears INCONSISTENT state
- Self-healing: if next scrub finds no issues, PG is clean again

---

#### Q3.5: Reporting and Observability - PARKED

**Deferred**: Report format, storage, metrics, alerting details

### Q4: Resource Control ✅ DECIDED

**Goal**: Minimize scrub impact on client I/O while maintaining data integrity verification.

#### Batch Sizes
- **Shallow scrub**: 500 blobs per batch
  - Index reads are non-blocking (no lock contention)
  - ~5 B+tree nodes, ~4KB response per replica
  - Optimize for throughput
- **Deep scrub**: 25 blobs per batch
  - Aligned with Ceph production (5-25 objects)
  - I/O bounded: full data read + SHA256 computation
  - Memory/CPU bounded

#### Sleep Configuration
```cpp
uint64_t scrub_sleep_shallow_us = 0;        // No sleep (fast)
uint64_t scrub_sleep_deep_us = 10000;       // 10ms (10,000μs)
```
- **Unit**: Microseconds for fine-grained control
- **No maximum**: Trust admin (like Ceph's `osd_scrub_sleep`)
- **Granularity**: Global config (not per-PG)

#### Concurrent Scrub Limits
```cpp
std::atomic<uint32_t> active_scrub_count{0};
uint32_t max_concurrent_scrubs = 1;  // Default: 1 per instance
```
- **Enforcement**: Atomic compare-exchange
- **Scope**: Per HomeObject instance

#### Manual Control: NO_SCRUB Flags
```cpp
std::atomic<bool> no_scrub_{false};           // Block all scrubbing
std::atomic<bool> no_deep_scrub_{false};      // Block deep only
```

**Why Instance-Level (not per-PG)?**
> All PGs on a HomeObject instance share the same physical resources: disk I/O, CPU, memory, and network bandwidth. Disabling scrubbing on individual PGs does not reduce system load, as other PGs will continue scrubbing and consuming resources. Instance-level flags provide meaningful load control by stopping all scrubbing activity on the node.

**HTTP API:**
```
POST /scrub/disable           # Set NO_SCRUB
POST /scrub/disable_deep      # Set NO_DEEP_SCRUB
DELETE /scrub/disable         # Clear NO_SCRUB
DELETE /scrub/disable_deep    # Clear NO_DEEP_SCRUB
GET /scrub/status             # Check current state
```

**Behavior:**
- Checked before starting new scrubs
- Checked between batches (pauses ongoing scrubs)
- Ephemeral (reset on pod restart)

#### Block During Recovery
```cpp
bool can_continue_scrub(pg_id_t pg_id, ScrubType type) {
  // Check global flags
  if (no_scrub_.load()) return false;
  if (type == DEEP && no_deep_scrub_.load()) return false;

  // Check PG state
  auto pg = get_pg(pg_id);
  auto state = pg->state.load();

  // Don't scrub if recovering or already scrubbing
  return !(state & PGStateMask::BASELINE_RESYNC) &&
         !(state & PGStateMask::SCRUBBING);
}
```

**Mid-scrub recovery**: Finish current batch, then exit gracefully.

#### Resource Limits
**Timeouts (per batch):**
- Shallow: 5 seconds
- Deep: 60 seconds
- Spot check: 10 seconds

**Limits:**
- Spot check batch: 25 blobs (same as deep - equally expensive)

#### Deferred to Post-MVP
- **Load-based throttling**: Latency-based preferred over system load
- **Time windows**: `scrub_begin_hour`, `scrub_end_hour` (Ceph-like)
- **Preemption**: Pause/resume with checkpointing

### Q5: Initiation & Scheduling ✅ DECIDED

**Goal**: Provide both manual control and automatic scheduling for scrub operations.

---

#### Manual Trigger

**HTTP API:**
```
POST /scrub?pg_id=123&deep=false    # Manual scrub (single PG)
POST /scrub/repair?pg_id=123        # Manual repair (deferred, API reserved)
GET /scrub/task/{task_id}           # Query task status
```

**Unified Task Tracking:**
- All scrubs (manual + auto-scheduled) use same `ScrubTaskRegistry` infrastructure
- Each scrub gets a `task_id` for querying progress
- Enables consistent monitoring across all scrub types

---

#### Auto-Scheduled Scrubbing

**Background Scheduler:**
```cpp
// Scheduler runs every 10 minutes (configurable)
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

**Check Frequency:**
```cpp
uint64_t scrub_scheduler_interval_sec = 600;  // 10 minutes (configurable)
```
- **Rationale**: Very cheap operation (timestamp comparisons), provides responsive scheduling

---

#### Scheduling Intervals

**Configuration:**
```cpp
// Shallow scrub intervals
uint64_t scrub_min_interval_sec = 86400;          // 1 day
uint64_t scrub_max_interval_sec = 604800;         // 7 days

// Deep scrub intervals
uint64_t scrub_deep_min_interval_sec = 604800;    // 7 days
uint64_t scrub_deep_max_interval_sec = 2592000;   // 30 days

// Randomization (avoid thundering herd)
float scrub_interval_randomize_ratio = 0.5;       // ±50% jitter (Ceph-aligned)
```

**Scheduling Logic:**
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

**Randomization:**
- Adds ±50% jitter to `min_interval`
- Example: 1 day min → schedule between 12-36 hours
- Prevents thundering herd when all PGs created simultaneously

---

#### PG Selection Algorithm

**Priority Order:** Oldest scrub time first, urgent PGs (past max_interval) prioritized

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

---

#### Concurrency Control

**Instance-Level Limits:**
```cpp
uint32_t max_concurrent_scrubs = 3;        // Total scrubs (shallow + deep)
uint32_t max_concurrent_deep_scrubs = 1;   // Deep scrub limit (subset of total)

std::atomic<uint32_t> active_scrub_count_{0};
std::atomic<uint32_t> active_deep_scrub_count_{0};
```

**Enforcement - Leader Side (Before Initiating):**
```cpp
bool try_reserve_scrub_slot(ScrubType type) {
  // Check total limit
  if (active_scrub_count_.load() >= max_concurrent_scrubs_) {
    return false;
  }

  // Check deep-specific limit
  if (type == DEEP &&
      active_deep_scrub_count_.load() >= max_concurrent_deep_scrubs_) {
    return false;
  }

  // Reserve slots
  active_scrub_count_.fetch_add(1);
  if (type == DEEP) {
    active_deep_scrub_count_.fetch_add(1);
  }
  return true;
}
```

**Enforcement - Follower Side (In Request Handler):**
```cpp
void on_scrub_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  auto request = decode_scrub_request(rpc_data->request_blob());

  // Check concurrent limit BEFORE processing
  if (!try_reserve_scrub_slot(request.type)) {
    auto error_response = encode_error("RESOURCE_EXHAUSTED");
    rpc_data->send_response(error_response);
    return;
  }

  // Process request in reactor
  iomanager.run_on(iomgr::reactor_regex::random_worker,
    [this, request, rpc_data]() {
      auto result = query_blobs_in_shard(...);
      auto response_blob = encode_scrub_response(result);
      rpc_data->send_response(response_blob);

      // Release slot after completion
      release_scrub_slot(request.type);
    });
}
```

**No Priority Boost for INCONSISTENT PGs:**
- INCONSISTENT PGs follow normal scheduling
- Admin uses manual trigger if urgent verification needed

---

#### Persistence

**Two-Level Persistence Structure:**

**1. PG-Level Scrub Metadata** (inline into PG superblock):
```cpp
struct pg_scrub_metadata {
  uint64_t last_scrub_time;        // Unix timestamp
  uint64_t last_deep_scrub_time;   // Unix timestamp
  ScrubState state;                // CLEAN, INCONSISTENT
  std::optional<task_id_t> active_task_id;  // If scrub ongoing
};
```
- **Storage**: Inline into PG superblock
- **Version compatibility**: Add fields as optional, default to zero on read from older versions
- **Long-lived**: Persists across restarts

**2. Scrub Task State** (one metablk per task):
```cpp
struct scrub_task_state {
  task_id_t task_id;
  pg_id_t pg_id;
  ScrubType type;
  uint64_t start_time;

  // Progress tracking (for deep scrub resume)
  shard_id_t current_shard;
  blob_id_t current_blob_id;
  uint64_t max_blob_id;

  // Findings
  std::vector<InconsistencyRecord> inconsistencies;
  uint64_t blobs_scanned;

  // Status
  TaskStatus status;  // RUNNING, COMPLETED, FAILED, ABORTED
};
```

**Restart Behavior:**
- **Shallow scrub**: Abandon (mark as ABORTED), reschedule based on `last_scrub_time`
- **Deep scrub**: Resume from last checkpointed shard
  - **Checkpoint frequency**: After each shard completes

**Task Retention Policy:**
```cpp
uint32_t scrub_task_retention_count = 100;   // Keep last N tasks
uint64_t scrub_task_retention_days = 7;      // Keep tasks from last N days
```
- **Logic**: Retain task if it meets **EITHER** condition
- **Cleanup**: Periodic background task removes old task metablks

## Scrub Types Detail

### Shallow Scrub (Index Metadata Comparison)
**What it checks:**
- For each shard in PG:
  - List of (shard_id, blob_id) → MultiBlkId mappings
  - Blob counts match
  - No orphaned/missing entries

**What it doesn't check:**
- Actual blob data
- Blob checksums

**Cost:** Low (index-only reads)

**Frequency:** Daily

### Deep Scrub (Data Checksum Comparison)
**What it checks:**
- Everything in shallow scrub, PLUS:
- For each blob:
  - Compute MD5/SHA256 hash of blob payload
  - Compare hash across all replicas
  - Verify blob headers are consistent

**Cost:** High (full data reads + hash computation)

**Frequency:** Weekly

## Open Questions (To Discuss)

1. **Communication**: HTTP vs extending replication protocol vs custom RPC?
2. **Peer Discovery**: How does leader know follower addresses/ports for HTTP calls?
3. **LSN Sync**: How to ensure all replicas scrub at same consistent point?
4. **Inconsistency Handling**: Auto-repair or report-only? Leader as source of truth?
5. **Failure Cases**: What if follower is down during scrub? Retry? Partial scrub OK?
6. **Performance**: What's acceptable impact on normal operations?
7. **Scope**: Per-PG scrub? Per-shard? Entire cluster?

## Next Steps

1. Decide on cross-replica communication mechanism (Q1)
2. Define consistency point strategy (Q2)
3. Design inconsistency detection & repair workflow (Q3)
4. Establish resource control policies (Q4)
5. Design scheduling & configuration (Q5)
6. Create detailed implementation plan
7. Implement Phase 1: Shallow Scrub MVP
8. Implement Phase 2: Deep Scrub with repair

## References
- Ceph Scrubbing: https://docs.ceph.com/en/latest/rados/configuration/osd-config-ref/#scrubbing
- HomeObject code:
  - BlobHeader checksums: src/lib/homestore_backend/hs_homeobject.hpp:386-458
  - verify_blob(): src/lib/homestore_backend/hs_blob_manager.cpp:631-676
  - Index iteration: src/lib/homestore_backend/index_kv.cpp:135-166
  - PG state: src/include/homeobject/pg_manager.hpp:18-19
  - GC Manager pattern: src/lib/homestore_backend/gc_manager.{hpp,cpp}
