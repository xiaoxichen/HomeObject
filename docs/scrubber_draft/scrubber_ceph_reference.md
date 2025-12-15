# Ceph Scrubbing Reference - Key Findings for HomeObject Design

## Overview
This document summarizes key architectural patterns from Ceph's scrubbing implementation that inform HomeObject's scrubber design decisions.

---

## 1. Resource Control & Rate Limiting

### Ceph Configuration Options

**Load-Based Throttling:**
```
osd_scrub_load_threshold = 0.5 (default)
```
- **Meaning**: Allow scrubbing when `system_load / num_cpus < 0.5`
- **Implication for HomeObject**: We should check system load before starting scrubs

**Sleep Between Operations:**
```
osd_scrub_sleep = 0 (default, seconds)
osd_scrub_extended_sleep = 0 (default, seconds during off-peak)
```
- **Meaning**: Inject delay between scrub operations to reduce impact
- **Ceph pattern**: Sleep=0 by default, relies on load threshold instead

**Batch/Chunk Size:**
```
osd_scrub_chunk_min = 5 objects (default)
osd_scrub_chunk_max = 25 objects (default)
```
- **Key insight**: Ceph scrubs in **very small batches** (5-25 objects)
- **Reasoning**: Minimize latency impact on client I/O
- **For HomeObject blobs**: We proposed 100K blobs/batch - **TOO LARGE**
  - Should reduce to **100-1000 blobs per batch** max
  - Align with Ceph's philosophy: many small batches, not few large ones

**Concurrent Scrubs:**
```
osd_max_scrubs = 1 (default)
```
- **Meaning**: Only 1 concurrent scrub per OSD (storage node)
- **For HomeObject**: **1-2 concurrent PG scrubs per node** is reasonable

**Scrubbing During Recovery:**
```
osd_scrub_during_recovery = false (default)
```
- **Meaning**: Don't scrub while PGs are recovering
- **For HomeObject**: Should check `PGStateMask::BASELINE_RESYNC` before scrubbing

### Recommendation for Q4 (Resource Control):
1. ✅ **Max concurrent scrubs**: 1 per HomeObject instance (configurable)
2. ✅ **Batch size**: Start with **100-500 blobs**, NOT 100K
   - Rationale: Ceph uses 5-25 objects, we have similar I/O patterns
3. ✅ **Sleep between batches**: Optional delay (default 0), configurable
4. ⚠️ **Load threshold**: Consider adding later, not MVP
5. ✅ **Block during recovery**: Don't scrub if `PGStateMask::BASELINE_RESYNC` set

---

## 2. Scheduling & Initiation

### Ceph Scheduling Options

**Periodic Scrub Intervals:**
```
osd_scrub_min_interval = 1 day (default)
osd_scrub_max_interval = 7 days (default)
osd_scrub_interval_randomize_ratio = 0.5 (default)
```
- **Meaning**:
  - Scrub each PG at least once per week (max_interval)
  - Prefer not more often than once per day (min_interval)
  - Add ±50% randomization to avoid thundering herd

**Time Windows:**
```
osd_scrub_begin_hour = 0 (default, midnight)
osd_scrub_end_hour = 24 (default, always)
```
- **Meaning**: Restrict scrubbing to specific hours (e.g., 2am-6am for production)
- **Default**: Anytime (0-24)

**Prioritization:**
```
MOSDRepScrub message fields:
  - int32_t priority = 0
  - bool high_priority = false
  - bool allow_preemption = false
```
- **Meaning**: Admin can trigger high-priority scrubs that preempt normal ops

### Ceph Initiation Flow

**Manual Trigger:**
- Admin sends `MOSDScrub2` message to specific OSD
- Message specifies: `vector<spg_t> scrub_pgs`, `bool repair`, `bool deep`
- **For HomeObject**: HTTP endpoint `/scrub?pg_id=X&deep=true&repair=false`

**Auto-Scheduled:**
- Each PG tracks `last_scrub_time` and `last_deep_scrub_time`
- Background task checks: `now - last_scrub_time > max_interval`
- Schedules scrub if PG is healthy and not recovering

### Recommendation for Q5 (Initiation & Scheduling):
1. ✅ **Manual trigger**: HTTP endpoint for admin
   ```
   POST /scrub?pg_id=123&deep=false&repair=false
   ```
2. ✅ **Auto-scheduled**: Periodic background task
   - Check every hour: which PGs need scrubbing
   - Scrub shallow once per day (configurable)
   - Scrub deep once per week (configurable)
3. ✅ **Persistence**: Store in scrub metablk:
   ```cpp
   struct scrub_info_superblk {
     pg_id_t pg_id;
     uint64_t last_scrub_time;        // timestamp
     uint64_t last_deep_scrub_time;   // timestamp
     int64_t last_scrub_lsn;
     uint64_t last_scrub_max_blob_id;
     ScrubState state;  // CLEAN, INCONSISTENT
   };
   ```
4. ⚠️ **Time windows**: Not MVP, add later if needed
5. ✅ **Randomization**: Add jitter to avoid all PGs scrubbing simultaneously

---

## 3. Inconsistency Resolution (Authoritative Replica Selection)

### Ceph's Algorithm (`be_select_auth_object`)

**Selection Priority:**
```cpp
// Create list with primary first (ceph/src/osd/PGBackend.cc:line ~1256)
list<pg_shard_t> shards;
for (auto& replica : replicas) {
  if (replica == primary) continue;
  shards.push_back(replica);
}
shards.push_front(primary);  // Primary first!

// Iterate in order, select first valid replica
for (auto& shard : shards) {
  if (has_read_error || has_stat_error || missing_attrs) {
    shard_errors++;
    continue;  // Skip this replica
  }
  // First replica without errors becomes authoritative
  auth = shard;
  break;
}
```

**Key Insights:**
1. **Primary preference**: Primary (leader) is checked first
   - If primary is error-free → primary is authoritative
   - Only if primary has errors → check followers
2. **First-valid wins**: First replica without errors becomes auth
3. **Error detection**: Check for:
   - Read errors (I/O failure)
   - Stat errors (metadata corruption)
   - Missing attributes (incomplete object)
   - Checksum mismatches (data corruption)

**Comparison Against Auth (`be_compare_scrub_objects`):**
```cpp
for (auto& replica : replicas) {
  if (replica == auth) {
    mark_as_selected_auth();
    continue;
  }
  // Compare replica against auth
  if (size_mismatch || digest_mismatch || attr_mismatch) {
    inconsistent_replicas.insert(replica);
    error_count++;
  } else {
    // Replica matches auth - good replica
    good_replicas.insert(replica);
  }
}

// Build authoritative list: auth + good_replicas
authoritative[object] = good_replicas;
```

**Majority Voting:**
- Ceph does NOT use majority voting for auth selection
- **Instead**: Selects one auth (preferring primary), then validates others against it
- **Repair**: Copies from auth to inconsistent replicas

### Comparison with Our Design

**Our spot check approach:**
```
Query all replicas for blob X
Leader: ALIVE
Follower1: ALIVE
Follower2: NOT_FOUND

→ Majority (2/3) says ALIVE → truth
→ Follower2 has inconsistency
```

**Ceph's approach:**
```
Primary (leader) is auth by default (if no errors)
Compare followers against primary
If follower != primary → follower is wrong
```

**Key Difference:**
- **Ceph assumes primary correctness** unless it has detectableerrors
- **We assume majority correctness** via spot check voting

**Which is better?**
- **Ceph's approach**: Simpler, faster, works when primary is trusted
- **Our approach**: More robust if leader can be silently corrupted
- **Recommendation**: **Use Ceph's approach for MVP**
  - Default to leader as source of truth
  - If leader has errors (tombstone GC ambiguity), use majority vote
  - Simpler implementation, aligns with Raft's leader-based model

### Recommendation for Q3.2 (Source of Truth):
1. ✅ **Primary preference**: Leader is auth by default
2. ✅ **Fallback to majority**: Only if leader has ambiguous state (e.g., post-GC tombstone)
3. ✅ **For 2-replica case**: Leader wins by default (no majority possible)
4. ✅ **Admin override**: Flag inconsistency, require manual investigation if ambiguous

---

## 4. Thread Safety & Concurrency

### Ceph's Approach

**Message Handling:**
- Scrub messages (`MOSDRepScrub`) are dispatched via `MOSDFastDispatchOp`
- Handler runs in OSD's thread pool
- Acquires PG lock before accessing PG state

**Scrub State Machine:**
```cpp
enum ScrubState {
  INACTIVE,
  NEW_CHUNK,
  BUILD_MAP,      // Build scrub map for chunk
  WAIT_REPLICAS,  // Wait for replica maps
  COMPARE_MAPS,   // Compare collected maps
  WAIT_DIGEST_UPDATES,
  FINISH
};
```
- State transitions guarded by PG lock
- Each state processes a chunk, then yields

**Preemption:**
```cpp
osd_scrub_max_preemptions = 5 (default)
```
- Allow scrub to be paused if high-priority client I/O arrives
- Scrub resumes after client I/O completes

### HomeStore/HomeObject Patterns

**Verified Patterns:**

1. **IndexTable thread safety** (index_table.hpp:193-198):
```cpp
template<typename ReqT>
btree_status_t get(ReqT& greq) const {
  if (is_stopping()) return btree_status_t::stopping;
  incr_pending_request_num();  // Atomic increment
  auto ret = Btree<K,V>::get(greq);
  decr_pending_request_num();  // Atomic decrement
  return ret;
}
```
- **Findings**: Read operations (like query_blobs_in_shard) are thread-safe
- Atomic counters track pending requests for graceful shutdown
- No explicit locking needed for read-only index queries

2. **PG State atomic operations** (pg_manager.hpp):
```cpp
void set_state(PGStateMask mask) {
  state.fetch_or(static_cast<uint64_t>(mask), std::memory_order_relaxed);
}

bool is_state_set(PGStateMask mask) const {
  return (state.load(std::memory_order_relaxed) & static_cast<uint64_t>(mask)) != 0;
}
```
- **Findings**: PG state flags use `std::atomic` with fetch_or/fetch_and
- ✅ **Solves concurrent scrub prevention**: Use atomic test-and-set pattern

3. **Data service handler dispatch** (HomeStore raft_repl_dev.cpp:1256-1260):
```cpp
iomanager.run_on_forget(iomgr::reactor_regex::random_worker,
  [this, response, rreqs]() {
    handle_fetch_data_response(std::move(response), std::move(rreqs));
  });
```
- **Pattern**: Dispatch from nuraft_mesg thread to iomgr reactor
- **For scrub handlers**: Must use same pattern when accessing index_table

### Recommendation for Scrubber Thread Safety:

**1. Concurrent Scrub Prevention:**
```cpp
bool HSHomeObject::start_pg_scrub(pg_id_t pg_id) {
  auto pg = _pg_map.at(pg_id);

  // Atomic test-and-set using fetch_or
  auto old_state = pg->state.fetch_or(
    static_cast<uint64_t>(PGStateMask::SCRUBBING),
    std::memory_order_acquire
  );

  if (old_state & static_cast<uint64_t>(PGStateMask::SCRUBBING)) {
    // Already scrubbing
    return false;
  }

  // Successfully set SCRUBBING flag, proceed with scrub
  return true;
}
```
- ✅ **Atomic**: No race condition possible
- ✅ **Simple**: Single atomic operation

**2. Handler Dispatch Pattern:**
```cpp
void HSHomeObject::on_scrub_request(intrusive_ptr<GenericRpcData>& rpc_data) {
  // Running in nuraft_mesg service thread!

  // Decode request
  auto request = decode_scrub_request(rpc_data->request_blob());

  // Dispatch to iomgr reactor
  iomanager.run_on(iomgr::reactor_regex::random_worker, [this, request, rpc_data]() {
    // Now in correct thread context for index_table access
    auto result = query_blobs_in_shard(request.pg_id, request.shard_id,
                                       request.start_blob_id, request.batch_size);

    // Encode response
    auto response_blob = encode_scrub_response(result);
    rpc_data->send_response(response_blob);
  });
}
```
- ✅ **Thread-safe**: All index_table access in reactor thread
- ✅ **Pattern-proven**: Same as FETCH_DATA handler

**3. Index Query (Already Thread-Safe):**
```cpp
// query_blobs_in_shard already uses incr/decr_pending_request_num
// No additional locking needed
auto results = query_blobs_in_shard(pg_id, shard_seq_num, start_blob_id, batch_size);
```

---

## 5. Message Format

### Ceph's Wire Protocol

**Scrub Request (MOSDRepScrub.h:30-40):**
```cpp
struct MOSDRepScrub {
  spg_t pgid;              // PG to scrub
  eversion_t scrub_from;   // LSN start
  eversion_t scrub_to;     // LSN end
  hobject_t start;         // Object range start (inclusive)
  hobject_t end;           // Object range end (exclusive)
  bool deep;               // Deep vs shallow
  bool allow_preemption;   // Can be paused
  int32_t priority;        // Scrub priority
  bool high_priority;      // High-pri flag
};
```

**Key Insights:**
- Uses both LSN range (`scrub_from`/`scrub_to`) AND object range (`start`/`end`)
- **Chunky scrubbing**: Scrub subset of objects per request
- Includes priority and preemption flags

### Recommendation for HomeObject Message Format:

**ScrubRequest:**
```cpp
struct ScrubRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t range_start;   // Inclusive
  blob_id_t range_end;     // Exclusive (max_blob_id from scrub task)
  uint32_t batch_size;     // Max blobs to return (100-500)
  ScrubType type;          // SHALLOW, DEEP
};
```

**ScrubResponse (Shallow):**
```cpp
struct ShallowScrubResponse {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t range_start;
  blob_id_t range_end;
  vector<blob_id_t> alive_blobs;  // Filtered tombstones
  bool has_more;                  // More blobs beyond range_end
};
```
- **Size estimate**: 500 blobs × 8 bytes = 4KB (well under 4MB limit)

**SpotCheckRequest:**
```cpp
struct SpotCheckRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  vector<blob_id_t> blob_ids;  // Specific blobs to check
};
```

**SpotCheckResponse:**
```cpp
enum BlobState { ALIVE, TOMBSTONE, NOT_FOUND };

struct BlobStateInfo {
  blob_id_t blob_id;
  BlobState state;
};

struct SpotCheckResponse {
  pg_id_t pg_id;
  shard_id_t shard_id;
  vector<BlobStateInfo> blobs;
};
```

**Serialization:**
- **Recommendation**: Use simple struct serialization (like Ceph's `encode/decode`)
- **Alternative**: flatbuffers (if versioning needed, but adds complexity)
- **MVP**: Struct with manual encode/decode, add versioning later

---

## Summary of Key Takeaways

### Critical Changes from Our Initial Design:

1. **❌ Batch Size**: 100K blobs is TOO LARGE
   - **Change to**: 100-500 blobs per batch
   - **Rationale**: Ceph uses 5-25 objects, minimize latency impact

2. **✅ Concurrent Scrubs**: 1 per node is correct
   - Matches Ceph's `osd_max_scrubs = 1`

3. **✅ Atomic Flag Operations**: Use `fetch_or` for SCRUBBING flag
   - Verified in HomeObject code (pg_manager.hpp)
   - Solves concurrent scrub prevention race condition

4. **✅ Thread Safety**: Use iomgr dispatch pattern
   - Verified in HomeStore FETCH_DATA handler
   - Dispatch from nuraft_mesg thread to reactor before index access

5. **⚠️ Auth Selection**: Prefer leader over majority voting
   - **Simpler**: Ceph defaults to primary, only uses comparison
   - **Change from spot check design**: Use leader as auth, compare followers
   - **Fallback**: Majority vote only if leader state is ambiguous

6. **✅ Scheduling**: Auto-schedule + manual trigger
   - Daily shallow, weekly deep (like Ceph)
   - HTTP endpoint for admin manual trigger
   - Add randomization jitter to avoid thundering herd

### Confidence Levels After Ceph Analysis:

- **Resource Control (Q4)**: 95% confidence → well-validated by Ceph patterns
- **Scheduling (Q5)**: 95% confidence → clear Ceph precedent
- **Thread Safety**: 90% confidence → verified in HomeStore code
- **Auth Selection**: 85% confidence → Ceph's simpler approach validated
- **Batch Size**: 90% confidence → Ceph's small-batch philosophy proven

### Next Steps:

1. **Update critical review** with Ceph findings
2. **Revise Q4/Q5 sections** in main design doc with concrete recommendations
3. **Update open questions** based on Ceph patterns
4. **Ready for user discussion** when they return from rest

---

## References

- Ceph OSD Scrub Configuration: `/Users/xiaoxchen/Code/ceph/src/common/options.cc`
- Ceph Scrub Messages: `/Users/xiaoxchen/Code/ceph/src/messages/MOSDRepScrub.h`
- Ceph Auth Selection: `/Users/xiaoxchen/Code/ceph/src/osd/PGBackend.cc` (lines ~1240-1350)
- Ceph Scrub Comparison: `/Users/xiaoxchen/Code/ceph/src/osd/PG.cc` (lines ~3050-3200)
- HomeStore Index Thread Safety: `/Users/xiaoxchen/Code/HomeStore/src/include/homestore/index/index_table.hpp`
- HomeStore Handler Dispatch: `/Users/xiaoxchen/Code/HomeStore/src/lib/replication/repl_dev/raft_repl_dev.cpp:1256`
- HomeObject PG State: `/Users/xiaoxchen/Code/HomeObject/src/include/homeobject/pg_manager.hpp`
