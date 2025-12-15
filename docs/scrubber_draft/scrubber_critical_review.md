# Scrubber Design - Critical Review & Potential Issues

## Overview
This document contains critical analysis of architectural decisions made so far, identifying potential issues, edge cases, and areas needing deeper thought.

---

## ✅ Solid Decisions (High Confidence)

### 1. Communication Mechanism (Q1)
**Decision:** nuraft_messenger data service with Follower→Leader flow

**Validation:**
- ✅ Confirmed `ReplDev::group_msg_service()` exists in production code
- ✅ HomeStore uses it for PUSH_DATA/FETCH_DATA - proven pattern
- ✅ 4MB payload tested - sufficient for index data
- ✅ UUID-based addressing avoids network discovery issues

**Confidence Level:** 95% - This is well-validated

### 2. Follower→Leader Direction (Q1)
**Decision:** Followers send indexes to leader, leader compares

**Validation:**
- ✅ Simpler state management (no progress tracking per-follower)
- ✅ Enables majority voting (leader has all replica data)
- ✅ Natural timeout/partial scrub handling
- ✅ Followers work independently at own pace

**Confidence Level:** 95% - Well-reasoned decision

---

## ⚠️ Decisions Needing Deeper Validation

### 1. blob_id Range Approach (Q2)

**Decision:** Use blob_id ranges [x, y) instead of LSN for consistency

**Concerns Identified:**

**A) blob_id is PG-level, not shard-level**
```
PG has single blob_sequence_num counter
Shard A creates blob → blob_id = 1000
Shard B creates blob → blob_id = 1001
Shard A creates blob → blob_id = 1002
```

**Implication for scrub:**
- When scrubbing Shard A with range [0, max_blob_id=1002]:
  - Expected: blobs [1000, 1002]
  - Index query filters by `shard_id == A`, so correctly returns [1000, 1002]
  - ✅ Works correctly!

**But wait - what if max_blob_id keeps increasing during scrub?**
- Scrub starts: max_blob_id = 1000 (captured at task start)
- While scrubbing: new blobs created → blob_id now 1050
- Scrub requests range [0, 1000) - misses blobs [1000, 1050)
- ✅ This is intentional - we scrub snapshot at task start time

**Validation:** ✅ Seems correct, but needs code review to confirm

---

**B) Open Shard New Blob Scenario**

**Scenario:**
```
Time T0: Scrub task starts, max_blob_id = 1000
Time T1: Scrub requests Shard A, range [0, 1000)
Time T2: New blob 1001 created on Shard A (between T0 and T1)
Time T3: Follower processes request, returns blobs including 1001
```

**Problem:** Follower has blob 1001 (within its index), but it's outside scrub range [0, 1000)

**Question:** Should follower:
- A) Return all blobs in its index (including 1001)?
- B) Filter to only [0, 1000)?

**Current design says:** Request specifies range [start, end), follower should respect it

**But our query does:**
```cpp
auto start_key = BlobRoute{shard_id, start_blob_id};
auto end_key = BlobRoute{shard_id, max_blob_id};  // from request
// Query range [start_key, end_key]
```

**Validation:** ✅ Query respects range, so follower won't return blob 1001 if max=1000

**BUT WAIT:** What if blob 1001 was created BEFORE T0, but replicated to follower AFTER T1?

```
Time T0: Leader has blobs [1...1000], starts scrub, max=1000
Time T0: Follower has blobs [1...999] (lag), blob 1000 in-flight
Time T1: Scrub request [0, 1000) sent
Time T2: Follower receives blob 1000 (replication caught up)
Time T3: Follower processes scrub request
```

**Follower sees:** blob 1000 in index, within range [0, 1000) → returns it
**Leader sees:** blob 1000 in its own index → no inconsistency

**Validation:** ✅ This works correctly! Blob 1000 was created before scrub start, within range.

**Real edge case:**
```
Time T0: Scrub starts, leader max_blob_id = 1000
Time T0: Follower lagging, has blobs [1...998]
Time T1: Leader creates blob 1001, 1002 (AFTER scrub started)
Time T2: Blob 1001, 1002 replicate to follower
Time T3: Scrub request [0, 1000) arrives at follower
```

**Follower has:** [1...998, 1001, 1002]
**Query [0, 1000) returns:** [1...998] only (1001, 1002 outside range)
**Leader has:** [1...1000]
**Comparison:** Follower missing [999, 1000]

**Spot check:** Query blob 999, 1000 on all replicas
- If follower caught up → has 999, 1000 → resolved (lag)
- If follower still missing → real inconsistency

**Validation:** ✅ Spot check handles this correctly!

**Confidence Level:** 85% - Logic seems sound, but complex edge cases need testing

---

### 2. Filtering Tombstones (Q3.1)

**Decision:** Filter tombstones from batch scrub, include in spot check

**Implementation concern:**

**Batch scrub response:**
```cpp
// Handler code (on each replica)
auto results = query_blobs_in_shard(pg_id, shard_id, start, end);
// Filter tombstones
vector<blob_id_t> alive_blobs;
for (auto& blob_info : results) {
  if (blob_info.pbas != tombstone_pbas) {
    alive_blobs.push_back(blob_info.blob_id);
  }
}
return alive_blobs;
```

**Spot check response:**
```cpp
// Handler code
for (auto& blob_id : request.blob_ids) {
  auto result = get_blob_from_index(shard_id, blob_id);
  if (result.hasValue()) {
    if (result.value() == tombstone_pbas) {
      response.add({blob_id, TOMBSTONE});
    } else {
      response.add({blob_id, ALIVE});
    }
  } else {
    response.add({blob_id, NOT_FOUND});
  }
}
```

**Edge case:** Blob deleted between batch scrub and spot check

```
Time T1: Batch scrub - Follower has blob 500 (ALIVE)
Time T2: delete_blob(500) committed on leader
Time T3: delete_blob(500) replicated to follower → tombstone
Time T4: Spot check blob 500
  Leader: TOMBSTONE
  Follower: TOMBSTONE
  → No inconsistency (resolved via replication)
```

**Validation:** ✅ Spot check correctly handles this!

**Edge case 2:** GC runs between batch and spot check

```
Time T1: Batch scrub
  Leader: blob 500 TOMBSTONE (filtered out)
  Follower: blob 500 ALIVE (included)
  → Difference detected: "follower has extra blob 500"

Time T2: GC runs on leader, removes tombstone entry

Time T3: Spot check blob 500
  Leader: NOT_FOUND (tombstone GC'd)
  Follower: ALIVE
  Replica 2: ALIVE
  → Majority vote: ALIVE → Leader is wrong?
```

**Problem:** Leader GC'd tombstone, lost evidence that blob was deleted!

**Implications:**
- If majority of replicas still have blob alive → appears leader lost data
- But actually leader correctly deleted and GC'd the blob
- Follower just lagging on delete replication

**Mitigation:**
- Check if GC recently ran on leader? (hard to track)
- Delay tombstone GC longer? (accumulates tombstones)
- Accept this ambiguity? (admin manually investigates)

**Recommendation:** Document this as known limitation - tombstone GC creates ambiguity
- Admin should check replication lag when investigating
- If lag is high, likely just lag, not corruption

**Confidence Level:** 70% - Known ambiguity, may need better heuristic

---

### 3. Spot Check Timing (Q3.1)

**Decision:** Defer spot check to end of PG scan

**Concern:** By end of PG scan, lag might have resolved for early shards but not late shards

**Scenario:**
```
PG has 100 shards, scrub takes 10 minutes

Time T0: Start scrubbing Shard 1, find difference
Time T5: Scrubbing Shard 50
Time T10: Finished all shards, start spot check
Time T10: Spot check blobs from Shard 1 (found 10 min ago)
  → Lag already resolved
```

**Is this bad?** No, spot check correctly filters out lag - works as intended!

**Alternative scenario:**
```
Time T0: Shard 1 has corruption (real inconsistency)
Time T10: Spot check still finds inconsistency (doesn't resolve via lag)
  → Correctly detected!
```

**Validation:** ✅ Deferring spot check is actually fine - gives lag more time to resolve

**BUT:** What if spot check takes long time, and NEW lag appears?

```
Time T0: Scrub Shard 1, no differences
Time T10: Start spot check for Shard 50 differences
Time T11: Shard 1 has new write, lag develops
Time T12: Spot check completes
Time T13: Report says "No issues" but Shard 1 now inconsistent
```

**Implication:** Scrub is **point-in-time check at scrub_lsn**, not continuous monitor
- New writes after scrub_lsn are not checked
- This is by design - scrub checks historical state
- Next scrub run will catch new issues

**Validation:** ✅ This is acceptable - scrub is snapshot-based

**Confidence Level:** 90% - Correct by design

---

## 🔴 Critical Gaps & Unresolved Issues

### 1. Two-Replica Quorum Problem

**Scenario:** PG with 2 replicas (leader + 1 follower)

**Spot check finds:**
```
Blob 500:
  Leader: ALIVE
  Follower: NOT_FOUND
```

**Question:** Which is correct? No majority to vote!

**Options:**
- A) Default to leader as source of truth
  - Simple, but what if leader is corrupted?
- B) Require admin manual decision
  - Safe, but slows down resolution
- C) Use Raft term/LSN to determine freshness
  - Complex, might not help (both could be at same LSN)

**Current decision:** Q3.2 parked - "use quorum"
**Problem:** Doesn't work for 2-replica case!

**Recommendation:** Need explicit rule for 2-replica:
- Default to leader, but:
- If leader is minority (1 vs many), flag for manual review
- Admin must investigate logs/metrics to determine truth

**Severity:** HIGH - Impacts common 2-replica deployment

---

### 2. Batch Size Calculation

**Problem:** How many blobs per scrub request batch?

**Constraints:**
- Response size: blob_count * ~8 bytes (blob_id only for shallow)
- 4MB payload tested → ~500K blobs per batch
- But: very large shards might have 10M+ blobs

**Current design:** Uses `max_num_in_batch` parameter, but value TBD

**Options:**
- A) Fixed batch: 100K blobs
  - Simple, but might be too large/small for some shards
- B) Dynamic: estimate based on shard size
  - Better, but requires querying shard metadata first
- C) Configurable: admin sets batch size
  - Flexible, but more config complexity

**Question:** What if single shard has 50M blobs?
- 50M / 100K = 500 batches
- 500 round-trips per shard
- Scrub time: 500 * (network_latency + query_time)
- Could take hours!

**Mitigation:** Rate limiting will help, but need realistic batch size estimate

**Recommendation:** Start with 100K blob batch, make configurable

**Severity:** MEDIUM - Affects performance, not correctness

---

### 3. Handler Thread Safety

**Problem:** nuraft_mesg handler runs in service thread, not HomeObject's reactor

**Current code pattern (from HomeStore FETCH_DATA):**
```cpp
void on_fetch_data_received(GenericRpcData& rpc_data) {
  // Running in nuraft_mesg service thread!

  // Need to dispatch to correct reactor:
  iomanager.run_on(..., [&]() {
    // Now in correct thread context
    // Access index_table safely
  });
}
```

**Questions:**
- Which reactor should scrub handler run on?
  - Random worker reactor (like GC)?
  - Specific reactor for PG?
- Is `index_table->query()` thread-safe?
  - Need to check IndexTable implementation
  - Likely needs to run on specific thread

**Impact:** If handler accesses index_table from wrong thread → crash/corruption

**Recommendation:** Copy FETCH_DATA handler pattern exactly
- Dispatch to appropriate reactor
- Code review HomeStore FETCH_DATA to understand thread model

**Severity:** HIGH - Correctness/safety issue

---

### 4. Scrub Task Lifecycle & Crashes

**Scenario:** Leader crashes mid-scrub

**Current design:** "Scrub simply fails, no state recovery"

**Questions:**
1. Who clears the SCRUBBING flag?
   - If leader crashes, flag stays set forever?
   - Next leader needs to clear stale flags on startup?

2. If new leader elected, should it:
   - A) Restart scrub from beginning?
   - B) Wait for next scheduled scrub?
   - C) Try to resume? (complex, no partial state)

**Recommendation:**
- On leader election, clear SCRUBBING flag for all PGs
- Don't auto-restart scrub (could thrash during leader flapping)
- Wait for next scheduled run

**But:** What if scrub is critical (admin-triggered to investigate issue)?
- Admin needs to manually re-trigger after leader stabilizes

**Severity:** MEDIUM - Operational clarity needed

---

### 5. Concurrent Scrub Prevention

**Problem:** What if scrub already running when new scrub triggered?

**Current design:** Check SCRUBBING flag before starting

**Race condition:**
```
Thread 1: Check SCRUBBING flag → not set
Thread 2: Check SCRUBBING flag → not set
Thread 1: Set SCRUBBING flag, start scrub
Thread 2: Set SCRUBBING flag, start scrub
→ Two scrubs running concurrently!
```

**Mitigation:** Use atomic compare-and-swap on flag?
```cpp
if (pg_state.is_state_set(SCRUBBING)) {
  return Error("Scrub already in progress");
}
// Atomic:
if (!pg_state.compare_exchange(expected=0, desired=SCRUBBING)) {
  return Error("Scrub already in progress");
}
```

**Severity:** MEDIUM - Can cause confusion/wasted work

---

### 6. max_blob_id Staleness During Long Scrub

**Scenario:**
```
Time T0: Scrub starts, max_blob_id = 1000
Time T0-T60: Scrubbing 100 shards (60 minutes)
Time T60: Spot check completes
Time T60: During scrub, 50K new blobs created (max_blob_id now 51000)
```

**Result:** Scrub report says "PG is CLEAN up to blob_id 1000"
**Reality:** 50K new blobs were never checked!

**Is this a problem?**
- By design: scrub checks snapshot at start time
- New blobs will be checked in next scrub run
- ✅ This is acceptable

**But:** Scrub report should clearly state:
```
Scrub Report:
  PG: 123
  Start Time: T0
  End Time: T60
  Scrubbed LSN: 5000
  Scrubbed blob_id range: [0, 1000)
  Status: CLEAN
  Note: Blobs [1001, 51000) created during scrub, not checked
```

**Severity:** LOW - Clarity issue, not correctness

---

## 🤔 Architectural Questions to Revisit

### 1. Should Leader Include Itself in Spot Check?

**Current design:** Leader queries all replicas including itself

**Alternative:** Leader reuses its own batch scrub results (optimization)

**Trade-off:**
- Reuse: Saves one index query (small optimization)
- Re-query: Handles case where leader's index changed during scrub
  - Blob deleted between batch and spot check
  - Leader's view is now different

**Example:**
```
Batch scrub (T0): Leader has blob 500 ALIVE
Spot check (T10): Leader re-queries → blob 500 now TOMBSTONE
```

If leader reused batch result → reports "blob 500 ALIVE"
But leader now thinks "blob 500 TOMBSTONE"
→ Inconsistent report!

**Recommendation:** Always re-query leader in spot check (consistency > optimization)

**Severity:** LOW - Optimization vs correctness trade-off

---

### 2. Should Spot Check Retry on All Differences or Sample?

**Scenario:** Batch scrub finds 10,000 differences

**Current design:** Spot check all 10,000 blobs

**Problem:** If follower is severely lagging (1000 LSNs behind):
- All 10,000 are likely replication lag
- Spot check creates 10,000 index queries × N replicas
- Huge overhead!

**Alternative:** Sample-based spot check
- Randomly select 100 blobs from 10,000
- Spot check those 100
- If >90% resolve (lag) → assume all are lag
- If <50% resolve → real issues, check all

**Trade-off:**
- Sampling: Much faster, but might miss real issues
- Check all: Thorough, but slow

**Recommendation:**
- If differences > threshold (e.g., 1000):
  - Check LSN lag first
  - If lag > 100 LSNs, assume lag, don't spot check (wait for next scrub)
- If differences < threshold:
  - Spot check all

**Severity:** MEDIUM - Performance vs thoroughness

---

### 3. Deep Scrub Shared Index Scan?

**Question:** Can shallow and deep scrub share index scan?

**Current design (implicit):**
- Shallow scrub: query index, compare blob_ids
- Deep scrub: query index, then read each blob data, compare checksums

**Optimization:** Combine into single pass
```
For each blob in index:
  Compare index entry across replicas (shallow check)
  IF shallow OK:
    Read blob data, compare checksums (deep check)
```

**Trade-off:**
- Separate: Simple, modular, but double index scan
- Combined: Efficient, but complex, no shallow-only option

**Recommendation:** Keep separate for MVP
- Shallow = report-only, find issues quickly
- Deep = expensive, run less frequently
- If needed, combine later as optimization

**Severity:** LOW - Optimization, not critical

---

## 📊 Summary of Risk Levels

### Critical (Must Address Before MVP)
1. ❌ Two-replica quorum problem - no decision rule
2. ❌ Handler thread safety - must use correct reactor
3. ⚠️ Concurrent scrub prevention - race condition

### High (Should Address for MVP)
4. ⚠️ Tombstone GC ambiguity - known limitation, needs documentation
5. ⚠️ Scrub flag lifecycle on leader crash - operational confusion

### Medium (Can Defer)
6. ⚠️ Batch size calculation - affects performance
7. ⚠️ Spot check sampling strategy - performance vs thoroughness
8. ⚠️ Large difference set handling - 10K+ differences

### Low (Nice to Have)
9. ℹ️ Leader self-query in spot check - correctness vs optimization
10. ℹ️ Scrub report clarity - UI/UX issue
11. ℹ️ Deep/shallow combined scan - optimization

---

## Recommended Actions Before Implementation

### Immediate (Before Starting Q4/Q5 Discussion)
1. ✅ Decide two-replica handling strategy
2. ✅ Review HomeStore FETCH_DATA handler for thread model
3. ✅ Design scrub flag atomic operations

### Before MVP Implementation
4. ✅ Define batch size parameter (start with 100K, configurable)
5. ✅ Document tombstone GC ambiguity in design doc
6. ✅ Design scrub report format (include blob_id ranges, timestamps)

### During Implementation
7. Test edge cases:
   - Scrub during high write load (moving target)
   - Scrub with lagging follower
   - Leader crash mid-scrub
   - Concurrent scrub attempts
8. Code review against HomeStore patterns (thread safety, error handling)

### Post-MVP
9. Performance testing with large shards (10M+ blobs)
10. Spot check optimization (sampling, LSN lag threshold)
11. Combined shallow/deep scan optimization

---

## Final Assessment

**Overall Design Soundness:** 80%

**Strong Points:**
- Communication mechanism well-validated
- Follower→Leader flow well-reasoned
- blob_id range approach handles most edge cases
- Spot check elegantly filters lag

**Weak Points:**
- Two-replica case unresolved (critical gap)
- Thread safety needs verification (safety issue)
- Tombstone GC creates ambiguity (known limitation)
- Performance untested for very large PGs

**Recommendation:** Address critical gaps (1, 2, 3) before proceeding to implementation.
Q4 and Q5 discussion can proceed in parallel.

---

## 🔍 Ceph Reference Analysis - Validation & Updates

### Summary

After analyzing Ceph's scrubbing implementation (`/Users/xiaoxchen/Code/ceph`), several critical findings emerged that **validate** our design decisions and **require changes** to others.

For detailed analysis, see: `/Users/xiaoxchen/Code/HomeObject/docs/scrubber_ceph_reference.md`

### ✅ Validated Decisions

**1. Communication Pattern** - Confirmed ✅
- Ceph uses similar request/response pattern via messages
- Our nuraft_messenger approach is sound

**2. Concurrent Scrub Limit** - Confirmed ✅
- Ceph: `osd_max_scrubs = 1` (default)
- Our proposal: 1-2 concurrent scrubs per node ✅ **CORRECT**

**3. Thread Safety Pattern** - Confirmed ✅
- HomeStore verified: `index_table->get()` uses atomic `incr/decr_pending_request_num`
- Read operations are thread-safe (index_table.hpp:193-198)
- **Dispatcher pattern verified**: HomeStore FETCH_DATA uses `iomanager.run_on_forget(reactor_regex::random_worker, ...)`
- **Action**: Copy exact pattern for scrub handlers

**4. Atomic Flag Operations** - Confirmed ✅
- HomeObject PG state uses `fetch_or/fetch_and` (pg_manager.hpp)
- **Solution for concurrent scrub prevention**:
  ```cpp
  auto old_state = pg->state.fetch_or(PGStateMask::SCRUBBING, std::memory_order_acquire);
  if (old_state & PGStateMask::SCRUBBING) {
    return Error("Already scrubbing");
  }
  ```
- ✅ **Solves race condition** identified in gap #5

### ❌ Critical Changes Required

**1. Batch Size - WRONG, Must Fix** ⚠️
- **Our proposal**: 100K blobs per batch
- **Ceph reality**: 5-25 objects per chunk (default)
- **Problem**: 100K is **2000-4000× larger** than Ceph!
  - Will cause massive latency spikes
  - Blocks client I/O for too long
  - Against proven production patterns

- **New recommendation**: **100-500 blobs per batch** (max)
  - Aligns with Ceph's philosophy: many small batches
  - Configurable via `osd_scrub_chunk_max` equivalent
  - Start conservative (100), tune based on testing

- **Severity**: **HIGH** - Performance/availability issue
- **Confidence**: 95% (Ceph production-proven)

**2. Auth Selection Strategy - Simpler Approach** ⚠️
- **Our design**: Majority voting via spot check
- **Ceph reality**: **Primary (leader) is auth by default**
  - Algorithm (PGBackend.cc:~1256):
    1. Check primary first
    2. If primary has errors → check followers
    3. First replica without errors → authoritative
    4. Compare all others against auth
  - **No majority voting** for selection

- **Recommendation**: **Use Ceph's simpler approach**
  - Leader is authoritative (unless has detectable errors)
  - Compare followers against leader
  - **Fallback to majority**: Only if leader state ambiguous (e.g., tombstone GC'd)
  - **For 2-replica**: Leader wins (aligns with Raft model)

- **Why change?**
  - ✅ Simpler implementation
  - ✅ Faster (no multi-round spot check needed)
  - ✅ Aligns with Raft's leader-based trust
  - ✅ Production-validated in Ceph

- **Severity**: MEDIUM - Simplification opportunity
- **Confidence**: 85% (Ceph-validated, fits Raft model)

### 📋 Concrete Recommendations for Q4 & Q5

**Q4: Resource Control**
1. **Max concurrent scrubs**: 1 per node (Ceph default)
2. **Batch size**: Start with **100 blobs** (NOT 100K), configurable up to 500
3. **Sleep between batches**: Optional delay (default 0), configurable
4. **Load threshold**: Defer to post-MVP (Ceph has it, but not critical)
5. **Block during recovery**: Check `PGStateMask::BASELINE_RESYNC` before starting

**Q5: Initiation & Scheduling**
1. **Manual trigger**: HTTP endpoint `/scrub?pg_id=X&deep=true`
2. **Auto-schedule**: Background task, check hourly
   - Scrub shallow once per day (configurable)
   - Scrub deep once per week (configurable)
3. **Persistence**: Store in scrub metablk (last_scrub_time, last_deep_scrub_time)
4. **Randomization**: Add ±25% jitter to avoid thundering herd
5. **Time windows**: Defer to post-MVP

### Updated Risk Assessment

**Critical Gaps - RESOLVED:**
1. ✅ **Two-replica quorum**: Use leader as auth (Ceph pattern)
2. ✅ **Handler thread safety**: Verified pattern (iomanager.run_on)
3. ✅ **Concurrent scrub prevention**: Atomic fetch_or (verified in code)

**New Critical Issue:**
4. ❌ **Batch size**: Must reduce from 100K to 100-500 (HIGH severity)

**Overall Design Soundness: 85% → 92%** (improved after Ceph validation)

**Strong Points (Updated):**
- Communication mechanism well-validated ✅
- Follower→Leader flow well-reasoned ✅
- blob_id range approach handles edge cases ✅
- Spot check filters lag ✅
- **Thread safety verified** ✅ (NEW)
- **Atomic operations confirmed** ✅ (NEW)
- **Ceph patterns align with design** ✅ (NEW)

**Remaining Weak Points:**
- Batch size needs correction (100K → 100-500)
- Auth selection can be simplified (use leader-first like Ceph)
- Tombstone GC ambiguity (known limitation, document)

**Recommendation**:
- ✅ Critical gaps addressed via Ceph analysis and code verification
- ⚠️ Must fix batch size before implementation
- ✅ Ready to proceed with Q4/Q5 discussion with concrete recommendations
