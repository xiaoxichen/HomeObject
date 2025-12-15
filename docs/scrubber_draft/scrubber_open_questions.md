# Scrubber Design - Open Questions & Clarifications

## Critical Questions for Discussion

### Q4: Resource Control (Not Yet Discussed)

**1. I/O Rate Limiting**
- Should scrub reuse GC's RateLimiter pattern?
- What percentage of I/O bandwidth should scrub be allowed to consume?
  - GC uses 10% of throughput (~7680 blocks/sec on HDD)
  - Should scrub be similar? Lower? Configurable?
- Should rate limit be per-PG or global?

**2. Concurrent Scrub Limits**
- Max number of PGs scrubbed concurrently?
  - Suggestion: 1-2 concurrent scrubs to minimize impact
- Should limit be configurable?

**3. Scheduling Windows**
- Should scrub only run during off-peak hours?
- Or run anytime with rate limiting?
- Ceph allows configurable time windows

**4. Priority/Preemption**
- If client I/O spikes during scrub, should scrub pause/slow down?
- Or rely on rate limiting to keep impact minimal?

---

### Q5: Initiation & Scheduling (Not Yet Discussed)

**1. Trigger Mechanisms**
- Manual: HTTP endpoint to trigger scrub on specific PG?
- Auto-scheduled: Periodic scrubs (daily shallow, weekly deep)?
- Both?

**2. PG Selection**
- Scrub all PGs in round-robin?
- Prioritize PGs that haven't been scrubbed recently?
- Allow admin to specify PG priority?

**3. Shallow vs Deep Scheduling**
- Separate schedules? (daily shallow, weekly deep)
- Or: shallow first, escalate to deep if issues found?

**4. Persistence of Schedule State**
- Track: last_scrub_time per PG
- Track: next_scheduled_scrub_time per PG
- Where to persist? (scrub metablk)

---

### Deep Scrub Details (Deferred from Shallow Scrub Discussion)

**1. Checksum Algorithm**
- BlobHeader supports: CRC32 (implemented), MD5, SHA1 (defined but not implemented)
- For deep scrub, should we:
  - A) Use existing BlobHeader.hash (CRC32 only for now)
  - B) Compute new MD5/SHA256 during scrub (implement on-the-fly)
  - C) Store MD5/SHA256 in BlobHeader going forward (requires migration)

**2. Checksum Storage**
- Current: BlobHeader.hash is stored with blob data on disk
- Deep scrub needs to:
  - Read blob data from each replica
  - Extract/compute checksum
  - Compare checksums across replicas
- Question: Read entire blob or just header?

**3. Deep Scrub Response Format**
```
DeepScrubResponse {
  blobs: [
    {
      blob_id: X,
      state: ALIVE,
      checksum: <32 bytes>,  // MD5/SHA256
      size: <bytes>
    }
  ]
}
```

**4. Checksum Mismatch Handling**
- If checksums differ: which replica has correct data?
- Majority voting on checksum?
- Re-read and re-compute to rule out transient read errors?

---

## Implementation Details Needing Clarification

### 1. Message Format - Exact Wire Protocol

**ScrubRequest serialization:**
```cpp
// Option A: Use flatbuffers (like baseline resync)?
// Option B: Simple struct serialization?
// Option C: JSON (easier debugging)?

struct ScrubRequest {
  pg_id_t pg_id;
  shard_id_t shard_id;
  blob_id_t range_start;
  blob_id_t range_end;
  ScrubType type; // SHALLOW, DEEP
};
```

**ScrubResponse size limits:**
- If shard has 1M blobs, response = 1M * (8 bytes blob_id) = 8MB
- Exceeds 4MB tested payload size
- Need batching within response? Or rely on range_start/range_end batching?

### 2. Handler Registration Timing

**When to call `bind_data_service_request()`?**
- Option A: During PG creation (per-PG registration)
  - Pro: Clean lifecycle, handler removed when PG destroyed
  - Con: Need to register for each PG separately

- Option B: Once at HomeObject init (global registration)
  - Pro: Single registration, dispatch by group_id
  - Con: Handler persists even if PG destroyed

**Current thought:** Option B seems cleaner - register once at init, dispatch internally by pg_id.

### 3. Scrub Metablk Details

**Scope:**
```cpp
struct scrub_info_superblk {
  pg_id_t pg_id;
  int64_t last_scrub_lsn;
  uint64_t last_scrub_time;
  uint64_t last_scrub_max_blob_id;
  ScrubState state;  // CLEAN, INCONSISTENT
  ScrubType last_type; // SHALLOW, DEEP

  // Statistics
  uint64_t total_blobs_scanned;
  uint64_t inconsistencies_found;
  uint64_t last_scrub_duration_ms;
};
```

**Questions:**
- One metablk per PG? Or single metablk for all PGs?
- How to handle metablk updates during scrub progress? (CP flush?)

### 4. Thread Safety & Reactor Dispatch

**Handler Context:**
- Handler runs in nuraft_mesg service thread
- Need to dispatch to HomeObject's iomgr reactor
- Pattern from HomeStore PUSH_DATA/FETCH_DATA handlers?

**Index Access:**
- Is `index_table->query()` thread-safe?
- Need to acquire locks before querying?
- Check existing index access patterns

### 5. Failure Scenarios

**Leader Crashes During Scrub:**
- Scrub simply fails (no partial state to recover)
- Next scrub starts fresh
- Question: Should new leader auto-restart scrub? Or wait for next scheduled run?

**Follower Crashes During Scrub:**
- Leader times out waiting for response
- Proceed with remaining replicas (partial scrub)
- Mark PG as "scrub incomplete" vs "INCONSISTENT"?

**Network Partition:**
- Follower unreachable via data_service_request
- Same as follower crash - timeout and proceed
- Log warning about partial scrub

**Concurrent Scrub Requests:**
- If scrub already running on PG, reject new request?
- Or queue/cancel previous?
- Check SCRUBBING flag before starting

---

## Code Verification Needed

### 1. Batch Size Determination

**Question:** How to choose blob_id batch size for scrub requests?

Current code shows `query_blobs_in_shard()` takes `max_num_in_batch` parameter.

**Options:**
- Fixed batch size (e.g., 10000 blobs)?
- Dynamic based on estimated response size (blob_count * 8 bytes)?
- Configurable?

**Related:** What if single shard has 10M blobs? Need pagination strategy.

### 2. LSN Query for Replication Lag Check

**Need to verify:**
- How to get current LSN from each replica?
- Is it in `peer_info.last_commit_lsn`? (YES - verified in pg_manager.hpp:88)
- Should spot check skip if LSN lag > threshold (e.g., 1000 LSNs)?

### 3. Index Query with State Filter

**Current `query_blobs_in_shard()` returns:**
```cpp
vector<BlobInfo> {shard_id, blob_id, pbas}
```

**For scrub, need to check if pbas == tombstone_pbas**

**Verified:** `tombstone_pbas{0, 0, 0}` is defined (hs_homeobject.hpp:477)

**Question:** Should we:
- Filter tombstones in query_blobs_in_shard() before returning? (modify function)
- Filter in scrub handler after receiving results? (cleaner, no API change)

**Recommendation:** Filter in scrub handler - don't modify existing API.

### 4. get_peer_info() Usage

**Verified:** `HS_PG::get_peer_info(vector<peer_info>&)` exists (hs_homeobject.hpp:360)

**Returns:** `peer_info{id, name, last_commit_lsn, last_succ_resp_us, can_vote}`

**Usage for scrub:**
```cpp
std::vector<peer_info> peers;
pg->get_peer_info(peers);

for (auto& peer : peers) {
  // Send scrub request to peer.id
  // Check peer.last_commit_lsn for lag detection
}
```

---

## Design Gaps Identified

### 1. Scrub Progress Reporting

**Gap:** How does admin track scrub progress for long-running PG?

**Considerations:**
- Scrub might take minutes/hours for large PG
- Admin should see: "Scrubbing PG X, 5/20 shards complete, 50K/1M blobs scanned"
- Where to store progress? (in-memory only, or persist?)
- HTTP endpoint to query progress?

**Related to Q3.5 (Reporting) - parked**

### 2. Scrub Cancellation

**Gap:** Can admin cancel in-progress scrub?

**Considerations:**
- Scrub is read-only, safe to cancel anytime
- Set flag: `scrub_cancelled[pg_id] = true`
- Handler checks flag periodically, stops processing
- Clean up SCRUBBING state flag

**Not critical for MVP, but worth considering**

### 3. Deep Scrub Optimization

**Gap:** Deep scrub reads full blob data - very expensive

**Optimization ideas:**
- Read only first N bytes for checksum? (breaks checksumming)
- Cache checksums from shallow scrub? (checksums not in index)
- Read blob in chunks with early termination if checksum already mismatches?

**Defer optimization until MVP tested**

### 4. Quorum Math

**Gap:** What is "quorum" for inconsistency detection?

**Scenarios:**
- 3 replicas: 2/3 agree → majority
- 2 replicas: 1 says X, 1 says Y → tie, cannot determine
- 5 replicas: 3/5 agree → majority

**For 2-replica case:**
- Cannot use majority voting
- Default to leader as source of truth? Or manual intervention?

**Needs decision in Q3.2 (currently parked)**

### 5. Tombstone GC Timing

**Gap:** When are tombstones actually removed from index?

**Current understanding:**
- delete_blob() marks blob as tombstone (pbas = {0,0,0})
- GC removes tombstone entries during purge_reserved_chunk()
- Question: How long until tombstone is GC'd? Configurable?

**Impact on scrub:**
- If GC runs between batch scrub and spot check, causes ambiguity
- Short GC interval → more false positives
- Long GC interval → tombstones accumulate

**Need to understand GC tombstone retention policy**

---

## Architectural Decisions to Validate Tomorrow

### 1. Should spot check query leader's own index again?

**Current design:** Spot check queries ALL replicas (including leader)

**Question:** Leader already has its index from batch scrub. Should it:
- A) Re-query its own index during spot check (consistent, but redundant)
- B) Reuse batch scrub results (optimization, but what if leader's index changed?)

**Recommendation:** Re-query (Option A) - simple, consistent, negligible cost

### 2. Spot check granularity

**Current design:** Batch all questionable blobs from entire PG scan, then spot check

**Alternative:** Spot check after each shard (more immediate, but more network round-trips)

**Trade-off:**
- Batch approach: Fewer network calls, but delay in confirming inconsistencies
- Per-shard approach: Faster feedback, but more overhead

**Recommendation:** Keep batched approach - cleaner, more efficient

### 3. When to set SCRUBBING flag?

**Options:**
- A) Set at PG scrub task start (before any shard scans)
- B) Set per-shard (while scrubbing shard X)
- C) Don't set at all (scrub is background, doesn't affect state)

**Recommendation:** Option A - set at task start, clear at task end. Provides visibility.

---

## Summary for Tomorrow's Discussion

**High Priority:**
1. Q4: Resource Control - rate limiting, concurrent scrubs, scheduling windows
2. Q5: Initiation & Scheduling - manual vs auto, PG selection, shallow/deep cadence

**Medium Priority:**
3. Deep Scrub checksum strategy (which algorithm, storage, mismatch handling)
4. Message format details (serialization, size limits, batching)
5. Quorum math for 2-replica case

**Low Priority (Can Defer to Implementation):**
6. Progress reporting and cancellation
7. Thread safety verification
8. Optimization strategies

**Questions for User:**
- Should we maintain separate schedules for shallow vs deep scrub?
- For 2-replica PG, how to handle inconsistency (no majority)?
- Should scrub auto-start on new leader, or only on schedule?
