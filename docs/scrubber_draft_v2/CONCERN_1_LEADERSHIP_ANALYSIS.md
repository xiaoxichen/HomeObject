# Concern #1: Leadership Change Handling - Deep Dive

**Status**: ✅ RESOLVED
**Problem**: Design doesn't specify how scrubber detects and handles leadership changes mid-scrub.
**Solution**: Hybrid approach (Option C) - periodic `is_leader()` check + force-clear on new leader
**Updated in**: scrubber_design_formal.md Section 2.6

---

## Problem Statement

**Current Design Says**:
> "Runs on PG leader replica only (leadership changes abort in-flight scrubs)"

**What's Missing**: HOW does this happen?

---

## Concrete Failure Scenarios

### Scenario A: Stuck SCRUBBING Flag

```
T0: Node A (leader) starts scrub
    → Sets PGStateMask::SCRUBBING via fetch_or
    → active_task_id = 12345

T1: Network partition isolates Node A
    → Node B elected as new leader
    → Node A doesn't know it lost leadership

T2: Node A continues scrubbing (thinks it's still leader)
    → Processes batches 1, 2, 3...

T3: Node B (new leader) wants to start scrub
    → Checks: pg->state & SCRUBBING → TRUE
    → try_start_scrub() returns false
    → ❌ Cannot start scrub

T4: Node A finishes scrub, clears SCRUBBING flag
    → But Node A is partitioned, state change doesn't replicate
    → ❌ Flag stuck on Nodes B and C

Result: PG stuck in SCRUBBING state forever on new leader's side
```

### Scenario B: Duplicate Scrubs

```
T0: Node A (leader) starts scrub, sets SCRUBBING
T1: Node A loses leadership, Node B becomes leader
T2: Node A doesn't detect loss, continues scrubbing
T3: Node B also starts scrub (if flag not set yet, or force-clears)
T4: ❌ Two scrubs running simultaneously
```

### Scenario C: Resource Leak

```
T0: Node A reserves scrub slot: active_scrub_count++
T1: Node A loses leadership, aborts scrub
T2: Node A forgets to release slot: active_scrub_count--
T3: ❌ Slot leaked, future scrubs blocked
```

---

## Questions to Answer

1. **Detection**: How does old leader know it lost leadership?
2. **Abort Timing**: When should old leader stop scrubbing?
3. **Flag Cleanup**: Who clears SCRUBBING flag?
4. **Partition Handling**: What if old leader is unreachable?
5. **New Leader Behavior**: Should new leader wait or force-clear?

---

## Investigation Results: How HomeObject Handles Leadership

### Finding 1: is_leader() Check Pattern

**Current HomeObject Pattern** (from put_blob, delete_blob, create_shard):
```cpp
// Before starting leader-only operation
if (!repl_dev->is_leader()) {
    LOGW("failed to do operation, not leader");
    return folly::makeUnexpected(BlobError::NOT_LEADER);
}
```

**Usage**: Check at operation START, reject if not leader.

**Gap for Scrubber**: Long-running operation (10M blobs = 12 hours for deep scrub)
- Check at start is not sufficient
- Need periodic checks during execution

### Finding 2: Raft Callbacks Available

**From HomeStore RaftReplDev** (raft_repl_dev.cpp:2035-2043):
```cpp
case nuraft::cb_func::Type::BecomeFollower: {
    RD_LOGD("Raft channel: Received BecomeFollower");
    become_follower_cb();
    return nuraft::cb_func::ReturnCode::Ok;
}
case nuraft::cb_func::Type::BecomeLeader: {
    RD_LOGD("Raft channel: Received BecomeLeader");
    become_leader_cb();
    return nuraft::cb_func::ReturnCode::Ok;
}
```

**Available Callbacks**:
- `BecomeFollower`: Called when node loses leadership
- `BecomeLeader`: Called when node gains leadership

**Question**: Can HomeObject hook into these callbacks?

### Finding 3: No Existing Long-Running Operations

HomeObject operations are all short-lived:
- `put_blob`: Single write, completes in ms
- `delete_blob`: Single write, completes in ms
- `create_shard`: Single write, completes in ms

**Scrubber is FIRST long-running leader-only operation in HomeObject!**

---

## Solution Options

### Option A: Periodic is_leader() Check (Simple)

**Approach**: Check leadership before each batch

```cpp
void scrub_pg(pg_id_t pg_id, ScrubType type) {
    for (each shard) {
        for (each batch) {
            // Check leadership BEFORE processing batch
            if (!can_continue_scrub(pg_id, type)) {
                abort_scrub(pg_id, "Leadership lost");
                return;
            }

            process_batch();
            sleep_between_batches();
        }
    }
}

bool can_continue_scrub(pg_id_t pg_id, ScrubType type) {
    // Check NO_SCRUB flags
    if (no_scrub_.load()) return false;
    if (type == DEEP && no_deep_scrub_.load()) return false;

    // Check PG state
    auto pg = get_pg(pg_id);
    auto state = pg->state.load();
    if (state & PGStateMask::BASELINE_RESYNC) return false;
    if (state & PGStateMask::SCRUBBING == 0) return false;  // Someone cleared our flag

    // ✅ NEW: Check leadership
    auto repl_dev = pg->repl_dev_;
    if (!repl_dev->is_leader()) {
        LOGW("Leadership lost during scrub for pg={}", pg_id);
        return false;
    }

    return true;
}
```

**Pros**:
- ✅ Simple, no new infrastructure
- ✅ Follows existing HomeObject pattern
- ✅ Detects loss within 1 batch latency (2ms shallow, 110ms deep)

**Cons**:
- ❌ Polling (not event-driven)
- ❌ Old leader still wastes 1 batch worth of work

**Cleanup on Abort**:
```cpp
void abort_scrub(pg_id_t pg_id, const char* reason) {
    LOGW("Aborting scrub for pg={}, reason: {}", pg_id, reason);

    // Clear SCRUBBING flag
    auto pg = get_pg(pg_id);
    pg->state.fetch_and(~static_cast<uint64_t>(PGStateMask::SCRUBBING));

    // Release concurrency slots
    active_scrub_count_.fetch_sub(1);
    if (current_type == DEEP) {
        active_deep_scrub_count_.fetch_sub(1);
    }

    // Update task state
    auto task = get_scrub_task(current_task_id);
    task->status = TaskStatus::ABORTED;
    task->abort_reason = reason;
    persist_scrub_task(task);
}
```

---

### Option B: Raft Callback Hook (Event-Driven)

**Approach**: Hook into RaftReplDev BecomeFollower callback

**Challenge**: Need to extend HomeStore's ReplDev interface to expose callbacks to HomeObject.

**Pseudocode** (requires HomeStore changes):
```cpp
// In HomeObject
class HSHomeObject : public ReplDevCallbackHandler {
    void on_become_follower(group_id_t group_id) override {
        auto pg_id = group_to_pg(group_id);

        // Abort any running scrub
        if (active_scrubs_.contains(pg_id)) {
            abort_scrub(pg_id, "Leadership lost");
        }
    }
};
```

**Pros**:
- ✅ Event-driven (immediate notification)
- ✅ Clean architecture

**Cons**:
- ❌ Requires HomeStore interface changes
- ❌ More complex
- ❌ Longer implementation time

---

### Option C: Hybrid (Recommended)

**Approach**: Periodic check + force-clear on new leader

**Old Leader** (check before each batch):
```cpp
if (!repl_dev->is_leader()) {
    abort_scrub();  // Clean exit
    return;
}
```

**New Leader** (on becoming leader):
```cpp
// Check if PG stuck in SCRUBBING with no active task
if (pg->state & SCRUBBING && pg->scrub_metadata.active_task_id == 0) {
    LOGW("Force-clearing stuck SCRUBBING flag from previous leader");
    pg->state.fetch_and(~SCRUBBING);
}
```

**Recovery from Partitioned Old Leader**:
- Old leader eventually checks is_leader(), aborts
- OR: New leader force-clears after timeout (e.g., 2x batch timeout)

**Pros**:
- ✅ Simple implementation
- ✅ Handles both clean and dirty cases
- ✅ No HomeStore changes needed
- ✅ Self-healing

**Cons**:
- ❌ Small window where both leaders might scrub (1 batch duration)

---

## Recommendation

**Use Option C (Hybrid)** for MVP:

1. **Immediate (Phase 1)**:
   - Add `is_leader()` check to `can_continue_scrub()`
   - Abort gracefully on leadership loss

2. **Enhancement (Phase 2)**:
   - Add force-clear logic on new leader election
   - Add timeout-based recovery for stuck flags

**Rationale**:
- Simplest solution
- No HomeStore changes required
- Handles 99% of cases cleanly
- Edge cases (partition) self-heal eventually

