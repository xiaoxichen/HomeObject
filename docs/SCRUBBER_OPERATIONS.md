# Cross-Replica Scrubber Operations Guide

**Document Version:** 1.0
**Date:** 2025-12-14
**Status:** Final Operations Guide
**Target Audience:** SRE, DevOps, Operators

**Related Documents:**
- [SCRUBBER_DESIGN.md](./SCRUBBER_DESIGN.md) - Architecture and design rationale
- [SCRUBBER_IMPLEMENTATION.md](./SCRUBBER_IMPLEMENTATION.md) - Implementation details

---

## 1. Overview

### 1.1 What is Scrubbing?

The cross-replica scrubber is a background verification feature integrated into HomeObject Storage Manager that periodically checks data consistency across replicas within a Placement Group (PG).

**Two Scrub Modes:**
- **Shallow Scrub**: Fast metadata-only verification (checks blob existence)
  - Frequency: Daily (configurable)
  - Duration: ~40 seconds for 10M blobs

- **Deep Scrub**: Comprehensive data verification (compares checksums)
  - Frequency: Weekly (configurable)
  - Duration: ~12 hours for 10M blobs

### 1.2 When Scrubbing Runs

Scrubbing is **automatically scheduled** by the Storage Manager:
- Runs only on PG leader replicas
- Scheduled based on configurable intervals with randomization
- Can be manually triggered via HTTP API
- Can be temporarily disabled for maintenance

**Integration**: Scrubbing is part of Storage Manager, not a separate service. Configuration is included in the main HomeObject configuration file.

---

## 2. Monitoring

### 2.1 Key Metrics

All scrub metrics are exposed via Prometheus endpoint: `http://<homeobject-pod>:8080/metrics`

#### Activity Metrics

| Metric | Type | Description | Normal Range |
|--------|------|-------------|--------------|
| `homeobject_scrub_active_count` | Gauge | Currently running scrubs | 0-3 |
| `homeobject_scrub_deep_active_count` | Gauge | Currently running deep scrubs | 0-1 |
| `homeobject_scrub_tasks_total{type}` | Counter | Total scrub tasks initiated | Increasing |
| `homeobject_scrub_tasks_completed{type}` | Counter | Completed scrub tasks | ≥95% of total |
| `homeobject_scrub_tasks_failed{type}` | Counter | Failed scrub tasks | <5% of total |

#### Performance Metrics

| Metric | Type | Description | Expected Value |
|--------|------|-------------|----------------|
| `homeobject_scrub_duration_seconds{type}` | Histogram | Scrub duration | P99: shallow<60s, deep<14h |
| `homeobject_scrub_blobs_scanned_total` | Counter | Total blobs verified | Increasing |
| `homeobject_scrub_rpc_duration_ms{type}` | Histogram | RPC latency per batch | P99: shallow<10ms, deep<100ms |

#### Inconsistency Metrics

| Metric | Type | Description | Alert If |
|--------|------|-------------|----------|
| `homeobject_scrub_inconsistencies_found{type}` | Counter | Inconsistencies by type | Any increase |
| `homeobject_scrub_pgs_inconsistent` | Gauge | PGs marked inconsistent | > 0 |
| `homeobject_scrub_spot_check_filtered` | Counter | Transient diffs filtered | >50% of initial diffs = replication lag |

**Inconsistency Types**:
- `missing_blob`: Some replicas have blob, others don't
- `checksum_mismatch`: All replicas have blob, checksums differ
- `state_mismatch`: Some replicas mark blob as deleted, others as alive

#### Operational Metrics

| Metric | Type | Description | Alert If |
|--------|------|-------------|----------|
| `homeobject_scrub_scheduler_runs` | Counter | Scheduler executions | Not increasing (stalled) |
| `homeobject_scrub_no_scrub_flag` | Gauge | Scrubbing disabled (1=yes) | 1 for >1 hour |

### 2.2 Critical Alerts

Configure these alerts in your monitoring system:

#### CRITICAL - Page On-Call

```yaml
# Data inconsistency detected
Alert: homeobject_scrub_inconsistencies_found > 0
Severity: CRITICAL
Action: Investigate immediately - potential data corruption

# Scrubber crashing
Alert: rate(homeobject_scrub_tasks_failed[15m]) > 0.5
Severity: CRITICAL
Action: Check logs for errors, may indicate system issues
```

#### WARNING - Investigate During Business Hours

```yaml
# High failure rate
Alert: rate(homeobject_scrub_tasks_failed[1h]) / rate(homeobject_scrub_tasks_total[1h]) > 0.05
Severity: WARNING
Action: Review logs, check network/replica health

# Scrubs not completing on time
Alert: time() - homeobject_pg_last_scrub_time > homeobject_scrub_max_interval_sec * 1.1
Severity: WARNING
Action: Check if scrubs are running, review resource limits

# Slow scrub performance
Alert: histogram_quantile(0.99, rate(homeobject_scrub_duration_seconds_bucket{type="shallow"}[24h])) > 120
Severity: WARNING
Action: Review system load, consider increasing concurrency or reducing sleep
```

#### INFO - Awareness Only

```yaml
# Scrubbing manually disabled
Alert: homeobject_scrub_no_scrub_flag == 1 for >1h
Severity: INFO
Action: Verify if intentional, re-enable if maintenance complete
```

### 2.3 Log Monitoring

#### Key Log Patterns

**Log Levels:**
- `ERROR`: Scrub failures, exceptions - requires investigation
- `WARN`: Inconsistencies detected, replica timeouts - review required
- `INFO`: Scrub lifecycle events (started, completed) - normal operation
- `DEBUG`: Batch-level details - for performance analysis

**Important Log Messages:**

```
# Scrub lifecycle
[INFO] Scrub task {task_id} initiated for pg={pg_id}, type={shallow|deep}
[INFO] Scrub completed successfully for pg={pg_id}, no inconsistencies
[WARN] Persistent inconsistency detected: pg={pg_id}, shard={shard_id}, blob={blob_id}, type={type}

# Errors
[ERROR] Scrub failed for pg={pg_id}: {error_message}
[WARN] Leadership lost during scrub for pg={pg_id}, aborting task {task_id}
[WARN] Replica timeout for pg={pg_id}: {peer_id}
```

#### Log Queries

**Find scrub failures (last 24h):**
```
level:ERROR AND message:*scrub*failed* AND timestamp:[now-24h TO now]
```

**Find all inconsistencies:**
```
level:WARN AND message:*inconsistency*detected*
```

**Track scrub completion rate:**
```
level:INFO AND message:*scrub*completed*
```

---

## 3. Configuration & Tuning

### 3.1 Configuration Parameters

All parameters are configured in the main HomeObject configuration file under the `scrubber` section.

**Location**: `/etc/homeobject/config.yaml` (or as configured for Storage Manager)

#### Complete Parameter Reference

| Parameter | Default | Range | Impact |
|-----------|---------|-------|--------|
| **Batch Sizes** |
| `shallow_batch_size` | 500 | 100-2000 | Higher = faster scrub, more network load |
| `deep_batch_size` | 25 | 10-100 | Higher = faster scrub, more I/O impact |
| **Throttling (microseconds)** |
| `sleep_shallow_us` | 0 | 0-1000000 | Higher = slower scrub, less impact on client I/O |
| `sleep_deep_us` | 10000 | 0-1000000 | Higher = slower scrub, less disk I/O impact |
| **Timeouts (seconds)** |
| `timeout_shallow_sec` | 5 | 1-300 | Increase for slow networks or large clusters |
| `timeout_deep_sec` | 60 | 1-300 | Increase for large blobs or slow storage |
| `timeout_spot_check_sec` | 10 | 1-300 | Adjust based on replication lag |
| **Concurrency Limits** |
| `max_concurrent_scrubs` | 3 | 1-10 | Total active scrubs (shallow + deep) |
| `max_concurrent_deep_scrubs` | 1 | 1-10 | Active deep scrubs (subset of total) |
| **Scheduling Intervals (seconds)** |
| `scrub_min_interval_sec` | 86400 | 3600-604800 | Target time between shallow scrubs (1 day) |
| `scrub_max_interval_sec` | 604800 | min-2592000 | Hard deadline for shallow scrubs (7 days) |
| `deep_min_interval_sec` | 604800 | 3600-604800 | Target time between deep scrubs (7 days) |
| `deep_max_interval_sec` | 2592000 | min-5184000 | Hard deadline for deep scrubs (30 days) |
| `interval_randomize_ratio` | 0.5 | 0.0-1.0 | Jitter to prevent thundering herd (±50%) |
| `scheduler_interval_sec` | 600 | 60-3600 | How often scheduler checks for scrub-due PGs (10 min) |
| **Task Retention** |
| `task_retention_count` | 100 | 10-1000 | Keep last N completed tasks |
| `task_retention_days` | 7 | 1-90 | Keep tasks from last N days |
| **Spot-Check** |
| `spot_check_delay_ms` | 100 | 50-500 | Wait before re-verify to filter replication lag |
| `spot_check_batch_size` | 25 | 10-100 | Blobs per spot-check request |

**Example Configuration:**

```yaml
homeobject:
  # ... other Storage Manager config ...

  scrubber:
    # Conservative production settings
    max_concurrent_scrubs: 3
    max_concurrent_deep_scrubs: 1

    # Batch sizes (defaults are good)
    shallow_batch_size: 500
    deep_batch_size: 25

    # Throttling (adjust based on client impact)
    sleep_shallow_us: 0
    sleep_deep_us: 10000  # 10ms

    # Scheduling (daily shallow, weekly deep)
    scrub_min_interval_sec: 86400     # 1 day
    scrub_max_interval_sec: 604800    # 7 days
    deep_min_interval_sec: 604800     # 7 days
    deep_max_interval_sec: 2592000    # 30 days
```

### 3.2 Common Tuning Scenarios

#### High Client Latency During Scrub

**Symptom**: Client P99 latency increases when scrubs are active

**Solution**:
```yaml
scrubber:
  sleep_deep_us: 20000        # Increase from 10000 (20ms sleep)
  max_concurrent_scrubs: 1     # Reduce from 3
  max_concurrent_deep_scrubs: 1
```

**Impact**: Scrubs will run slower but with less client I/O impact.

#### Scrubs Not Completing Within Max Interval

**Symptom**: `homeobject_pg_last_scrub_time` exceeding `max_interval`

**Solution**:
```yaml
scrubber:
  sleep_deep_us: 5000          # Reduce from 10000 (5ms sleep)
  max_concurrent_scrubs: 5     # Increase from 3
  max_concurrent_deep_scrubs: 2 # Increase from 1
```

**Impact**: Faster scrub completion, but higher resource usage.

**Alternative**: Increase max intervals if scrubs are healthy but just slow:
```yaml
scrubber:
  scrub_max_interval_sec: 1209600    # 14 days instead of 7
  deep_max_interval_sec: 5184000     # 60 days instead of 30
```

#### Network Bandwidth Saturation

**Symptom**: High network usage during shallow scrubs

**Solution**:
```yaml
scrubber:
  max_concurrent_scrubs: 2      # Reduce from 3
  shallow_batch_size: 250       # Reduce from 500
  sleep_shallow_us: 5000        # Add 5ms throttle
```

#### Disk I/O Saturation

**Symptom**: High disk I/O wait during deep scrubs

**Solution**:
```yaml
scrubber:
  sleep_deep_us: 20000          # Increase from 10000
  deep_batch_size: 15           # Reduce from 25
  max_concurrent_deep_scrubs: 1  # Keep at 1
```

#### Large Blobs (>1MB average)

**Symptom**: Deep scrubs extremely slow or timing out

**Solution**:
```yaml
scrubber:
  deep_batch_size: 10           # Reduce from 25
  timeout_deep_sec: 120         # Increase from 60
  sleep_deep_us: 50000          # Increase throttling (50ms)
  deep_max_interval_sec: 5184000 # Extend deadline to 60 days
```

### 3.3 Configuration Changes

**Apply configuration changes:**

1. Edit HomeObject/Storage Manager configuration file
2. Restart HomeObject pods/service to load new config
3. Verify via HTTP API:
   ```bash
   curl http://localhost:8080/scrub/status | jq '.config'
   ```

**Note**: Configuration changes require restart (hot-reload not supported in MVP).

---

## 4. Troubleshooting

### 4.1 Common Issues

#### Scrubs Not Starting

**Symptom**: No scrub tasks initiated, scheduler running but no activity

**Quick Checks**:
```bash
# Check if scrubbing is enabled
curl localhost:8080/scrub/status | jq '.scrubbing_enabled'

# Check scheduler is running
curl localhost:8080/metrics | grep homeobject_scrub_scheduler_runs
# Should increment every 10 minutes

# Check PG eligibility
curl localhost:8080/pgs | jq '.[] | {pg_id, last_scrub_time, is_leader, state}'
```

**Common Causes**:
- Scrubbing disabled via API (check `no_scrub_flag`)
- All PGs recently scrubbed (within `min_interval`)
- No PGs in leader role (check Raft health)
- Concurrency limit reached (check `active_scrubs`)

**Solution**:
```bash
# Re-enable if disabled
curl -X DELETE localhost:8080/scrub/disable

# Manually trigger to test
curl -X POST "localhost:8080/scrub?pg_id=<PG_ID>"
```

#### High Scrub Failure Rate

**Symptom**: Many tasks with `status=failed`

**Quick Checks**:
```bash
# Get recent failures
curl localhost:8080/scrub/tasks?status=failed&limit=10 | jq '.[] | {pg_id, error_message}'

# Check logs
grep "scrub.*failed" /var/log/homeobject.log | tail -20
```

**Common Error Patterns**:
- `RPC timeout`: Network issues or slow replicas → Increase timeout
- `Leadership lost`: Frequent leader changes → Check Raft stability
- `RESOURCE_EXHAUSTED`: Concurrency limits → Increase limits or wait
- `Index query failed`: Storage corruption → Escalate to engineering

#### Data Inconsistency Detected (CRITICAL)

**Symptom**: Alert fired, `homeobject_scrub_inconsistencies_found > 0`

**Immediate Actions**:
```bash
# Get inconsistency details
PG_ID=<from-alert>
TASK_ID=$(curl localhost:8080/pg/$PG_ID/scrub | jq -r '.last_completed_task_id')
curl localhost:8080/scrub/task/$TASK_ID/inconsistencies > inconsistency_report.json

# Verify not transient - re-scrub
curl -X POST "localhost:8080/scrub?pg_id=$PG_ID&deep=true"
```

**What to Check**:
1. **Inconsistency type** (missing_blob, checksum_mismatch, state_mismatch)
2. **Affected blob count** (isolated or widespread?)
3. **Replica pattern** (same replica always affected = disk corruption)
4. **Recent events** (pod restarts, network issues during writes?)

**Escalation**:
- **Checksum mismatch**: CRITICAL - potential silent data corruption, escalate immediately
- **Missing blob**: HIGH - data loss, investigate replication logs
- **State mismatch**: MEDIUM - may be GC-related, verify timing

#### Scrubs Running Too Slow

**Symptom**: Deep scrubs not completing within `max_interval`

**Quick Checks**:
```bash
# Check actual duration
curl localhost:8080/scrub/tasks?type=deep&status=completed&limit=5 | \
  jq '.[] | {duration: (.end_time - .start_time)}'

# Check system resources
iostat -x 5  # Check disk I/O
top          # Check CPU
```

**Solutions**: See Section 3.2 tuning scenarios above

### 4.2 Emergency Controls

#### Disable Scrubbing

**When**: Production incident, client latency spike, need to free up resources

```bash
# Disable all scrubbing immediately
curl -X POST localhost:8080/scrub/disable

# Verify disabled
curl localhost:8080/scrub/status | jq '.scrubbing_enabled'
# Should return: false

# In-flight scrubs will complete gracefully
# Monitor: curl localhost:8080/scrub/status | jq '.active_scrubs'
```

**Effect**: Immediate - new scrubs won't start, active scrubs continue but will be last

#### Re-enable Scrubbing

```bash
# Re-enable after incident resolved
curl -X DELETE localhost:8080/scrub/disable

# Verify enabled
curl localhost:8080/scrub/status | jq '.scrubbing_enabled'
# Should return: true

# Scrubs will resume within scheduler_interval_sec (default 10 min)
```

**Note**: Disable/enable flags are ephemeral - cleared on pod restart

---

## 5. Quick Reference

### 5.1 Essential Commands

```bash
# Check global scrub status
curl localhost:8080/scrub/status

# Check if scrubbing is enabled
curl localhost:8080/scrub/status | jq '.scrubbing_enabled'

# Get active scrub count
curl localhost:8080/scrub/status | jq '.active_scrubs, .active_deep_scrubs'

# List all PGs and their scrub status
curl localhost:8080/pgs | jq '.[] | {pg_id, last_scrub_time, state}'

# Check specific PG scrub metadata
curl localhost:8080/pg/<PG_ID>/scrub

# Manually trigger scrub
curl -X POST "localhost:8080/scrub?pg_id=<PG_ID>"              # Shallow scrub
curl -X POST "localhost:8080/scrub?pg_id=<PG_ID>&deep=true"    # Deep scrub

# Query task status
curl localhost:8080/scrub/task/<TASK_ID>

# Get inconsistency details for a task
curl localhost:8080/scrub/task/<TASK_ID>/inconsistencies

# Disable scrubbing (emergency)
curl -X POST localhost:8080/scrub/disable

# Disable only deep scrubs
curl -X POST "localhost:8080/scrub/disable?deep_only=true"

# Re-enable scrubbing
curl -X DELETE localhost:8080/scrub/disable

# View metrics
curl localhost:8080/metrics | grep homeobject_scrub

# Check scheduler health
curl localhost:8080/metrics | grep homeobject_scrub_scheduler_runs
```

### 5.2 Quick Diagnostics

```bash
# Is scrubbing working?
curl localhost:8080/scrub/status | jq '{
  enabled: .scrubbing_enabled,
  active: .active_scrubs,
  max: .max_concurrent_scrubs
}'

# Recent scrub success rate
curl localhost:8080/metrics | grep -E "homeobject_scrub_tasks_(total|completed|failed)"

# Any inconsistencies found?
curl localhost:8080/metrics | grep homeobject_scrub_inconsistencies_found

# Check for stuck scrubs (running >24h)
curl localhost:8080/scrub/tasks?status=running | \
  jq --arg now "$(date +%s)" '.[] | select((($now | tonumber) - .start_time) > 86400)'
```

### 5.3 Configuration Lookup

```bash
# View current configuration
curl localhost:8080/scrub/status | jq '.config'

# Quick config check
curl localhost:8080/scrub/status | jq '.config | {
  shallow_batch: .shallow_batch_size,
  deep_batch: .deep_batch_size,
  shallow_sleep_us: .sleep_shallow_us,
  deep_sleep_us: .sleep_deep_us,
  max_concurrent: .max_concurrent_scrubs
}'
```

---

## Related Documentation

For more details, see:
- **[SCRUBBER_DESIGN.md](./SCRUBBER_DESIGN.md)** - Architecture, design decisions, workflows
- **[SCRUBBER_IMPLEMENTATION.md](./SCRUBBER_IMPLEMENTATION.md)** - Data structures, algorithms, APIs

---

**End of Operations Guide**
