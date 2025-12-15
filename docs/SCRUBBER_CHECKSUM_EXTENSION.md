# Deep Scrub Checksum Algorithm Extension

**Document Version:** 1.0
**Date:** 2025-12-15
**Status:** Future Enhancement (Post-MVP)
**Related Documents:**
- [SCRUBBER_DESIGN.md](./SCRUBBER_DESIGN.md) - Main scrubber design document
- [SCRUBBER_IMPLEMENTATION.md](./SCRUBBER_IMPLEMENTATION.md) - Implementation guide

---

## Executive Summary

This document describes an optional extension to the HomeObject scrubber that enables stronger checksum algorithms during deep scrub operations. This feature is designed for critical data (e.g., financial records) where enhanced verification is desired without impacting write-path performance.

### Key Decision

**No changes to write path.** CRC32 remains the checksum algorithm for all blob writes. During deep scrub, replicas can optionally compute stronger checksums (xxHash64 or SHA256) from in-memory data for cross-replica comparison.

---

## 1. Background

### 1.1 Current Implementation

HomeObject currently uses **CRC32** for blob data integrity:

- **Write Path**: Blobs are written with CRC32 checksum stored in `BlobHeader` (hs_homeobject.hpp:389-394)
- **Implementation**: Intel ISA-L library (`crc32_ieee`) with hardware acceleration
- **Location**: `hs_blob_manager.cpp:154` (write), `hs_blob_manager.cpp:589` (verify)

### 1.2 Why CRC32 is Sufficient for Write Path

Based on probability analysis (SCRUBBER_PROOF_READING_ANALYSIS.md):

- **Undetected corruption probability**: 2.3e-10 for 128KB blobs
- **System-level risk**: Acceptably low for general-purpose object storage
- **Performance**: Hardware-accelerated, minimal CPU overhead (~10-20 µs per 128KB)

### 1.3 Motivation for Extension

Certain use cases (financial data, regulatory compliance) may require:
- **Stronger verification**: Cryptographic-grade checksums (SHA256)
- **Configurable policies**: Per-shard or per-request checksum algorithm selection
- **No write penalty**: Verification-only, no impact on normal I/O operations

---

## 2. Design

### 2.1 Core Principles

1. **Write Path Unchanged**: CRC32 remains the only checksum stored on disk
2. **Scrub-Time Computation**: Stronger checksums computed on-demand during deep scrub
3. **In-Memory Processing**: No additional disk I/O; reuse data already read for verification
4. **Optional Feature**: Disabled by default; enabled via configuration or API

### 2.2 Algorithm Options

| Algorithm | Throughput | Hash Size | Use Case |
|-----------|-----------|-----------|----------|
| **CRC32** | 10-20 GB/s | 4 bytes | Default (MVP) |
| **xxHash64** | 30-50 GB/s | 8 bytes | High-performance verification |
| **SHA256** | 200-400 MB/s | 32 bytes | Cryptographic-grade verification |

**Recommendation**: xxHash64 for most cases; SHA256 for compliance/audit requirements.

### 2.3 Verification Workflow

```
┌─────────────────────────────────────────────────────────────┐
│             DEEP SCRUB VERIFICATION FLOW                     │
└─────────────────────────────────────────────────────────────┘

Follower receives scrub request for blob_id range:
  ↓
For each blob in range:
  ↓
  1. Read blob data from disk (header + payload)
  ↓
  2. Verify CRC32 from header
     ├─ Mismatch → Report corruption, skip to next blob
     └─ Match → Continue
  ↓
  3. Check deep_scrub_checksum_algorithm config
     ├─ CRC32 → Use already-computed CRC32 from step 2
     ├─ xxHash64 → Compute xxHash64(in-memory data)
     └─ SHA256 → Compute SHA256(in-memory data)
  ↓
  4. Send (blob_id, checksum_algorithm, checksum_value) to leader
  ↓
Leader compares checksums across replicas:
  - Same algorithm + same value → Consistent
  - Same algorithm + different value → Inconsistent (report)
  - Different algorithms → Error (misconfiguration)
```

### 2.4 Configuration

#### Instance-Level Configuration (Default)

```yaml
homeobject:
  scrubber:
    # Algorithm for deep scrub checksum verification
    # Options: CRC32 (default), XXHASH64, SHA256
    deep_scrub_checksum_algorithm: CRC32
```

#### HTTP API Override (Per-Request)

```bash
# Manual deep scrub with SHA256 verification
curl -X POST "http://localhost:8080/api/v1/scrub/pg/{pg_id}/deep" \
  -H "Content-Type: application/json" \
  -d '{"checksum_algorithm": "SHA256"}'
```

#### Future: Shard-Level Policies (Post-Initial Extension)

```yaml
homeobject:
  scrubber:
    shard_policies:
      - shard_pattern: "financial-*"
        deep_scrub_checksum_algorithm: SHA256
      - shard_pattern: "logs-*"
        deep_scrub_checksum_algorithm: CRC32
```

---

## 3. Implementation Notes

### 3.1 Code Changes

**Minimal changes required**:

1. **Configuration Parameter**:
   - Add `deep_scrub_checksum_algorithm` to scrubber config

2. **Deep Scrub Request Handler** (followers):
   ```cpp
   // After reading blob and verifying CRC32:
   uint8_t checksum[32]; // Max size for SHA256
   size_t checksum_len = 0;

   switch (config.deep_scrub_checksum_algorithm) {
     case XXHASH64:
       *(uint64_t*)checksum = xxHash64(blob_data, blob_size);
       checksum_len = 8;
       break;
     case SHA256:
       compute_sha256(blob_data, blob_size, checksum);
       checksum_len = 32;
       break;
     case CRC32:
     default:
       *(uint32_t*)checksum = crc32_value; // Already computed
       checksum_len = 4;
       break;
   }

   // Send (blob_id, algorithm, checksum, checksum_len) to leader
   ```

3. **Leader Comparison Logic**:
   - Validate all replicas use same algorithm
   - Compare checksums byte-by-byte

### 3.2 Performance Impact

For **25 blobs/batch** (deep scrub batch size), **128KB average blob size**:

| Algorithm | Compute Time | vs Baseline | Impact |
|-----------|--------------|-------------|---------|
| CRC32 | ~250 µs | 0% | Already computed for verification |
| xxHash64 | ~75 µs | -70% | **Faster than CRC32** |
| SHA256 | ~7-15 ms | +3000-6000% | Still acceptable with 10ms sleep |

**Conclusion**: Even SHA256 overhead is acceptable given:
- 10ms inter-batch sleep already in place
- Deep scrub runs weekly (not latency-sensitive)
- Network/disk I/O dominates total time

---

## Appendix A: Checksum Algorithm Details

### A.1 CRC32 (Current)

- **Library**: Intel ISA-L (`crc32_ieee`)
- **Hardware Acceleration**: Yes (SSE4.2, AVX-512)
- **Collision Resistance**: 2^-32 (acceptable for error detection)

### A.2 xxHash64 (Recommended Extension)

- **Library**: [xxHash](https://github.com/Cyan4973/xxHash)
- **Performance**: 30-50 GB/s (faster than CRC32 in software)
- **Collision Resistance**: 2^-64 (strong for non-cryptographic use)

### A.3 SHA256 (Compliance Extension)

- **Library**: OpenSSL or ISA-L crypto
- **Performance**: 200-400 MB/s
- **Collision Resistance**: 2^-256 (cryptographic-grade)

---

## Appendix B: Configuration Example

```yaml
homeobject:
  scrubber:
    # Checksum algorithm for deep scrub verification
    # CRC32: Default, hardware-accelerated, sufficient for most use cases
    # XXHASH64: Faster than CRC32, better collision resistance
    # SHA256: Cryptographic-grade, use for compliance/audit requirements
    deep_scrub_checksum_algorithm: CRC32

    # Future: Per-shard policies
    # shard_checksum_policies:
    #   - shard_pattern: "financial-*"
    #     deep_scrub_checksum_algorithm: SHA256
    #   - shard_pattern: "analytics-*"
    #     deep_scrub_checksum_algorithm: XXHASH64
```

---

## Appendix C: API Examples

### C.1 Query Current Configuration

```bash
curl http://localhost:8080/api/v1/scrub/config
```

**Response**:
```json
{
  "deep_scrub_checksum_algorithm": "CRC32"
}
```

### C.2 Trigger Deep Scrub with SHA256

```bash
curl -X POST "http://localhost:8080/api/v1/scrub/pg/42/deep" \
  -H "Content-Type: application/json" \
  -d '{
    "checksum_algorithm": "SHA256",
    "priority": "high"
  }'
```

**Response**:
```json
{
  "task_id": "scrub-20251215-143022-pg42-deep",
  "pg_id": 42,
  "scrub_type": "deep",
  "checksum_algorithm": "SHA256",
  "status": "in_progress"
}
```
