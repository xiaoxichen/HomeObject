# ADR 0001 — Lift homeobject onto HomeStore v8 (off Folly)

- **Status:** Accepted (planning), not yet started
- **Date:** 2026-08-17
- **Authors:** planning captured from GitHub Copilot CLI session; decisions
  confirmed by repository maintainer
- **Related:**
  - Origin proposal: [eBay/HomeObject#429](https://github.com/eBay/HomeObject/issues/429) — *"Lifting homeobject onto HomeStore v8"* by @szmyd
  - Reference implementation: homeblocks commit `c43fa2d` — *"Remove Folly and
    redesign the public API onto the v8 coroutine stack"*
  - `folly-to-v8-migration-dict` (homeblocks memory note; full type dictionary)
  - `sync-get-on-reactor-deadlock` (homeblocks memory note)
  - Current baseline: HomeObject `main` @ commit `3f7af3f` (SDSTOR-21848)

> **How to use this ADR.** This document is the resumable context for the lift.
> It records the *decisions*, the *staged plan*, and the *verified reference
> material* (v8 header excerpts, sisl primitives, conan-cache paths, file
> inventory, exact build/test commands). Treat sections 6–11 as authoritative
> facts we already checked against the actual conan-cached headers on disk;
> re-verify them only if the pinned versions in section 3 change.

---

## 1. Context

homeobject is a blob store built on HomeStore. It is currently pinned to the
folly-futures branch of the stack (homestore `^7.5.2`, sisl `^13.2`, iomgr
transitive `^12`, C++20). HomeStore v8 removes Folly entirely and redesigns the
public API onto stdexec coroutines and `std::expected`. homeblocks — the same
Folly baseline — has already been lifted; issue #429 catalogues the mechanical
reuse and the three areas homeblocks did not have (real multi-member raft
listener, snapshot/baseline resync, and GC with executors in the work path).

**Why now.** The v8 stack is now available and homeblocks has proven the
recipe. Continuing on v7 accrues rebase debt against every subsequent
sisl/iomgr/homestore release, and blocks homeobject from picking up the
coroutine improvements the stack is standardizing on.

**Scope of this ADR.** Everything necessary to lift homeobject to compile, link,
and pass its full test suite against homestore v8, keeping the repository
buildable at every PR checkpoint. Out of scope: production rollout, on-disk
format changes (v8 preserves the format), or SDK/consumer migration guidance
(will be a separate release note).

## 2. Baseline → target

| | Baseline (main) | Target (this ADR) |
|---|---|---|
| homestore | `^7.5.2@oss/master` | `^8.0.0@oss/dev` |
| sisl | `^13.2.3@oss/master` | `^14.8@oss/dev` |
| iomgr | transitive (also directly `#include <iomgr/iomgr.hpp>` in `gc_manager.cpp`) | `^13.0@oss/dev`, explicit `requires` |
| nuraft_mesg | transitive via homestore | `^5` transitive (peer_id_t/replica_id_t/group_id_t come from here) |
| C++ std | 20 | **23** (coroutines + `<expected>`) |
| Futures library | Folly | **gone** — replaced by `sisl::async::task<T>` (== `exec::task<T>`) |
| Error carrier | `folly::Expected<T, ManagerError>` | `std::expected<T, ManagerError>`; **manager error structs keep `{code, current_leader}` shape** (see §4-D) |

homeobject version bump: `4.1.8` → `5.0.1` (already reflected in the WIP
`conanfile.py` diff; **5.0** signals the API break).

## 3. Verified environment

All of the following was verified during ADR authoring against the actual
conan-cached headers on disk. If any pinned version changes, re-run the probes
in §11 before continuing.

| Package | Version | Conan cache path used for verification |
|---|---|---|
| homestore | `8.0.0@oss/dev` | `/root/.conan2/p/homesf0842b89343ab/es/src/include/homestore/…` |
| sisl | `14.8.1@oss/dev` | `/root/.conan2/p/sislc1f993cf7f540/es/include/sisl/…` |
| iomgr | `13.0.0@oss/dev` | `/root/.conan2/p/iomgr1d8d167309aaf/es/src/include/iomgr/…` |
| nuraft_mesg | `5.x` (transitive) | `/root/.conan2/p/nuraf0b856398d3675/es/include/nuraft_mesg/…` |
| nuraft | `2.4.9` (compat cppstd=20) | (via homestore) |
| GCC | 13.3.0 (Ubuntu) | libstdc++11 with `<expected>` support |
| Conan | 2.21.0 | remote `ebay-local` |

All four editable candidates (`sisl`, `iomgr`, `nuraft_mesg`, `homestore`) can
be built editable from `~/dev/oss` when needed; the conan cache already has
pre-built binaries at the versions above, so a first-pass build does **not**
need editable mode.

## 4. Decision summary

**Adopt the migration plan in issue #429 with three refinements (bundled into
the first PR) and a staged PR sequence anchored on a memory-backend green
trunk.**

### Decision details (Q&A that shaped this ADR)

Each of these was raised during planning and answered by the repo maintainer.
Preserve the reasoning here; do not relitigate.

**4-A. Public API contract & stdexec exposure — Option (a): direct exposure.**
`sisl::async::task<T>` is a type alias for `exec::task<T>` from stdexec (P2300,
on track for C++26). homeobject's public headers will transitively require
stdexec on the consumer include path. Sisl explicitly labels `<sisl/async/task.hpp>`
as an opt-in consumer header for stacks that co-build against the same stdexec
(iomgr, nuraft_mesg, homestore all do). Homeblocks made the same choice.
Rejected: PIMPL-style type erasure (kills `when_all` composition; heap alloc
per await) and `std::future` facade (defeats the entire lift).

**4-B. Error translation at the homestore boundary — keep `{code, current_leader}`
and translate lossy → lossless-log at the boundary.** Homeobject's manager
errors (`BlobError`, `ShardError`, `PGError`) carry
`std::optional<peer_id_t> current_leader` for the NOT_LEADER client redirect
that homestore's flat `std::error_condition` cannot represent. At every
`co_await` on a homestore op, translate `std::error_condition` back into the
homeobject error struct, populating `current_leader` from
`repl_dev->get_leader_id()`. For homestore codes with no homeobject equivalent
(`QUIENCE_STATE`, `UNREADY_STATE`, `REPLACE_MEMBER_TASK_MISMATCH`,
`TERM_MISMATCH`), map to the closest existing code (`UNKNOWN` /
`REPLICATION_ERROR`) **and** log the raw `error_condition.message()` at the
translation site so we don't lose visibility. Current `toBlobError` in
`hs_blob_manager.cpp:26–63` is the model; extend it, add
`toShardError`/`toPGError` counterparts.

**4-C. GC threading — Option A: iomgr named reactors, dedicated pool.** Preserve
the "GC has its own worker pool" invariant. Instead of
`folly::IOThreadPoolExecutor`, spawn dedicated iomgr reactors with
`iomgr.create_reactor("gc_worker_N", loop_type_t::io_loop, …)` and dispatch
per-chunk work with `iomgr.run_on_forget(reactor, fn)`. GC coroutines will
`co_await` homestore ops natively (io completions resume on any reactor, so
this composes correctly). `folly::MPMCQueue<chunk_id_t>` → hand-rolled
mutex-guarded `std::queue` (reserved-chunk queue is low-throughput; do not add
a new dep for it). Rejected: plain `std::thread` pool + `run_on` bridge (adds
cross-thread hop per IO) and moodycamel + std::thread (uses none of v8's
primitives).

**4-D. `sync_get` — off-reactor only. The only alternative is `co_await`.**
Accepted alternatives:
- `sisl::async::sync_get(task)` — blocks caller, **must be off-reactor**.
- `sisl::async::detach(task)` — fire-and-forget when result unneeded.
- Convert caller to coroutine and `co_await` — best when possible;
  propagates upward.
- There is **no** safe "sync-wait on a reactor without parking it" — that is
  fundamentally what the sync_get warning is about.

Concrete site-by-site policy:
- Test fixtures / `fixture_app.cpp` main → `sync_get` (main thread is not a
  reactor). ✅
- HTTP handlers (sisl httplib dispatches on non-reactor threads) →
  `sync_get`. ✅
- GC dedicated pool (per 4-C runs on named iomgr reactors, not the general
  worker pool, but IS a reactor) → **must `co_await`, never `sync_get`.** ⚠
- Commit thread, `repl_dev_listener` callbacks, any `run_on` body →
  **must `co_await`.** ⚠

Enforcement: at every `sync_get` call site, add a comment of the form
`// sync_get: off-reactor OK — main/http` so reviewers can spot regressions
in future PRs.

**4-E. PR strategy — staged PRs against a green-trunk canary.** Keep `main`
green throughout the migration by gating the `homestore_backend` subdirectory
under a CMake option (see §7) and always building the memory backend's
`memory_test` binary. Each staged PR must leave `memory_test` green; PRs 5+
additionally leave `homestore_test_*` green once the state machine and
managers land.

## 5. Non-goals & explicit deferrals

- No on-disk format change. v8 preserves the format; the lift is API-only.
- No consumer SDK migration (nuobject, callers) in this ADR. That is a
  downstream follow-up once homeobject 5.0 ships.
- No new HTTP framework research. Migrating to sisl httplib matches
  homeblocks; deviations require a separate ADR.
- No refactor of `HeapChunkSelector` beyond the `.h` → `.hpp` rename and any
  signature drift required by v8's `ChunkSelector` base. It stays functionally
  equivalent.
- Not adopting nuraft_mesg's `Result/AsyncResult` at the homeobject public
  surface. We keep our own `Manager<E>::Result` alias built on
  `std::expected<T, ManagerError>` for the reason in 4-B.

## 6. Migration plan (staged PRs)

Each PR is testable per §7. Trunk = `main`. Long-lived integration branch is
optional — I'll open PRs directly into `main` as long as `memory_test` stays
green.

### PR 1 — environment + memory-backend green trunk (~3–4 d)

Combines steps 1–4 from issue #429 as one atomic change so the trunk has a
running canary from the very first merge.

Changes:
- `conanfile.py`: pin sisl `^14.8@oss/dev`, homestore `^8.0.0@oss/dev`,
  iomgr `^13.0@oss/dev` (explicit); C++ std → 23; version → 5.0.1. Add
  `stdexec` to `requires()` **explicitly** (do not rely on transitive).
  (Diff already staged.)
- `CMakeLists.txt` line 4: `set(CMAKE_CXX_STANDARD 20)` → `23`. This is a
  **critical fix**: without it, CMake silently overrides Conan's `-std=c++23`
  with `-std=c++20` and `<expected>` fails to resolve. Verify with the
  `Warning: Standard CMAKE_CXX_STANDARD value defined in conan_toolchain.cmake`
  line disappearing from the log.
- `CMakeLists.txt` top level: add
  `option(HO_BUILD_HOMESTORE_BACKEND "Build homestore backend" ON)`.
- `src/lib/CMakeLists.txt`: gate `add_subdirectory(homestore_backend)` on the
  option (see §7 for exact code).
- Rename these 5 project-side includes from `.h` to `.hpp` (v8 no longer
  ships the `.h` names): in `src/lib/homestore_backend/`
  `heap_chunk_selector.h`, `hs_homeobject.hpp`, `replication_state_machine.hpp`,
  `replication_state_machine.cpp`, `gc_manager.hpp`. Grep audit:
  ```bash
  grep -rE 'homestore/(blk|chunk_selector|vchunk|replication/repl_dev|replication/repl_decls)\.h[>"]' src
  # must return zero matches after this PR.
  ```
- `src/include/homeobject/common.hpp`: replace Folly with C++23 primitives
  (public API — see §8-A for exact new shape). Keep `peer_id_t = boost::uuids::uuid`
  and the log macros unchanged.
- **Memory backend cleanup** (`src/lib/memory_backend/*` + `src/lib/*.cpp` at
  the abstract-manager layer + `src/lib/tests/*`):
  - `folly::ConcurrentHashMap<K, V>` → `std::unordered_map<K, V>` +
    `std::shared_mutex`. Two sites: `mem_homeobject.hpp` (`ShardIndex::btree_`
    and `index_svc`).
  - `folly::makeUnexpected(e)` in `mem_*.cpp` bodies → `co_return std::unexpected(e)`
    (function becomes a coroutine returning `AsyncResult<T>`).
  - `folly::makeSemiFuture(x)` → `co_return x`.
  - `folly::Unit` → `std::monostate`.
  - `folly::Init` in `fixture_app.cpp` → delete (sisl_options + logging init
    already present in the fixture).
  - `.get()` chained onto async results in tests → `sisl::async::sync_get(...)`
    with the enforcement comment from 4-D. Test `.then([&](...){...})` on the
    result → refactor to `sync_get` then act on the value directly (folly's
    lazy `.then` on a settled value is idiomatic only in folly; drop it).
  - `folly::executors::GlobalExecutor`, `.via(...)`, `folly::InlineExecutor` in
    `BlobManagerTest.cpp` → delete along with the `folly::Init` line.
- **Add stdexec to conanfile requirements** even though it comes transitively.
  Explicit dep documents the coupling.

Verification (must all pass):
```bash
# 1. Configure clean (no CXX_STANDARD warning)
conan build -s:h build_type=Debug -s:h compiler.cppstd=23 \
    -s:b compiler.cppstd=23 -b missing \
    -o "homeobject/*:HO_BUILD_HOMESTORE_BACKEND=False" . 2>&1 | tee /tmp/pr1.log
# 2. memory_test builds and runs
ctest -R "MemoryTest" --output-on-failure
# 3. Folly is gone from the OFF-build closure
grep -rE '#include\s*<folly' src/lib/memory_backend src/lib/tests \
    src/include src/lib/*.cpp src/lib/*.hpp
# ^ must return zero.
# 4. Full ON build still fails (expected — hs_backend not migrated yet), but
#    the error set no longer contains `homestore/*\.h` and no longer contains
#    `CXX_STANDARD ... modified to 20`.
```

The passing `MemoryTest`+`MemoryTestIO` runs are the trunk-green anchor. Every
subsequent PR must leave them green.

### PR 2 — replication_state_machine + hs_homeobject headers cleanup (~2–3 d)

- Re-derive **every** `repl_dev_listener` override in
  `replication_state_machine.{hpp,cpp}` against v8's `repl_dev_listener` header
  (see §8-B for the verbatim v8 interface — do NOT assume signatures; match
  what's in the header).
- `repl_result_ctx<T>::promise_` (`folly::Promise<T>`) →
  `sisl::async::value_awaitable<T>` held via a `std::shared_ptr` (matches v8's
  own `m_data_received_promise` pattern in `repl_req_ctx`). Producer calls
  `.complete(v)` on the commit thread; consumer `co_await`s in the
  issuing coroutine.
- `ho_repl_ctx::data_bufs_` `folly::small_vector<..., 3>` →
  `boost::container::small_vector<..., 3>`.
- Header hygiene: remove the `<folly/futures/Future.h>` block from
  `replication_state_machine.hpp` and `hs_homeobject.hpp`; move whatever
  `.cpp`-only include is still needed into the `.cpp`.
- New v8 callbacks to add default-body stubs for now:
  `on_no_space_left`, `on_log_replay_done`, `on_become_leader`,
  `on_become_follower`. Wire real behavior in later PRs.

Test in this PR (still on OFF-trunk):
- Toggle `HO_BUILD_HOMESTORE_BACKEND=ON` locally: `replication_state_machine.cpp`
  compiles standalone even though the managers don't. Verify with
  `cmake --build build/Debug --target homeobject_homestore 2>&1 | grep -c "error:"`
  showing errors only in manager `.cpp`s (blob/shard/pg), not in the state
  machine or its header.
- Trunk (OFF) still green: `ctest -R MemoryTest`.

### PR 3 — hs managers: blob → shard → pg (~7–10 d, may split further)

- `hs_blob_manager.cpp` first (heaviest at 35 folly touches, unlocks the
  biggest surface). Convert every `AsyncResult<T>`-returning method into a
  coroutine; `co_await repl_dev->async_alloc_write(...)`,
  `co_await repl_dev->async_read(...)`. Translate every returned
  `std::error_condition` via the extended `toBlobError` from 4-B, populating
  `current_leader`.
- `hs_shard_manager.cpp` (22 touches).
- `hs_pg_manager.cpp` (50 touches — heaviest single file; may split into a
  sub-PR of its own if the review gets unmanageable).
- Alongside: `hs_homeobject.hpp/cpp`. The `HSReplApplication::lookup_peer`
  URI parse (`folly::Uri`) becomes a 5-line hand-roll (endpoint is fixed
  `http://<host>:<port>`).
- Sisl v14 cast-macro cleanup wherever the compiler complains: `r_cast<T>` →
  `reinterpret_cast<T>`, `s_cast<T>` → `static_cast<T>`. **Special-case
  `uintptr_cast(p)`** — expand to `reinterpret_cast<uint8_t*>(p)`, not
  `static_cast` or `<uint32_t*>` (homeblocks caught 5 latent pointer bugs
  from a bad expansion of `uintptr_cast` on superblock chunk-id pointers).
  Grep every site before merging.
- `Clock` → `sisl::Clock`, `MetricsGroupWrapper` → `MetricsGroup`,
  `ReportFormat::kTextFormat` → `TEXT_FORMAT` wherever they appear.

Verification:
- Trunk (`HO_BUILD_HOMESTORE_BACKEND=ON`) rebuilds cleanly for the whole
  `homeobject_homestore` library.
- `homestore_test_pg`, `homestore_test_shard`, `homestore_test_blob` all
  build and pass.
- `memory_test` still green.

**After PR 3, CI's default trunk build should flip to `HO_BUILD_HOMESTORE_BACKEND=ON`.**

### PR 4 — CP callbacks (~1 d)

- `hs_cp_callbacks.cpp` — `MyCPCallbacks::cp_flush` returns
  `folly::Future<bool>` → `sisl::async::task<bool>`; the body's
  `return folly::makeFuture<bool>(true)` → `co_return true`.
- `on_switchover_cp` / `cp_cleanup` / `cp_progress_percent` stay sync — no
  change needed.

### PR 5 — index_kv (~1 d)

- `index_kv.cpp` folly cleanup (only 3 touches).
- **Deadlock discipline check for `IndexTable::destroy`:** v8's default
  `destroy()` is a coroutine that `co_await cp_mgr().trigger_cp_flush(true)`
  internally. If homeobject invokes `destroy()` in any path that runs on a
  reactor and then blocks on the returned task via `sync_get`, that will
  deadlock. Every call site must either `co_await` or be off-reactor. Audit:
  ```bash
  grep -rn "\.destroy()" src/lib/homestore_backend/
  ```

### PR 6 — snapshot / baseline resync (~2–3 d)

- `snapshot_receive_handler.cpp` (10 folly touches).
- `pg_blob_iterator.cpp` (3 folly touches, becomes a coroutine).
- `create_snapshot` in `replication_state_machine.cpp` — was
  `AsyncReplResult<>` returning `folly::makeSemiFuture(...)`; now
  `async_status` with `co_return homestore::ok()`.
- `read_snapshot_obj` and `write_snapshot_obj` are **sync** in v8 (return
  `int` and `void` respectively); simplify the wrappers.
- `apply_snapshot` returns plain `bool` in v8 (was async); simplify.

### PR 7 — GC manager (~3–4 d)

**Highest architectural risk PR after the state machine.** Per 4-C:

- `folly::IOThreadPoolExecutor` pools (`m_gc_executor`, `m_egc_executor`) →
  a `pdev_gc_actor`-owned vector of `iomgr::io_thread_addr_t` (named
  reactors created via `iomgr.create_reactor("gc_pdev_N_worker_M", loop_type_t::io_loop)`).
  Wire the "num GC threads" and "num EGC threads" config to the reactor
  count.
- `folly::ConcurrentHashMap<uint32_t, shared<pdev_gc_actor>> m_pdev_gc_actors`
  → `std::unordered_map<...>` + `std::shared_mutex`.
- `folly::MPMCQueue<chunk_id_t> m_reserved_chunk_queue` → mutex-guarded
  `std::queue<chunk_id_t>` + `std::condition_variable`. Small queue,
  low-throughput.
- `folly::ConcurrentHashMap<BlobRouteByChunk, BlobRouteValue>` used in
  `copy_valid_data` → `std::unordered_map<...>` + `std::mutex` (short-lived
  local map).
- Per-chunk fan-out via `folly::collectAllUnsafe` →
  `sisl::async::when_all(std::vector<task<...>>)`.
- `folly::SemiFuture<bool> add_gc_task(...)` returning
  `folly::Promise<bool>` → `sisl::async::task<bool>` returning a `value_awaitable`-based
  coroutine. Or: return `bool` from a `co_await`-able entry.
- Restore deadlock discipline: since GC coroutines run *on* dedicated reactors
  now, every call into a homestore op (data_service alloc/read/free,
  IndexTable operations) MUST be `co_await`ed. No `sync_get` inside GC.
- Update `HS_BACKEND_DYNAMIC_CONFIG(max_read_write_block_count_per_second)`
  and any other config knobs that used to size folly pools.

Verification:
- `hs_gc_tests` builds and passes.
- Deliberate stress: run `hs_gc_tests` under sanitizer (`-o
  "homeobject/*:sanitize=True"`) to catch reactor deadlock or use-after-free
  in the new queue/map primitives.

### PR 8 — HTTP admin (~1–2 d)

- `hs_http_manager.{hpp,cpp}`: 22 Pistache handler signatures → sisl `http_server`
  (httplib) equivalents. Homeblocks pattern in
  `homeblocks/.../hs_http_manager.*` at commit c43fa2d is the model.
- `folly::EvictingCacheMap<std::string, shared_ptr<GCJobInfo>> gc_jobs_map_{100}`
  → hand-rolled 30-line LRU (`std::list` for recency + `std::unordered_map`
  keyed on job_id, iterator into the list), OR sisl's `sisl::LRUCache` if
  available. Cache size is 100 items; simplest wins.
- `folly::Future<folly::Unit> trigger_gc_for_pg(...)` →
  `sisl::async::task<std::monostate>`.
- Handler bodies with `folly::collectAllUnsafe` + `.via(InlineExecutor)`
  → `sisl::async::when_all` + direct `co_await` (handlers run off-reactor
  on httplib threads, so `sync_get` is legal).

### PR 9 — tests, drop folly, verify grep clean (~2–3 d)

- Any remaining test-side folly touches (`hs_gc_tests.cpp`,
  `test_homestore_backend_dynamic.cpp`, `test_heap_chunk_selector.cpp`,
  `hs_repl_test_helper.hpp` — all small).
- Remove any lingering `find_package(Folly ...)` or transitive folly hints
  from CMake. Homestore v8 no longer transitively provides folly.
- `conanfile.py`: verify no `folly` in `requirements()` (should never have
  been there directly, but check).
- Final grep audit:
  ```bash
  grep -rE 'folly|Folly' src conanfile.py CMakeLists.txt cmake/
  # ^ must return zero (except in comments citing the migration for context).
  ```

## 7. CMake gating scheme (verbatim)

Add to top of `/home/HomeObject/CMakeLists.txt`, after `set(CMAKE_CXX_STANDARD 23)`:

```cmake
option(HO_BUILD_HOMESTORE_BACKEND
       "Build the homestore-backed HomeObject library (disable during v8 lift)"
       ON)
```

In `/home/HomeObject/src/lib/CMakeLists.txt`, replace the unconditional
`add_subdirectory(homestore_backend)` with:

```cmake
if(HO_BUILD_HOMESTORE_BACKEND)
    add_subdirectory(homestore_backend)
else()
    message(STATUS "homeobject: skipping homestore_backend (HO_BUILD_HOMESTORE_BACKEND=OFF)")
endif()
```

Memory backend and `tests/` always build. `homeobject_core` (the abstract
manager surface) always builds and is linked by `homeobject_memory`.

**Why this works.** `memory_test` is defined in
`src/lib/memory_backend/CMakeLists.txt` and links `homeobject_memory` +
`test_fixture` + object files of `homeobject_core`. Nothing in that closure
reaches into `homestore_backend/`. As long as (a) `homeobject_core`,
(b) `homeobject_memory`, and (c) `test_fixture` all compile,
`memory_test` builds and runs — regardless of the state of
`homestore_backend`.

**During the lift, CI should build with `HO_BUILD_HOMESTORE_BACKEND=OFF` on
`main` between PR 1 and PR 3 lands.** After PR 3, flip to `=ON`. The option
can stay in tree indefinitely; it's small, useful for future partial builds,
and self-documenting.

## 8. Type & API replacement dictionary

### 8-A. `common.hpp` new shape (public API contract)

Before:
```cpp
#include <folly/Expected.h>
#include <folly/Unit.h>
#include <folly/futures/Future.h>

template <class E>
class Manager {
public:
    template <typename T> using Result      = folly::Expected<T, E>;
    template <typename T> using AsyncResult = folly::SemiFuture<Result<T>>;
    using NullResult      = Result<folly::Unit>;
    using NullAsyncResult = AsyncResult<folly::Unit>;
    virtual ~Manager() = default;
};
```

After:
```cpp
#include <expected>
#include <variant>
#include <sisl/async/task.hpp>   // sisl::async::task == exec::task (stdexec)

template <class E>
class Manager {
public:
    template <typename T> using Result      = std::expected<T, E>;
    template <typename T> using AsyncResult = sisl::async::task<Result<T>>;
    using NullResult      = Result<std::monostate>;
    using NullAsyncResult = AsyncResult<std::monostate>;
    virtual ~Manager() = default;
};
```

`BlobError`, `ShardError`, `PGError`, `PGInfo`, `PGMember`, `Blob`, `ShardInfo`
in the public headers are **unchanged**. `struct BlobError { code; optional<peer_id_t> current_leader; }`
is preserved verbatim (see 4-B).

### 8-B. Folly → v8/stdexec mechanical dictionary

Apply 1:1 wherever these appear. If a call site has a semantic wrinkle (e.g.,
executor choice, error handling), see the corresponding hard-part section in
issue #429.

| Folly (v7) | v8 / stdexec replacement | Notes |
|---|---|---|
| `folly::Expected<T, E>` | `std::expected<T, E>` | libstdc++ 13+, C++23 |
| `folly::Unexpected<E>` / `folly::makeUnexpected(e)` | `std::unexpected<E>` / `std::unexpected(e)` | In coroutines: `co_return std::unexpected(e);` |
| `folly::Unit` / `folly::Unit{}` | `std::monostate` / `std::monostate{}` | |
| `folly::Future<T>` / `folly::SemiFuture<T>` | `sisl::async::task<T>` | = `exec::task<T>` (stdexec) |
| `folly::makeFuture(x)` / `folly::makeSemiFuture(x)` | `co_return x` inside a coroutine | Function signature becomes `task<T>` |
| `.thenValue(fn)` / `.deferValue(fn)` | `co_await` then evaluate the closure body inline | |
| `folly::Promise<T>` | `sisl::async::value_awaitable<T>` (via `std::shared_ptr` for lifetime) | Producer `.complete(v)`, consumer `co_await`. Non-movable — pin the address. |
| `folly::makePromiseContract<T>()` | Construct a shared `value_awaitable<T>` | Contract split (producer/consumer) done manually via the shared ptr. |
| `.setValue(v)` on a Promise | `.complete(v)` on the `value_awaitable` | |
| `.getSemiFuture()` on a Promise | `co_await *shared_awaitable` | |
| `folly::collectAll(vec)` / `folly::collectAllUnsafe(vec)` | `sisl::async::when_all(std::vector<task<T>>)` | Errors-as-values; does NOT short-circuit; default-constructs missing slots on child throw. See sisl `when_all.hpp` docstring. |
| `folly::Init` | Delete | sisl_options + `sisl::logging_init` already present in fixture main |
| `folly::InlineExecutor` / `folly::QueuedImmediateExecutor` / `.via(...)` / `folly::getGlobalCPUExecutor()` / `folly::getGlobalIOExecutor()` / `folly::getKeepAliveToken(...)` | Delete | Coroutines have sticky scheduler affinity; no executor plumbing needed. |
| `folly::ConcurrentHashMap<K, V>` | `std::unordered_map<K, V>` + `std::shared_mutex` | Sites: memory backend (`ShardIndex`, `index_svc`), `gc_manager.hpp` (`m_pdev_gc_actors`), gc `copy_valid_data` scratch map. |
| `folly::MPMCQueue<T>` | `std::queue<T>` + `std::mutex` + `std::condition_variable` | Only site: gc `m_reserved_chunk_queue`. Low throughput; no need for lock-free. |
| `folly::EvictingCacheMap<K, V>` | Hand-rolled ~30-line LRU (`std::list` + `std::unordered_map`) OR `sisl::LRUCache` if present | Only site: `hs_http_manager.hpp` `gc_jobs_map_{100}` |
| `folly::Uri` | Hand-roll (endpoint format is `http://<host>:<port>`, 5 lines) | Only site: `HSReplApplication::lookup_peer` in `hs_homeobject.cpp` |
| `folly::small_vector<T, N>` | `boost::container::small_vector<T, N>` | Already used elsewhere in v8 (`repl_decls.hpp:blkid_list_t`). |
| `folly::IOThreadPoolExecutor` (GC) | `iomgr.create_reactor(name, loop_type_t::io_loop, ...)` for dedicated reactors + `iomgr.run_on_forget(reactor, fn)` to dispatch | See 4-C. |
| `.get()` in tests / control plane | `sisl::async::sync_get(task)` | **Off-reactor only**; add `// sync_get: off-reactor OK — <site>` comment per 4-D. |

sisl v14 removed casting shortcuts; expand them at every site:

| Removed macro | Expand to |
|---|---|
| `r_cast<T>(x)` | `reinterpret_cast<T>(x)` |
| `s_cast<T>(x)` | `static_cast<T>(x)` |
| `uintptr_cast(p)` | `reinterpret_cast<uint8_t*>(p)` — **not** `static_cast`, **not** `<uint32_t*>`. Grep every site; homeblocks had 5 latent bugs from bad expansions. |
| `Clock` | `sisl::Clock` |
| `MetricsGroupWrapper` | `sisl::MetricsGroup` |
| `ReportFormat::kTextFormat` | `sisl::ReportFormat::TEXT_FORMAT` |

### 8-C. Homestore identifier renames

| v7 | v8 |
|---|---|
| `homestore::ReplDev` | `homestore::repl_dev` |
| `homestore::ReplDevListener` | `homestore::repl_dev_listener` |
| `homestore::ReplApplication` | `homestore::repl_application` |
| `homestore::BlkId` | `homestore::blk_id` |
| `homestore::MultiBlkId` | `homestore::multi_blk_id` |
| `homestore::AsyncReplResult<T>` | `homestore::async_result<T>` (= `sisl::async::task<result<T>>`) |
| `homestore::AsyncReplResult<>` (void) | `homestore::async_status` (= `async_result<std::monostate>`) |
| `homestore::ReplResult<T, E>` | `homestore::Result<T, E>` (= `std::expected<T, E>`) — data-rpc surface only |
| `homestore::NullReplResult` | `homestore::status` (= `result<std::monostate>`) |
| Header `homestore/replication/repl_dev.h` | `homestore/replication/repl_dev.hpp` |
| Header `homestore/replication/repl_decls.h` | `homestore/replication/repl_decls.hpp` |
| Header `homestore/blk.h` | `homestore/blk.hpp` |
| Header `homestore/chunk_selector.h` | `homestore/chunk_selector.hpp` |
| Header `homestore/vchunk.h` | `homestore/vchunk.hpp` |

### 8-D. Behavioural flips (semantic, not just rename)

- **`repl_dev::alloc_blks(size, hints, out_blkids)` returns `status`.** A
  value means **success** (`status.has_value() == true`). Invert every old
  `if (r) { /* was error */ }` to `if (!r) { /* is error */ }`.
- **`apply_snapshot(shared<snapshot_context>)` returns plain `bool` now.**
  Was async in v7.
- **`read_snapshot_obj(...)` returns plain `int` now.** Was async in v7.
- **`on_fetch_data(...)` returns `sisl::async::task<iomgr::io_result>`**
  and has a default implementation that reads via `data_service().async_read`.
  Override only if fetching-decision logic is non-default.
- **`get_blk_alloc_hints(...)` returns `result<blk_alloc_hints>`** (=
  `std::expected<blk_alloc_hints, std::error_condition>`) — not
  `ReplResult<...>` anymore.
- **`alloc_local_blks(...)` returns `ReplServiceError`** directly (not a
  `Result`).
- **New callbacks on `repl_dev_listener`** — add default-body stubs first,
  wire real behavior later: `on_no_space_left(repl_lsn_t, blob)`,
  `on_log_replay_done(group_id_t)`, `on_become_leader(group_id_t)`,
  `on_become_follower(group_id_t)`, `on_config_rollback(int64_t)`.
- **`repl_req_ctx` publishes two `value_awaitable<std::monostate>` fields:**
  `m_data_received_promise`, `m_data_written_promise`. Model
  `repl_result_ctx<T>::promise_` on the same shape.

## 9. Homestore v8 API reference (verified verbatim excerpts)

Sourced from the v8 headers in the conan cache at
`/root/.conan2/p/homesf0842b89343ab/es/src/include/homestore/`. Re-verify if
the pinned version changes.

### 9-A. Error surface — `error.hpp`

```cpp
namespace homestore {
    template <class T> using result       = std::expected<T, std::error_condition>;
    template <class T> using async_result = sisl::async::task<result<T>>;
    using status       = result<std::monostate>;
    using async_status = async_result<std::monostate>;
    inline status ok() noexcept { return status{std::monostate{}}; }
}
```

`ReplServiceError` and `repl_data_rpc_error_code` are both registered as
`std::error_condition` enums (see `repl_decls.hpp` / `repl_dev.hpp`). Callers
branch with `r.error() == ReplServiceError::NOT_LEADER`, etc.

### 9-B. `repl_dev` public interface (selected — `repl_dev.hpp`)

```cpp
class repl_dev {
public:
    virtual status alloc_blks(uint32_t data_size, const blk_alloc_hints& hints,
                              std::vector<multi_blk_id>& out_blkids) = 0;

    virtual sisl::async::task<iomgr::io_result>
        async_write(const std::vector<multi_blk_id>& blkids,
                    sisl::sg_list const& value,
                    io_batch* batch = nullptr, trace_id_t tid = 0) = 0;

    virtual void async_write_journal(const std::vector<multi_blk_id>& blkids,
                                     sisl::blob const& header, sisl::blob const& key,
                                     uint32_t data_size, repl_req_ptr_t ctx,
                                     trace_id_t tid = 0) = 0;

    virtual void async_alloc_write(sisl::blob const& header, sisl::blob const& key,
                                   sisl::sg_list const& value, repl_req_ptr_t ctx,
                                   io_batch* batch = nullptr, trace_id_t tid = 0) = 0;

    virtual sisl::async::task<iomgr::io_result>
        async_read(multi_blk_id const& blkid, sisl::sg_list& sgs, uint32_t size,
                   io_batch* batch = nullptr, trace_id_t tid = 0) = 0;

    virtual sisl::async::task<iomgr::io_result>
        async_free_blks(int64_t lsn, multi_blk_id const& blkid,
                        trace_id_t tid = 0) = 0;

    virtual async_status become_leader() = 0;
    virtual bool is_leader() const = 0;
    virtual replica_id_t get_leader_id() const = 0;
    virtual std::vector<peer_info> get_replication_status() const = 0;
    virtual std::vector<replica_id_t> get_replication_quorum() = 0;
    virtual group_id_t group_id() const = 0;
    virtual uint32_t get_blk_size() const = 0;
    virtual bool is_ready_for_traffic() const = 0;
    // ... plus stage/purge/pause/resume, data_request_uni/bidirectional, etc.
};
```

Note that `async_alloc_write` and `async_write_journal` are **`void`-returning
scheduling calls** — the caller waits on `ctx`'s embedded `value_awaitable`s
(or the outer `repl_result_ctx<T>::promise_`).

### 9-C. `repl_dev_listener` callbacks (selected)

```cpp
class repl_dev_listener {
public:
    virtual void on_commit(int64_t lsn, sisl::blob const& header,
                           sisl::blob const& key,
                           std::vector<multi_blk_id> const& blkids,
                           cintrusive<repl_req_ctx>& ctx) = 0;
    virtual void notify_committed_lsn(int64_t lsn) {}
    virtual bool on_pre_commit(int64_t lsn, sisl::blob const& header,
                               sisl::blob const& key,
                               cintrusive<repl_req_ctx>& ctx) { return true; }
    virtual void on_rollback(int64_t lsn, sisl::blob const& header,
                             sisl::blob const& key,
                             cintrusive<repl_req_ctx>& ctx) {}
    virtual void on_config_rollback(int64_t lsn) {}
    virtual void on_restart() {}
    virtual void on_error(ReplServiceError error, sisl::blob const& header,
                          sisl::blob const& key,
                          cintrusive<repl_req_ctx>& ctx) {}
    virtual result<blk_alloc_hints> get_blk_alloc_hints(
        sisl::blob const& header, uint32_t data_size,
        cintrusive<repl_req_ctx>& hs_ctx) { return blk_alloc_hints{}; }
    virtual void on_destroy(const group_id_t& group_id) {}
    virtual void on_start_replace_member(const std::string& task_id,
        const replica_member_info& out, const replica_member_info& in,
        trace_id_t tid) {}
    virtual void on_complete_replace_member(...) {}
    virtual void on_clean_replace_member_task(...) {}
    virtual void on_remove_member(const replica_id_t& member, trace_id_t tid) {}

    // Snapshot
    virtual async_status create_snapshot(shared<snapshot_context> ctx) {
        co_return ok();
    }
    virtual bool apply_snapshot(shared<snapshot_context> ctx) { return true; }
    virtual shared<snapshot_context> last_snapshot() { return nullptr; }
    virtual int  read_snapshot_obj(shared<snapshot_context> ctx,
                                   shared<snapshot_obj> obj) { return 0; }
    virtual void write_snapshot_obj(shared<snapshot_context> ctx,
                                    shared<snapshot_obj> obj) {}
    virtual void free_user_snp_ctx(void*& user_snp_ctx) {}

    virtual sisl::async::task<iomgr::io_result>
        on_fetch_data(int64_t lsn, sisl::blob const& header,
                      multi_blk_id const& blkid, sisl::sg_list& sgs) {
        co_return co_await data_service().async_read(blkid, sgs, sgs.size);
    }

    // v8 NEW
    virtual void on_no_space_left(repl_lsn_t lsn, sisl::blob const& header) {}
    virtual void on_log_replay_done(const group_id_t& group_id) {}
    virtual void on_become_leader(const group_id_t& group_id) {}
    virtual void on_become_follower(const group_id_t& group_id) {}
};
```

### 9-D. `repl_req_ctx` public members (embedded awaitables)

```cpp
struct repl_req_ctx : public boost::intrusive_ref_counter<repl_req_ctx, ...>,
                      sisl::ObjLifeCounter<repl_req_ctx> {
    // ...
    sisl::async::value_awaitable<std::monostate> m_data_received_promise;
    sisl::async::value_awaitable<std::monostate> m_data_written_promise;
    sisl::io_blob_list_t m_pkts;
    std::mutex m_state_mtx;
    // ...
};
```

Model `ho_repl_ctx` / `repl_result_ctx<T>::promise_` on this pattern —
`value_awaitable<T>` held via `std::shared_ptr` so the producer and consumer
share ownership independently of the ctx lifetime.

### 9-E. CP callbacks — `checkpoint/cp_mgr.hpp`

```cpp
class CPCallbacks {
public:
    virtual std::unique_ptr<CPContext> on_switchover_cp(CP* cur_cp,
                                                        CP* new_cp) = 0;
    virtual sisl::async::task<bool> cp_flush(CP* cp) = 0;
    virtual void cp_cleanup(CP* cp) = 0;
    virtual int  cp_progress_percent() = 0;
    virtual void repair_slow_cp() {}
};
```

### 9-F. IndexTable::destroy (relevant to §PR-5 audit)

```cpp
sisl::async::task<btree_status_t> destroy() override {
    if (is_stopping()) co_return btree_status_t::stopping;
    incr_pending_request_num();
    // ...
    // co_await, NOT sync_wait: flush runs on iomgr worker reactors, so
    // suspending yields the reactor back to its iomgr loop to service the
    // flush -- a blocking wait would deadlock (parked reactor can't flush).
    std::ignore = co_await cp_mgr().trigger_cp_flush(true /* force */);
    m_sb.destroy();
    // ...
    co_return btree_status_t::success;
}
```

### 9-G. `repl_application` interface (unchanged shape, snake_case names)

```cpp
class repl_application {
public:
    virtual repl_impl_type get_impl_type() const = 0;
    virtual bool need_timeline_consistency() const = 0;
    virtual shared<repl_dev_listener> create_repl_dev_listener(group_id_t) = 0;
    virtual void destroy_repl_dev_listener(group_id_t) = 0;
    virtual void on_repl_devs_init_completed() = 0;
    virtual std::pair<std::string, uint16_t> lookup_peer(replica_id_t) const = 0;
    virtual replica_id_t get_my_repl_id() const = 0;
    virtual uint32_t get_my_repl_svc_port() const = 0;
};
```

### 9-H. `blk_data_service` async surface — `blkdata_service.hpp`

Everything returns `sisl::async::task<iomgr::io_result>`. All `blkid`
overloads and the `io_batch* batch = nullptr` parameter are new; if we pass
`batch`, the batch destructor submits (RAII):

```cpp
sisl::async::task<iomgr::io_result>
    async_alloc_write(sisl::sg_list const& sgs, blk_alloc_hints const& hints,
                      multi_blk_id& out_blkids, io_batch* batch = nullptr);
sisl::async::task<iomgr::io_result>
    async_write(sisl::sg_list const&, multi_blk_id const&, io_batch* = nullptr);
sisl::async::task<iomgr::io_result>
    async_read(multi_blk_id const&, uint8_t* buf, uint32_t, io_batch* = nullptr);
sisl::async::task<iomgr::io_result>
    async_read(multi_blk_id const&, sisl::sg_list& sgs, uint32_t, io_batch* = nullptr);
sisl::async::task<iomgr::io_result> async_free_blk(multi_blk_id const&);
result<multi_blk_id>          alloc_blks(uint32_t size, blk_alloc_hints const&);
result<std::vector<blk_id>>   alloc_blk_list(uint32_t size, blk_alloc_hints const&);
status                        commit_blk(multi_blk_id const&);
status                        free_blk_now(multi_blk_id const&);
[[nodiscard]] io_batch        begin_batch();
```

## 10. sisl / iomgr / nuraft_mesg primitive reference

### 10-A. `sisl::async` — from `<sisl/async/task.hpp>`, `<sisl/async/value_awaitable.hpp>`, `<sisl/async/when_all.hpp>`, `<sisl/async/coro.hpp>`

```cpp
namespace sisl::async {
    template <typename T> using task = exec::task<T>;

    template <typename T>
    struct value_awaitable {
        // Non-movable; producer + consumer share via std::shared_ptr.
        // Producer:
        void complete(T v) noexcept;
        // Consumer: co_await *this
    };

    // Runtime fan-out (stdexec's when_all is variadic).
    template <typename T>
    task<std::vector<T>> when_all(std::vector<task<T>> tasks);
    // Errors-as-values; does NOT short-circuit on child error.

    // Bridge sync↔coroutine.
    template <typename Task>
    inline auto sync_get(Task&& t);   // Blocks caller. OFF-REACTOR ONLY.

    template <typename T>
    inline void detach(task<T> t);    // Fire-and-forget.
}
```

### 10-B. iomgr — from `<iomgr/iomgr.hpp>` and `<iomgr/io_op.hpp>`

```cpp
namespace iomgr {
    // io_result — the one data-plane error type.
    using io_result = std::expected<std::size_t, std::error_condition>;

    // Named reactor creation (for GC dedicated pool).
    void create_reactor(const std::string& name, loop_type_t loop_type,
                        thread_state_notifier_t&& notifier = nullptr);
    void create_worker_reactors();  // The general pool; GC does NOT use this.

    // Dispatch to a specific reactor or a regex-selected reactor.
    int run_on_forget(IOReactor* reactor, const auto& fn);
    int run_on_forget(reactor_regex rr, const auto& fn);
    int run_on_wait(IOReactor* reactor, const auto& fn);
    int run_on_wait(reactor_regex rr, const auto& fn);
    template <typename... Args> int run_on(bool wait, Args&&... args);

    IOReactor* this_reactor() const;
    bool       am_i_worker_reactor() const;
    IOReactor* sync_io_reactor() const;
}
```

### 10-C. nuraft_mesg types (transitive) — from `<nuraft_mesg/common.hpp>`

```cpp
namespace nuraft_mesg {
    using peer_id_t    = boost::uuids::uuid;
    using group_id_t   = boost::uuids::uuid;
    using group_type_t = std::string;
    using svr_id_t     = int32_t;
    template <typename T> using result       = std::expected<T, std::error_condition>;
    template <typename T> using async_task   = sisl::async::task<result<T>>;
    using null_result     = result<void>;
    using null_async_task = async_task<void>;
}
```

homeobject's `peer_id_t = boost::uuids::uuid` (in `common.hpp`) stays as-is;
same underlying type, no ODR issue.

## 11. Build & test commands (exact)

### 11-A. Full ON build (target end-state)

```bash
cd /home/HomeObject
conan build -s:h build_type=Debug -s:h compiler.cppstd=23 \
    -s:b compiler.cppstd=23 -b missing . 2>&1 | tee /tmp/conan_build.log
```

### 11-B. OFF-trunk build (during PRs 1–2)

```bash
cd /home/HomeObject
conan build -s:h build_type=Debug -s:h compiler.cppstd=23 \
    -s:b compiler.cppstd=23 -b missing \
    -o "homeobject/*:HO_BUILD_HOMESTORE_BACKEND=False" .
```

Note: The `HO_BUILD_HOMESTORE_BACKEND` option must also be exposed to Conan
via `options` in `conanfile.py` if we want to toggle it this way. Alternative
(simpler for PR 1): expose only as a CMake option, and drive from Conan via
`tc.variables["HO_BUILD_HOMESTORE_BACKEND"] = "OFF"` in `generate()`. Pick the
cleaner option once we write PR 1 — the ADR does not mandate which.

### 11-C. Running memory-backend tests

```bash
cd build/Debug
ctest -R MemoryTest --output-on-failure --verbose
```

### 11-D. Running homestore-backend tests (after PR 3)

```bash
cd build/Debug
ctest -R "HomestoreTest" --output-on-failure --verbose
```

### 11-E. Grep audits (run at each PR)

```bash
# Zero folly touches remaining in the code path we've migrated:
grep -rE 'folly|#include\s*<folly' src/lib/memory_backend src/lib/tests \
    src/include src/lib/*.cpp src/lib/*.hpp
# Old .h include forms (v8 uses .hpp):
grep -rE 'homestore/(blk|chunk_selector|vchunk|replication/repl_dev|replication/repl_decls)\.h[>"]' src
# Removed sisl cast macros:
grep -rE '\b(r_cast|s_cast|uintptr_cast)\b' src
# CXX_STANDARD 20 leftover (should be 23):
grep -n "CXX_STANDARD" CMakeLists.txt
```

## 12. Verified deep-cut findings (probe results)

During ADR authoring I ran the current (WIP-conanfile) build to characterize
what the errors look like today. Snapshot of that state — expect these to
disappear as we work through the PRs:

- **Dependency resolution is clean:** `sisl/14.8.1@oss/dev`,
  `iomgr/13.0.0@oss/dev`, `homestore/8.0.0@oss/dev` all resolve and are
  already cached (`Already installed! (X of 36)`).
- **First-layer stop:** every TU that reaches `common.hpp` fails on
  `folly/Expected.h: No such file or directory`. Test TUs additionally fail
  on `folly/executors/GlobalExecutor.h` and `folly/init/Init.h`.
- **Second-layer stop (only visible after neutralizing common.hpp):**
  `CMakeLists.txt:4` forces `set(CMAKE_CXX_STANDARD 20)` and CMake overrides
  Conan's `-std=c++23` — visible in log as
  `Warning: Standard CMAKE_CXX_STANDARD value defined in conan_toolchain.cmake to 23 has been modified to 20 by /home/HomeObject/CMakeLists.txt`.
  This makes `<expected>` unresolvable. **PR 1 must fix this.**
- **Third-layer stop:** `homestore/chunk_selector.h: No such file or directory`
  — v8 renamed to `.hpp`. Affects 5 project headers/cpps.
- **Full-count:** 259 folly touchpoints across 30 files.

## 13. Risk register

| # | Risk | Mitigation |
|---|---|---|
| 1 | `sync_get`-on-reactor deadlock (see `sync-get-on-reactor-deadlock` memory) | Explicit `// sync_get: off-reactor OK — <site>` comment convention. Reviewer grep audit. Sanitizer run of `hs_gc_tests` after PR 7. |
| 2 | `IndexTable::destroy()` blocking wait on a reactor caller | Audit every `.destroy()` call site during PR 5 (must be `co_await` or off-reactor). |
| 3 | Bad `uintptr_cast` expansion silently producing wrong pointer type | Grep every `uintptr_cast` site during PR 3; homeblocks caught 5 latent bugs from this. |
| 4 | `alloc_blks` truthiness flip missed at some site | Grep `alloc_blks` and hand-audit truthiness at every call. |
| 5 | Lazy `exec::task` never driven (silent no-op) | Mark async manager entry points `[[nodiscard]]`. `std::ignore = task` is NOT enough — it's a silent no-op. |
| 6 | Buffer lifetime across `co_await` (frame-borrowed data goes stale) | Follow homeblocks `sgs_keepalive` pattern — keep buffers/sg_lists frame-owned across suspends. |
| 7 | New v8 listener callbacks silently unimplemented (default no-op) | List: `on_no_space_left`, `on_log_replay_done`, `on_become_leader`, `on_become_follower`, `on_config_rollback`. Wire real behavior in PR 6 (snapshot/resync) and PR 3 (managers). Track them explicitly. |
| 8 | `HSReplApplication::lookup_peer` URI parse regression | Hand-roll parser + unit test in PR 3. |
| 9 | `homeobject::replace_member_task` vs `homestore::replace_member_task` name collision | Both structs exist and coexist; keep uses fully qualified. Grep for `using namespace homestore` in project code (should be none). |
| 10 | Stdexec version skew across sisl/iomgr/nuraft_mesg/homestore/homeobject | Pin stdexec explicitly in `conanfile.py`. Verify the same stdexec Conan hash resolves for all 5 packages after PR 1. |
| 11 | CI regressing between PRs 1 and 3 | CI runs `HO_BUILD_HOMESTORE_BACKEND=OFF` build on `main` between PR 1 and PR 3. Flip to ON after PR 3 lands. |

## 14. Consequences

- **API break for homeobject consumers** (nuobject, etc.). `Manager<E>::AsyncResult<T>`
  changes shape from `folly::SemiFuture<folly::Expected<T, E>>` to
  `sisl::async::task<std::expected<T, E>>`. Consumers must have stdexec on
  their include path. Version bump 4.x → 5.0 signals this. Release notes will
  spell out the exact migration steps.
- **Manager error structs are stable.** `BlobError`, `ShardError`, `PGError`
  all retain `{code, current_leader}`. NOT_LEADER redirect semantics preserved.
- **Homeobject no longer depends on Folly, directly or transitively.**
  `conanfile.py` will not mention Folly; no `find_package(Folly)` anywhere.
- **New dependency: stdexec** — as an explicit `requires` in `conanfile.py`.
  It's already transitively required by sisl/homestore/iomgr, but making it
  explicit documents the coupling.
- **Test surface expands slightly:** the memory-backend `memory_test` becomes
  the trunk canary. Coverage does not change; execution surface does.
- **Threading model unchanged for GC.** Semantically identical to the folly
  world: dedicated GC worker pool + rate limiter + per-chunk fan-out.
  Implementation swaps to iomgr named reactors + sisl `when_all` + a small
  hand-rolled MPMC replacement.

## 15. Follow-ups (post-lift)

- Release notes for homeobject 5.0 consumer migration.
- Downstream nuobject lift (separate ADR).
- Remove the `HO_BUILD_HOMESTORE_BACKEND=OFF` code path from CI once
  homeblocks-style GC/HTTP work is battle-tested, or keep it as a fast
  memory-only test option.
- Add a repo-wide clang-tidy rule to catch `sync_get` on a reactor
  (custom check flagging any `sync_get(...)` inside a function whose
  transitive callers include an iomgr reactor entry point). Aspirational.
- Consider revisiting the value/design of the `Manager<E>::NullAsyncResult`
  alias once callers settle: with `std::monostate` it's slightly clunky and
  callers frequently want a `status`-style alias.

## Appendix A — File inventory (folly-touchpoint density)

Ranked by folly-touch count as of `main` @ `3f7af3f`. Use this to plan
per-PR scope.

| File | folly refs | PR | Notes |
|---|---:|---|---|
| `src/include/homeobject/common.hpp` | 7 | 1 | Public API contract; must land first |
| `src/lib/tests/fixture_app.cpp` | 2 | 1 | `folly::Init` drop |
| `src/lib/tests/BlobManagerTest.cpp` | 4 | 1 | `.get()` → `sync_get`, drop `GlobalExecutor` |
| `src/lib/tests/PGManagerTest.cpp` | 0 | 1 | Verify no leaks |
| `src/lib/tests/ShardManagerTest.cpp` | 0 | 1 | Verify no leaks |
| `src/lib/homeobject_impl.hpp` | 3 | 1 | Core; `folly::Expected` refs |
| `src/lib/homeobject_impl.cpp` | 3 | 1 | Core |
| `src/lib/blob_manager.cpp` | 5 | 1 | Core (abstract) |
| `src/lib/shard_manager.cpp` | 6 | 1 | Core (abstract) |
| `src/lib/pg_manager.cpp` | 2 | 1 | Core (abstract) |
| `src/lib/memory_backend/mem_homeobject.hpp` | 3 | 1 | `ConcurrentHashMap` |
| `src/lib/memory_backend/mem_homeobject.cpp` | 0 | 1 | Trivial |
| `src/lib/memory_backend/mem_blob_manager.cpp` | 2 | 1 | `makeUnexpected`, `makeSemiFuture` |
| `src/lib/memory_backend/mem_shard_manager.cpp` | 1 | 1 | Same |
| `src/lib/memory_backend/mem_pg_manager.cpp` | 8 | 1 | Same |
| `src/lib/homestore_backend/hs_homeobject.hpp` | 3 | 2 | Header includes cleanup |
| `src/lib/homestore_backend/hs_homeobject.cpp` | 3 | 3 | `HSReplApplication::lookup_peer` folly::Uri |
| `src/lib/homestore_backend/replication_state_machine.hpp` | 5 | 2 | Repl listener re-derive |
| `src/lib/homestore_backend/replication_state_machine.cpp` | 15 | 2 | State machine bodies |
| `src/lib/homestore_backend/hs_blob_manager.cpp` | 35 | 3 | Heaviest single manager |
| `src/lib/homestore_backend/hs_shard_manager.cpp` | 22 | 3 | |
| `src/lib/homestore_backend/hs_pg_manager.cpp` | 50 | 3 | Heaviest single file |
| `src/lib/homestore_backend/hs_cp_callbacks.cpp` | 2 | 4 | Small |
| `src/lib/homestore_backend/index_kv.cpp` | 3 | 5 | + `destroy()` audit |
| `src/lib/homestore_backend/snapshot_receive_handler.cpp` | 10 | 6 | |
| `src/lib/homestore_backend/pg_blob_iterator.cpp` | 3 | 6 | Becomes coroutine |
| `src/lib/homestore_backend/gc_manager.hpp` | 14 | 7 | Executors, MPMCQueue, ConcurrentHashMap |
| `src/lib/homestore_backend/gc_manager.cpp` | 25 | 7 | Per-chunk fan-out |
| `src/lib/homestore_backend/hs_http_manager.hpp` | 4 | 8 | Pistache handlers, EvictingCacheMap |
| `src/lib/homestore_backend/hs_http_manager.cpp` | 6 | 8 | Handler bodies |
| `src/lib/homestore_backend/tests/hs_gc_tests.cpp` | 4 | 9 | Test-side folly cleanup |
| `src/lib/homestore_backend/tests/test_homestore_backend_dynamic.cpp` | 2 | 9 | |
| `src/lib/homestore_backend/tests/test_heap_chunk_selector.cpp` | 2 | 9 | |
| `src/lib/homestore_backend/tests/hs_repl_test_helper.hpp` | 3 | 9 | |

## Appendix B — Distinct Folly primitives found in the code

Full set enumerated by the audit grep
(`grep -rnoE 'folly::[A-Za-z_][A-Za-z_0-9]*' src`):

`ConcurrentHashMap, EvictingCacheMap, Executor, Expected, Future,
IOThreadPoolExecutor, Init, InlineExecutor, MPMCQueue, Promise,
QueuedImmediateExecutor, SemiFuture, Unit, Uri, collectAll, collectAllUnsafe,
getGlobalCPUExecutor, getGlobalIOExecutor, getKeepAliveToken, makeFuture,
makePromiseContract, makeSemiFuture, makeUnexpected, small_vector`

Every one of these has a replacement in §8-B. If a new one appears during the
lift, add it here + §8-B before proceeding.

## Appendix C — Cross-reference to issue #429

The issue's playbook maps onto this ADR as follows:

| Issue #429 section | Where addressed in this ADR |
|---|---|
| Baseline → target table | §2 |
| Reusable playbook (mechanical dictionary) | §8-B, §8-C, §8-D |
| Hard part #1 — `replication_state_machine` re-derive | §6 PR 2, §9-C, §9-D |
| Hard part #2 — snapshot / baseline-resync | §6 PR 6 |
| Hard part #3 — GC executors → reactors | §4-C, §6 PR 7 |
| Hard part #4 — error type preservation | §4-B, §5 |
| Hard part #5 — HTTP Pistache → sisl httplib | §6 PR 8 |
| Hard part #6 — CP callbacks | §6 PR 4, §9-E |
| Suggested sequence | §6 PRs 1–9 (equivalent, restructured for green-trunk) |
| Gotchas (sync_get, lazy task, buffer lifetime, alloc_blks truthiness, uintptr_cast) | §13 risk register |
| Effort estimate | §6 per-PR estimates sum to 21–29 person-days, matching the issue's 20–27 |

Three additions this ADR makes beyond issue #429 (accepted refinements):

1. **CMake `CXX_STANDARD 20 → 23` bump** in PR 1 — the issue's step 1 mentions
   C++23 but doesn't call out the CMake override that hides it. Verified in §12.
2. **`HO_BUILD_HOMESTORE_BACKEND` gate + memory-backend green trunk** — §7 and
   §6 PR 1. Enables mid-lift testability.
3. **`HSReplApplication::lookup_peer` folly::Uri site** — §6 PR 3, §13 risk 8.
   Called out explicitly since it's the one non-mechanical Folly usage outside
   GC and HTTP.
