# TiKV #4239: iterator reuse verification

This change reuses MVCC scanners and their RocksDB iterators across bounded ranges. Every range change resets physical bounds and performs a seek. Unsupported iterators fall back to a fresh scanner. The focused checks below found no remaining failure in the covered paths; they do not establish full-suite or production readiness.

## Revisions and scope

- Tested TiKV implementation: `1696a4794eec1afe0d907d8133e242872418557c`.
- Published TiKV dependency-pin update: `4a060a483348e2a3298fdc2a97353e96f27c0649`. Its only differences from the tested TiKV commit are `Cargo.toml` and `Cargo.lock`; the 23 shipping source/test/documentation files are unchanged. It pins the equivalent wrapper revision below and updates the three lockfile package sources. The cleaned shipping checkout passed `cargo metadata --locked --offline --format-version 1`; this validates dependency resolution, not a new build or benchmark run.
- Tested rust-rocksdb commit: `ab3a8c400f1c8b29b24f75c44459e6bbebc9a981`.
- Published equivalent wrapper commit: `cc93046a101b90573430548f932017c5761ea582`, branch `iterator/reset-bounds-and-seek-signed`. It corrects author/DCO identity metadata. Both commits have the identical complete Git tree `c967576c5377906f18691f3df23916bb6979e9fb`, and `git diff --exit-code` between them passed. The measured source and original run metadata remain pinned to `ab3a8c…`; no rerun on the republished commit is claimed. The raw JSON records this mapping in `publication_source_equivalence`. Wrapper review: [rust-rocksdb #864](https://github.com/tikv/rust-rocksdb/pull/864); its DCO check passed at publication.
- Validation host: Linux container with an 8 GiB memory limit.
- At measurement time, the benchmark checkout intentionally retained base Git HEAD to avoid build-info recompilation. Its 23 shipping TiKV files and 3 shipping wrapper files were checked byte-for-byte against the tested source pins above; `tikv-4239-source-verification2.json` retains their SHA256 list.

Unsupported native iterator configurations now reject bounds reset before mutation. Scanner replacements retain statistics from successful CF resets that preceded another CF's refusal. A `statistics_on_error` sink also preserves undrained statistics and attempted reset work when range validation, physical reset, or replacement-scanner construction fails. Each error is returned immediately without changing its identity.

## Correctness checks: 35 passed

| Area | Passed | Execution and evidence |
| --- | ---: | --- |
| rust-rocksdb iterator tests | 14 | Normal Cargo focused target: 7 new reset tests and 7 existing iterator tests; `tikv-4239-narrow-focus.log` |
| Coprocessor integration | 5 | Same normal Cargo target; `tikv-4239-narrow-focus.log` |
| `tikv_kv` cursor | 3 | 2 new reset tests and the existing prefix-seek test; `tikv-4239-cursor-tests.log` |
| Hybrid snapshot iterator | 1 | Extended existing `snapshot::tests::test_iterator`; `tikv-4239-hybrid-tests.log` |
| MVCC scanner reuse | 11 | Selected from the complete TiKV unit-test binary built by normal Cargo; `tikv-4239-mvcc-tests-final.log` |
| Region iterator reset | 1 | Normal Cargo build with a serial LLVM backend and 16 codegen units; `tikv-4239-region-tests.log` |

Coverage includes forward and reverse scans, disjoint/overlapping/reordered ranges, partial and exhausted scans, empty ranges, lazy default-CF cursor creation, key-only and commit-timestamp option changes, MVCC visibility and locks, sticky cacheability, snapshot retention, owned bounds, region limits, cursor EOF/missing-range caches, and read/seek statistics. Gap records are not decoded. Native tests exercise multiple SST generations and tombstones. Unsupported engine, prefix, Titan, tailing, and missing-bound cases exercise rejection or fallback without changing the unsupported iterator.

The partial-reset regression test rejects either lock-CF or default-CF reset after an earlier CF succeeds, in both directions. It checks seek counts and processed data for callers that retain earlier statistics and for `TikvStorage`, which drains them before switching ranges. Three additional regressions cover invalid-range errors, injected physical-reset errors, and injected fallback-construction errors, verifying retained statistics and original error propagation.

### Build qualification

Temporary focused workspace members selected the relevant wrapper/integration sources while retaining the full tests feature anchor. The native and build-dependency unit graph was checked against the full graph. At the close of stage one, these validation entry points and local dependency path patches were removed; `tests/Cargo.toml` was restored and the root manifest/lockfile matched the stage-one dependency-pin commit. The follow-up below records the later test-only changes and dependency pin. The evidence JSON retains the historical harness/configuration for reproduction and records the final shipping checks in `shipping_verification`.

Early attempts to compile the TiKV library tests with one codegen unit exceeded the memory limit. All final selected targets were built through normal Cargo. For the complete TiKV library-test binary, `RUSTC_WRAPPER=/tmp/tikv-4239-final-unit-rustc-wrapper.py` preserved Cargo's arguments and appended `-Ccodegen-units=16 -Zno-parallel-backend` to the large test compilation. Cargo built the complete binary with 767 test/benchmark entries; the 11 MVCC reuse tests passed, with 756 entries filtered out. The Region test also used normal Cargo with the serial-backend/codegen adjustment. That stage-one run is targeted verification, not a full-suite pass. The later complete executions are recorded separately below. The final Cargo JSON, build stdout, and wrapper log are retained as `tikv-4239-unit-final2.jsonl`, `tikv-4239-unit-final2.log`, and `tikv-4239-final-unit-rustc-wrapper.log`.

## Performance comparison: final2

Final2 completed four runs of all 18 cases on one unchanged executable; all four saved baselines and executable hash checks passed. These results use the tested source pins above. Earlier measurements remain separate and are excluded.

For every one of the 16 multi-range variants, both paired comparisons measured less user CPU time with reuse: reductions ranged from **16.04% to 64.89%**. The single-range controls changed sign between the two pairs, so they show no stable benefit. This variation also limits how precisely the multi-range percentages should be interpreted; these are local comparative smoke measurements.

The comparison used one unchanged executable in order **fresh1, reuse1, reuse2, fresh2**. Fresh and reuse use the same generated data and read options. The control creates a new scanner for every range. Each run covers 18 cases: nine scenario/range-count configurations, each in both directions. Every iteration creates one `TikvStorage`, drains all requested ranges, and collects statistics; fixture generation and full value/order checks remain outside the timed loop.

The Rust build uses a mixed test profile: scan-path packages are optimized at level 3; unrelated Rust packages remain at level 0. Assertions and overflow checks remain enabled. TiKV uses 16 codegen units and a serial LLVM backend to stay within memory limits. Native RocksDB is compiled separately by CMake in `Release` mode with `-O3 -DNDEBUG`; this does not make the Rust benchmark a release-profile build.

Each case uses 1 second of warm-up, 2 seconds of measurement, and 10 Criterion samples. Measurement is `criterion_cpu_time::PosixTime::UserTime` (process user CPU time), on repeatedly scanned, cache-warmed data with identical RocksDB perf counters enabled. The fixture reads a bare `Arc<RocksSnapshot>` through `SnapshotStore` and `TikvStorage`; it does not run the full RegionSnapshot/Raftstore/request chain. These results do not establish request wall-clock latency, release-profile, cold-cache, or saturated disk performance.

Physical-tombstone fixtures flush puts and deletes into separate generations, pin the original snapshot, disable automatic compaction, and assert retained SST entry counts. Thus the benchmark verifies that the requested physical deletion markers remain present. Hard seeks and physical bounds avoid walking linearly through gaps.

| Scenario | Ranges | Forward: paired mean change range | Reverse: paired mean change range |
| --- | ---: | --- | --- |
| Dense | 1 | -26.09% to +11.06% | -9.97% to +19.34% |
| Dense | 64 | -62.35% to -38.71% | -64.89% to -44.28% |
| Dense | 512 | -54.69% to -43.63% | -41.18% to -35.65% |
| Sparse ranges | 512 | -40.66% to -27.99% | -44.75% to -29.38% |
| Sparse data | 512 | -56.78% to -34.84% | -42.16% to -32.90% |
| Physical tombstones | 512 | -44.98% to -26.12% | -38.03% to -25.61% |
| Wide physical tombstone gaps | 64 | -33.54% to -16.04% | -45.15% to -38.13% |
| MVCC deletes | 512 | -49.72% to -47.24% | -26.65% to -19.36% |
| Mostly empty | 512 | -54.34% to -32.63% | -36.95% to -34.32% |

The accompanying [CSV](scanner_reuse_4239.csv) retains mean and median for all four rounds; all values were cross-checked against the 72 original Criterion estimate records. The table reports signed mean changes: negative values mean lower user CPU time. The single-range ranges span zero across the two independent pairs; they are not statistical confidence intervals. Percentage change is `100 × (reuse / fresh − 1)`, paired as fresh1/reuse1 and fresh2/reuse2. Reported min/max values are two independent paired observations, **not pooled confidence intervals**. Short runs are comparative smoke measurements; precise percentage or production throughput claims require longer repetitions and a representative release workload.


## Reproduction

The adjacent [merged benchmark profile](scanner_reuse_4239_profile.toml) records the effective test-profile settings without changing shipping repository profiles. [Raw verification evidence](scanner_reuse_4239.json) retains the source-file hashes, build command and environment, serial-backend wrapper, temporary verification member and harness, manifest changes, all four run arguments, and each round's original Criterion estimates and samples. Restore the pinned sources and recorded local verification configuration to reproduce this exact mixed-profile comparison; use one unchanged executable for all four rounds.

The normal repository benchmark target and fresh/reuse commands are documented in `tests/benches/coprocessor_executors/multi_range_scan.rs`. Its default `cargo bench` profile differs from this constrained-host configuration, so results from a default release benchmark should be identified separately. The JSON includes the temporary focused harness used here so that the memory workaround and measurement entry point are reviewable.

## Additional verification: 2026-10-16

This follow-up adds two regression tests. A SHA256 comparison against the stage-one evidence found changes only in `tests/integrations/coprocessor/test_scanner_reuse.rs` and the wrapper's `tests/cases/test_iterator.rs`; the recorded scanner and iterator implementation files are unchanged. The stage-one performance results remain attributed to their original source pins, executable, mixed Rust profile, and warm-cache user-CPU measurement. No new benchmark run is claimed here.

| Validation | Result | Scope and evidence |
| --- | --- | --- |
| Canonical rust-rocksdb test entry point | 140 passed; 3 native crashes, from 143 independently executed cases | `tests/tests.rs` and its original case modules, selected through a temporary workspace member; `tikv-4239-next-wrapper-isolated/results.json` and the per-case logs |
| rust-rocksdb iterator cases | All 15 passed, included in the 140 above | Eight bounds-reset tests and seven existing iterator tests, including the new retained multi-L1 range-tombstone regression |
| Coprocessor scanner integration | All 6 passed | Existing five cases plus the new real Region/Hybrid cache-fallback case; `tikv-4239-next-focus-tests.log` |
| Complete TiKV library test binary | 762 passed; 4 failed; 1 ignored in the corrected-runtime rerun (157.12 s); initially 759 passed, 7 failed, 1 ignored (153.04 s) | One unchanged binary, SHA256 `4450893e92ed624008620bdd48ccbf0b183fd3f791e47eb110e96b8e01131a53`; 751 tests plus 16 benchmark smoke entries, no filtering. Both full-run receipts and logs are retained; neither run is a full-suite pass. |

The wrapper's initial single-process run aborted with `SIGSEGV`. Each of its 143 listed cases was then run in a separate process so the abort could not hide later results. The three crashing cases are `cases::test_rocksdb_options::test_block_based_options`, `cases::test_statistics::test_db_statistics`, and `cases::test_statistics::test_shared_db_statistics`. Each also crashes with `SIGSEGV` when selected separately on the unmodified wrapper revision `797c014576c9f01a854bd681711c617c5920c9c6`, using the same native build cache. Only these three cases were compared against that baseline; no full baseline-suite pass is claimed. Their root cause remains unresolved. These results establish 140 isolated case passes and three baseline-reproduced crashes, **not a successful single-process wrapper suite**.

The new native regression, `test_reset_bounds_and_seek_across_l1_files_and_retained_range_tombstones`, forces small uncompressed output files and asserts at least three L1 SSTs after compaction. A snapshot taken before `DeleteRange` preserves the original versions and the range tombstone. SST entry counts and raw value-byte totals distinguish the 65 retained put versions from at least three range-tombstone fragments. The test also checks the old snapshot's complete contents. On the newer snapshot, one reused iterator moves across files in both directions, reaches EOF, expands its bounds, revisits earlier ranges, and reads both empty and reinserted portions of the deleted range. Every result is checked against an explicit key/value oracle and a newly created iterator.

The new storage integration, `test_scanner_reuse_region_cache_fallback_preserves_snapshot_bounds_and_statistics`, uses `RegionSnapshot<HybridEngineSnapshot>` with a real `RegionCacheMemoryEngine`. Writes reach disk and cache through `RegionCacheWriteBatchObserver`, and the snapshot sequence numbers are checked. All three MVCC CFs use the cache branch; rejected resets leave the original iterator unchanged, and `SnapshotStore` must recreate the scanner. Forward and reverse multi-range reads are compared with fresh cache scanners, an independent pinned disk snapshot, and explicit expected rows. Writes made after snapshot acquisition remain invisible, even when their commit timestamps precede the read timestamp. The test also covers records outside the region, an exclusive region endpoint, skipped locked gaps, a real lock conflict, recovery after an invalid range switch, retained statistics, and repeated statistics drains. This exercises the storage/Region/Hybrid chain; it is not an RPC or Raftstore-cluster test.

All added targets were built through normal Cargo. The native RocksDB/Titan artifacts remained fresh, and the 69-unit native/build-dependency closure was compared with the historical full-feature graph. Temporary selection members preserve the original feature anchor; they do not replace the implementation under test. Large Rust targets use the recorded serial LLVM/codegen adjustment to fit the host memory limit. These qualifications and per-process receipts belong with the additional evidence. The original stage-one JSON, CSV, and performance profile are preserved separately.

Follow-up code/dependency revisions: TiKV `fe1162424b1a991fc91606c22f64acfde6e1e6f2` includes integration test `e9b2e8644a5c97bb42550fdddbfb146d7bf319ae`, the direct tests dependency on `hybrid_engine`, and the lockfile update; rust-rocksdb `778cab5e4bcbfa3ebdc88fa7b38c8d97c0d0b43f` adds the L1/tombstone regression to `cc93046…`. All 30 changed TiKV paths and all three wrapper paths were checked byte-for-byte against these published revisions before this report/evidence update. These changes do not alter the measured runtime implementation.

The initial TiKV library run could not load the plugin fixture's Rust `libstd` dependency. Prepending the toolchain target-libdir and `target/debug/deps` to `LD_LIBRARY_PATH` fixed all five plugin cases when the same binary was rerun. Three rerun failures involve this container's host facilities: `test_thread_load_statistic` and `test_peer_resolve` fail while obtaining thread statistics through `/proc/<getpid>/task`, and `test_hardware_info` cannot enumerate network interfaces. A separate read-only probe records mismatched process and `/proc` PIDs and `EPERM` from `getifaddrs`/route-netlink. The relevant source files are byte-identical to the base. These host failures have not been executed on a TiKV baseline or a standard runner; the diagnosis does not establish a full-suite pass.

The fourth rerun failure is the existing `test_old_value_with_hint_min_ts`: an assertion expected zero block reads and observed one, after its old-value assertion had succeeded. It passed in the first full run and in 20 later isolated executions of the identical binary. The test uses non-waiting CF flushes before the assertion and builds a fresh delta scanner without invoking range reuse; asynchronous flush timing is a plausible cause, not a baseline-verified diagnosis. The full-run failure remains recorded. No test or production code was changed to hide it, and isolated passes do not replace a full-suite pass.

The separate [follow-up evidence](scanner_reuse_4239_followup.json) includes both complete-library run logs, the runtime search-path correction, all 143 isolated wrapper outcomes and three baseline comparisons, the six integration outcomes, 20 isolated hint-test outcomes, host/source diagnostics, temporary configuration snapshots, source hashes, and cleaned shipping checks. The original stage-one JSON/CSV/profile remain unchanged.

## Remaining validation

`make dev`, the full workspace test suites and CI, release-profile benchmarks, cold-I/O tests, and production workload tests have not completed. The additional storage integration covers the real Region/Hybrid cache-fallback chain, but not an RPC/Raftstore-cluster request or a Titan-backed deployment. The canonical wrapper suite still aborts in a single process; its 140 isolated passes and three baseline-reproduced native crashes must not be presented as a full-suite pass. The complete TiKV library binary's result is recorded in Additional verification above. These limitations remain explicit when presenting the change or requesting review.
