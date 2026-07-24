// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Test-only transaction anomaly checker.
//!
//! The checker is hooked into [`super::scheduler`] right after a write command
//! is processed (i.e. `task.process_write` returns) and before its staged
//! writes are proposed. For every user key the command touches, it synthesizes
//! the post-state from "snapshot state + this command's staged modifies" and
//! verifies that the following anomaly predicate does not hold:
//!
//! ```text
//! CF_WRITE contains a committed record with write.start_ts == S and
//! write_type in {Put, Delete}
//! AND CF_LOCK contains a normal (non-pessimistic, non-shared) lock with
//! lock.ts == S
//! ```
//!
//! Normal 2PC never leaves such a state: commit/rollback put the CF_WRITE
//! record and the CF_LOCK deletion into the same atomic write batch. When the
//! predicate is hit, the checker writes an `error!` log (and optionally a JSON
//! evidence file) carrying enough context to attribute the producer, and
//! optionally panics to freeze the scene.
//!
//! The checker is strictly test-only: it is disabled by default and has zero
//! overhead when disabled. It never changes transaction semantics; any failure
//! inside the checker itself is downgraded to a warning log.

use std::{
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use collections::HashMap;
use engine_traits::{CF_LOCK, CF_WRITE};
use tikv_kv::{Modify, Snapshot, SnapshotExt};
use tikv_util::Either;
use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteRef, WriteType};

use super::commands::WriteResult;
use crate::storage::{
    config::TxnAnomalyCheckerConfig,
    metrics::CommandKind,
    mvcc::{self, MvccReader, TxnCommitRecord},
};

/// Marker of the error log and evidence produced when the checker hits. It is
/// used by the repro testbed to locate hits in TiKV logs.
pub const CHECKER_HIT_MARKER: &str = "TXN_ANOMALY_CHECKER_HIT";
/// Marker of the startup log that proves a checker-enabled build is deployed.
pub const CHECKER_BUILD_MARKER: &str = "TXN_ANOMALY_CHECKER_BUILD";
/// Version of the checker itself, recorded in evidence files.
pub const CHECKER_VERSION: &str = "1";

/// Global sequence number to keep evidence file names unique.
static EVIDENCE_SEQ: AtomicU64 = AtomicU64::new(0);

/// The anomaly is introduced by the staged writes of the command itself.
const ORIGIN_STAGED: &str = "staged";
/// The anomaly already exists in the snapshot before this command.
const ORIGIN_PRE_EXISTING: &str = "pre-existing";

/// A detected violation of the anomaly predicate.
struct Violation {
    origin: &'static str,
    key: Key,
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
    lock: Lock,
    write_record: Write,
}

/// Staged modifies of one command, grouped by user key.
#[derive(Default)]
struct KeyModifies {
    /// Committed Put/Delete write record staged by `Put(CF_WRITE)`, with the
    /// commit_ts decoded from the internal key.
    staged_write: Option<(TimeStamp, Write)>,
    /// Normal lock staged by `Put(CF_LOCK)`.
    staged_lock: Option<Lock>,
    /// `Delete(CF_LOCK)` staged by the command.
    staged_delete_lock: bool,
    /// The CF_LOCK value staged by the command cannot be parsed as a normal
    /// lock (suspected shared locks). The key is skipped to avoid false
    /// positives.
    skip: bool,
}

/// Commands that must be skipped as a whole: flashback intentionally co-writes
/// write records and locks; pause/flush/raw commands are not normal 2PC
/// commands.
fn is_skipped_tag(tag: CommandKind) -> bool {
    matches!(
        tag,
        CommandKind::flashback_to_version_write
            | CommandKind::flashback_to_version_rollback_lock
            | CommandKind::pause
            | CommandKind::flush
            | CommandKind::raw_compare_and_swap
            | CommandKind::raw_atomic_store
    )
}

/// Checks the write result of a finished command against the anomaly
/// predicate. Returns the number of violations found (for tests). Failures of
/// the checker itself are logged and swallowed.
pub fn check_write_result<S: Snapshot>(
    snapshot: &S,
    tag: CommandKind,
    write_result: &WriteResult,
    config: &TxnAnomalyCheckerConfig,
) -> usize {
    if !config.enabled || is_skipped_tag(tag) {
        return 0;
    }
    match collect_violations(snapshot, write_result, config) {
        Ok(violations) if !violations.is_empty() => {
            report(snapshot, tag, write_result, config, &violations);
            violations.len()
        }
        Ok(_) => 0,
        Err(e) => {
            warn!("txn anomaly checker failed, skipped"; "err" => ?e);
            0
        }
    }
}

fn collect_violations<S: Snapshot>(
    snapshot: &S,
    write_result: &WriteResult,
    config: &TxnAnomalyCheckerConfig,
) -> mvcc::Result<Vec<Violation>> {
    // Group staged modifies by user key.
    let mut keys: HashMap<Key, KeyModifies> = HashMap::default();
    for m in &write_result.to_be_write.modifies {
        match m {
            Modify::Put(cf, k, v) if *cf == CF_WRITE => {
                let write = match WriteRef::parse(v) {
                    Ok(w) => w.to_owned(),
                    Err(e) => {
                        warn!("txn anomaly checker: cannot parse staged CF_WRITE value, skipped";
                            "key" => log_wrappers::Value::key(k.as_encoded()), "err" => ?e);
                        continue;
                    }
                };
                // Rollback/Lock records do not participate in the predicate.
                if !matches!(write.write_type, WriteType::Put | WriteType::Delete) {
                    continue;
                }
                let commit_ts = k.decode_ts()?;
                let user_key = k.clone().truncate_ts()?;
                keys.entry(user_key).or_default().staged_write = Some((commit_ts, write));
            }
            Modify::Put(cf, k, v) if *cf == CF_LOCK => {
                let entry = keys.entry(k.clone()).or_default();
                match txn_types::parse_lock(v) {
                    Ok(Either::Left(lock)) => {
                        // Pessimistic locks are not the subject of the predicate.
                        if lock.lock_type != LockType::Pessimistic {
                            entry.staged_lock = Some(lock);
                        }
                    }
                    Ok(Either::Right(_)) => {
                        warn!("txn anomaly checker: staged CF_LOCK value holds shared locks, skipped";
                            "key" => log_wrappers::Value::key(k.as_encoded()));
                        entry.skip = true;
                    }
                    Err(e) => {
                        warn!("txn anomaly checker: cannot parse staged CF_LOCK value, skipped";
                            "key" => log_wrappers::Value::key(k.as_encoded()), "err" => ?e);
                        entry.skip = true;
                    }
                }
            }
            Modify::Delete(cf, k) if *cf == CF_LOCK => {
                keys.entry(k.clone()).or_default().staged_delete_lock = true;
            }
            _ => {}
        }
    }

    let mut reader = MvccReader::new(snapshot.clone(), None, false);
    let mut violations = vec![];
    for (user_key, staged) in &keys {
        if staged.skip {
            continue;
        }
        let snapshot_lock = match reader.load_lock(user_key)? {
            Some(Either::Left(lock)) if lock.lock_type != LockType::Pessimistic => Some(lock),
            Some(Either::Right(_)) => {
                warn!("txn anomaly checker: snapshot CF_LOCK value holds shared locks, skipped";
                    "key" => log_wrappers::Value::key(user_key.as_encoded()));
                continue;
            }
            _ => None,
        };

        // Pre-state check: the snapshot itself already satisfies the predicate.
        if config.check_pre_state {
            if let Some(lock) = &snapshot_lock {
                if let Some((commit_ts, write)) =
                    find_committed_record(&mut reader, user_key, lock.ts)?
                {
                    violations.push(Violation {
                        origin: ORIGIN_PRE_EXISTING,
                        key: user_key.clone(),
                        start_ts: lock.ts,
                        commit_ts,
                        lock: lock.clone(),
                        write_record: write,
                    });
                }
            }
        }

        // Forward check: a committed record is staged while a normal lock with
        // the same start_ts still exists in the post-state.
        if let Some((commit_ts, write)) = &staged.staged_write {
            let post_lock = if let Some(lock) = &staged.staged_lock {
                Some(lock)
            } else if staged.staged_delete_lock {
                None
            } else {
                snapshot_lock.as_ref()
            };
            if let Some(lock) = post_lock {
                if lock.ts == write.start_ts {
                    violations.push(Violation {
                        origin: ORIGIN_STAGED,
                        key: user_key.clone(),
                        start_ts: write.start_ts,
                        commit_ts: *commit_ts,
                        lock: lock.clone(),
                        write_record: write.clone(),
                    });
                }
            }
        }

        // Backward check: a normal lock is staged while the snapshot already
        // holds a committed record with the same start_ts.
        if let Some(lock) = &staged.staged_lock {
            if let Some((commit_ts, write)) = find_committed_record(&mut reader, user_key, lock.ts)?
            {
                violations.push(Violation {
                    origin: ORIGIN_STAGED,
                    key: user_key.clone(),
                    start_ts: lock.ts,
                    commit_ts,
                    lock: lock.clone(),
                    write_record: write,
                });
            }
        }
    }
    Ok(violations)
}

/// Finds the committed Put/Delete record of `(key, start_ts)` on the snapshot,
/// if any. This is the same read path that `rollback_lock` uses to decide the
/// customer-site panic.
fn find_committed_record<S: Snapshot>(
    reader: &mut MvccReader<S>,
    key: &Key,
    start_ts: TimeStamp,
) -> mvcc::Result<Option<(TimeStamp, Write)>> {
    match reader.get_txn_commit_record(key, start_ts)? {
        TxnCommitRecord::SingleRecord { commit_ts, write }
            if matches!(write.write_type, WriteType::Put | WriteType::Delete) =>
        {
            Ok(Some((commit_ts, write)))
        }
        _ => Ok(None),
    }
}

fn report<S: Snapshot>(
    snapshot: &S,
    tag: CommandKind,
    write_result: &WriteResult,
    config: &TxnAnomalyCheckerConfig,
    violations: &[Violation],
) {
    let first = &violations[0];
    let ctx = &write_result.ctx;
    let ext = snapshot.ext();
    let snap_region_id = ext.get_region_id().unwrap_or(0);
    let snap_term = ext.get_term().map(|t| t.get()).unwrap_or(0);
    let modifies_summary = summarize_modifies(&write_result.to_be_write.modifies);
    let other_keys: Vec<String> = violations[1..]
        .iter()
        .map(|v| hex::encode(v.key.as_encoded()))
        .collect();
    let now = chrono::Utc::now();
    let backtrace = std::backtrace::Backtrace::force_capture().to_string();

    error!("{}", CHECKER_HIT_MARKER;
        "origin" => first.origin,
        "command" => tag.get_str(),
        "region_id" => ctx.get_region_id(),
        "key" => log_wrappers::Value::key(first.key.as_encoded()),
        "start_ts" => first.start_ts.into_inner(),
        "commit_ts" => first.commit_ts.into_inner(),
        "lock" => ?first.lock,
        "write_record" => ?first.write_record,
        "modifies" => ?modifies_summary,
        "other_violated_keys" => ?other_keys,
    );

    if !config.evidence_dir.is_empty() {
        let evidence = serde_json::json!({
            "marker": CHECKER_HIT_MARKER,
            "checker_version": CHECKER_VERSION,
            "build_marker": CHECKER_BUILD_MARKER,
            "origin": first.origin,
            "command": tag.get_str(),
            "region_id": ctx.get_region_id(),
            "region_epoch": format!("{:?}", ctx.get_region_epoch()),
            "peer": format!("{:?}", ctx.get_peer()),
            "key": hex::encode(first.key.as_encoded()),
            "start_ts": first.start_ts.into_inner(),
            "commit_ts": first.commit_ts.into_inner(),
            "lock": format!("{:?}", first.lock),
            "write_record": format!("{:?}", first.write_record),
            "modifies": modifies_summary,
            "other_violated_keys": other_keys,
            "snapshot_region_id": snap_region_id,
            "snapshot_term": snap_term,
            "utc": now.to_rfc3339(),
            "backtrace": backtrace,
        });
        match write_evidence_file(&config.evidence_dir, &evidence, now) {
            Ok(path) => {
                error!("txn anomaly checker evidence written";
                    "marker" => CHECKER_HIT_MARKER,
                    "path" => %path.display(),
                );
            }
            Err(e) => {
                warn!("txn anomaly checker: failed to write evidence file";
                    "dir" => config.evidence_dir.as_str(), "err" => ?e);
                warn!("txn anomaly checker evidence dump"; "evidence" => %evidence);
            }
        }
    }

    if config.panic_on_hit {
        panic!(
            "{}: origin={}, command={}, key={:?}, start_ts={}, commit_ts={}",
            CHECKER_HIT_MARKER,
            first.origin,
            tag.get_str(),
            log_wrappers::Value::key(first.key.as_encoded()),
            first.start_ts.into_inner(),
            first.commit_ts.into_inner(),
        );
    }
}

/// Writes the evidence JSON to `<dir>/hit-<utc>-<seq>.json` and fsyncs both
/// the file and the directory.
fn write_evidence_file(
    dir: &str,
    evidence: &serde_json::Value,
    now: chrono::DateTime<chrono::Utc>,
) -> std::io::Result<PathBuf> {
    fs::create_dir_all(dir)?;
    let seq = EVIDENCE_SEQ.fetch_add(1, Ordering::Relaxed);
    let file_name = format!("hit-{}-{}.json", now.format("%Y%m%dT%H%M%S%.6fZ"), seq);
    let path = Path::new(dir).join(file_name);
    let mut file = fs::File::create(&path)?;
    file.write_all(serde_json::to_string_pretty(evidence)?.as_bytes())?;
    file.sync_all()?;
    fs::File::open(dir)?.sync_all()?;
    Ok(path)
}

/// Builds a compact one-line description for every staged modify.
fn summarize_modifies(modifies: &[Modify]) -> Vec<String> {
    modifies
        .iter()
        .map(|m| match m {
            Modify::Put(cf, k, v) => {
                let mut s = format!("put {} {}", cf, hex::encode(k.as_encoded()));
                if *cf == CF_LOCK {
                    if let Ok(Either::Left(lock)) = txn_types::parse_lock(v) {
                        s += &format!(" lock_ts={} type={:?}", lock.ts.into_inner(), lock.lock_type);
                    }
                } else if *cf == CF_WRITE {
                    if let Ok(w) = WriteRef::parse(v) {
                        s += &format!(
                            " start_ts={} type={:?}",
                            w.start_ts.into_inner(),
                            w.write_type
                        );
                    }
                }
                s
            }
            Modify::Delete(cf, k) => format!("delete {} {}", cf, hex::encode(k.as_encoded())),
            Modify::PessimisticLock(k, lock) => format!(
                "pessimistic_lock {} start_ts={} for_update_ts={}",
                hex::encode(k.as_encoded()),
                lock.start_ts.into_inner(),
                lock.for_update_ts.into_inner()
            ),
            Modify::DeleteRange(cf, start, end, notify_only) => format!(
                "delete_range {} {} {} notify_only={}",
                cf,
                hex::encode(start.as_encoded()),
                hex::encode(end.as_encoded()),
                notify_only
            ),
            Modify::Ingest(_) => "ingest".to_owned(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        panic::{catch_unwind, AssertUnwindSafe},
        path::{Path, PathBuf},
        sync::{atomic::AtomicU64, mpsc::channel, Arc},
        thread,
    };

    use kvproto::kvrpcpb::{
        ApiVersion, AssertionLevel, Context, PrewriteRequestPessimisticAction::*,
    };
    use tikv_kv::WriteData;
    use txn_types::Mutation;

    use super::*;
    use crate::storage::{
        config::Config,
        kv::Engine,
        lock_manager::MockLockManager,
        test_util::{
            acquire_pessimistic_lock, delete_pessimistic_lock, expect_ok_callback,
            new_acquire_pessimistic_lock_command,
        },
        txn::{
            commands,
            commands::{ReleasedLocks, ResponsePolicy},
            ProcessResult,
        },
        types::StorageCallbackType,
        Storage, TestEngineBuilder, TestStorageBuilderApiV1, TypedCommand,
    };

    type TestStorage = Storage<crate::storage::kv::RocksEngine, MockLockManager, api_version::ApiV1>;

    /// Schedules a command and waits for its callback result, without
    /// asserting on it.
    fn sched_and_wait<T: StorageCallbackType + Send + 'static>(
        storage: &TestStorage,
        cmd: TypedCommand<T>,
    ) -> crate::storage::Result<T> {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                cmd,
                Box::new(move |res| {
                    tx.send(res).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap()
    }

    fn checker_config(dir: &Path, panic_on_hit: bool) -> TxnAnomalyCheckerConfig {
        TxnAnomalyCheckerConfig {
            enabled: true,
            panic_on_hit,
            check_pre_state: true,
            evidence_dir: dir.to_str().unwrap().to_owned(),
        }
    }

    fn evidence_files(dir: &Path) -> Vec<PathBuf> {
        match fs::read_dir(dir) {
            Ok(entries) => entries
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.file_name()
                        .and_then(|n| n.to_str())
                        .map_or(false, |n| n.starts_with("hit-"))
                })
                .collect(),
            Err(_) => vec![],
        }
    }

    fn read_evidence(dir: &Path) -> serde_json::Value {
        let files = evidence_files(dir);
        assert_eq!(files.len(), 1, "expect exactly one evidence file");
        serde_json::from_str(&fs::read_to_string(&files[0]).unwrap()).unwrap()
    }

    fn make_write_result(modifies: Vec<Modify>) -> WriteResult {
        WriteResult::new(
            Context::default(),
            WriteData::from_modifies(modifies),
            1,
            ProcessResult::Res,
            vec![],
            ReleasedLocks::new(),
            vec![],
            vec![],
            ResponsePolicy::OnApplied,
            vec![],
        )
    }

    fn make_lock(key: &Key, ts: TimeStamp, ttl: u64) -> Lock {
        Lock::new(
            LockType::Put,
            key.to_raw().unwrap(),
            ts,
            ttl,
            None,
            TimeStamp::zero(),
            1,
            TimeStamp::zero(),
            false,
        )
    }

    fn seed_lock<E: Engine>(engine: &E, key: &Key, ts: TimeStamp, ttl: u64) {
        let lock = make_lock(key, ts, ttl);
        crate::storage::mvcc::tests::write(
            engine,
            &Context::default(),
            vec![Modify::Put(CF_LOCK, key.clone(), lock.to_bytes())],
        );
    }

    fn seed_commit_record<E: Engine>(engine: &E, key: &Key, start_ts: TimeStamp, commit_ts: TimeStamp) {
        let write = Write::new(WriteType::Put, start_ts, Some(b"v".to_vec()));
        crate::storage::mvcc::tests::write(
            engine,
            &Context::default(),
            vec![Modify::Put(
                CF_WRITE,
                key.clone().append_ts(commit_ts),
                write.as_ref().to_bytes(),
            )],
        );
    }

    /// A staged `Put(CF_WRITE)` without a matching `Delete(CF_LOCK)` must hit.
    #[test]
    fn test_staged_forward_hit() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_lock(&engine, &key, 10.into(), 3000);
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let write = Write::new(WriteType::Put, 10.into(), Some(b"v".to_vec()));
        let wr = make_write_result(vec![Modify::Put(
            CF_WRITE,
            key.clone().append_ts(20.into()),
            write.as_ref().to_bytes(),
        )]);
        let config = checker_config(dir.path(), false);
        let hits = check_write_result(&snapshot, CommandKind::commit, &wr, &config);
        assert_eq!(hits, 1);

        let evidence = read_evidence(dir.path());
        assert_eq!(evidence["marker"], CHECKER_HIT_MARKER);
        assert_eq!(evidence["origin"], ORIGIN_STAGED);
        assert_eq!(evidence["command"], "commit");
        assert_eq!(evidence["start_ts"], 10);
        assert_eq!(evidence["commit_ts"], 20);
        assert_eq!(
            evidence["key"],
            hex::encode(key.as_encoded()),
        );
    }

    /// A staged normal lock on top of an existing committed record with the
    /// same start_ts must hit.
    #[test]
    fn test_staged_backward_hit() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_commit_record(&engine, &key, 10.into(), 20.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let lock = make_lock(&key, 10.into(), 3000);
        let wr = make_write_result(vec![Modify::Put(CF_LOCK, key.clone(), lock.to_bytes())]);
        let config = checker_config(dir.path(), false);
        let hits = check_write_result(&snapshot, CommandKind::prewrite, &wr, &config);
        assert_eq!(hits, 1);
        assert_eq!(read_evidence(dir.path())["origin"], ORIGIN_STAGED);
    }

    /// A staged commit record together with the lock deletion (the normal
    /// commit shape) must not hit.
    #[test]
    fn test_no_hit_when_lock_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_lock(&engine, &key, 10.into(), 3000);
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let write = Write::new(WriteType::Put, 10.into(), Some(b"v".to_vec()));
        let wr = make_write_result(vec![
            Modify::Put(CF_WRITE, key.clone().append_ts(20.into()), write.as_ref().to_bytes()),
            Modify::Delete(CF_LOCK, key.clone()),
        ]);
        let config = checker_config(dir.path(), false);
        assert_eq!(check_write_result(&snapshot, CommandKind::commit, &wr, &config), 0);
        assert!(evidence_files(dir.path()).is_empty());
    }

    /// A staged Rollback write record with a remaining lock must not hit
    /// (rollback records are excluded from the predicate).
    #[test]
    fn test_no_hit_for_rollback_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_lock(&engine, &key, 10.into(), 3000);
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let write = Write::new_rollback(10.into(), true);
        let wr = make_write_result(vec![Modify::Put(
            CF_WRITE,
            key.clone().append_ts(10.into()),
            write.as_ref().to_bytes(),
        )]);
        let config = checker_config(dir.path(), false);
        assert_eq!(check_write_result(&snapshot, CommandKind::rollback, &wr, &config), 0);
        assert!(evidence_files(dir.path()).is_empty());
    }

    /// Disabled checker must be a no-op even on a fabricated anomaly.
    #[test]
    fn test_disabled_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_lock(&engine, &key, 10.into(), 3000);
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let write = Write::new(WriteType::Put, 10.into(), Some(b"v".to_vec()));
        let wr = make_write_result(vec![Modify::Put(
            CF_WRITE,
            key.clone().append_ts(20.into()),
            write.as_ref().to_bytes(),
        )]);
        let mut config = checker_config(dir.path(), false);
        config.enabled = false;
        assert_eq!(check_write_result(&snapshot, CommandKind::commit, &wr, &config), 0);
        assert!(evidence_files(dir.path()).is_empty());
    }

    /// `panic-on-hit = true` must panic with the marker after recording.
    #[test]
    fn test_panic_on_hit() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_lock(&engine, &key, 10.into(), 3000);
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let write = Write::new(WriteType::Put, 10.into(), Some(b"v".to_vec()));
        let wr = make_write_result(vec![Modify::Put(
            CF_WRITE,
            key.clone().append_ts(20.into()),
            write.as_ref().to_bytes(),
        )]);
        let config = checker_config(dir.path(), true);
        let result = catch_unwind(AssertUnwindSafe(|| {
            check_write_result(&snapshot, CommandKind::commit, &wr, &config)
        }));
        let err = result.expect_err("expect panic on hit");
        let msg = err
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(msg.contains(CHECKER_HIT_MARKER), "panic msg: {}", msg);
        // Evidence is recorded before panicking.
        assert_eq!(read_evidence(dir.path())["origin"], ORIGIN_STAGED);
    }

    /// A staged unparseable CF_LOCK value (suspected shared locks) must be
    /// skipped without a false positive.
    #[test]
    fn test_shared_lock_value_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let mut engine = TestEngineBuilder::new().build().unwrap();
        let key = Key::from_raw(b"k");
        seed_commit_record(&engine, &key, 10.into(), 20.into());
        let snapshot = engine.snapshot(Default::default()).unwrap();

        let shared = txn_types::SharedLocks::new();
        let wr = make_write_result(vec![Modify::Put(CF_LOCK, key.clone(), shared.to_bytes())]);
        let config = checker_config(dir.path(), false);
        assert_eq!(check_write_result(&snapshot, CommandKind::prewrite, &wr, &config), 0);
        assert!(evidence_files(dir.path()).is_empty());
    }

    fn build_checker_storage(dir: &Path) -> TestStorage {
        let mut config = Config::default();
        config.set_api_version(ApiVersion::V1);
        config.txn_anomaly_checker = checker_config(dir, false);
        TestStorageBuilderApiV1::new(MockLockManager::new())
            .config(config)
            .build()
            .unwrap()
    }

    #[test]
    fn test_no_false_positive_optimistic_2pc() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();
        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        Mutation::make_put(k2.clone(), b"v".to_vec()),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k1, k2], 10.into(), 20.into(), Context::default()),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_pessimistic_2pc() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();
        let k = Key::from_raw(b"k");

        acquire_pessimistic_lock(&storage, k.clone(), 10, 10);
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(Mutation::make_put(k.clone(), b"v".to_vec()), DoPessimisticCheck)],
                    k.to_raw().unwrap(),
                    10.into(),
                    3000,
                    10.into(),
                    1,
                    TimeStamp::zero(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Commit::new(vec![k], 10.into(), 20.into(), Context::default()),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_rollback_and_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();
        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");

        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        Mutation::make_put(k2.clone(), b"v".to_vec()),
                    ],
                    b"k1".to_vec(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Rollback::new(vec![k1], 10.into(), Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        storage
            .sched_txn_command(
                commands::Cleanup::new(k2, 10.into(), TimeStamp::zero(), Context::default()),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_pessimistic_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let k = Key::from_raw(b"k");

        acquire_pessimistic_lock(&storage, k.clone(), 10, 10);
        delete_pessimistic_lock(&storage, k, 10, 10);

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_txn_heart_beat() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();
        let k = Key::from_raw(b"k");

        storage
            .sched_txn_command(
                commands::Prewrite::with_lock_ttl(
                    vec![Mutation::make_put(k.clone(), b"v".to_vec())],
                    b"k".to_vec(),
                    10.into(),
                    100,
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // A larger advise_ttl rewrites the lock via put_lock.
        storage
            .sched_txn_command(
                commands::TxnHeartBeat::new(k, 10.into(), 110, 0, Context::default()),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_resolve_lock() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();

        // ResolveLock: commit branch.
        let k = Key::from_raw(b"k");
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), b"v".to_vec())],
                    b"k".to_vec(),
                    10.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut txn_status = collections::HashMap::default();
        txn_status.insert(TimeStamp::from(10), TimeStamp::from(20));
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // ResolveLock: rollback branch.
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![Mutation::make_put(k.clone(), b"v".to_vec())],
                    b"k".to_vec(),
                    30.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        let mut txn_status = collections::HashMap::default();
        txn_status.insert(TimeStamp::from(30), TimeStamp::zero());
        storage
            .sched_txn_command(
                commands::ResolveLockReadPhase::new(txn_status, None, Context::default()),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // ResolveLockLite: rollback and commit branches.
        let k1 = Key::from_raw(b"k1");
        let k2 = Key::from_raw(b"k2");
        storage
            .sched_txn_command(
                commands::Prewrite::with_defaults(
                    vec![
                        Mutation::make_put(k1.clone(), b"v".to_vec()),
                        Mutation::make_put(k2.clone(), b"v".to_vec()),
                    ],
                    b"k1".to_vec(),
                    40.into(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(
                    40.into(),
                    TimeStamp::zero(),
                    vec![k1],
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::ResolveLockLite::new(40.into(), 50.into(), vec![k2], Context::default()),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    #[test]
    fn test_no_false_positive_check_txn_status() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();

        // rollback_if_not_exist on a missing txn writes a rollback record.
        let k0 = Key::from_raw(b"k0");
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k0,
                    9.into(),
                    11.into(),
                    11.into(),
                    true,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // min_commit_ts push rewrites the lock via put_lock.
        let k = Key::from_raw(b"k");
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(k.clone(), b"v".to_vec())],
                    b"k".to_vec(),
                    10.into(),
                    10_000_000,
                    false,
                    1,
                    12.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();
        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k,
                    10.into(),
                    15.into(),
                    15.into(),
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        assert!(evidence_files(dir.path()).is_empty());
    }

    /// Pre-seed an anomalous state directly into the engine, then let a normal
    /// command touch the key. The checker must report a pre-existing hit
    /// before the command itself would run into the customer-site panic.
    #[test]
    fn test_hit_pre_existing() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let (tx, rx) = channel();
        let k = Key::from_raw(b"k");

        // Prewrite with a non-zero min_commit_ts and a large TTL, so that the
        // later check_txn_status neither expires the lock (which would run
        // into the original panic) nor skips the put_lock rewrite (which
        // keeps the key in the command's modifies).
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(k.clone(), b"v".to_vec())],
                    b"k".to_vec(),
                    10.into(),
                    10_000_000,
                    false,
                    1,
                    12.into(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    Context::default(),
                ),
                expect_ok_callback(tx.clone(), 0),
            )
            .unwrap();
        rx.recv().unwrap();

        // Inject the anomalous committed record directly, bypassing the txn
        // layer, to simulate a state produced before the checker was deployed.
        let write = Write::new(WriteType::Put, 10.into(), Some(b"v".to_vec()));
        let mut region_modifies = collections::HashMap::default();
        region_modifies.insert(
            0_u64,
            vec![Modify::Put(
                CF_WRITE,
                k.clone().append_ts(20.into()),
                write.as_ref().to_bytes(),
            )],
        );
        storage
            .get_engine()
            .modify_on_kv_engine(region_modifies)
            .unwrap();

        storage
            .sched_txn_command(
                commands::CheckTxnStatus::new(
                    k.clone(),
                    10.into(),
                    15.into(),
                    15.into(),
                    false,
                    false,
                    false,
                    true,
                    Context::default(),
                ),
                expect_ok_callback(tx, 0),
            )
            .unwrap();
        rx.recv().unwrap();

        let evidence = read_evidence(dir.path());
        assert_eq!(evidence["origin"], ORIGIN_PRE_EXISTING);
        assert_eq!(evidence["command"], "check_txn_status");
        assert_eq!(evidence["start_ts"], 10);
        assert_eq!(evidence["commit_ts"], 20);
        assert_eq!(evidence["key"], hex::encode(k.as_encoded()));
    }

    /// A small concurrent pessimistic "REPLACE"-like workload on one hot key
    /// must not produce any false positive.
    #[test]
    fn test_no_false_positive_concurrent_replace() {
        let dir = tempfile::tempdir().unwrap();
        let storage = build_checker_storage(dir.path());
        let k = Key::from_raw(b"hot");
        // Globally increasing ts allocator, like a tiny TSO.
        let ts_alloc = Arc::new(AtomicU64::new(1000));

        let mut handles = vec![];
        for _ in 0..4 {
            let storage = storage.clone();
            let k = k.clone();
            let ts_alloc = ts_alloc.clone();
            handles.push(thread::spawn(move || {
                for i in 0..10 {
                    let value = format!("v{}", i).into_bytes();
                    // A REPLACE-like pessimistic write. On write conflict (a
                    // faster thread committed a larger ts first), roll back
                    // and retry with a fresh ts, like TiDB does.
                    for attempt in 0..100 {
                        assert!(attempt < 99, "too many retries");
                        let start_ts = ts_alloc.fetch_add(2, Ordering::Relaxed);
                        let res = sched_and_wait(
                            &storage,
                            new_acquire_pessimistic_lock_command(
                                vec![(k.clone(), false)],
                                start_ts,
                                start_ts,
                                false,
                                false,
                            ),
                        );
                        if res.is_err() {
                            continue;
                        }
                        let res = sched_and_wait(
                            &storage,
                            commands::PrewritePessimistic::new(
                                vec![(
                                    Mutation::make_put(k.clone(), value.clone()),
                                    DoPessimisticCheck,
                                )],
                                k.to_raw().unwrap(),
                                start_ts.into(),
                                3000,
                                start_ts.into(),
                                1,
                                TimeStamp::zero(),
                                TimeStamp::default(),
                                None,
                                false,
                                AssertionLevel::Off,
                                vec![],
                                Context::default(),
                            ),
                        );
                        if res.is_err() {
                            // Clean up the pessimistic lock of the abandoned
                            // attempt before retrying.
                            delete_pessimistic_lock(&storage, k.clone(), start_ts, start_ts);
                            continue;
                        }
                        sched_and_wait(
                            &storage,
                            commands::Commit::new(
                                vec![k.clone()],
                                start_ts.into(),
                                (start_ts + 1).into(),
                                Context::default(),
                            ),
                        )
                        .unwrap();
                        break;
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert!(evidence_files(dir.path()).is_empty());
    }
}
