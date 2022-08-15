// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
};

use engine_rocks::{RocksEngine, RocksEngineIterator, RocksWriteBatchVec};
use engine_traits::{
    IterOptions, Iterable, Iterator, Mutable, WriteBatch, WriteBatchExt, CF_DEFAULT, CF_LOCK,
    CF_WRITE,
};
use tikv_util::sys::thread::StdThreadBuildWrapper;
use txn_types::{Key, Lock, LockType, TimeStamp, Write, WriteRef, WriteType};

use super::Result;

// TODO: make this as a configurable parameter.
const BATCH_SIZE: usize = 256;

/// `ResetToVersionState` is the current state of the reset-to-version process.
/// TODO: report the progress to the user.
#[derive(Debug, Clone)]
pub enum ResetToVersionState {
    /// `RemovingWrite` means we are removing stale data in the `WRITE` and
    /// `DEFAULT` cf
    RemovingWrite { scanned: usize, deleted: usize },
    /// `RemovingWrite` means we are removing stale data in the `LOCK` cf
    RemovingLock { scanned: usize, deleted: usize },
    /// `Done` means we have finished the task
    Done {
        total_scanned: usize,
        total_deleted: usize,
    },
}

impl ResetToVersionState {
    pub fn scanned(&mut self) -> &mut usize {
        match self {
            ResetToVersionState::RemovingWrite {
                scanned,
                deleted: _,
            } => scanned,
            ResetToVersionState::RemovingLock {
                scanned,
                deleted: _,
            } => scanned,
            ResetToVersionState::Done {
                total_scanned,
                total_deleted: _,
            } => total_scanned,
        }
    }

    pub fn deleted(&mut self) -> &mut usize {
        match self {
            ResetToVersionState::RemovingWrite {
                scanned: _,
                deleted,
            } => deleted,
            ResetToVersionState::RemovingLock {
                scanned: _,
                deleted,
            } => deleted,
            ResetToVersionState::Done {
                total_scanned: _,
                total_deleted,
            } => total_deleted,
        }
    }
}

/// `ResetToVersionWorker` is the worker that does the actual reset-to-version
/// work.
pub struct ResetToVersionWorker {
    /// `ts` is the timestamp to reset to.
    ts: TimeStamp,
    /// `write_iter` is the iterator to scan through the WRITE cf
    write_iter: RocksEngineIterator,
    /// `lock_iter` is the iterator to scan through the LOCK cf
    lock_iter: RocksEngineIterator,
    /// `write_batch` is the write batch to write the data to the engine
    write_batch: RocksWriteBatchVec,
    /// `total_scanned` is the total number of keys scanned
    total_scanned: usize,
    /// `total_deleted` is the total number of keys deleted
    total_deleted: usize,
    /// `state` is current state of this task
    state: Arc<Mutex<ResetToVersionState>>,
}

/// `Batch` means a batch of writes load from the engine.
/// We scan writes in batches to prevent huge memory usage.
struct Batch {
    writes: Vec<(Vec<u8>, Write)>,
    has_more: bool,
}

#[allow(dead_code)]
impl ResetToVersionWorker {
    pub fn new(
        ts: TimeStamp,
        mut write_iter: RocksEngineIterator,
        mut lock_iter: RocksEngineIterator,
        write_batch: RocksWriteBatchVec,
        state: Arc<Mutex<ResetToVersionState>>,
    ) -> Self {
        *state.lock().unwrap() = ResetToVersionState::RemovingWrite {
            scanned: 0,
            deleted: 0,
        };
        write_iter.seek_to_first().unwrap();
        lock_iter.seek_to_first().unwrap();
        Self {
            ts,
            write_iter,
            lock_iter,
            write_batch,
            total_scanned: 0,
            total_deleted: 0,
            state,
        }
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, Write)>> {
        if self.write_iter.valid()? {
            {
                let mut state = self.state.lock().unwrap();
                debug_assert!(matches!(
                    *state,
                    ResetToVersionState::RemovingWrite {
                        scanned: _,
                        deleted: _
                    }
                ));
                *state.scanned() += 1;
                self.total_scanned += 1;
            }
            let write = box_try!(WriteRef::parse(self.write_iter.value())).to_owned();
            let key = self.write_iter.key().to_vec();
            self.write_iter.next()?;
            return Ok(Some((key, write)));
        }
        Ok(None)
    }

    fn scan_next_batch(&mut self) -> Result<Batch> {
        let mut writes = Vec::with_capacity(BATCH_SIZE);
        let mut has_more = true;
        for _ in 0..BATCH_SIZE {
            if let Some((key, write)) = self.next_write()? {
                let commit_ts = Key::decode_ts_from(keys::origin_key(&key))?;
                if commit_ts > self.ts {
                    writes.push((key, write));
                }
            } else {
                has_more = false;
                break;
            }
        }
        Ok(Batch { writes, has_more })
    }

    pub fn process_next_batch(&mut self) -> Result<bool> {
        let Batch { writes, has_more } = self.scan_next_batch()?;
        let mut deleted = 0;
        for (key, write) in writes {
            self.write_batch.delete_cf(CF_WRITE, &key)?;
            deleted += 1;
            // If it's not a deletion short value, we need to delete it from the
            // `CF_DEFAULT` as well.
            if write.short_value.is_none() && write.write_type != WriteType::Delete {
                self.write_batch.delete_cf(
                    CF_DEFAULT,
                    Key::from_encoded_slice(&key)
                        .truncate_ts()
                        .unwrap()
                        .append_ts(write.start_ts)
                        .as_encoded(),
                )?;
                deleted += 1;
            }
        }
        if !self.write_batch.is_empty() {
            self.write_batch.write()?;
            self.write_batch.clear();
        }
        // Update the counter.
        *self.state.lock().unwrap().deleted() += deleted;
        self.total_deleted += deleted;
        // Step into the next phase.
        if !has_more {
            *self.state.lock().unwrap() = ResetToVersionState::RemovingLock {
                scanned: 0,
                deleted: 0,
            };
        }
        Ok(has_more)
    }

    pub fn process_next_batch_lock(&mut self) -> Result<bool> {
        let mut has_more = true;
        let mut deleted = 0;
        for _ in 0..BATCH_SIZE {
            if self.lock_iter.valid()? {
                {
                    let mut state = self.state.lock().unwrap();
                    debug_assert!(matches!(
                        *state,
                        ResetToVersionState::RemovingLock {
                            scanned: _,
                            deleted: _
                        }
                    ));
                    *state.scanned() += 1;
                    self.total_scanned += 1;
                }
                let key = self.lock_iter.key();
                self.write_batch.delete_cf(CF_LOCK, key)?;
                deleted += 1;
                let lock = box_try!(Lock::parse(self.lock_iter.value()));
                // If it's not a deletion short value, we need to delete it from the
                // `CF_DEFAULT` as well.
                if lock.short_value.is_none() && lock.lock_type != LockType::Delete {
                    self.write_batch.delete_cf(CF_DEFAULT, key)?;
                    deleted += 1;
                }
                // TODO: handle the case that the TS to reset is greater than the resolved TS.
                self.lock_iter.next()?;
            } else {
                has_more = false;
                break;
            }
        }
        if !self.write_batch.is_empty() {
            self.write_batch.write()?;
            self.write_batch.clear();
        }
        // Update the counter.
        *self.state.lock().unwrap().deleted() += deleted;
        self.total_deleted += deleted;
        if !has_more {
            *self.state.lock().unwrap() = ResetToVersionState::Done {
                total_scanned: self.total_scanned,
                total_deleted: self.total_deleted,
            };
        }
        Ok(has_more)
    }
}

/// `ResetToVersionManager` is the manager that manages the reset-to-version
/// process. User should interact with `ResetToVersionManager` instead of using
/// `ResetToVersionWorker` directly.
pub struct ResetToVersionManager {
    /// Current state of the reset-to-version process.
    state: Arc<Mutex<ResetToVersionState>>,
    /// The engine we are working on
    engine: RocksEngine,
    /// Current working worker
    worker_handle: RefCell<Option<JoinHandle<()>>>,
}

impl Clone for ResetToVersionManager {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            engine: self.engine.clone(),
            worker_handle: RefCell::new(None),
        }
    }
}

#[allow(dead_code)]
impl ResetToVersionManager {
    pub fn new(engine: RocksEngine) -> Self {
        let state = Arc::new(Mutex::new(ResetToVersionState::RemovingWrite {
            scanned: 0,
            deleted: 0,
        }));
        ResetToVersionManager {
            state,
            engine,
            worker_handle: RefCell::new(None),
        }
    }

    /// Start a reset-to-version process which reset version to `ts`.
    pub fn start(&self, ts: TimeStamp) {
        let readopts = IterOptions::new(None, None, false);
        let write_iter = self
            .engine
            .iterator_opt(CF_WRITE, readopts.clone())
            .unwrap();
        let lock_iter = self.engine.iterator_opt(CF_LOCK, readopts).unwrap();
        let mut worker = ResetToVersionWorker::new(
            ts,
            write_iter,
            lock_iter,
            self.engine.write_batch(),
            self.state.clone(),
        );
        let props = tikv_util::thread_group::current_properties();
        if self.worker_handle.borrow().is_some() {
            warn!("A reset-to-version process is already in progress! Wait until it finish first.");
            self.wait();
        }
        *self.worker_handle.borrow_mut() = Some(
            thread::Builder::new()
                .name("reset_to_version".to_string())
                .spawn_wrapper(move || {
                    tikv_util::thread_group::set_properties(props);
                    tikv_alloc::add_thread_memory_accessor();

                    loop {
                        match worker.process_next_batch() {
                            Ok(has_more) => {
                                if !has_more {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Reset to version failed when resetting write/default cfs"; "error" => ?e);
                                break;
                            }
                        }
                    }
                    loop {
                        match worker.process_next_batch_lock() {
                            Ok(has_more) => {
                                if !has_more {
                                    break;
                                }
                            }
                            Err(e) => {
                                error!("Reset to version failed when resetting lock cf"; "error" => ?e);
                                break;
                            }
                        }
                    }
                    info!("Reset to version done!"; "total_scanned" => worker.total_scanned);
                    tikv_alloc::remove_thread_memory_accessor();
                }).unwrap(),
        );
    }

    /// Current process state.
    pub fn state(&self) -> ResetToVersionState {
        self.state.lock().unwrap().clone()
    }

    /// Wait until the process finished.
    pub fn wait(&self) {
        self.worker_handle.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{MiscExt, SyncMutable, ALL_CFS, CF_LOCK};
    use tempfile::Builder;

    use super::*;

    struct CFData<'a> {
        // key, start_ts, commit_ts, write_type, is_short_value
        write: Vec<(&'a [u8], u64, u64, WriteType, bool)>,
        // key, start_ts
        default: Vec<(&'a [u8], u64)>,
        // key, start_ts, lock_type, is_short_value
        lock: Vec<(&'a [u8], u64, LockType, bool)>,
    }

    #[test]
    fn test_basic() {
        let tmp = Builder::new().prefix("test_basic").tempdir().unwrap();
        let path = tmp.path().to_str().unwrap();
        let engine = engine_rocks::util::new_engine(path, ALL_CFS).unwrap();

        let cf_data = CFData {
            write: vec![
                (b"k".as_slice(), 106, 107, WriteType::Put, false),
                (b"k".as_slice(), 104, 105, WriteType::Put, false),
                (b"k".as_slice(), 102, 103, WriteType::Put, false),
                (b"k".as_slice(), 100, 101, WriteType::Put, false),
                (b"k".as_slice(), 98, 99, WriteType::Put, false),
                (b"k".as_slice(), 96, 97, WriteType::Put, true), // Committed with a short value.
                (b"k".as_slice(), 94, 95, WriteType::Put, false),
                (b"k".as_slice(), 92, 93, WriteType::Put, false),
                (b"k".as_slice(), 90, 91, WriteType::Delete, false),
                (b"k".as_slice(), 88, 89, WriteType::Put, false),
            ],
            default: vec![
                (b"k".as_slice(), 108), // Prewritten `Put` with an non-short value.
                (b"k".as_slice(), 106),
                (b"k".as_slice(), 104),
                (b"k".as_slice(), 102),
                (b"k".as_slice(), 100),
                (b"k".as_slice(), 98),
                (b"k".as_slice(), 94),
                (b"k".as_slice(), 92),
                (b"k".as_slice(), 88),
            ],
            lock: vec![
                (b"k".as_slice(), 110, LockType::Delete, false),
                (b"k".as_slice(), 109, LockType::Put, true),
                (b"k".as_slice(), 108, LockType::Put, false),
            ],
        };
        prepare_data_for_cfs(&engine, &cf_data);

        // (ts_to_reset, expected_cf_data, expected_scanned, expected_deleted)
        let test_cases: Vec<(TimeStamp, CFData, usize, usize)> = vec![
            (
                107.into(),
                CFData {
                    write: vec![
                        (b"k".as_slice(), 106, 107, WriteType::Put, false),
                        (b"k".as_slice(), 104, 105, WriteType::Put, false),
                        (b"k".as_slice(), 102, 103, WriteType::Put, false),
                        (b"k".as_slice(), 100, 101, WriteType::Put, false),
                        (b"k".as_slice(), 98, 99, WriteType::Put, false),
                        (b"k".as_slice(), 96, 97, WriteType::Put, true),
                        (b"k".as_slice(), 94, 95, WriteType::Put, false),
                        (b"k".as_slice(), 92, 93, WriteType::Put, false),
                        (b"k".as_slice(), 90, 91, WriteType::Delete, false),
                        (b"k".as_slice(), 88, 89, WriteType::Put, false),
                    ],
                    default: vec![
                        (b"k".as_slice(), 106),
                        (b"k".as_slice(), 104),
                        (b"k".as_slice(), 102),
                        (b"k".as_slice(), 100),
                        (b"k".as_slice(), 98),
                        (b"k".as_slice(), 94),
                        (b"k".as_slice(), 92),
                        (b"k".as_slice(), 88),
                    ],
                    lock: vec![],
                },
                13, // 10 keys in `CF_WRITE` + 3 keys in `CF_LOCK`
                4,  // 3 keys in `CF_LOCK` + 1 key in `CF_DEFAULT`
            ),
            (
                104.into(),
                CFData {
                    write: vec![
                        (b"k".as_slice(), 102, 103, WriteType::Put, false),
                        (b"k".as_slice(), 100, 101, WriteType::Put, false),
                        (b"k".as_slice(), 98, 99, WriteType::Put, false),
                        (b"k".as_slice(), 96, 97, WriteType::Put, true),
                        (b"k".as_slice(), 94, 95, WriteType::Put, false),
                        (b"k".as_slice(), 92, 93, WriteType::Put, false),
                        (b"k".as_slice(), 90, 91, WriteType::Delete, false),
                        (b"k".as_slice(), 88, 89, WriteType::Put, false),
                    ],
                    default: vec![
                        (b"k".as_slice(), 102),
                        (b"k".as_slice(), 100),
                        (b"k".as_slice(), 98),
                        (b"k".as_slice(), 94),
                        (b"k".as_slice(), 92),
                        (b"k".as_slice(), 88),
                    ],
                    lock: vec![],
                },
                10, // 10 keys in `CF_WRITE`
                4,  // 2 keys in `CF_WRITE` + 2 keys in `CF_DEFAULT`
            ),
            (
                96.into(),
                CFData {
                    write: vec![
                        (b"k".as_slice(), 94, 95, WriteType::Put, false),
                        (b"k".as_slice(), 92, 93, WriteType::Put, false),
                        (b"k".as_slice(), 90, 91, WriteType::Delete, false),
                        (b"k".as_slice(), 88, 89, WriteType::Put, false),
                    ],
                    default: vec![
                        (b"k".as_slice(), 94),
                        (b"k".as_slice(), 92),
                        (b"k".as_slice(), 88),
                    ],
                    lock: vec![],
                },
                8, // 8 keys in `CF_WRITE`
                7, // 4 keys in `CF_WRITE` + 3 keys in `CF_DEFAULT`
            ),
            (
                91.into(),
                CFData {
                    write: vec![
                        (b"k".as_slice(), 90, 91, WriteType::Delete, false),
                        (b"k".as_slice(), 88, 89, WriteType::Put, false),
                    ],
                    default: vec![(b"k".as_slice(), 88)],
                    lock: vec![],
                },
                4, // 4 keys in `CF_WRITE`
                4, // 2 keys in `CF_WRITE` + 2 keys in `CF_DEFAULT`
            ),
            (
                89.into(),
                CFData {
                    write: vec![(b"k".as_slice(), 88, 89, WriteType::Put, false)],
                    default: vec![(b"k".as_slice(), 88)],
                    lock: vec![],
                },
                2, // 2 keys in `CF_WRITE`
                1, // 1 key in `CF_WRITE`
            ),
        ];

        for (idx, test_case) in test_cases.iter().enumerate() {
            let manager = ResetToVersionManager::new(engine.clone());
            manager.start(test_case.0);
            manager.wait();
            // Check the remaining data.
            let remaining_writes = get_all_keys(&engine, CF_WRITE);
            let remaining_defaults = get_all_keys(&engine, CF_DEFAULT);
            let remaining_locks = get_all_keys(&engine, CF_LOCK);
            assert_eq!(
                remaining_writes.len(),
                test_case.1.write.len(),
                "test case {}",
                idx
            );
            if test_case.1.write.len() > 0 {
                assert_eq!(
                    Key::from_encoded_slice(&remaining_writes[0])
                        .decode_ts()
                        .unwrap(),
                    test_case.1.write[0].2.into(),
                    "{}",
                    idx
                );
            }
            assert_eq!(
                remaining_defaults.len(),
                test_case.1.default.len(),
                "{}",
                idx
            );
            if test_case.1.default.len() > 0 {
                assert_eq!(
                    Key::from_encoded_slice(&remaining_defaults[0])
                        .decode_ts()
                        .unwrap(),
                    test_case.1.default[0].1.into(),
                    "{}",
                    idx
                );
            }
            assert_eq!(
                remaining_locks.len(),
                test_case.1.lock.len(),
                "test case {}",
                idx
            );
            assert_eq!(*manager.state().scanned(), test_case.2, "test case {}", idx);
            assert_eq!(*manager.state().deleted(), test_case.3, "test case {}", idx);
        }
    }

    fn prepare_data_for_cfs(engine: &RocksEngine, cf_data: &CFData) {
        let value = b"v".to_vec();
        let mut kv = vec![];
        // Write CF.
        for (key, start_ts, commit_ts, write_type, is_short_value) in cf_data.write.iter() {
            let write = Write::new(
                *write_type,
                start_ts.into(),
                if *is_short_value {
                    Some(value.clone())
                } else {
                    None
                },
            );
            kv.push((
                CF_WRITE,
                Key::from_raw(key).append_ts(commit_ts.into()),
                write.as_ref().to_bytes(),
            ));
        }
        // Default CF.
        for (key, start_ts) in cf_data.default.iter() {
            kv.push((
                CF_DEFAULT,
                Key::from_raw(key).append_ts(start_ts.into()),
                value.clone(),
            ));
        }
        // Lock CF.
        for (key, start_ts, tp, is_short_value) in cf_data.lock.iter() {
            let lock = Lock::new(
                *tp,
                vec![],
                start_ts.into(),
                0,
                if *is_short_value && *tp == LockType::Put {
                    Some(value.clone())
                } else {
                    None
                },
                0.into(),
                0,
                TimeStamp::zero(),
            );
            kv.push((
                CF_LOCK,
                Key::from_raw(key).append_ts(start_ts.into()),
                lock.to_bytes(),
            ));
        }
        // Write all keys to the engine.
        for &(cf, ref k, ref v) in &kv {
            engine
                .put_cf(cf, &keys::data_key(k.as_encoded()), v)
                .unwrap();
        }
        // Flush all.
        engine.flush_cf(CF_WRITE, true).unwrap();
        engine.flush_cf(CF_DEFAULT, true).unwrap();
        engine.flush_cf(CF_LOCK, true).unwrap();
    }

    fn get_all_keys(engine: &RocksEngine, cf: &str) -> Vec<Vec<u8>> {
        let mut iter = engine
            .iterator_opt(cf, IterOptions::new(None, None, false))
            .unwrap();
        iter.seek_to_first().unwrap();
        let mut keys = vec![];
        while iter.valid().unwrap() {
            keys.push(iter.key().to_vec());
            iter.next().unwrap();
        }
        keys
    }
}
