// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use engine_rocks::{RocksEngine, RocksEngineIterator, RocksWriteBatch};
use engine_traits::{
    IterOptions, Iterable, Iterator, Mutable, SeekKey, WriteBatch, WriteBatchExt, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use tikv_util::sys::thread::StdThreadBuildWrapper;
use txn_types::{Key, TimeStamp, Write, WriteRef};

use super::Result;

const BATCH_SIZE: usize = 256;

/// `ResetToVersionState` is the current state of the reset-to-version process.
/// todo: Report this to the user.
#[derive(Debug, Clone)]
pub enum ResetToVersionState {
    /// `RemovingWrite` means we are removing stale data in the `WRITE` and `DEFAULT` cf
    RemovingWrite { scanned: usize },
    /// `RemovingWrite` means we are removing stale data in the `LOCK` cf
    RemovingLock { scanned: usize },
    /// `Done` means we have finshed the task
    Done,
}

impl ResetToVersionState {
    pub fn scanned(&mut self) -> &mut usize {
        match self {
            ResetToVersionState::RemovingWrite { scanned } => scanned,
            ResetToVersionState::RemovingLock { scanned } => scanned,
            _ => unreachable!(),
        }
    }
}

/// `ResetToVersionWorker` is the worker that does the actual reset-to-version work.
pub struct ResetToVersionWorker {
    /// `ts` is the timestamp to reset to.
    ts: TimeStamp,
    /// `write_iter` is the iterator to scan through the WRITE cf
    write_iter: RocksEngineIterator,
    /// `lock_iter` is the iterator to scan through the LOCK cf
    lock_iter: RocksEngineIterator,
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
        mut write_iter: RocksEngineIterator,
        mut lock_iter: RocksEngineIterator,
        ts: TimeStamp,
        state: Arc<Mutex<ResetToVersionState>>,
    ) -> Self {
        *state
            .lock()
            .expect("failed to lock `state` in `ResetToVersionWorker::new`") =
            ResetToVersionState::RemovingWrite { scanned: 0 };
        write_iter.seek(SeekKey::Start).unwrap();
        lock_iter.seek(SeekKey::Start).unwrap();
        Self {
            write_iter,
            lock_iter,
            ts,
            state,
        }
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, Write)>> {
        if self.write_iter.valid().unwrap() {
            let mut state = self
                .state
                .lock()
                .expect("failed to lock ResetToVersionWorker::state");
            debug_assert!(matches!(
                *state,
                ResetToVersionState::RemovingWrite { scanned: _ }
            ));
            *state.scanned() += 1;
            drop(state);
            let write = box_try!(WriteRef::parse(self.write_iter.value())).to_owned();
            let key = self.write_iter.key().to_vec();
            self.write_iter.next().unwrap();
            return Ok(Some((key, write)));
        }
        Ok(None)
    }

    fn scan_next_batch(&mut self, batch_size: usize) -> Result<Batch> {
        let mut writes = Vec::with_capacity(batch_size);
        let mut has_more = true;
        for _ in 0..batch_size {
            if let Some((key, write)) = self.next_write()? {
                let commit_ts = box_try!(Key::decode_ts_from(keys::origin_key(&key)));
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

    pub fn process_next_batch(
        &mut self,
        batch_size: usize,
        wb: &mut RocksWriteBatch,
    ) -> Result<bool> {
        let Batch { writes, has_more } = self.scan_next_batch(batch_size)?;
        for (key, write) in writes {
            let default_key = Key::from_encoded_slice(&key)
                .truncate_ts()
                .unwrap()
                .append_ts(write.start_ts);
            box_try!(wb.delete_cf(CF_WRITE, &key));
            box_try!(wb.delete_cf(CF_DEFAULT, default_key.as_encoded()));
        }
        wb.write().unwrap();
        wb.clear();
        Ok(has_more)
    }

    pub fn process_next_batch_lock(
        &mut self,
        batch_size: usize,
        wb: &mut RocksWriteBatch,
    ) -> Result<bool> {
        let mut has_more = true;
        for _ in 0..batch_size {
            if self.lock_iter.valid().unwrap() {
                let mut state = self
                    .state
                    .lock()
                    .expect("failed to lock ResetToVersionWorker::state");
                debug_assert!(matches!(
                    *state,
                    ResetToVersionState::RemovingLock { scanned: _ }
                ));
                *state.scanned() += 1;
                drop(state);

                box_try!(wb.delete_cf(CF_LOCK, self.lock_iter.key()));
                self.lock_iter.next().unwrap();
            } else {
                has_more = false;
                break;
            }
        }
        wb.write().unwrap();
        Ok(has_more)
    }
}

/// `ResetToVersionManager` is the manager that manages the reset-to-version process.
/// User should interact with `ResetToVersionManager` instead of using `ResetToVersionWorker` directly.  
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
            .iterator_cf_opt(CF_WRITE, readopts.clone())
            .unwrap();
        let lock_iter = self.engine.iterator_cf_opt(CF_LOCK, readopts).unwrap();
        let mut worker = ResetToVersionWorker::new(write_iter, lock_iter, ts, self.state.clone());
        let mut wb = self.engine.write_batch();
        let props = tikv_util::thread_group::current_properties();
        if self.worker_handle.borrow().is_some() {
            warn!("A reset-to-version process is already in progress! Wait until it finish first.");
            self.wait();
        }
        *self.worker_handle.borrow_mut() = Some(std::thread::Builder::new()
            .name("reset_to_version".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                tikv_alloc::add_thread_memory_accessor();

                while worker.process_next_batch(BATCH_SIZE, &mut wb).expect("reset_to_version failed when removing invalid writes") {
                }
                *worker.state.lock()
                        .expect("failed to lock `ResetToVersionWorker::state` in `ResetToVersionWorker::process_next_batch`")
                    = ResetToVersionState::RemovingLock { scanned: 0 };
                while worker.process_next_batch_lock(BATCH_SIZE, &mut wb).expect("reset_to_version failed when removing invalid locks") {
                }
                *worker.state.lock()
                        .expect("failed to lock `ResetToVersionWorker::state` in `ResetToVersionWorker::process_next_batch_lock`")
                    = ResetToVersionState::Done;
                info!("Reset to version done!");
                tikv_alloc::remove_thread_memory_accessor();
            })
            .expect("failed to spawn reset_to_version thread"));
    }

    /// Current process state.
    pub fn state(&self) -> ResetToVersionState {
        self.state
            .lock()
            .expect("failed to lock `state` in `ResetToVersionManager::state`")
            .clone()
    }

    /// Wait until the process finished.
    pub fn wait(&self) {
        self.worker_handle.take().unwrap().join().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::{
        raw::{ColumnFamilyOptions, DBOptions},
        raw_util::CFOptions,
        Compat,
    };
    use engine_traits::{WriteBatch, WriteBatchExt, CF_LOCK, CF_RAFT};
    use tempfile::Builder;
    use txn_types::{Lock, LockType, WriteType};

    use super::*;

    #[test]
    fn test_basic() {
        let tmp = Builder::new().prefix("test_basic").tempdir().unwrap();
        let path = tmp.path().to_str().unwrap();
        let fake_engine = Arc::new(
            engine_rocks::raw_util::new_engine_opt(
                path,
                DBOptions::new(),
                vec![
                    CFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_WRITE, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
                    CFOptions::new(CF_RAFT, ColumnFamilyOptions::new()),
                ],
            )
            .unwrap(),
        );

        let write = vec![
            // key, start_ts, commit_ts
            (b"k", 104, 105),
            (b"k", 102, 103),
            (b"k", 100, 101),
            (b"k", 98, 99),
        ];
        let default = vec![
            // key, start_ts
            (b"k", 104),
            (b"k", 102),
            (b"k", 100),
            (b"k", 98),
        ];
        let lock = vec![
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"k", 100, 0, LockType::Put, false),
            (b"k", 100, 0, LockType::Delete, false),
            (b"k", 100, 0, LockType::Put, true),
            (b"k", 100, 0, LockType::Delete, true),
        ];
        let mut kv = vec![];
        for (key, start_ts, commit_ts) in write {
            let write = Write::new(WriteType::Put, start_ts.into(), None);
            kv.push((
                CF_WRITE,
                Key::from_raw(key).append_ts(commit_ts.into()),
                write.as_ref().to_bytes(),
            ));
        }
        for (key, ts) in default {
            kv.push((
                CF_DEFAULT,
                Key::from_raw(key).append_ts(ts.into()),
                b"v".to_vec(),
            ));
        }
        for (key, ts, for_update_ts, tp, short_value) in lock {
            let v = if short_value {
                Some(b"v".to_vec())
            } else {
                None
            };
            let lock = Lock::new(
                tp,
                vec![],
                ts.into(),
                0,
                v,
                for_update_ts.into(),
                0,
                TimeStamp::zero(),
            );
            kv.push((CF_LOCK, Key::from_raw(key), lock.to_bytes()));
        }
        let mut wb = fake_engine.c().write_batch();
        for &(cf, ref k, ref v) in &kv {
            wb.put_cf(cf, &keys::data_key(k.as_encoded()), v).unwrap();
        }
        wb.write().unwrap();

        let manager = ResetToVersionManager::new(fake_engine.c().clone());
        manager.start(100.into());
        manager.wait();

        let readopts = IterOptions::new(None, None, false);
        let mut write_iter = fake_engine
            .c()
            .iterator_cf_opt(CF_WRITE, readopts.clone())
            .unwrap();
        write_iter.seek(SeekKey::Start).unwrap();
        let mut remaining_writes = vec![];
        while write_iter.valid().unwrap() {
            let write = WriteRef::parse(write_iter.value()).unwrap().to_owned();
            let key = write_iter.key().to_vec();
            write_iter.next().unwrap();
            remaining_writes.push((key, write));
        }
        let mut default_iter = fake_engine
            .c()
            .iterator_cf_opt(CF_DEFAULT, readopts.clone())
            .unwrap();
        default_iter.seek(SeekKey::Start).unwrap();
        let mut remaining_defaults = vec![];
        while default_iter.valid().unwrap() {
            let key = default_iter.key().to_vec();
            let value = default_iter.value().to_vec();
            default_iter.next().unwrap();
            remaining_defaults.push((key, value));
        }

        let mut lock_iter = fake_engine.c().iterator_cf_opt(CF_LOCK, readopts).unwrap();
        lock_iter.seek(SeekKey::Start).unwrap();
        let mut remaining_locks = vec![];
        while lock_iter.valid().unwrap() {
            let lock = Lock::parse(lock_iter.value()).unwrap().to_owned();
            let key = lock_iter.key().to_vec();
            lock_iter.next().unwrap();
            remaining_locks.push((key, lock));
        }

        // Writes which start_ts >= 100 should be removed.
        assert_eq!(remaining_writes.len(), 1);
        let (key, _) = &remaining_writes[0];
        // So the only write left is the one with start_ts = 99
        assert_eq!(
            Key::from_encoded(key.clone()).decode_ts().unwrap(),
            99.into()
        );
        // Defaults corresponding to the removed writes should be removed.
        assert_eq!(remaining_defaults.len(), 1);
        let (key, _) = &remaining_defaults[0];
        assert_eq!(
            Key::from_encoded(key.clone()).decode_ts().unwrap(),
            98.into()
        );
        // All locks should be removed.
        assert!(remaining_locks.is_empty());
    }
}
