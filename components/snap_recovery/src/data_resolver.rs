// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    ops::Bound,
    result,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Instant,
};

use engine_rocks::{RocksEngine, RocksEngineIterator, RocksWriteBatchVec};
use engine_traits::{
    IterOptions, Iterable, Iterator, Mutable, WriteBatch, WriteBatchExt, WriteOptions, CF_DEFAULT,
    CF_LOCK, CF_WRITE,
};
use futures::channel::mpsc::UnboundedSender;
use kvproto::recoverdatapb::ResolveKvDataResponse;
use thiserror::Error;
use tikv_util::sys::thread::StdThreadBuildWrapper;
use txn_types::{Key, TimeStamp, Write, WriteRef};

pub type Result<T> = result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("Engine {0:?}")]
    Engine(#[from] engine_traits::Error),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

/// `DataResolverManager` is the manager that manages the resolve kv data
/// process.
/// currently, we do not support retry the data resolver, tidb-operator does not
/// support apply a restore twice TODO: in future, BR may able to retry if some
/// accident
pub struct DataResolverManager {
    /// The engine we are working on
    engine: RocksEngine,
    /// progress info
    tx: UnboundedSender<ResolveKvDataResponse>,
    /// Current working workers
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    resolved_ts: TimeStamp,
}

impl Clone for DataResolverManager {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            tx: self.tx.clone(),
            workers: Arc::new(Mutex::new(Vec::new())),
            resolved_ts: self.resolved_ts,
        }
    }
}

#[allow(dead_code)]
impl DataResolverManager {
    pub fn new(
        engine: RocksEngine,
        tx: UnboundedSender<ResolveKvDataResponse>,
        resolved_ts: TimeStamp,
    ) -> Self {
        DataResolverManager {
            engine,
            tx,
            workers: Arc::new(Mutex::new(Vec::new())),
            resolved_ts,
        }
    }
    /// Start a delete kv data process which delete all data by resolved_ts.
    pub fn start(&self) {
        self.resolve_lock();
        self.resolve_write();
    }

    fn resolve_lock(&self) {
        let mut readopts = IterOptions::new(None, None, false);
        readopts.set_hint_min_ts(Bound::Excluded(self.resolved_ts.into_inner()));
        let lock_iter = self.engine.iterator_opt(CF_LOCK, readopts).unwrap();
        let mut worker = LockResolverWorker::new(lock_iter, self.tx.clone());
        let mut wb = self.engine.write_batch();
        let props = tikv_util::thread_group::current_properties();

        let handle = std::thread::Builder::new()
            .name("cleanup_lock".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                tikv_alloc::add_thread_memory_accessor();

                worker
                    .cleanup_lock(&mut wb)
                    .expect("cleanup lock failed when delete data from invalid cf");

                tikv_alloc::remove_thread_memory_accessor();
            })
            .expect("failed to spawn resolve_kv_data thread");
        self.workers.lock().unwrap().push(handle);
    }

    fn resolve_write(&self) {
        let mut readopts = IterOptions::new(None, None, false);
        readopts.set_hint_min_ts(Bound::Excluded(self.resolved_ts.into_inner()));
        let write_iter = self
            .engine
            .iterator_opt(CF_WRITE, readopts.clone())
            .unwrap();
        let mut worker = WriteResolverWorker::new(write_iter, self.resolved_ts, self.tx.clone());
        let mut wb = self.engine.write_batch();
        let props = tikv_util::thread_group::current_properties();

        let handle = std::thread::Builder::new()
            .name("resolve_write".to_string())
            .spawn_wrapper(move || {
                tikv_util::thread_group::set_properties(props);
                tikv_alloc::add_thread_memory_accessor();

                if let Err(e) = worker.resolve_write(&mut wb) {
                    error!("failed to resolve write cf"; 
                    "error" => ?e);
                }

                tikv_alloc::remove_thread_memory_accessor();
            })
            .expect("failed to spawn resolve_kv_data thread");

        self.workers.lock().unwrap().push(handle);
    }

    // join and wait until the thread exit
    pub fn wait(&self) {
        let mut last_error = None;
        for h in self.workers.lock().unwrap().drain(..) {
            info!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join manager thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            safe_panic!("failed to join manager thread: {:?}", e);
        }
    }
}
/// `LockResolverWorker` is the worker that does the clean lock cf.
pub struct LockResolverWorker {
    lock_iter: RocksEngineIterator,
    /// send progress of this task
    tx: UnboundedSender<ResolveKvDataResponse>,
}

#[allow(dead_code)]
impl LockResolverWorker {
    pub fn new(
        mut lock_iter: RocksEngineIterator,
        tx: UnboundedSender<ResolveKvDataResponse>,
    ) -> Self {
        lock_iter.seek_to_first().unwrap();
        Self { lock_iter, tx }
    }
    pub fn cleanup_lock(&mut self, wb: &mut RocksWriteBatchVec) -> Result<bool> {
        let mut key_count: u64 = 0;
        while self.lock_iter.valid().unwrap() {
            box_try!(wb.delete_cf(CF_LOCK, self.lock_iter.key()));
            self.lock_iter.next().unwrap();
            key_count += 1;
        }
        info!("clean up lock cf. delete key count {}", key_count);
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        box_try!(wb.write_opt(&write_opts));
        let mut response = ResolveKvDataResponse::default();

        response.set_resolved_key_count(key_count);
        if let Err(e) = self.tx.unbounded_send(response) {
            warn!("send the cleanup lock key failure {}", e);
            if e.is_disconnected() {
                warn!("channel is disconnected.");
                return Ok(false);
            }
        }
        Ok(true)
    }
}

// TODO: as we tested, this size may more effective than set to 256 (max write
// batch) a more robust test need to figure out what is best.
const BATCH_SIZE_LIMIT: usize = 1024 * 1024;
/// `WriteResolverWorker` is the worker that does the actual delete data work.
pub struct WriteResolverWorker {
    batch_size_limit: usize,
    /// `resolved_ts` is the timestamp to data delete to.
    resolved_ts: TimeStamp,
    write_iter: RocksEngineIterator,
    /// send progress of this task
    tx: UnboundedSender<ResolveKvDataResponse>,
}

/// `Batch` means a batch of writes load from the engine.
/// We scan writes in batches to prevent huge memory usage.
struct Batch {
    writes: Vec<(Vec<u8>, Write)>,
    has_more: bool,
}

#[allow(dead_code)]
impl WriteResolverWorker {
    pub fn new(
        mut write_iter: RocksEngineIterator,
        resolved_ts: TimeStamp,
        tx: UnboundedSender<ResolveKvDataResponse>,
    ) -> Self {
        write_iter.seek_to_first().unwrap();
        Self {
            batch_size_limit: BATCH_SIZE_LIMIT,
            write_iter,
            resolved_ts,
            tx,
        }
    }
    pub fn resolve_write(&mut self, wb: &mut RocksWriteBatchVec) -> Result<()> {
        let now = Instant::now();
        while self.batch_resolve_write(wb)? {}
        info!("resolve write";
        "spent_time" => now.elapsed().as_secs(),
        );
        Ok(())
    }

    fn next_write(&mut self) -> Result<Option<(Vec<u8>, Write)>> {
        if self.write_iter.valid().unwrap() {
            let write = box_try!(WriteRef::parse(self.write_iter.value())).to_owned();
            let key = self.write_iter.key().to_vec();
            self.write_iter.next().unwrap();
            return Ok(Some((key, write)));
        }
        Ok(None)
    }

    fn scan_next_batch(&mut self) -> Result<Batch> {
        let mut writes = Vec::with_capacity(self.batch_size_limit);
        let mut has_more = true;

        for _ in 0..self.batch_size_limit {
            if let Some((key, write)) = self.next_write()? {
                let commit_ts = box_try!(Key::decode_ts_from(keys::origin_key(&key)));
                if commit_ts > self.resolved_ts {
                    writes.push((key, write));
                }
            } else {
                has_more = false;
                break;
            }
        }
        Ok(Batch { writes, has_more })
    }

    // delete key.commit_ts > resolved-ts in write cf and default cf
    fn batch_resolve_write(&mut self, wb: &mut RocksWriteBatchVec) -> Result<bool> {
        let Batch { writes, has_more } = self.scan_next_batch()?;
        if has_more && writes.is_empty() {
            return Ok(has_more);
        }

        let batch = writes.clone();
        let mut max_ts: TimeStamp = 0.into();
        for (key, write) in writes {
            let default_key = Key::from_encoded_slice(&key)
                .truncate_ts()
                .unwrap()
                .append_ts(write.start_ts);
            box_try!(wb.delete_cf(CF_WRITE, &key));
            box_try!(wb.delete_cf(CF_DEFAULT, default_key.as_encoded()));

            let commit_ts = box_try!(Key::decode_ts_from(keys::origin_key(&key)));
            if commit_ts > max_ts {
                max_ts = commit_ts;
            }
        }
        info!(
            "flush delete in write/default cf.";
            "delete_key_count" => batch.len(),
        );
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        wb.write_opt(&write_opts)?;

        let mut response = ResolveKvDataResponse::default();

        response.set_resolved_key_count(batch.len().try_into().unwrap());
        response.set_current_commit_ts(max_ts.into_inner());
        if let Err(e) = self.tx.unbounded_send(response) {
            warn!("send the resolved key failure {}", e);
            if e.is_disconnected() {
                warn!("channel is disconnected.");
                return Ok(has_more);
            }
        }

        Ok(has_more)
    }
}
#[cfg(test)]
mod tests {
    use engine_traits::{WriteBatch, WriteBatchExt, ALL_CFS, CF_LOCK};
    use futures::channel::mpsc;
    use tempfile::Builder;
    use txn_types::{Lock, LockType, WriteType};

    use super::*;

    #[test]
    fn test_data_resolver() {
        let tmp = Builder::new()
            .prefix("test_data_resolver")
            .tempdir()
            .unwrap();
        let path = tmp.path().to_str().unwrap();
        let fake_engine = engine_rocks::util::new_engine(path, ALL_CFS).unwrap();

        // insert some keys, and resolved base on 100
        // write cf will remain one key
        let write = vec![
            // key, start_ts, commit_ts
            (b"k", 189, 190),
            (b"k", 122, 123),
            (b"k", 110, 111),
            (b"k", 98, 99),
        ];
        let default = vec![
            // key, start_ts
            (b"k", 189),
            (b"k", 122),
            (b"k", 110),
            (b"k", 98),
        ];
        let lock = vec![
            // key, start_ts, for_update_ts, lock_type, short_value, check
            (b"k", 100, 0, LockType::Put, false),
            (b"k", 100, 0, LockType::Delete, false),
            (b"k", 99, 0, LockType::Put, true),
            (b"k", 98, 0, LockType::Delete, true),
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
        let mut wb = fake_engine.write_batch();
        for &(cf, ref k, ref v) in &kv {
            wb.put_cf(cf, &keys::data_key(k.as_encoded()), v).unwrap();
        }
        wb.write().unwrap();

        let (tx, _) = mpsc::unbounded();
        let resolver = DataResolverManager::new(fake_engine.clone(), tx, 100.into());
        resolver.start();
        // wait to delete finished
        resolver.wait();

        // write cf will remain only one key
        let readopts = IterOptions::new(None, None, false);
        let mut write_iter = fake_engine
            .iterator_opt(CF_WRITE, readopts.clone())
            .unwrap();
        write_iter.seek_to_first().unwrap();
        let mut remaining_writes = vec![];
        while write_iter.valid().unwrap() {
            let write = WriteRef::parse(write_iter.value()).unwrap().to_owned();
            let key = write_iter.key().to_vec();
            write_iter.next().unwrap();
            remaining_writes.push((key, write));
        }

        // default cf will remain only one key
        let mut default_iter = fake_engine
            .iterator_opt(CF_DEFAULT, readopts.clone())
            .unwrap();
        default_iter.seek_to_first().unwrap();
        let mut remaining_defaults = vec![];
        while default_iter.valid().unwrap() {
            let key = default_iter.key().to_vec();
            let value = default_iter.value().to_vec();
            default_iter.next().unwrap();
            remaining_defaults.push((key, value));
        }

        // lock cf will be clean
        let mut lock_iter = fake_engine.iterator_opt(CF_LOCK, readopts).unwrap();
        lock_iter.seek_to_first().unwrap();
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
