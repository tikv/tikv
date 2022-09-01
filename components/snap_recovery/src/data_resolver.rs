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

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

/// `DataResolverManager` is the manager that manages the resolve kv data
/// process.
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
            resolved_ts: self.resolved_ts.clone(),
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
        self.wait();
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

                worker.resolve_write(&mut wb);

                tikv_alloc::remove_thread_memory_accessor();
            })
            .expect("failed to spawn resolve_kv_data thread");

        self.workers.lock().unwrap().push(handle);
    }

    // join and wait until the thread exit
    fn wait(&self) {
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
            wb.delete_cf(CF_LOCK, self.lock_iter.key());
            self.lock_iter.next().unwrap();
            key_count += 1;
        }
        info!("clean up lock cf. delete key count {}", key_count);
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        if let Err(e) = wb.write_opt(&write_opts) {
            panic!(
                "snapshot recovery, fail to write to disk while delete lock, the error is {:?}",
                e,
            );
        }
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
    pub fn resolve_write(&mut self, wb: &mut RocksWriteBatchVec) {
        let now = Instant::now();
        while self
            .batch_resolve_write(wb)
            .expect("resolve kv data failed when deleting in an invalid cf")
        {}
        info!("resolve write takes {}", now.elapsed().as_secs());
    }

    fn next_batch(&mut self) -> Result<Option<(Vec<u8>, Write)>> {
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
            if let Some((key, write)) = self.next_batch()? {
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
        if has_more && 0 == writes.len() {
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

            if write.start_ts > max_ts {
                max_ts = write.start_ts;
            }
        }
        info!(
            "flush delete in write/default cf. delete key count {}",
            batch.len()
        );
        let mut write_opts = WriteOptions::new();
        write_opts.set_sync(true);
        if let Err(e) = wb.write_opt(&write_opts) {
            panic!(
                "snapshot recovery, fail to write to disk while delete data, the error is {:?}",
                e,
            );
        }

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
mod tests {}
