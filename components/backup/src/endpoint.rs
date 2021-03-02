// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::f64::INFINITY;
use std::fmt;
use std::sync::atomic::*;
use std::sync::*;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{borrow::Cow, time::*};

use concurrency_manager::ConcurrencyManager;
use configuration::Configuration;
use engine_rocks::raw::DB;
use engine_traits::{name_to_cf, CfName, IterOptions, SstCompressionType, DATA_KEY_PREFIX_LEN};
use external_storage::*;
use file_system::{IOType, WithIOType};
use futures::channel::mpsc::*;
use kvproto::backup::*;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use raft::StateRole;
use raftstore::coprocessor::RegionInfoProvider;
use raftstore::store::util::find_peer;
use tikv::config::BackupConfig;
use tikv::storage::kv::{Engine, ScanMode, SnapContext, Snapshot};
use tikv::storage::mvcc::Error as MvccError;
use tikv::storage::txn::{
    EntryBatch, Error as TxnError, SnapshotStore, TxnEntryScanner, TxnEntryStore,
};
use tikv::storage::Statistics;
use tikv_util::impl_display_as_debug;
use tikv_util::time::Limiter;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use txn_types::{Key, Lock, TimeStamp};
use yatp::task::callback::{Handle, TaskCell};
use yatp::ThreadPool;

use crate::metrics::*;
use crate::writer::BackupWriterBuilder;
use crate::Error;
use crate::*;

const BACKUP_BATCH_LIMIT: usize = 1024;

// if thread pool has been idle for such long time, we will shutdown it.
const IDLE_THREADPOOL_DURATION: u64 = 30 * 60 * 1000; // 30 mins

#[derive(Clone)]
struct Request {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_ts: TimeStamp,
    end_ts: TimeStamp,
    limiter: Limiter,
    backend: StorageBackend,
    cancel: Arc<AtomicBool>,
    is_raw_kv: bool,
    cf: CfName,
    compression_type: CompressionType,
    compression_level: i32,
}

/// Backup Task.
pub struct Task {
    request: Request,
    pub(crate) resp: UnboundedSender<BackupResponse>,
}

impl_display_as_debug!(Task);

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask")
            .field("start_ts", &self.request.start_ts)
            .field("end_ts", &self.request.end_ts)
            .field(
                "start_key",
                &log_wrappers::Value::key(&self.request.start_key),
            )
            .field("end_key", &log_wrappers::Value::key(&self.request.end_key))
            .field("is_raw_kv", &self.request.is_raw_kv)
            .field("cf", &self.request.cf)
            .finish()
    }
}

#[derive(Clone)]
struct LimitedStorage {
    limiter: Limiter,
    storage: Arc<dyn ExternalStorage>,
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let cancel = Arc::new(AtomicBool::new(false));

        let speed_limit = req.get_rate_limit();
        let limiter = Limiter::new(if speed_limit > 0 {
            speed_limit as f64
        } else {
            INFINITY
        });
        let cf = name_to_cf(req.get_cf()).ok_or_else(|| crate::Error::InvalidCf {
            cf: req.get_cf().to_owned(),
        })?;

        let task = Task {
            request: Request {
                start_key: req.get_start_key().to_owned(),
                end_key: req.get_end_key().to_owned(),
                start_ts: req.get_start_version().into(),
                end_ts: req.get_end_version().into(),
                backend: req.get_storage_backend().clone(),
                limiter,
                cancel: cancel.clone(),
                is_raw_kv: req.get_is_raw_kv(),
                cf,
                compression_type: req.get_compression_type(),
                compression_level: req.get_compression_level(),
            },
            resp,
        };
        Ok((task, cancel))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.request.cancel.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct BackupRange {
    start_key: Option<Key>,
    end_key: Option<Key>,
    region: Region,
    leader: Peer,
    is_raw_kv: bool,
    cf: CfName,
}

impl BackupRange {
    /// Get entries from the scanner and save them to storage
    fn backup<E: Engine>(
        &self,
        writer_builder: BackupWriterBuilder,
        engine: &E,
        concurrency_manager: ConcurrencyManager,
        backup_ts: TimeStamp,
        begin_ts: TimeStamp,
        storage: &LimitedStorage,
    ) -> Result<(Vec<File>, Statistics)> {
        assert!(!self.is_raw_kv);

        let mut ctx = Context::default();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.leader.clone());

        // Update max_ts and check the in-memory lock table before getting the snapshot
        concurrency_manager.update_max_ts(backup_ts);
        concurrency_manager
            .read_range_check(
                self.start_key.as_ref(),
                self.end_key.as_ref(),
                |key, lock| {
                    Lock::check_ts_conflict(
                        Cow::Borrowed(lock),
                        &key,
                        backup_ts,
                        &Default::default(),
                    )
                },
            )
            .map_err(MvccError::from)
            .map_err(TxnError::from)?;

        // Currently backup always happens on the leader, so we don't need
        // to set key ranges and start ts to check.
        assert!(!ctx.get_replica_read());
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };

        let start_snapshot = Instant::now();
        let snapshot = match engine.snapshot(snap_ctx) {
            Ok(s) => s,
            Err(e) => {
                error!(?e; "backup snapshot failed");
                return Err(e.into());
            }
        };
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["snapshot"])
            .observe(start_snapshot.elapsed().as_secs_f64());
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false, /* fill_cache */
            Default::default(),
            false,
        );
        let start_key = self.start_key.clone();
        let end_key = self.end_key.clone();
        // Incremental backup needs to output delete records.
        let incremental = !begin_ts.is_zero();
        let mut scanner = snap_store
            .entry_scanner(start_key, end_key, begin_ts, incremental)
            .unwrap();

        let start_scan = Instant::now();
        let mut files: Vec<File> = Vec::with_capacity(2);
        let mut batch = EntryBatch::with_capacity(BACKUP_BATCH_LIMIT);
        let mut last_key = self
            .start_key
            .clone()
            .map_or_else(Vec::new, |k| k.into_raw().unwrap());
        let mut cur_key = self
            .end_key
            .clone()
            .map_or_else(Vec::new, |k| k.into_raw().unwrap());
        let mut writer = writer_builder.build(last_key.clone())?;
        loop {
            if let Err(e) = scanner.scan_entries(&mut batch) {
                error!(?e; "backup scan entries failed");
                return Err(e.into());
            };
            if batch.is_empty() {
                break;
            }
            debug!("backup scan entries"; "len" => batch.len());

            let entries = batch.drain();
            if writer.need_split_keys() {
                let res = {
                    entries.as_slice().get(0).map_or_else(
                        || Err(Error::Other(box_err!("get entry error"))),
                        |x| match x.to_key() {
                            Ok(k) => {
                                cur_key = k.into_raw().unwrap();
                                writer_builder.build(cur_key.clone())
                            }
                            Err(e) => {
                                error!(?e; "backup save file failed");
                                Err(Error::Other(box_err!("Decode error: {:?}", e)))
                            }
                        },
                    )
                };
                match writer.save(&storage.storage) {
                    Ok(mut split_files) => {
                        for file in split_files.iter_mut() {
                            file.set_start_key(last_key.clone());
                            file.set_end_key(cur_key.clone());
                        }
                        last_key = cur_key.clone();
                        files.append(&mut split_files);
                    }
                    Err(e) => {
                        error_unknown!(?e; "backup save file failed");
                        return Err(e);
                    }
                }
                match res {
                    Ok(w) => {
                        writer = w;
                    }
                    Err(e) => {
                        error_unknown!(?e; "backup writer failed");
                        return Err(e);
                    }
                }
            }

            // Build sst files.
            if let Err(e) = writer.write(entries, true) {
                error_unknown!(?e; "backup build sst failed");
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["scan"])
            .observe(start_scan.elapsed().as_secs_f64());

        if writer.need_flush_keys() {
            match writer.save(&storage.storage) {
                Ok(mut split_files) => {
                    cur_key = self
                        .end_key
                        .clone()
                        .map_or_else(Vec::new, |k| k.into_raw().unwrap());
                    for file in split_files.iter_mut() {
                        file.set_start_key(last_key.clone());
                        file.set_end_key(cur_key.clone());
                    }
                    files.append(&mut split_files);
                }
                Err(e) => {
                    error_unknown!(?e; "backup save file failed");
                    return Err(e);
                }
            }
        }

        let stat = scanner.take_statistics();
        Ok((files, stat))
    }

    fn backup_raw<E: Engine>(
        &self,
        writer: &mut BackupRawKVWriter,
        engine: &E,
    ) -> Result<Statistics> {
        assert!(self.is_raw_kv);

        let mut ctx = Context::default();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.leader.clone());
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };
        let snapshot = match engine.snapshot(snap_ctx) {
            Ok(s) => s,
            Err(e) => {
                error!(?e; "backup raw kv snapshot failed");
                return Err(e.into());
            }
        };
        let start = Instant::now();
        let mut statistics = Statistics::default();
        let cfstatistics = statistics.mut_cf_statistics(self.cf);
        let mut option = IterOptions::default();
        if let Some(end) = self.end_key.clone() {
            option.set_upper_bound(end.as_encoded(), DATA_KEY_PREFIX_LEN);
        }
        let mut cursor = snapshot.iter_cf(self.cf, option, ScanMode::Forward)?;
        if let Some(begin) = self.start_key.clone() {
            if !cursor.seek(&begin, cfstatistics)? {
                return Ok(statistics);
            }
        } else if !cursor.seek_to_first(cfstatistics) {
            return Ok(statistics);
        }
        let mut batch = vec![];
        loop {
            while cursor.valid()? && batch.len() < BACKUP_BATCH_LIMIT {
                batch.push(Ok((
                    cursor.key(cfstatistics).to_owned(),
                    cursor.value(cfstatistics).to_owned(),
                )));
                cursor.next(cfstatistics);
            }
            if batch.is_empty() {
                break;
            }
            debug!("backup scan raw kv entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(..), false) {
                error_unknown!(?e; "backup raw kv build sst failed");
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["raw_scan"])
            .observe(start.elapsed().as_secs_f64());
        Ok(statistics)
    }

    fn backup_raw_kv_to_file<E: Engine>(
        &self,
        engine: &E,
        db: Arc<DB>,
        storage: &LimitedStorage,
        file_name: String,
        cf: CfName,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
    ) -> Result<(Vec<File>, Statistics)> {
        let mut writer = match BackupRawKVWriter::new(
            db,
            &file_name,
            cf,
            storage.limiter.clone(),
            compression_type,
            compression_level,
        ) {
            Ok(w) => w,
            Err(e) => {
                error_unknown!(?e; "backup writer failed");
                return Err(e);
            }
        };
        let stat = match self.backup_raw(&mut writer, engine) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };
        // Save sst files to storage.
        match writer.save(&storage.storage) {
            Ok(files) => Ok((files, stat)),
            Err(e) => {
                error_unknown!(?e; "backup save file failed");
                Err(e)
            }
        }
    }
}

#[derive(Clone)]
pub struct ConfigManager(Arc<RwLock<BackupConfig>>);

impl configuration::ConfigManager for ConfigManager {
    fn dispatch(&mut self, change: configuration::ConfigChange) -> configuration::Result<()> {
        self.0.write().unwrap().update(change);
        Ok(())
    }
}

#[cfg(test)]
impl ConfigManager {
    fn set_num_threads(&self, num_threads: usize) {
        self.0.write().unwrap().num_threads = num_threads;
    }
}

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    store_id: u64,
    pool: RefCell<ControlThreadPool>,
    pool_idle_threshold: u64,
    db: Arc<DB>,
    config_manager: ConfigManager,
    concurrency_manager: ConcurrencyManager,

    pub(crate) engine: E,
    pub(crate) region_info: R,
}

/// The progress of a backup task
pub struct Progress<R: RegionInfoProvider> {
    store_id: u64,
    next_start: Option<Key>,
    end_key: Option<Key>,
    region_info: R,
    finished: bool,
    is_raw_kv: bool,
    cf: CfName,
}

impl<R: RegionInfoProvider> Progress<R> {
    fn new(
        store_id: u64,
        next_start: Option<Key>,
        end_key: Option<Key>,
        region_info: R,
        is_raw_kv: bool,
        cf: CfName,
    ) -> Self {
        Progress {
            store_id,
            next_start,
            end_key,
            region_info,
            finished: false,
            is_raw_kv,
            cf,
        }
    }

    /// Forward the progress by `ranges` BackupRanges
    ///
    /// The size of the returned BackupRanges should <= `ranges`
    fn forward(&mut self, limit: usize) -> Vec<BackupRange> {
        if self.finished {
            return Vec::new();
        }
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key_ = self
            .next_start
            .clone()
            .map_or_else(Vec::new, |k| k.into_encoded());

        let start_key = self.next_start.clone();
        let end_key = self.end_key.clone();
        let raw_kv = self.is_raw_kv;
        let cf_name = self.cf;
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
                let mut count = 0;
                for info in iter {
                    let region = &info.region;
                    if end_key.is_some() {
                        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
                        if end_slice <= region.get_start_key() {
                            // We have reached the end.
                            // The range is defined as [start, end) so break if
                            // region start key is greater or equal to end key.
                            break;
                        }
                    }
                    if info.role == StateRole::Leader {
                        let ekey = get_min_end_key(end_key.as_ref(), &region);
                        let skey = get_max_start_key(start_key.as_ref(), &region);
                        assert!(!(skey == ekey && ekey.is_some()), "{:?} {:?}", skey, ekey);
                        let leader = find_peer(region, store_id).unwrap().to_owned();
                        let backup_range = BackupRange {
                            start_key: skey,
                            end_key: ekey,
                            region: region.clone(),
                            leader,
                            is_raw_kv: raw_kv,
                            cf: cf_name,
                        };
                        tx.send(backup_range).unwrap();
                        count += 1;
                        if count >= limit {
                            break;
                        }
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!(?e; "backup seek region failed");
        }

        let branges: Vec<_> = rx.iter().collect();
        if let Some(b) = branges.last() {
            // The region's end key is empty means it is the last
            // region, we need to set the `finished` flag here in case
            // we run with `next_start` set to None
            if b.region.get_end_key().is_empty() || b.end_key == self.end_key {
                self.finished = true;
            }
            self.next_start = b.end_key.clone();
        } else {
            self.finished = true;
        }
        branges
    }
}

struct ControlThreadPool {
    size: usize,
    workers: Option<Arc<ThreadPool<TaskCell>>>,
    last_active: Instant,
}

impl ControlThreadPool {
    fn new() -> Self {
        ControlThreadPool {
            size: 0,
            workers: None,
            last_active: Instant::now(),
        }
    }

    fn spawn<F>(&mut self, func: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let workers = self.workers.as_ref().unwrap();
        let w = workers.clone();
        workers.spawn(move |_: &mut Handle<'_>| {
            func();
            // Debug service requires jobs in the old thread pool continue to run even after
            // the pool is recreated. So the pool needs to be ref counted and dropped after
            // task has finished.
            drop(w);
        });
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to expend or there
    /// are too many idle threads. Otherwise do nothing.
    fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        let workers = Arc::new(
            yatp::Builder::new(thd_name!("bkwkr"))
                .max_thread_count(new_size)
                .build_callback_pool(),
        );
        let _ = self.workers.replace(workers);
        self.size = new_size;
        BACKUP_THREAD_POOL_SIZE_GAUGE.set(new_size as i64);
    }

    fn heartbeat(&mut self) {
        self.last_active = Instant::now();
    }

    /// Shutdown the thread pool if it has been idle for a long time.
    fn check_active(&mut self, idle_threshold: Duration) {
        if self.last_active.elapsed() >= idle_threshold {
            self.size = 0;
            if let Some(w) = self.workers.take() {
                let start = Instant::now();
                drop(w);
                slow_log!(start.elapsed(), "backup thread pool shutdown too long");
            }
        }
    }
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(
        store_id: u64,
        engine: E,
        region_info: R,
        db: Arc<DB>,
        config: BackupConfig,
        concurrency_manager: ConcurrencyManager,
    ) -> Endpoint<E, R> {
        Endpoint {
            store_id,
            engine,
            region_info,
            pool: RefCell::new(ControlThreadPool::new()),
            pool_idle_threshold: IDLE_THREADPOOL_DURATION,
            db,
            config_manager: ConfigManager(Arc::new(RwLock::new(config))),
            concurrency_manager,
        }
    }

    pub fn get_config_manager(&self) -> ConfigManager {
        self.config_manager.clone()
    }

    fn spawn_backup_worker(
        &self,
        prs: Arc<Mutex<Progress<R>>>,
        request: Request,
        tx: UnboundedSender<BackupResponse>,
    ) {
        let start_ts = request.start_ts;
        let end_ts = request.end_ts;
        let backup_ts = request.end_ts;
        let engine = self.engine.clone();
        let db = self.db.clone();
        let store_id = self.store_id;
        let concurrency_manager = self.concurrency_manager.clone();
        let batch_size = self.config_manager.0.read().unwrap().batch_size;
        let sst_max_size = self.config_manager.0.read().unwrap().sst_max_size.0;

        // TODO: make it async.
        self.pool.borrow_mut().spawn(move || {
            tikv_alloc::add_thread_memory_accessor();
            let _with_io_type = WithIOType::new(IOType::Export);
            defer!({
                tikv_alloc::remove_thread_memory_accessor();
            });

            // Check if we can open external storage.
            let backend = match create_storage(&request.backend) {
                Ok(backend) => backend,
                Err(err) => {
                    error_unknown!(?err; "backup create storage failed");
                    let mut response = BackupResponse::default();
                    response.set_error(crate::Error::Io(err).into());
                    if let Err(err) = tx.unbounded_send(response) {
                        error_unknown!(?err; "backup failed to send response");
                    }
                    return;
                }
            };

            let storage = LimitedStorage {
                limiter: request.limiter,
                storage: backend,
            };

            loop {
                let (batch, is_raw_kv, cf) = {
                    // Release lock as soon as possible.
                    // It is critical to speed up backup, otherwise workers are
                    // blocked by each other.
                    let mut progress = prs.lock().unwrap();
                    let batch = progress.forward(batch_size);
                    if batch.is_empty() {
                        return;
                    }
                    (batch, progress.is_raw_kv, progress.cf)
                };

                for brange in batch {
                    if request.cancel.load(Ordering::SeqCst) {
                        warn!("backup task has canceled"; "range" => ?brange);
                        return;
                    }
                    // TODO: make file_name unique and short
                    let key = brange.start_key.clone().and_then(|k| {
                        // use start_key sha256 instead of start_key to avoid file name too long os error
                        let input = if is_raw_kv {
                            k.into_encoded()
                        } else {
                            k.into_raw().unwrap()
                        };
                        file_system::sha256(&input).ok().map(hex::encode)
                    });
                    let name = backup_file_name(store_id, &brange.region, key);
                    let ct = to_sst_compression_type(request.compression_type);

                    let (res, start_key, end_key) = if is_raw_kv {
                        (
                            brange.backup_raw_kv_to_file(
                                &engine,
                                db.clone(),
                                &storage,
                                name,
                                cf,
                                ct,
                                request.compression_level,
                            ),
                            brange.start_key.map_or_else(Vec::new, |k| k.into_encoded()),
                            brange.end_key.map_or_else(Vec::new, |k| k.into_encoded()),
                        )
                    } else {
                        let writer_builder = BackupWriterBuilder::new(
                            store_id,
                            storage.limiter.clone(),
                            brange.region.clone(),
                            db.clone(),
                            ct,
                            request.compression_level,
                            sst_max_size,
                        );
                        (
                            brange.backup(
                                writer_builder,
                                &engine,
                                concurrency_manager.clone(),
                                backup_ts,
                                start_ts,
                                &storage,
                            ),
                            brange
                                .start_key
                                .map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                            brange
                                .end_key
                                .map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        )
                    };

                    let mut response = BackupResponse::default();
                    match res {
                        Err(e) => {
                            error_unknown!(?e; "backup region failed";
                                "region" => ?brange.region,
                                "start_key" => &log_wrappers::Value::key(&start_key),
                                "end_key" => &log_wrappers::Value::key(&end_key),
                            );
                            response.set_error(e.into());
                        }
                        Ok((mut files, stat)) => {
                            debug!("backup region finish";
                            "region" => ?brange.region,
                            "start_key" => &log_wrappers::Value::key(&start_key),
                            "end_key" => &log_wrappers::Value::key(&end_key),
                            "details" => ?stat);

                            for file in files.iter_mut() {
                                if is_raw_kv {
                                    file.set_start_key(start_key.clone());
                                    file.set_end_key(end_key.clone());
                                }
                                file.set_start_version(start_ts.into_inner());
                                file.set_end_version(end_ts.into_inner());
                            }
                            response.set_files(files.into());
                        }
                    }
                    response.set_start_key(start_key);
                    response.set_end_key(end_key);

                    if let Err(e) = tx.unbounded_send(response) {
                        error_unknown!(?e; "backup failed to send response");
                        return;
                    }
                }
            }
        });
    }

    pub fn handle_backup_task(&self, task: Task) {
        let Task { request, resp } = task;
        let is_raw_kv = request.is_raw_kv;
        let start_key = if request.start_key.is_empty() {
            None
        } else {
            // TODO: if is_raw_kv is written everywhere. It need to be simplified.
            if is_raw_kv {
                Some(Key::from_encoded(request.start_key.clone()))
            } else {
                Some(Key::from_raw(&request.start_key))
            }
        };
        let end_key = if request.end_key.is_empty() {
            None
        } else if is_raw_kv {
            Some(Key::from_encoded(request.end_key.clone()))
        } else {
            Some(Key::from_raw(&request.end_key))
        };

        let prs = Arc::new(Mutex::new(Progress::new(
            self.store_id,
            start_key,
            end_key,
            self.region_info.clone(),
            is_raw_kv,
            request.cf,
        )));
        let concurrency = self.config_manager.0.read().unwrap().num_threads;
        self.pool.borrow_mut().adjust_with(concurrency);
        for _ in 0..concurrency {
            self.spawn_backup_worker(prs.clone(), request.clone(), resp.clone());
        }
    }
}

impl<E: Engine, R: RegionInfoProvider> Runnable for Endpoint<E, R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        if task.has_canceled() {
            warn!("backup task has canceled"; "task" => %task);
            return;
        }
        info!("run backup task"; "task" => %task);
        self.handle_backup_task(task);
        self.pool.borrow_mut().heartbeat();
    }
}

impl<E: Engine, R: RegionInfoProvider> RunnableWithTimer for Endpoint<E, R> {
    fn on_timeout(&mut self) {
        let pool_idle_duration = Duration::from_millis(self.pool_idle_threshold);
        self.pool.borrow_mut().check_active(pool_idle_duration);
    }

    fn get_interval(&self) -> Duration {
        Duration::from_millis(self.pool_idle_threshold)
    }
}

/// Get the min end key from the given `end_key` and `Region`'s end key.
fn get_min_end_key(end_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_end = if region.get_end_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_end_key()))
    };
    if region.get_end_key().is_empty() {
        end_key.cloned()
    } else if end_key.is_none() {
        region_end
    } else {
        let end_slice = end_key.as_ref().unwrap().as_encoded().as_slice();
        if end_slice < region.get_end_key() {
            end_key.cloned()
        } else {
            region_end
        }
    }
}

/// Get the max start key from the given `start_key` and `Region`'s start key.
fn get_max_start_key(start_key: Option<&Key>, region: &Region) -> Option<Key> {
    let region_start = if region.get_start_key().is_empty() {
        None
    } else {
        Some(Key::from_encoded_slice(region.get_start_key()))
    };
    if start_key.is_none() {
        region_start
    } else {
        let start_slice = start_key.as_ref().unwrap().as_encoded().as_slice();
        if start_slice < region.get_start_key() {
            region_start
        } else {
            start_key.cloned()
        }
    }
}

/// Construct an backup file name based on the given store id, region, range start key and local unix timestamp.
/// A name consists with five parts: store id, region_id, a epoch version, the hash of range start key and timestamp.
/// range start key is used to keep the unique file name for file, to handle different tables exists on the same region.
/// local unix timestamp is used to keep the unique file name for file, to handle receive the same request after connection reset.
pub fn backup_file_name(store_id: u64, region: &Region, key: Option<String>) -> String {
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    match key {
        Some(k) => format!(
            "{}_{}_{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version(),
            k,
            since_the_epoch.as_millis()
        ),
        None => format!(
            "{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version()
        ),
    }
}

// convert BackupCompresionType to rocks db DBCompressionType
fn to_sst_compression_type(ct: CompressionType) -> Option<SstCompressionType> {
    match ct {
        CompressionType::Lz4 => Some(SstCompressionType::Lz4),
        CompressionType::Snappy => Some(SstCompressionType::Snappy),
        CompressionType::Zstd => Some(SstCompressionType::Zstd),
        CompressionType::Unknown => None,
    }
}

#[cfg(test)]
pub mod tests {
    use std::path::{Path, PathBuf};
    use std::{fs, thread};

    use engine_traits::MiscExt;
    use external_storage::{make_local_backend, make_noop_backend};
    use file_system::{IOOp, WithIORateLimit};
    use futures::executor::block_on;
    use futures::stream::StreamExt;
    use kvproto::metapb;
    use raftstore::coprocessor::RegionCollector;
    use raftstore::coprocessor::Result as CopResult;
    use raftstore::coprocessor::SeekRegionCallback;
    use raftstore::store::util::new_peer;
    use rand::Rng;
    use tempfile::TempDir;
    use tikv::storage::txn::tests::{must_commit, must_prewrite_put};
    use tikv::storage::{RocksEngine, TestEngineBuilder};
    use tikv_util::config::ReadableSize;
    use tikv_util::worker::Worker;
    use txn_types::SHORT_VALUE_MAX_LEN;

    use super::*;

    #[derive(Clone)]
    pub struct MockRegionInfoProvider {
        regions: Arc<Mutex<RegionCollector>>,
        cancel: Option<Arc<AtomicBool>>,
    }

    impl MockRegionInfoProvider {
        pub fn new() -> Self {
            MockRegionInfoProvider {
                regions: Arc::new(Mutex::new(RegionCollector::new())),
                cancel: None,
            }
        }
        pub fn set_regions(&self, regions: Vec<(Vec<u8>, Vec<u8>, u64)>) {
            let mut map = self.regions.lock().unwrap();
            for (mut start_key, mut end_key, id) in regions {
                if !start_key.is_empty() {
                    start_key = Key::from_raw(&start_key).into_encoded();
                }
                if !end_key.is_empty() {
                    end_key = Key::from_raw(&end_key).into_encoded();
                }
                let mut r = metapb::Region::default();
                r.set_id(id);
                r.set_start_key(start_key.clone());
                r.set_end_key(end_key);
                r.mut_peers().push(new_peer(1, 1));
                map.create_region(r, StateRole::Leader);
            }
        }
        fn canecl_on_seek(&mut self, cancel: Arc<AtomicBool>) {
            self.cancel = Some(cancel);
        }
    }

    impl RegionInfoProvider for MockRegionInfoProvider {
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> CopResult<()> {
            let from = from.to_vec();
            let regions = self.regions.lock().unwrap();
            if let Some(c) = self.cancel.as_ref() {
                c.store(true, Ordering::SeqCst);
            }
            regions.handle_seek_region(from, callback);
            Ok(())
        }
    }

    pub fn new_endpoint() -> (TempDir, Endpoint<RocksEngine, MockRegionInfoProvider>) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let concurrency_manager = ConcurrencyManager::new(1.into());
        let db = rocks.get_rocksdb().get_sync_db();
        (
            temp,
            Endpoint::new(
                1,
                rocks,
                MockRegionInfoProvider::new(),
                db,
                BackupConfig {
                    num_threads: 4,
                    batch_size: 8,
                    sst_max_size: ReadableSize::mb(144),
                },
                concurrency_manager,
            ),
        )
    }

    pub fn check_response<F>(rx: UnboundedReceiver<BackupResponse>, check: F)
    where
        F: FnOnce(Option<BackupResponse>),
    {
        let rx = rx.fuse();
        let (resp, rx) = block_on(rx.into_future());
        check(resp);
        let (none, _rx) = block_on(rx.into_future());
        assert!(none.is_none(), "{:?}", none);
    }

    fn make_unique_dir(path: &Path) -> PathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{:016x}", uid);
        let unique = path.join(tmp_suffix);
        fs::create_dir_all(&unique).unwrap();
        unique
    }

    #[test]
    fn test_control_thread_pool_adjust_keep_tasks() {
        use std::thread::sleep;

        let counter = Arc::new(AtomicU32::new(0));
        let mut pool = ControlThreadPool::new();
        pool.adjust_with(3);

        for i in 0..8 {
            let ctr = counter.clone();
            pool.spawn(move || {
                sleep(Duration::from_millis(100));
                ctr.fetch_or(1 << i, Ordering::SeqCst);
            });
        }

        sleep(Duration::from_millis(150));
        pool.adjust_with(4);

        for i in 8..16 {
            let ctr = counter.clone();
            pool.spawn(move || {
                sleep(Duration::from_millis(100));
                ctr.fetch_or(1 << i, Ordering::SeqCst);
            });
        }

        sleep(Duration::from_millis(250));
        assert_eq!(counter.load(Ordering::SeqCst), 0xffff);
    }

    #[test]
    fn test_seek_range() {
        let (_tmp, endpoint) = new_endpoint();

        endpoint.region_info.set_regions(vec![
            (b"".to_vec(), b"1".to_vec(), 1),
            (b"1".to_vec(), b"2".to_vec(), 2),
            (b"3".to_vec(), b"4".to_vec(), 3),
            (b"7".to_vec(), b"9".to_vec(), 4),
            (b"9".to_vec(), b"".to_vec(), 5),
        ]);
        // Test seek backup range.
        let test_seek_backup_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let start_key = if start_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(start_key))
                };
                let end_key = if end_key.is_empty() {
                    None
                } else {
                    Some(Key::from_raw(end_key))
                };
                let mut prs = Progress::new(
                    endpoint.store_id,
                    start_key,
                    end_key,
                    endpoint.region_info.clone(),
                    false,
                    engine_traits::CF_DEFAULT,
                );

                let mut ranges = Vec::with_capacity(expect.len());
                while ranges.len() != expect.len() {
                    let n = (rand::random::<usize>() % 3) + 1;
                    let mut r = prs.forward(n);
                    // The returned backup ranges should <= n
                    assert!(r.len() <= n);

                    if r.is_empty() {
                        // if return a empty vec then the progress is finished
                        assert_eq!(
                            ranges.len(),
                            expect.len(),
                            "got {:?}, expect {:?}",
                            ranges,
                            expect
                        );
                    }
                    ranges.append(&mut r);
                }

                for (a, b) in ranges.into_iter().zip(expect) {
                    assert_eq!(
                        a.start_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.0
                    );
                    assert_eq!(
                        a.end_key.map_or_else(Vec::new, |k| k.into_raw().unwrap()),
                        b.1
                    );
                }
            };

        // Test whether responses contain correct range.
        #[allow(clippy::blocks_in_if_conditions)]
        let test_handle_backup_task_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let tmp = TempDir::new().unwrap();
                let backend = external_storage::make_local_backend(tmp.path());
                let (tx, rx) = unbounded();
                let task = Task {
                    request: Request {
                        start_key: start_key.to_vec(),
                        end_key: end_key.to_vec(),
                        start_ts: 1.into(),
                        end_ts: 1.into(),
                        backend,
                        limiter: Limiter::new(INFINITY),
                        cancel: Arc::default(),
                        is_raw_kv: false,
                        cf: engine_traits::CF_DEFAULT,
                        compression_type: CompressionType::Unknown,
                        compression_level: 0,
                    },
                    resp: tx,
                };
                endpoint.handle_backup_task(task);
                let resps: Vec<_> = block_on(rx.collect());
                for a in &resps {
                    assert!(
                        expect
                            .iter()
                            .any(|b| { a.get_start_key() == b.0 && a.get_end_key() == b.1 }),
                        "{:?} {:?}",
                        resps,
                        expect
                    );
                }
                assert_eq!(resps.len(), expect.len());
            };

        // Backup range from case.0 to case.1,
        // the case.2 is the expected results.
        type Case<'a> = (&'a [u8], &'a [u8], Vec<(&'a [u8], &'a [u8])>);

        let case: Vec<Case> = vec![
            (b"", b"1", vec![(b"", b"1")]),
            (b"", b"2", vec![(b"", b"1"), (b"1", b"2")]),
            (b"1", b"2", vec![(b"1", b"2")]),
            (b"1", b"3", vec![(b"1", b"2")]),
            (b"1", b"4", vec![(b"1", b"2"), (b"3", b"4")]),
            (b"4", b"6", vec![]),
            (b"4", b"5", vec![]),
            (b"2", b"7", vec![(b"3", b"4")]),
            (b"7", b"8", vec![(b"7", b"8")]),
            (b"3", b"", vec![(b"3", b"4"), (b"7", b"9"), (b"9", b"")]),
            (b"5", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"7", b"", vec![(b"7", b"9"), (b"9", b"")]),
            (b"8", b"91", vec![(b"8", b"9"), (b"9", b"91")]),
            (b"8", b"", vec![(b"8", b"9"), (b"9", b"")]),
            (
                b"",
                b"",
                vec![
                    (b"", b"1"),
                    (b"1", b"2"),
                    (b"3", b"4"),
                    (b"7", b"9"),
                    (b"9", b""),
                ],
            ),
        ];
        for (start_key, end_key, ranges) in case {
            test_seek_backup_range(start_key, end_key, ranges.clone());
            test_handle_backup_task_range(start_key, end_key, ranges);
        }
    }

    #[test]
    fn test_handle_backup_task() {
        let (_guard, stats) = WithIORateLimit::new(0);
        let (tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts = TimeStamp::new(1);
        let mut alloc_ts = || *ts.incr();
        let mut backup_tss = vec![];
        // Multi-versions for key 0..9.
        for len in &[SHORT_VALUE_MAX_LEN - 1, SHORT_VALUE_MAX_LEN * 2] {
            for i in 0..10u8 {
                let start = alloc_ts();
                let commit = alloc_ts();
                let key = format!("{}", i);
                must_prewrite_put(
                    &engine,
                    key.as_bytes(),
                    &vec![i; *len],
                    key.as_bytes(),
                    start,
                );
                must_commit(&engine, key.as_bytes(), start, commit);
                backup_tss.push((alloc_ts(), len));
            }
        }
        // flush to disk so that read requests can be traced by TiKV limiter.
        engine
            .get_rocksdb()
            .flush_cf(engine_traits::CF_DEFAULT, true /*sync*/)
            .unwrap();
        engine
            .get_rocksdb()
            .flush_cf(engine_traits::CF_WRITE, true /*sync*/)
            .unwrap();

        // TODO: check key number for each snapshot.
        let limiter = Limiter::new(10.0 * 1024.0 * 1024.0 /* 10 MB/s */);
        for (ts, len) in backup_tss {
            stats.reset();
            let mut req = BackupRequest::default();
            req.set_start_key(vec![]);
            req.set_end_key(vec![b'5']);
            req.set_start_version(0);
            req.set_end_version(ts.into_inner());
            let (tx, rx) = unbounded();

            let tmp1 = make_unique_dir(tmp.path());
            req.set_storage_backend(make_local_backend(&tmp1));
            if len % 2 == 0 {
                req.set_rate_limit(10 * 1024 * 1024);
            }
            let (mut task, _) = Task::new(req, tx).unwrap();
            if len % 2 == 0 {
                // Make sure the rate limiter is set.
                assert!(task.request.limiter.speed_limit().is_finite());
                // Share the same rate limiter.
                task.request.limiter = limiter.clone();
            }
            endpoint.handle_backup_task(task);
            let (resp, rx) = block_on(rx.into_future());
            let resp = resp.unwrap();
            assert!(!resp.has_error(), "{:?}", resp);
            let file_len = if *len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
            let files = resp.get_files();
            info!("{:?}", files);
            assert_eq!(
                files.len(),
                file_len, /* default and write */
                "{:?}",
                resp
            );
            let (none, _rx) = block_on(rx.into_future());
            assert!(none.is_none(), "{:?}", none);
            assert_eq!(stats.fetch(IOType::Export, IOOp::Write), 0);
            assert_ne!(stats.fetch(IOType::Export, IOOp::Read), 0);
        }
    }

    #[test]
    fn test_scan_error() {
        let (tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        req.set_concurrency(4);
        let tmp1 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp1));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_kv_error(), "{:?}", resp);
            assert!(resp.get_error().get_kv_error().has_locked(), "{:?}", resp);
            assert_eq!(resp.get_files().len(), 0, "{:?}", resp);
        });

        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), start, commit);

        // Test whether it can correctly convert not leader to region error.
        engine.trigger_not_leader();
        let now = alloc_ts();
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        let tmp2 = make_unique_dir(tmp.path());
        req.set_storage_backend(make_local_backend(&tmp2));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_not_leader(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_cancel() {
        let (temp, mut endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut ts: TimeStamp = 1.into();
        let mut alloc_ts = || *ts.incr();
        let start = alloc_ts();
        let key = format!("{}", start);
        must_prewrite_put(
            &engine,
            key.as_bytes(),
            key.as_bytes(),
            key.as_bytes(),
            start,
        );
        // Commit the perwrite.
        let commit = alloc_ts();
        must_commit(&engine, key.as_bytes(), start, commit);

        let now = alloc_ts();
        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(now.into_inner());
        req.set_end_version(now.into_inner());
        req.set_concurrency(4);
        req.set_storage_backend(make_local_backend(temp.path()));

        // Cancel the task before starting the task.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
        // Cancel the task.
        cancel.store(true, Ordering::SeqCst);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });

        // Cancel the task during backup.
        let (tx, rx) = unbounded();
        let (task, cancel) = Task::new(req, tx).unwrap();
        endpoint.region_info.canecl_on_seek(cancel);
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            assert!(resp.is_none());
        });
    }

    #[test]
    fn test_busy() {
        let (_tmp, endpoint) = new_endpoint();
        let engine = endpoint.engine.clone();

        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"5".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_concurrency(4);
        req.set_storage_backend(make_noop_backend());

        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();
        // Pause the engine 6 seconds to trigger Timeout error.
        // The Timeout error is translated to server is busy.
        engine.pause(Duration::from_secs(6));
        endpoint.handle_backup_task(task);
        check_response(rx, |resp| {
            let resp = resp.unwrap();
            assert!(resp.get_error().has_region_error(), "{:?}", resp);
            assert!(
                resp.get_error().get_region_error().has_server_is_busy(),
                "{:?}",
                resp
            );
        });
    }

    #[test]
    fn test_adjust_thread_pool_size() {
        let (_tmp, endpoint) = new_endpoint();
        endpoint
            .region_info
            .set_regions(vec![(b"".to_vec(), b"".to_vec(), 1)]);

        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_storage_backend(make_noop_backend());

        let (tx, _) = unbounded();

        // expand thread pool is needed
        endpoint.get_config_manager().set_num_threads(15);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        // shrink thread pool only if there are too many idle threads
        endpoint.get_config_manager().set_num_threads(10);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        endpoint.get_config_manager().set_num_threads(3);
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 3);
    }

    pub struct EndpointWrapper<E: Engine, R: RegionInfoProvider> {
        inner: Arc<Mutex<Endpoint<E, R>>>,
    }
    impl<E: Engine, R: RegionInfoProvider> Runnable for EndpointWrapper<E, R> {
        type Task = Task;

        fn run(&mut self, task: Task) {
            self.inner.lock().unwrap().run(task);
        }
    }

    impl<E: Engine, R: RegionInfoProvider> RunnableWithTimer for EndpointWrapper<E, R> {
        fn on_timeout(&mut self) {
            self.inner.lock().unwrap().on_timeout();
        }

        fn get_interval(&self) -> Duration {
            self.inner.lock().unwrap().get_interval()
        }
    }

    #[test]
    fn test_thread_pool_shutdown_when_idle() {
        let (_, mut endpoint) = new_endpoint();

        // set the idle threshold to 100ms
        endpoint.pool_idle_threshold = 100;
        let endpoint = Arc::new(Mutex::new(endpoint));
        let worker = Worker::new("endpoint");
        let scheduler = {
            let inner = endpoint.clone();
            worker.start_with_timer("endpoint", EndpointWrapper { inner })
        };

        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_storage_backend(make_noop_backend());

        endpoint
            .lock()
            .unwrap()
            .get_config_manager()
            .set_num_threads(10);

        let (tx, resp_rx) = unbounded();
        let (task, _) = Task::new(req, tx).unwrap();

        // if not task arrive after create the thread pool is empty
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);

        scheduler.schedule(task).unwrap();
        // wait until the task finish
        let _ = block_on(resp_rx.into_future());
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool not yet shutdown
        thread::sleep(Duration::from_millis(50));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool shutdown if not task arrive more than 100ms
        thread::sleep(Duration::from_millis(160));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);
    }
    // TODO: region err in txn(engine(request))
}
