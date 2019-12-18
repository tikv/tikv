// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::cmp;
use std::fmt;
use std::sync::atomic::*;
use std::sync::*;
use std::time::*;

use engine::DB;
use engine_rocks::RocksIOLimiter;
use engine_traits::IOLimiter;
use external_storage::*;
use futures::lazy;
use futures::prelude::Future;
use futures::sync::mpsc::*;
use kvproto::backup::*;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use raft::StateRole;
use tikv::raftstore::store::util::find_peer;
use tikv::storage::kv::{Engine, RegionInfoProvider};
use tikv::storage::txn::{EntryBatch, SnapshotStore, TxnEntryScanner, TxnEntryStore};
use tikv::storage::Statistics;
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};
use txn_types::{Key, TimeStamp};

use crate::metrics::*;
use crate::*;

const WORKER_TAKE_RANGE: usize = 6;

// if thread pool has been idle for such long time, we will shutdown it.
const IDLE_THREADPOOL_DURATION: u64 = 30 * 60 * 1000; // 30 mins

/// Backup task.
pub struct Task {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_ts: TimeStamp,
    end_ts: TimeStamp,
    storage: LimitedStorage,
    pub(crate) resp: UnboundedSender<BackupResponse>,
    concurrency: u32,
    cancel: Arc<AtomicBool>,
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackupTask")
            .field("start_ts", &self.start_ts)
            .field("end_ts", &self.end_ts)
            .field("start_key", &hex::encode_upper(&self.start_key))
            .field("end_key", &hex::encode_upper(&self.end_key))
            .finish()
    }
}

#[derive(Clone)]
struct LimitedStorage {
    limiter: Option<Arc<RocksIOLimiter>>,
    storage: Arc<dyn ExternalStorage>,
}

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let cancel = Arc::new(AtomicBool::new(false));

        let limiter = if req.get_rate_limit() != 0 {
            Some(Arc::new(RocksIOLimiter::new(req.get_rate_limit() as _)))
        } else {
            None
        };
        let storage = LimitedStorage {
            storage: create_storage(req.get_storage_backend())?,
            limiter,
        };

        Ok((
            Task {
                start_key: req.get_start_key().to_owned(),
                end_key: req.get_end_key().to_owned(),
                start_ts: req.get_start_version().into(),
                end_ts: req.get_end_version().into(),
                resp,
                storage,
                concurrency: req.get_concurrency(),
                cancel: cancel.clone(),
            },
            cancel,
        ))
    }

    /// Check whether the task is canceled.
    pub fn has_canceled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
pub struct BackupRange {
    start_key: Option<Key>,
    end_key: Option<Key>,
    region: Region,
    leader: Peer,
}

impl BackupRange {
    /// Get entries from the scanner and save them to storage
    fn backup<E: Engine>(
        &self,
        writer: &mut BackupWriter,
        engine: &E,
        backup_ts: TimeStamp,
        begin_ts: TimeStamp,
    ) -> Result<Statistics> {
        let mut ctx = Context::default();
        ctx.set_region_id(self.region.get_id());
        ctx.set_region_epoch(self.region.get_region_epoch().to_owned());
        ctx.set_peer(self.leader.clone());
        let snapshot = match engine.snapshot(&ctx) {
            Ok(s) => s,
            Err(e) => {
                error!("backup snapshot failed"; "error" => ?e);
                return Err(e.into());
            }
        };
        let snap_store = SnapshotStore::new(
            snapshot,
            backup_ts,
            IsolationLevel::Si,
            false, /* fill_cache */
            Default::default(),
        );
        let start_key = self.start_key.clone();
        let end_key = self.end_key.clone();
        // Incremental backup needs to output delete records.
        let incremental = !begin_ts.is_zero();
        let mut scanner = snap_store
            .entry_scanner(start_key, end_key, begin_ts, incremental)
            .unwrap();

        let start = Instant::now();
        let mut batch = EntryBatch::with_capacity(1024);
        loop {
            if let Err(e) = scanner.scan_entries(&mut batch) {
                error!("backup scan entries failed"; "error" => ?e);
                return Err(e.into());
            };
            if batch.is_empty() {
                break;
            }
            debug!("backup scan entries"; "len" => batch.len());
            // Build sst files.
            if let Err(e) = writer.write(batch.drain(), true) {
                error!("backup build sst failed"; "error" => ?e);
                return Err(e);
            }
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["scan"])
            .observe(start.elapsed().as_secs_f64());
        let stat = scanner.take_statistics();
        Ok(stat)
    }
}

type BackupRes = (Vec<File>, Statistics);

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    store_id: u64,
    pool: RefCell<ControlThreadPool>,
    pool_idle_threshold: u64,
    db: Arc<DB>,

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
}

impl<R: RegionInfoProvider> Progress<R> {
    fn new(store_id: u64, next_start: Option<Key>, end_key: Option<Key>, region_info: R) -> Self {
        Progress {
            store_id,
            next_start,
            end_key,
            region_info,
            finished: Default::default(),
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
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
                let mut sended = 0;
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
                        };
                        tx.send(backup_range).unwrap();
                        sended += 1;
                        if sended >= limit {
                            break;
                        }
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!("backup seek region failed"; "error" => ?e);
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
    workers: Option<ThreadPool>,
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

    fn spawn<F>(&mut self, future: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.workers.as_ref().unwrap().spawn(future);
    }

    /// Lazily adjust the thread pool's size
    ///
    /// Resizing if the thread pool need to expend or there
    /// are too many idle threads. Otherwise do nothing.
    fn adjust_with(&mut self, new_size: usize) {
        if self.size >= new_size && self.size - new_size <= 10 {
            return;
        }
        let workers = ThreadPoolBuilder::new()
            .name_prefix("backup-worker")
            .pool_size(new_size)
            .build();
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
                w.shutdown();
            }
        }
    }
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(store_id: u64, engine: E, region_info: R, db: Arc<DB>) -> Endpoint<E, R> {
        Endpoint {
            store_id,
            engine,
            region_info,
            pool: RefCell::new(ControlThreadPool::new()),
            pool_idle_threshold: IDLE_THREADPOOL_DURATION,
            db,
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(self.pool_idle_threshold), ());
        timer
    }

    fn spawn_backup_worker(
        &self,
        prs: Arc<Mutex<Progress<R>>>,
        start_ts: TimeStamp,
        end_ts: TimeStamp,
        storage: LimitedStorage,
        tx: mpsc::Sender<(BackupRange, Result<BackupRes>)>,
        cancel: Arc<AtomicBool>,
    ) {
        let backup_ts = end_ts;
        let engine = self.engine.clone();
        let db = self.db.clone();
        let store_id = self.store_id;
        // TODO: make it async.
        self.pool.borrow_mut().spawn(lazy(move || loop {
            let branges = prs.lock().unwrap().forward(WORKER_TAKE_RANGE);
            if branges.is_empty() {
                return Ok(());
            }
            for brange in branges {
                if cancel.load(Ordering::SeqCst) {
                    warn!("backup task has canceled"; "range" => ?brange);
                    return Ok(());
                }
                // TODO: make file_name unique and short
                let key = brange.start_key.clone().and_then(|k| {
                    // use start_key sha256 instead of start_key to avoid file name too long os error
                    tikv_util::file::sha256(&k.into_raw().unwrap())
                        .ok()
                        .map(|b| hex::encode(b))
                });

                let name = backup_file_name(store_id, &brange.region, key);
                let mut writer = match BackupWriter::new(db.clone(), &name, storage.limiter.clone())
                {
                    Ok(w) => w,
                    Err(e) => {
                        error!("backup writer failed"; "error" => ?e);
                        return tx.send((brange, Err(e))).map_err(|_| ());
                    }
                };
                let stat = match brange.backup(&mut writer, &engine, backup_ts, start_ts) {
                    Ok(s) => s,
                    Err(e) => return tx.send((brange, Err(e))).map_err(|_| ()),
                };
                // Save sst files to storage.
                let files = match writer.save(&storage.storage) {
                    Ok(files) => files,
                    Err(e) => {
                        error!("backup save file failed"; "error" => ?e);
                        return tx.send((brange, Err(e))).map_err(|_| ());
                    }
                };
                let _ = tx.send((brange, Ok((files, stat)))).map_err(|_| ());
            }
        }));
    }

    pub fn handle_backup_task(&self, task: Task) {
        let start = Instant::now();
        let start_key = if task.start_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&task.start_key))
        };
        let end_key = if task.end_key.is_empty() {
            None
        } else {
            Some(Key::from_raw(&task.end_key))
        };

        let (res_tx, res_rx) = mpsc::channel();
        let prs = Arc::new(Mutex::new(Progress::new(
            self.store_id,
            start_key,
            end_key,
            self.region_info.clone(),
        )));
        let concurrency = cmp::max(1, task.concurrency) as usize;
        self.pool.borrow_mut().adjust_with(concurrency);
        for _ in 0..concurrency {
            self.spawn_backup_worker(
                prs.clone(),
                task.start_ts,
                task.end_ts,
                task.storage.clone(),
                res_tx.clone(),
                task.cancel.clone(),
            );
        }

        // Drop the extra sender so that for loop does not hang up.
        drop(res_tx);
        let mut summary = Statistics::default();
        let resp = task.resp;
        for (brange, res) in res_rx {
            let start_key = brange
                .start_key
                .map_or_else(|| vec![], |k| k.into_raw().unwrap());
            let end_key = brange
                .end_key
                .map_or_else(|| vec![], |k| k.into_raw().unwrap());
            let mut response = BackupResponse::default();
            match res {
                Ok((mut files, stat)) => {
                    debug!("backup region finish";
                        "region" => ?brange.region,
                        "start_key" => hex::encode_upper(&start_key),
                        "end_key" => hex::encode_upper(&end_key),
                        "details" => ?stat);
                    summary.add(&stat);
                    // Fill key range and ts.
                    for file in files.iter_mut() {
                        file.set_start_key(start_key.clone());
                        file.set_end_key(end_key.clone());
                        file.set_start_version(task.start_ts.into_inner());
                        file.set_end_version(task.end_ts.into_inner());
                    }
                    response.set_files(files.into());
                }
                Err(e) => {
                    error!("backup region failed";
                        "region" => ?brange.region,
                        "start_key" => hex::encode_upper(response.get_start_key()),
                        "end_key" => hex::encode_upper(response.get_end_key()),
                        "error" => ?e);
                    response.set_error(e.into());
                }
            }
            response.set_start_key(start_key);
            response.set_end_key(end_key);
            if let Err(e) = resp.unbounded_send(response) {
                error!("backup failed to send response"; "error" => ?e);
                break;
            }
        }
        let duration = start.elapsed();
        BACKUP_REQUEST_HISTOGRAM.observe(duration.as_secs_f64());
        info!("backup finished";
            "take" => ?duration,
            "summary" => ?summary);
    }
}

impl<E: Engine, R: RegionInfoProvider> Runnable<Task> for Endpoint<E, R> {
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

impl<E: Engine, R: RegionInfoProvider> RunnableWithTimer<Task, ()> for Endpoint<E, R> {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let pool_idle_duration = Duration::from_millis(self.pool_idle_threshold);
        self.pool
            .borrow_mut()
            .check_active(pool_idle_duration.clone());
        timer.add_task(pool_idle_duration, ());
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

/// Construct an backup file name based on the given store id and region.
/// A name consists with three parts: store id, region_id and a epoch version.
fn backup_file_name(store_id: u64, region: &Region, key: Option<String>) -> String {
    match key {
        Some(k) => format!(
            "{}_{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version(),
            k
        ),
        None => format!(
            "{}_{}_{}",
            store_id,
            region.get_id(),
            region.get_region_epoch().get_version()
        ),
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use external_storage::{make_local_backend, make_noop_backend, LocalStorage};
    use futures::{self, Future, Stream};
    use kvproto::metapb;
    use rand;
    use std::thread;
    use tempfile::TempDir;
    use tikv::raftstore::coprocessor::RegionCollector;
    use tikv::raftstore::coprocessor::SeekRegionCallback;
    use tikv::raftstore::store::util::new_peer;
    use tikv::storage::kv::Result as EngineResult;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::{RocksEngine, TestEngineBuilder};
    use tikv_util::time::Instant;
    use txn_types::SHORT_VALUE_MAX_LEN;

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
        fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> EngineResult<()> {
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
            .cfs(&[engine::CF_DEFAULT, engine::CF_LOCK, engine::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        (
            temp,
            Endpoint::new(1, rocks, MockRegionInfoProvider::new(), db),
        )
    }

    pub fn check_response<F>(rx: UnboundedReceiver<BackupResponse>, check: F)
    where
        F: FnOnce(Option<BackupResponse>),
    {
        let (resp, rx) = rx.into_future().wait().unwrap();
        check(resp);
        let (none, _rx) = rx.into_future().wait().unwrap();
        assert!(none.is_none(), "{:?}", none);
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
        #[allow(clippy::block_in_if_condition_stmt)]
        let test_handle_backup_task_range =
            |start_key: &[u8], end_key: &[u8], expect: Vec<(&[u8], &[u8])>| {
                let tmp = TempDir::new().unwrap();
                let ls = LocalStorage::new(tmp.path()).unwrap();
                let storage = LimitedStorage {
                    storage: Arc::new(ls) as _,
                    limiter: None,
                };
                let (tx, rx) = unbounded();
                let task = Task {
                    start_key: start_key.to_vec(),
                    end_key: end_key.to_vec(),
                    start_ts: 1.into(),
                    end_ts: 1.into(),
                    resp: tx,
                    storage,
                    concurrency: 4,
                    cancel: Arc::default(),
                };
                endpoint.handle_backup_task(task);
                let resps: Vec<_> = rx.collect().wait().unwrap();
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

        // TODO: check key number for each snapshot.
        let limiter = Arc::new(RocksIOLimiter::new(10 * 1024 * 1024 /* 10 MB/s */));
        for (ts, len) in backup_tss {
            let mut req = BackupRequest::default();
            req.set_start_key(vec![]);
            req.set_end_key(vec![b'5']);
            req.set_start_version(0);
            req.set_end_version(ts.into_inner());
            req.set_concurrency(4);
            let (tx, rx) = unbounded();
            // Empty path should return an error.
            Task::new(req.clone(), tx.clone()).unwrap_err();

            // Set an unique path to avoid AlreadyExists error.
            req.set_storage_backend(make_local_backend(&tmp.path().join(ts.to_string())));
            if len % 2 == 0 {
                req.set_rate_limit(10 * 1024 * 1024);
            }
            let (mut task, _) = Task::new(req, tx).unwrap();
            if len % 2 == 0 {
                // Make sure the rate limiter is set.
                assert!(task.storage.limiter.is_some());
                // Share the same rate limiter.
                task.storage.limiter = Some(limiter.clone());
            }
            endpoint.handle_backup_task(task);
            let (resp, rx) = rx.into_future().wait().unwrap();
            let resp = resp.unwrap();
            assert!(!resp.has_error(), "{:?}", resp);
            let file_len = if *len <= SHORT_VALUE_MAX_LEN { 1 } else { 2 };
            assert_eq!(
                resp.get_files().len(),
                file_len, /* default and write */
                "{:?}",
                resp
            );
            let (none, _rx) = rx.into_future().wait().unwrap();
            assert!(none.is_none(), "{:?}", none);
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
        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(now.to_string())));
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
        // Set an unique path to avoid AlreadyExists error.
        req.set_storage_backend(make_local_backend(&tmp.path().join(now.to_string())));
        let (tx, rx) = unbounded();
        let (task, _) = Task::new(req.clone(), tx).unwrap();
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
        let (task, cancel) = Task::new(req.clone(), tx).unwrap();
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
        let (task, _) = Task::new(req.clone(), tx).unwrap();
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

        // at lease spwan one thread
        req.set_concurrency(0);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 1);

        // expand thread pool is needed
        req.set_concurrency(15);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        // shrink thread pool only if there are too many idle threads
        req.set_concurrency(10);
        let (task, _) = Task::new(req.clone(), tx.clone()).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 15);

        req.set_concurrency(3);
        let (task, _) = Task::new(req, tx).unwrap();
        endpoint.handle_backup_task(task);
        assert!(endpoint.pool.borrow().size == 3);
    }

    #[test]
    fn test_thread_pool_shutdown_when_idle() {
        let (_, mut endpoint) = new_endpoint();

        // set the idle threshold to 100ms
        endpoint.pool_idle_threshold = 100;
        let mut backup_timer = endpoint.new_timer();
        let endpoint = Arc::new(Mutex::new(endpoint));
        let scheduler = {
            let endpoint = endpoint.clone();
            let (tx, rx) = tikv_util::mpsc::unbounded();
            thread::spawn(move || loop {
                let tick_time = backup_timer.next_timeout().unwrap();
                let timeout = tick_time.checked_sub(Instant::now()).unwrap_or_default();
                let task = match rx.recv_timeout(timeout) {
                    Ok(Some(task)) => Some(task),
                    _ => None,
                };
                if let Some(task) = task {
                    let mut endpoint = endpoint.lock().unwrap();
                    endpoint.run(task);
                }
                endpoint.lock().unwrap().on_timeout(&mut backup_timer, ());
            });
            tx
        };

        let mut req = BackupRequest::default();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(1);
        req.set_end_version(1);
        req.set_concurrency(10);
        req.set_storage_backend(make_noop_backend());

        let (tx, _) = futures::sync::mpsc::unbounded();
        let (task, _) = Task::new(req, tx).unwrap();

        // if not task arrive after create the thread pool is empty
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);

        scheduler.send(Some(task)).unwrap();
        // wait the task send to worker
        thread::sleep(Duration::from_millis(10));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool not yet shutdown
        thread::sleep(Duration::from_millis(50));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 10);

        // thread pool shutdown if not task arrive for 100ms
        thread::sleep(Duration::from_millis(50));
        assert_eq!(endpoint.lock().unwrap().pool.borrow().size, 0);
    }
    // TODO: region err in txn(engine(request))
}
