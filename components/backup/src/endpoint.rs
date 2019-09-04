// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// TODO: remove it after all code been merged.
#![allow(unused_variables)]
#![allow(dead_code)]

use std::cmp;
use std::fmt;
use std::sync::atomic::*;
use std::sync::*;
use std::time::*;

use engine::DB;
use external_storage::*;
use futures::sync::mpsc::*;
use futures::{lazy, Future};
use kvproto::backup::*;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use kvproto::metapb::*;
use raft::StateRole;
use tikv::raftstore::coprocessor::RegionInfoAccessor;
use tikv::raftstore::store::util::find_peer;
use tikv::server::transport::ServerRaftStoreRouter;
use tikv::storage::kv::{
    Engine, Error as EngineError, RegionInfoProvider, ScanMode, StatisticsSummary,
};
use tikv::storage::txn::{
    EntryBatch, Error as TxnError, Msg, Scanner, SnapshotStore, Store, TxnEntryScanner,
    TxnEntryStore,
};
use tikv::storage::{Key, Statistics};
use tikv_util::worker::{Runnable, RunnableWithTimer};
use tokio_threadpool::{Builder as ThreadPoolBuilder, ThreadPool};

use crate::metrics::*;
use crate::*;

/// Backup task.
pub struct Task {
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    start_ts: u64,
    end_ts: u64,

    storage: Arc<dyn ExternalStorage>,
    pub(crate) resp: UnboundedSender<BackupResponse>,

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

impl Task {
    /// Create a backup task based on the given backup request.
    pub fn new(
        req: BackupRequest,
        resp: UnboundedSender<BackupResponse>,
    ) -> Result<(Task, Arc<AtomicBool>)> {
        let start_key = req.get_start_key().to_owned();
        let end_key = req.get_end_key().to_owned();
        let start_ts = req.get_start_version();
        let end_ts = req.get_end_version();
        let storage = create_storage(req.get_path())?;
        let cancel = Arc::new(AtomicBool::new(false));
        Ok((
            Task {
                start_key,
                end_key,
                start_ts,
                end_ts,
                resp,
                storage,
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

    cancel: Arc<AtomicBool>,
}

impl BackupRange {
    fn has_canceled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

type BackupRes = (Vec<File>, Statistics);

/// The endpoint of backup.
///
/// It coordinates backup tasks and dispatches them to different workers.
pub struct Endpoint<E: Engine, R: RegionInfoProvider> {
    store_id: u64,
    workers: ThreadPool,
    db: Arc<DB>,

    pub(crate) engine: E,
    pub(crate) region_info: R,
}

impl<E: Engine, R: RegionInfoProvider> Endpoint<E, R> {
    pub fn new(store_id: u64, engine: E, region_info: R, db: Arc<DB>) -> Endpoint<E, R> {
        let workers = ThreadPoolBuilder::new().name_prefix("backworker").build();
        Endpoint {
            store_id,
            engine,
            region_info,
            // TODO: support more config.
            workers,
            db,
        }
    }

    fn seek_backup_range(
        &self,
        start_key: Option<Key>,
        end_key: Option<Key>,
        cancel: Arc<AtomicBool>,
    ) -> mpsc::Receiver<BackupRange> {
        let store_id = self.store_id;
        let (tx, rx) = mpsc::channel();
        let start_key_ = start_key
            .clone()
            .map_or_else(Vec::new, |k| k.into_encoded());
        let res = self.region_info.seek_region(
            &start_key_,
            Box::new(move |iter| {
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
                            cancel: cancel.clone(),
                        };
                        tx.send(backup_range).unwrap();
                    }
                }
            }),
        );
        if let Err(e) = res {
            // TODO: handle error.
            error!("backup seek region failed"; "error" => ?e);
        }
        rx
    }

    fn dispatch_backup_range(
        &self,
        brange: BackupRange,
        start_ts: u64,
        end_ts: u64,
        storage: Arc<dyn ExternalStorage>,
        tx: mpsc::Sender<(BackupRange, Result<BackupRes>)>,
    ) {
        // TODO: support incremental backup
        let _ = start_ts;

        let backup_ts = end_ts;
        let mut ctx = Context::new();
        ctx.set_region_id(brange.region.get_id());
        ctx.set_region_epoch(brange.region.get_region_epoch().to_owned());
        ctx.set_peer(brange.leader.clone());
        // TODO: make it async.
        let snapshot = match self.engine.snapshot(&ctx) {
            Ok(s) => s,
            Err(e) => {
                error!("backup snapshot failed"; "error" => ?e);
                return tx.send((brange, Err(e.into()))).unwrap();
            }
        };
        let db = self.db.clone();
        let store_id = self.store_id;
        self.workers.spawn(lazy(move || {
            if brange.has_canceled() {
                warn!("backup task has canceled"; "range" => ?brange);
                return Ok(());
            }
            let snap_store = SnapshotStore::new(
                snapshot,
                backup_ts,
                IsolationLevel::Si,
                false, /* fill_cache */
            );
            let start_key = brange.start_key.clone();
            let end_key = brange.end_key.clone();
            let mut scanner = snap_store
                .entry_scanner(start_key.clone(), end_key.clone())
                .unwrap();
            let mut batch = EntryBatch::with_capacity(1024);
            let name = backup_file_name(store_id, &brange.region);
            let mut writer = match BackupWriter::new(db, &name) {
                Ok(w) => w,
                Err(e) => {
                    error!("backup writer failed"; "error" => ?e);
                    return tx.send((brange, Err(e))).map_err(|_| ());
                }
            };
            let start = Instant::now();
            loop {
                if let Err(e) = scanner.scan_entries(&mut batch) {
                    error!("backup scan entries failed"; "error" => ?e);
                    return tx.send((brange, Err(e.into()))).map_err(|_| ());
                };
                if batch.is_empty() {
                    break;
                }
                debug!("backup scan entries"; "len" => batch.len());
                // Build sst files.
                if let Err(e) = writer.write(batch.drain()) {
                    error!("backup build sst failed"; "error" => ?e);
                    return tx.send((brange, Err(e))).map_err(|_| ());
                }
            }
            BACKUP_RANGE_HISTOGRAM_VEC
                .with_label_values(&["scan"])
                .observe(start.elapsed().as_secs_f64());
            // Save sst files to storage.
            let files = match writer.save(&storage) {
                Ok(files) => files,
                Err(e) => {
                    error!("backup save file failed"; "error" => ?e);
                    return tx.send((brange, Err(e))).map_err(|_| ());
                }
            };
            let stat = scanner.take_statistics();
            tx.send((brange, Ok((files, stat)))).map_err(|_| ())
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
        let rx = self.seek_backup_range(start_key, end_key, task.cancel);

        let (res_tx, res_rx) = mpsc::channel();
        for brange in rx {
            let tx = res_tx.clone();
            self.dispatch_backup_range(
                brange,
                task.start_ts,
                task.end_ts,
                task.storage.clone(),
                tx,
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
            let mut response = BackupResponse::new();
            response.set_start_key(start_key.clone());
            response.set_end_key(end_key.clone());
            match res {
                Ok((mut files, stat)) => {
                    info!("backup region finish";
                        "region" => ?brange.region,
                        "start_key" => hex::encode_upper(&start_key),
                        "end_key" => hex::encode_upper(&end_key),
                        "details" => ?stat);
                    summary.add(&stat);
                    // Fill key range and ts.
                    for file in files.iter_mut() {
                        file.set_start_key(start_key.clone());
                        file.set_end_key(end_key.clone());
                        file.set_start_version(task.start_ts);
                        file.set_end_version(task.end_ts);
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
        if task.start_ts == task.end_ts {
            self.handle_backup_task(task);
        } else {
            // TODO: support incremental backup
            error!("incremental backup is not supported yet");
        }
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
fn backup_file_name(store_id: u64, region: &Region) -> String {
    format!(
        "{}_{}_{}",
        store_id,
        region.get_id(),
        region.get_region_epoch().get_version()
    )
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use external_storage::LocalStorage;
    use futures::{Future, Stream};
    use kvproto::metapb;
    use std::collections::BTreeMap;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use tempfile::TempDir;
    use tikv::raftstore::coprocessor::RegionCollector;
    use tikv::raftstore::coprocessor::{RegionInfo, SeekRegionCallback};
    use tikv::raftstore::store::util::new_peer;
    use tikv::storage::kv::Result as EngineResult;
    use tikv::storage::mvcc::tests::*;
    use tikv::storage::SHORT_VALUE_MAX_LEN;
    use tikv::storage::{
        Mutation, Options, RocksEngine, Storage, TestEngineBuilder, TestStorageBuilder,
    };

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
                let rx = endpoint.seek_backup_range(start_key, end_key, Arc::default());
                let ranges: Vec<BackupRange> = rx.into_iter().collect();
                assert_eq!(
                    ranges.len(),
                    expect.len(),
                    "got {:?}, expect {:?}",
                    ranges,
                    expect
                );
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
                let (tx, rx) = unbounded();
                let task = Task {
                    start_key: start_key.to_vec(),
                    end_key: end_key.to_vec(),
                    start_ts: 1,
                    end_ts: 1,
                    resp: tx,
                    storage: Arc::new(ls),
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

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
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
        for (ts, len) in backup_tss {
            let mut req = BackupRequest::new();
            req.set_start_key(vec![]);
            req.set_end_key(vec![b'5']);
            req.set_start_version(ts);
            req.set_end_version(ts);
            let (tx, rx) = unbounded();
            // Empty path should return an error.
            Task::new(req.clone(), tx.clone()).unwrap_err();

            // Set an unique path to avoid AlreadyExists error.
            req.set_path(format!(
                "local://{}",
                tmp.path().join(format!("{}", ts)).display()
            ));
            let (task, _) = Task::new(req, tx).unwrap();
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

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
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
        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![b'5']);
        req.set_start_version(now);
        req.set_end_version(now);
        // Set an unique path to avoid AlreadyExists error.
        req.set_path(format!(
            "local://{}",
            tmp.path().join(format!("{}", now)).display()
        ));
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

        // Test whether it can correctly convert not leader to regoin error.
        engine.trigger_not_leader();
        let now = alloc_ts();
        req.set_start_version(now);
        req.set_end_version(now);
        // Set an unique path to avoid AlreadyExists error.
        req.set_path(format!(
            "local://{}",
            tmp.path().join(format!("{}", now)).display()
        ));
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

        let mut ts = 1;
        let mut alloc_ts = || {
            ts += 1;
            ts
        };
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
        let mut req = BackupRequest::new();
        req.set_start_key(vec![]);
        req.set_end_key(vec![]);
        req.set_start_version(now);
        req.set_end_version(now);
        req.set_path(format!("local://{}", temp.path().display()));

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

    // TODO: region err in txn(engine(request))
}
