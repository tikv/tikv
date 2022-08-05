// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Borrow,
    collections::{hash_map::RandomState, BTreeMap, HashMap},
    ops::{Bound, RangeBounds},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use engine_rocks::ReadPerfInstant;
use engine_traits::{CfName, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use futures::{channel::mpsc, executor::block_on, FutureExt, StreamExt};
use kvproto::raft_cmdpb::{CmdType, Request};
use raft::StateRole;
use raftstore::{coprocessor::RegionInfoProvider, RegionInfo};
use tikv::storage::CfStatistics;
use tikv_util::{
    box_err,
    sys::inspector::{
        self_thread_inspector, IoStat, ThreadInspector, ThreadInspectorImpl as OsInspector,
    },
    time::Instant,
    warn,
    worker::Scheduler,
    Either,
};
use tokio::sync::{oneshot, Mutex, RwLock};
use txn_types::{Key, Lock, LockType};

use crate::{
    errors::{Error, Result},
    metadata::store::BoxFuture,
    router::TaskSelector,
    Task,
};

/// wrap a user key with encoded data key.
pub fn wrap_key(v: Vec<u8>) -> Vec<u8> {
    // TODO: encode in place.
    let key = Key::from_raw(v.as_slice()).into_encoded();
    key
}

/// Transform a str to a [`engine_traits::CfName`]\(`&'static str`).
/// If the argument isn't one of `""`, `"DEFAULT"`, `"default"`, `"WRITE"`,
/// `"write"`, `"LOCK"`, `"lock"`... returns "ERR_CF". (Which would be ignored
/// then.)
pub fn cf_name(s: &str) -> CfName {
    match s {
        "" | "DEFAULT" | "default" => CF_DEFAULT,
        "WRITE" | "write" => CF_WRITE,
        "LOCK" | "lock" => CF_LOCK,
        "RAFT" | "raft" => CF_RAFT,
        _ => {
            Error::Other(box_err!("unknown cf name {}", s)).report("");
            "ERR_CF"
        }
    }
}

pub fn redact(key: &impl AsRef<[u8]>) -> log_wrappers::Value<'_> {
    log_wrappers::Value::key(key.as_ref())
}

/// RegionPager seeks regions with leader role in the range.
pub struct RegionPager<P> {
    regions: P,
    start_key: Vec<u8>,
    end_key: Vec<u8>,
    reach_last_region: bool,
}

impl<P: RegionInfoProvider> RegionPager<P> {
    pub fn scan_from(regions: P, start_key: Vec<u8>, end_key: Vec<u8>) -> Self {
        Self {
            regions,
            start_key,
            end_key,
            reach_last_region: false,
        }
    }

    pub fn next_page(&mut self, size: usize) -> Result<Vec<RegionInfo>> {
        if self.start_key >= self.end_key || self.reach_last_region {
            return Ok(vec![]);
        }

        let (mut tx, rx) = mpsc::channel(size);
        let end_key = self.end_key.clone();
        self.regions
            .seek_region(
                &self.start_key,
                Box::new(move |i| {
                    let r = i
                        .filter(|r| r.role == StateRole::Leader)
                        .take(size)
                        .take_while(|r| r.region.start_key < end_key)
                        .try_for_each(|r| tx.try_send(r.clone()));
                    if let Err(_err) = r {
                        warn!("failed to scan region and send to initlizer")
                    }
                }),
            )
            .map_err(|err| {
                Error::Other(box_err!(
                    "failed to seek region for start key {}: {}",
                    redact(&self.start_key),
                    err
                ))
            })?;
        let collected_regions = block_on(rx.collect::<Vec<_>>());
        self.start_key = collected_regions
            .last()
            .map(|region| region.region.end_key.to_owned())
            // no leader region found.
            .unwrap_or_default();
        if self.start_key.is_empty() {
            self.reach_last_region = true;
        }
        Ok(collected_regions)
    }
}

/// StopWatch is a utility for record time cost in multi-stage tasks.
/// NOTE: Maybe it should be generic over somewhat Clock type?
pub struct StopWatch(Instant);

impl StopWatch {
    /// Create a new stopwatch via current time.
    pub fn new() -> Self {
        Self(Instant::now_coarse())
    }

    /// Get time elapsed since last lap (or creation if the first time).
    pub fn lap(&mut self) -> Duration {
        let elapsed = self.0.saturating_elapsed();
        self.0 = Instant::now_coarse();
        elapsed
    }
}

/// Slot is a shareable slot in the slot map.
pub type Slot<T> = Mutex<T>;

/// SlotMap is a trivial concurrent map which sharding over each key.
/// NOTE: Maybe we can use dashmap for replacing the RwLock.
pub type SlotMap<K, V, S = RandomState> = RwLock<HashMap<K, Slot<V>, S>>;

/// Like `..=val`(a.k.a. `RangeToInclusive`), but allows `val` being a reference
/// to DSTs.
struct RangeToInclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToInclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Included(self.0)
    }
}

struct RangeToExclusiveRef<'a, T: ?Sized>(&'a T);

impl<'a, T: ?Sized> RangeBounds<T> for RangeToExclusiveRef<'a, T> {
    fn start_bound(&self) -> Bound<&T> {
        Bound::Unbounded
    }

    fn end_bound(&self) -> Bound<&T> {
        Bound::Excluded(self.0)
    }
}
#[derive(Default, Debug, Clone)]
pub struct SegmentMap<K: Ord, V>(BTreeMap<K, SegmentValue<K, V>>);

#[derive(Clone, Debug)]
pub struct SegmentValue<R, T> {
    pub range_end: R,
    pub item: T,
}

/// A container for holding ranges without overlapping.
/// supports fast(`O(log(n))`) query of overlapping and points in segments.
///
/// Maybe replace it with extended binary search tree or the real segment tree?
/// So it can contains overlapping segments.
pub type SegmentSet<T> = SegmentMap<T, ()>;

impl<K: Ord, V: Default> SegmentMap<K, V> {
    /// Try to add a element into the segment tree, with default value.
    /// (This is useful when using the segment tree as a `Set`, i.e.
    /// `SegmentMap<T, ()>`)
    ///
    /// - If no overlapping, insert the range into the tree and returns `true`.
    /// - If overlapping detected, do nothing and return `false`.
    pub fn add(&mut self, (start, end): (K, K)) -> bool {
        self.insert((start, end), V::default())
    }
}

impl<K: Ord, V> SegmentMap<K, V> {
    /// Remove all records in the map.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Like `add`, but insert a value associated to the key.
    pub fn insert(&mut self, (start, end): (K, K), value: V) -> bool {
        if self.is_overlapping((&start, &end)) {
            return false;
        }
        self.0.insert(
            start,
            SegmentValue {
                range_end: end,
                item: value,
            },
        );
        true
    }

    /// Find a segment with its associated value by the point.
    pub fn get_by_point<R>(&self, point: &R) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.0
            .range(RangeToInclusiveRef(point))
            .next_back()
            .filter(|(_, end)| <K as Borrow<R>>::borrow(&end.range_end) > point)
            .map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_value_by_point<R>(&self, point: &R) -> Option<&V>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(_, _, v)| v)
    }

    /// Like `get_by_point`, but omit the segment.
    pub fn get_interval_by_point<R>(&self, point: &R) -> Option<(&K, &K)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.get_by_point(point).map(|(k, v, _)| (k, v))
    }

    pub fn find_overlapping<R>(&self, range: (&R, &R)) -> Option<(&K, &K, &V)>
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        // o: The Start Key.
        // e: The End Key.
        // +: The Boundary of Candidate Range.
        // |------+-s----+----e----|
        // Firstly, we check whether the start point is in some range.
        // if true, it must be overlapping.
        if let Some(overlap_with_start) = self.get_by_point(range.0) {
            return Some(overlap_with_start);
        }
        // |--s----+-----+----e----|
        // Otherwise, the possibility of being overlapping would be there are some sub
        // range of the queried range...
        // |--s----+----e----+-----|
        // ...Or the end key is contained by some Range.
        // For faster query, we merged the two cases together.
        let covered_by_the_range = self
             .0
             // When querying possibility of overlapping by end key,
             // we don't want the range [end key, ...) become a candidate.
             // (which is impossible to overlapping with the range)
             .range(RangeToExclusiveRef(range.1))
             .next_back()
             .filter(|(start, end)| {
                 <K as Borrow<R>>::borrow(&end.range_end) > range.1
                     || <K as Borrow<R>>::borrow(start) > range.0
             });
        covered_by_the_range.map(|(k, v)| (k, &v.range_end, &v.item))
    }

    /// Check whether the range is overlapping with any range in the segment
    /// tree.
    pub fn is_overlapping<R>(&self, range: (&R, &R)) -> bool
    where
        K: Borrow<R>,
        R: Ord + ?Sized,
    {
        self.find_overlapping(range).is_some()
    }

    pub fn get_inner(&mut self) -> &mut BTreeMap<K, SegmentValue<K, V>> {
        &mut self.0
    }
}

/// transform a [`RaftCmdRequest`] to `(key, value, cf)` triple.
/// once it contains a write request, extract it, and return `Left((key, value,
/// cf))`, otherwise return the request itself via `Right`.
pub fn request_to_triple(mut req: Request) -> Either<(Vec<u8>, Vec<u8>, CfName), Request> {
    let (key, value, cf) = match req.get_cmd_type() {
        CmdType::Put => {
            let mut put = req.take_put();
            (put.take_key(), put.take_value(), put.cf)
        }
        CmdType::Delete => {
            let mut del = req.take_delete();
            (del.take_key(), Vec::new(), del.cf)
        }
        _ => return Either::Right(req),
    };
    Either::Left((key, value, cf_name(cf.as_str())))
}

/// `try_send!(s: Scheduler<T>, task: T)` tries to send a task to the scheduler,
/// once meet an error, would report it, with the current file and line (so it
/// is made as a macro). returns whether it success.
#[macro_export(crate)]
macro_rules! try_send {
    ($s:expr, $task:expr) => {
        match $s.schedule($task) {
            Err(err) => {
                $crate::errors::Error::from(err).report(concat!(
                    "[",
                    file!(),
                    ":",
                    line!(),
                    "]",
                    "failed to schedule task"
                ));
                false
            }
            Ok(_) => true,
        }
    };
}

/// a hacky macro which allow us enable all debug log via the feature
/// `backup_stream_debug`. because once we enable debug log for all crates, it
/// would soon get too verbose to read. using this macro now we can enable debug
/// log level for the crate only (even compile time...).
#[macro_export(crate)]
macro_rules! debug {
    ($($t: tt)+) => {
        if cfg!(feature = "backup-stream-debug") {
            tikv_util::info!(#"backup-stream", $($t)+)
        } else {
            tikv_util::debug!(#"backup-stream", $($t)+)
        }
    };
}

macro_rules! record_fields {
    ($m:expr,$cf:expr,$stat:expr, [ $(.$s:ident),+ ]) => {
        {
            let m = &$m;
            let cf = &$cf;
            let stat = &$stat;
            $( m.with_label_values(&[cf, stringify!($s)]).inc_by(stat.$s as _) );+
        }
    };
}

pub fn record_cf_stat(cf_name: &str, stat: &CfStatistics) {
    let m = &crate::metrics::INITIAL_SCAN_STAT;
    m.with_label_values(&[cf_name, "read_bytes"])
        .inc_by(stat.flow_stats.read_bytes as _);
    m.with_label_values(&[cf_name, "read_keys"])
        .inc_by(stat.flow_stats.read_keys as _);
    record_fields!(
        m,
        cf_name,
        stat,
        [
            .get,
            .next,
            .prev,
            .seek,
            .seek_for_prev,
            .over_seek_bound,
            .next_tombstone,
            .prev_tombstone,
            .seek_tombstone,
            .seek_for_prev_tombstone
        ]
    );
}

/// a shortcut for handing the result return from `Router::on_events`, when any
/// failure, send a fatal error to the `doom_messenger`.
pub fn handle_on_event_result(doom_messenger: &Scheduler<Task>, result: Vec<(String, Result<()>)>) {
    for (task, res) in result.into_iter() {
        if let Err(err) = res {
            try_send!(
                doom_messenger,
                Task::FatalError(
                    TaskSelector::ByName(task),
                    Box::new(err.context("failed to record event to local temporary files"))
                )
            );
        }
    }
}

/// tests whether the lock should be tracked or skipped.
pub fn should_track_lock(l: &Lock) -> bool {
    match l.lock_type {
        LockType::Put | LockType::Delete => true,
        // Lock or Pessimistic lock won't commit more data,
        // (i.e. won't break the integration of data between [Lock.start_ts, get_ts()))
        // it is safe for ignoring them and advancing resolved_ts.
        LockType::Lock | LockType::Pessimistic => false,
    }
}

pub struct CallbackWaitGroup {
    running: AtomicUsize,
    on_finish_all: std::sync::Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
}

/// A shortcut for making an opaque future type for return type or argument
/// type, which is sendable and not borrowing any variables.  
///
/// `fut![T]` == `impl Future<Output = T> + Send + 'static`
#[macro_export(crate)]
macro_rules! future {
    ($t:ty) => { impl core::future::Future<Output = $t> + Send + 'static };
}

impl CallbackWaitGroup {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicUsize::new(0),
            on_finish_all: std::sync::Mutex::default(),
        })
    }

    fn work_done(&self) {
        let last = self.running.fetch_sub(1, Ordering::SeqCst);
        if last == 1 {
            self.on_finish_all
                .lock()
                .unwrap()
                .drain(..)
                .for_each(|x| x())
        }
    }

    /// wait until all running tasks done.
    pub fn wait(&self) -> BoxFuture<()> {
        // Fast path: no uploading.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(()));
        }

        let (tx, rx) = oneshot::channel();
        self.on_finish_all.lock().unwrap().push(Box::new(move || {
            // The waiter may timed out.
            let _ = tx.send(());
        }));
        // try to acquire the lock again.
        if self.running.load(Ordering::SeqCst) == 0 {
            return Box::pin(futures::future::ready(()));
        }
        Box::pin(rx.map(|_| ()))
    }

    /// make a work, as long as the return value held, mark a work in the group
    /// is running.
    pub fn work(self: Arc<Self>) -> Work {
        self.running.fetch_add(1, Ordering::SeqCst);
        Work(self)
    }
}

pub struct Work(Arc<CallbackWaitGroup>);

impl Drop for Work {
    fn drop(&mut self) {
        self.0.work_done();
    }
}

struct ReadThroughputRecorder {
    // The system tool set.
    ins: Option<OsInspector>,
    begin: Option<IoStat>,
    // Once the system tool set get unavailable,
    // we would use the "ejector" -- RocksDB perf context.
    // NOTE: In fact I'm not sure whether we need the result of system level tool set --
    //       but this is the current implement of cdc. We'd better keep consistent with them.
    ejector: ReadPerfInstant,
}

impl ReadThroughputRecorder {
    fn start() -> Self {
        let r = self_thread_inspector().ok().and_then(|insp| {
            let stat = insp.io_stat().ok()??;
            Some((insp, stat))
        });
        match r {
            Some((ins, begin)) => Self {
                ins: Some(ins),
                begin: Some(begin),
                ejector: ReadPerfInstant::new(),
            },
            _ => Self {
                ins: None,
                begin: None,
                ejector: ReadPerfInstant::new(),
            },
        }
    }

    fn try_get_delta_from_unix(&self) -> Option<u64> {
        let ins = self.ins.as_ref()?;
        let begin = self.begin.as_ref()?;
        let end = ins.io_stat().ok()??;
        let bytes_read = end.read - begin.read;
        // FIXME: In our test environment, there may be too many caches hence the
        // `bytes_read` is always zero.
        // For now, we eject here and let rocksDB prove that we did read something when
        // the proc think we don't touch the block device (even in fact we didn't).
        // NOTE: In the real-world, we would accept the zero `bytes_read` value since
        // the cache did exists.
        #[cfg(test)]
        if bytes_read == 0 {
            // use println here so we can get this message even log doesn't enabled.
            println!("ejecting in test since no read recorded in procfs");
            return None;
        }
        Some(bytes_read)
    }

    fn end(self) -> u64 {
        self.try_get_delta_from_unix()
            .unwrap_or_else(|| self.ejector.delta().block_read_byte)
    }
}

/// try to record read throughput.
/// this uses the `proc` fs in the linux for recording the throughput.
/// if that failed, we would use the RocksDB perf context.
pub fn with_record_read_throughput<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let recorder = ReadThroughputRecorder::start();
    let r = f();
    (r, recorder.end())
}

/// test whether a key is in the range.
/// end key is exclusive.
/// empty end key means infinity.
pub fn is_in_range(key: &[u8], range: (&[u8], &[u8])) -> bool {
    match range {
        (start, b"") => key >= start,
        (start, end) => key >= start && key < end,
    }
}

/// test whether two ranges overlapping.
/// end key is exclusive.
/// empty end key means infinity.
pub fn is_overlapping(range: (&[u8], &[u8]), range2: (&[u8], &[u8])) -> bool {
    let (x1, y1) = range;
    let (x2, y2) = range2;
    match (x1, y1, x2, y2) {
        // 1:       |__________________|
        // 2:   |______________________|
        (_, b"", _, b"") => true,
        // 1:   (x1)|__________________|
        // 2:   |_________________|(y2)
        (x1, b"", _, y2) => x1 < y2,
        // 1:   |________________|(y1)
        // 2:    (x2)|_________________|
        (_, y1, x2, b"") => x2 < y1,
        // 1:  (x1)|________|(y1)
        // 2:    (x2)|__________|(y2)
        (x1, y1, x2, y2) => x2 < y1 && x1 < y2,
    }
}

#[cfg(test)]
mod test {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use engine_traits::WriteOptions;
    use futures::executor::block_on;

    use crate::utils::{is_in_range, CallbackWaitGroup, SegmentMap};

    #[test]
    fn test_range_functions() {
        #[derive(Debug)]
        struct InRangeCase<'a> {
            key: &'a [u8],
            range: (&'a [u8], &'a [u8]),
            expected: bool,
        }

        let cases = [
            InRangeCase {
                key: b"0001",
                range: (b"0000", b"0002"),
                expected: true,
            },
            InRangeCase {
                key: b"0003",
                range: (b"0000", b"0002"),
                expected: false,
            },
            InRangeCase {
                key: b"0002",
                range: (b"0000", b"0002"),
                expected: false,
            },
            InRangeCase {
                key: b"0000",
                range: (b"0000", b"0002"),
                expected: true,
            },
            InRangeCase {
                key: b"0018",
                range: (b"0000", b""),
                expected: true,
            },
            InRangeCase {
                key: b"0018",
                range: (b"0019", b""),
                expected: false,
            },
        ];

        for case in cases {
            assert!(
                is_in_range(case.key, case.range) == case.expected,
                "case = {:?}",
                case
            );
        }
    }

    #[test]
    fn test_segment_tree() {
        let mut tree = SegmentMap::default();
        assert!(tree.add((1, 4)));
        assert!(tree.add((4, 8)));
        assert!(tree.add((42, 46)));
        assert!(!tree.add((3, 8)));
        assert!(tree.insert((47, 88), "hello".to_owned()));
        assert_eq!(
            tree.get_value_by_point(&49).map(String::as_str),
            Some("hello")
        );
        assert_eq!(tree.get_interval_by_point(&3), Some((&1, &4)));
        assert_eq!(tree.get_interval_by_point(&7), Some((&4, &8)));
        assert_eq!(tree.get_interval_by_point(&90), None);
        assert!(tree.is_overlapping((&1, &3)));
        assert!(tree.is_overlapping((&7, &9)));
        assert!(!tree.is_overlapping((&8, &42)));
        assert!(!tree.is_overlapping((&9, &10)));
        assert!(tree.is_overlapping((&2, &10)));
        assert!(tree.is_overlapping((&0, &9999999)));
    }

    #[test]
    fn test_wait_group() {
        #[derive(Debug)]
        struct Case {
            bg_task: usize,
            repeat: usize,
        }

        fn run_case(c: Case) {
            for i in 0..c.repeat {
                let wg = CallbackWaitGroup::new();
                let cnt = Arc::new(AtomicUsize::new(c.bg_task));
                for _ in 0..c.bg_task {
                    let cnt = cnt.clone();
                    let work = wg.clone().work();
                    tokio::spawn(async move {
                        cnt.fetch_sub(1, Ordering::SeqCst);
                        drop(work);
                    });
                }
                block_on(tokio::time::timeout(Duration::from_secs(20), wg.wait())).unwrap();
                assert_eq!(cnt.load(Ordering::SeqCst), 0, "{:?}@{}", c, i);
            }
        }

        let cases = [
            Case {
                bg_task: 200000,
                repeat: 1,
            },
            Case {
                bg_task: 65535,
                repeat: 1,
            },
            Case {
                bg_task: 512,
                repeat: 1,
            },
            Case {
                bg_task: 2,
                repeat: 100000,
            },
            Case {
                bg_task: 1,
                repeat: 100000,
            },
            Case {
                bg_task: 0,
                repeat: 1,
            },
        ];

        let pool = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap();
        let _guard = pool.handle().enter();
        for case in cases {
            run_case(case)
        }
    }

    #[test]
    fn test_recorder() {
        use engine_traits::{Iterable, KvEngine, Mutable, WriteBatch, WriteBatchExt, CF_DEFAULT};
        use tempdir::TempDir;

        let p = TempDir::new("test_db").unwrap();
        let engine =
            engine_rocks::util::new_engine(p.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let mut wb = engine.write_batch();
        for i in 0..100 {
            wb.put_cf(CF_DEFAULT, format!("hello{}", i).as_bytes(), b"world")
                .unwrap();
        }
        let mut wopt = WriteOptions::new();
        wopt.set_sync(true);
        wb.write_opt(&wopt).unwrap();
        // force memtable to disk.
        engine.get_sync_db().compact_range(None, None);

        let (items, size) = super::with_record_read_throughput(|| {
            let mut items = vec![];
            let snap = engine.snapshot();
            snap.scan(CF_DEFAULT, b"", b"", false, |k, v| {
                items.push((k.to_owned(), v.to_owned()));
                Ok(true)
            })
            .unwrap();
            items
        });

        let items_size = items.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as u64;

        // considering the compression, we may get at least 1/2 of the real size.
        assert!(
            size > items_size / 2,
            "the size recorded is too small: {} vs {}",
            size,
            items_size
        );
        // considering the read amplification, we may get at most 2x of the real size.
        assert!(
            size < items_size * 2,
            "the size recorded is too big: {} vs {}",
            size,
            items_size
        );
    }
}
