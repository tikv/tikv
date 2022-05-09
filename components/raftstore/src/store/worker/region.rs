// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{
        BTreeMap,
        Bound::{Excluded, Included, Unbounded},
        VecDeque,
    },
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc,
    },
    time::Duration,
    u64,
};

use engine_traits::{DeleteStrategy, KvEngine, Mutable, Range, WriteBatch, CF_LOCK, CF_RAFT};
use fail::fail_point;
use file_system::{IOType, WithIOType};
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
use raft::eraftpb::Snapshot as RaftSnapshot;
use tikv_util::{
    box_err, box_try, defer, error, info, thd_name,
    time::Instant,
    warn,
    worker::{Runnable, RunnableWithTimer},
};
use yatp::{
    pool::{Builder, ThreadPool},
    task::future::TaskCell,
};

use super::metrics::*;
use crate::{
    coprocessor::CoprocessorHost,
    store::{
        self, check_abort,
        peer_storage::{
            JOB_STATUS_CANCELLED, JOB_STATUS_CANCELLING, JOB_STATUS_FAILED, JOB_STATUS_FINISHED,
            JOB_STATUS_PENDING, JOB_STATUS_RUNNING,
        },
        snap::{plain_file_used, Error, Result, SNAPSHOT_CFS},
        transport::CasualRouter,
        ApplyOptions, CasualMessage, SnapEntry, SnapKey, SnapManager,
    },
};

// used to periodically check whether we should delete a stale peer's range in region runner

#[cfg(test)]
pub const STALE_PEER_CHECK_TICK: usize = 1; // 1000 milliseconds

#[cfg(not(test))]
pub const STALE_PEER_CHECK_TICK: usize = 10; // 10000 milliseconds

// used to periodically check whether schedule pending applies in region runner
#[cfg(not(test))]
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 1_000; // 1000 milliseconds
#[cfg(test)]
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 200; // 200 milliseconds

const CLEANUP_MAX_REGION_COUNT: usize = 64;

/// Region related task
#[derive(Debug)]
pub enum Task<S> {
    Gen {
        region_id: u64,
        last_applied_index_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: S,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    },
    Apply {
        region_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl<S> Task<S> {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task<S> {
        Task::Destroy {
            region_id,
            start_key,
            end_key,
        }
    }
}

impl<S> Display for Task<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy {
                region_id,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {} [{}, {})",
                region_id,
                log_wrappers::Value::key(start_key),
                log_wrappers::Value::key(end_key)
            ),
        }
    }
}

#[derive(Clone)]
struct StalePeerInfo {
    // the start_key is stored as a key in PendingDeleteRanges
    // below are stored as a value in PendingDeleteRanges
    pub region_id: u64,
    pub end_key: Vec<u8>,
    // Once the oldest snapshot sequence exceeds this, it ensures that no one is
    // reading on this peer anymore. So we can safely call `delete_files_in_range`
    // , which may break the consistency of snapshot, of this peer range.
    pub stale_sequence: u64,
}

/// A structure records all ranges to be deleted with some delay.
/// The delay is because there may be some coprocessor requests related to these ranges.
#[derive(Clone, Default)]
struct PendingDeleteRanges {
    ranges: BTreeMap<Vec<u8>, StalePeerInfo>, // start_key -> StalePeerInfo
}

impl PendingDeleteRanges {
    /// Finds ranges that overlap with [start_key, end_key).
    fn find_overlap_ranges(
        &self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>, u64)> {
        let mut ranges = Vec::new();
        // find the first range that may overlap with [start_key, end_key)
        let sub_range = self.ranges.range((Unbounded, Excluded(start_key.to_vec())));
        if let Some((s_key, peer_info)) = sub_range.last() {
            if peer_info.end_key > start_key.to_vec() {
                ranges.push((
                    peer_info.region_id,
                    s_key.clone(),
                    peer_info.end_key.clone(),
                    peer_info.stale_sequence,
                ));
            }
        }

        // find the rest ranges that overlap with [start_key, end_key)
        for (s_key, peer_info) in self
            .ranges
            .range((Included(start_key.to_vec()), Excluded(end_key.to_vec())))
        {
            ranges.push((
                peer_info.region_id,
                s_key.clone(),
                peer_info.end_key.clone(),
                peer_info.stale_sequence,
            ));
        }
        ranges
    }

    /// Gets ranges that overlap with [start_key, end_key).
    pub fn drain_overlap_ranges(
        &mut self,
        start_key: &[u8],
        end_key: &[u8],
    ) -> Vec<(u64, Vec<u8>, Vec<u8>, u64)> {
        let ranges = self.find_overlap_ranges(start_key, end_key);

        for &(_, ref s_key, ..) in &ranges {
            self.ranges.remove(s_key).unwrap();
        }
        ranges
    }

    /// Removes and returns the peer info with the `start_key`.
    fn remove(&mut self, start_key: &[u8]) -> Option<(u64, Vec<u8>, Vec<u8>)> {
        self.ranges
            .remove(start_key)
            .map(|peer_info| (peer_info.region_id, start_key.to_owned(), peer_info.end_key))
    }

    /// Inserts a new range waiting to be deleted.
    ///
    /// Before an insert is called, it must call drain_overlap_ranges to clean the overlapping range.
    fn insert(&mut self, region_id: u64, start_key: &[u8], end_key: &[u8], stale_sequence: u64) {
        if !self.find_overlap_ranges(start_key, end_key).is_empty() {
            panic!(
                "[region {}] register deleting data in [{}, {}) failed due to overlap",
                region_id,
                log_wrappers::Value::key(start_key),
                log_wrappers::Value::key(end_key),
            );
        }
        let info = StalePeerInfo {
            region_id,
            end_key: end_key.to_owned(),
            stale_sequence,
        };
        self.ranges.insert(start_key.to_owned(), info);
    }

    /// Gets all stale ranges info.
    pub fn stale_ranges(&self, oldest_sequence: u64) -> impl Iterator<Item = (u64, &[u8], &[u8])> {
        self.ranges
            .iter()
            .filter(move |&(_, info)| info.stale_sequence < oldest_sequence)
            .map(|(start_key, info)| {
                (
                    info.region_id,
                    start_key.as_slice(),
                    info.end_key.as_slice(),
                )
            })
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }
}

#[derive(Clone)]
struct SnapContext<EK, R>
where
    EK: KvEngine,
{
    engine: EK,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_range: bool,
    pending_delete_ranges: PendingDeleteRanges,
    coprocessor_host: CoprocessorHost<EK>,
    router: R,
}

impl<EK, R> SnapContext<EK, R>
where
    EK: KvEngine,
    R: CasualRouter<EK>,
{
    /// Generates the snapshot of the Region.
    fn generate_snap(
        &self,
        region_id: u64,
        last_applied_index_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: EK::Snapshot,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) -> Result<()> {
        // do we need to check leader here?
        let snap = box_try!(store::do_snapshot::<EK>(
            self.mgr.clone(),
            &self.engine,
            kv_snap,
            region_id,
            last_applied_index_term,
            last_applied_state,
            for_balance,
        ));
        // Only enable the fail point when the region id is equal to 1, which is
        // the id of bootstrapped region in tests.
        fail_point!("region_gen_snap", region_id == 1, |_| Ok(()));
        if let Err(e) = notifier.try_send(snap) {
            info!(
                "failed to notify snap result, leadership may have changed, ignore error";
                "region_id" => region_id,
                "err" => %e,
            );
        }
        // The error can be ignored as snapshot will be sent in next heartbeat in the end.
        let _ = self
            .router
            .send(region_id, CasualMessage::SnapshotGenerated);
        Ok(())
    }

    /// Handles the task of generating snapshot of the Region. It calls `generate_snap` to do the actual work.
    fn handle_gen(
        &self,
        region_id: u64,
        last_applied_index_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: EK::Snapshot,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) {
        fail_point!("before_region_gen_snap", |_| ());
        SNAP_COUNTER.generate.all.inc();
        if canceled.load(Ordering::Relaxed) {
            info!("generate snap is canceled"; "region_id" => region_id);
            return;
        }

        let start = Instant::now();
        let _io_type_guard = WithIOType::new(if for_balance {
            IOType::LoadBalance
        } else {
            IOType::Replication
        });

        if let Err(e) = self.generate_snap(
            region_id,
            last_applied_index_term,
            last_applied_state,
            kv_snap,
            notifier,
            for_balance,
        ) {
            error!(%e; "failed to generate snap!!!"; "region_id" => region_id,);
            return;
        }

        SNAP_COUNTER.generate.success.inc();
        SNAP_HISTOGRAM
            .generate
            .observe(start.saturating_elapsed_secs());
    }

    /// Applies snapshot data of the Region.
    fn apply_snap(&mut self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<()> {
        info!("begin apply snap data"; "region_id" => region_id);
        fail_point!("region_apply_snap", |_| { Ok(()) });
        check_abort(&abort)?;
        let region_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState =
            match box_try!(self.engine.get_msg_cf(CF_RAFT, &region_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get region_state from {}",
                        log_wrappers::Value::key(&region_key)
                    ));
                }
            };

        // clear up origin data.
        let region = region_state.get_region().clone();
        let start_key = keys::enc_start_key(&region);
        let end_key = keys::enc_end_key(&region);
        check_abort(&abort)?;
        let overlap_ranges = self
            .pending_delete_ranges
            .drain_overlap_ranges(&start_key, &end_key);
        if !overlap_ranges.is_empty() {
            CLEAN_COUNTER_VEC
                .with_label_values(&["overlap-with-apply"])
                .inc();
            self.cleanup_overlap_regions(overlap_ranges)?;
        }
        self.delete_all_in_range(&[Range::new(&start_key, &end_key)])?;
        check_abort(&abort)?;
        fail_point!("apply_snap_cleanup_range");

        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState =
            match box_try!(self.engine.get_msg_cf(CF_RAFT, &state_key)) {
                Some(state) => state,
                None => {
                    return Err(box_err!(
                        "failed to get raftstate from {}",
                        log_wrappers::Value::key(&state_key)
                    ));
                }
            };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(region_id, term, idx);
        self.mgr.register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.deregister(&snap_key, &SnapEntry::Applying);
        });
        let mut s = box_try!(self.mgr.get_snapshot_for_applying(&snap_key));
        if !s.exists() {
            return Err(box_err!("missing snapshot file {}", s.path()));
        }
        check_abort(&abort)?;
        let timer = Instant::now();
        let options = ApplyOptions {
            db: self.engine.clone(),
            region,
            abort: Arc::clone(&abort),
            write_batch_size: self.batch_size,
            coprocessor_host: self.coprocessor_host.clone(),
        };
        s.apply(options)?;

        let mut wb = self.engine.write_batch();
        region_state.set_state(PeerState::Normal);
        box_try!(wb.put_msg_cf(CF_RAFT, &region_key, &region_state));
        box_try!(wb.delete_cf(CF_RAFT, &keys::snapshot_raft_state_key(region_id)));
        wb.write().unwrap_or_else(|e| {
            panic!("{} failed to save apply_snap result: {:?}", region_id, e);
        });
        info!(
            "apply new data";
            "region_id" => region_id,
            "time_takes" => ?timer.saturating_elapsed(),
        );
        Ok(())
    }

    /// Tries to apply the snapshot of the specified Region. It calls `apply_snap` to do the actual work.
    fn handle_apply(&mut self, region_id: u64, status: Arc<AtomicUsize>) {
        let _ = status.compare_exchange(
            JOB_STATUS_PENDING,
            JOB_STATUS_RUNNING,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        SNAP_COUNTER.apply.all.inc();
        // let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        // let timer = apply_histogram.start_coarse_timer();
        let start = Instant::now();

        match self.apply_snap(region_id, Arc::clone(&status)) {
            Ok(()) => {
                status.swap(JOB_STATUS_FINISHED, Ordering::SeqCst);
                SNAP_COUNTER.apply.success.inc();
            }
            Err(Error::Abort) => {
                warn!("applying snapshot is aborted"; "region_id" => region_id);
                assert_eq!(
                    status.swap(JOB_STATUS_CANCELLED, Ordering::SeqCst),
                    JOB_STATUS_CANCELLING
                );
                SNAP_COUNTER.apply.abort.inc();
            }
            Err(e) => {
                error!(%e; "failed to apply snap!!!");
                status.swap(JOB_STATUS_FAILED, Ordering::SeqCst);
                SNAP_COUNTER.apply.fail.inc();
            }
        }

        SNAP_HISTOGRAM
            .apply
            .observe(start.saturating_elapsed_secs());
        let _ = self.router.send(region_id, CasualMessage::SnapshotApplied);
    }

    /// Cleans up the data within the range.
    fn cleanup_range(&self, ranges: &[Range<'_>]) -> Result<()> {
        self.engine
            .delete_all_in_range(DeleteStrategy::DeleteFiles, ranges)
            .unwrap_or_else(|e| {
                error!("failed to delete files in range"; "err" => %e);
            });
        self.delete_all_in_range(ranges)?;
        self.engine
            .delete_all_in_range(DeleteStrategy::DeleteBlobs, ranges)
            .unwrap_or_else(|e| {
                error!("failed to delete files in range"; "err" => %e);
            });
        Ok(())
    }

    /// Gets the overlapping ranges and cleans them up.
    fn cleanup_overlap_regions(
        &mut self,
        overlap_ranges: Vec<(u64, Vec<u8>, Vec<u8>, u64)>,
    ) -> Result<()> {
        let oldest_sequence = self
            .engine
            .get_oldest_snapshot_sequence_number()
            .unwrap_or(u64::MAX);
        let mut ranges = Vec::with_capacity(overlap_ranges.len());
        let mut df_ranges = Vec::with_capacity(overlap_ranges.len());
        for (region_id, start_key, end_key, stale_sequence) in overlap_ranges.iter() {
            // `DeleteFiles` may break current rocksdb snapshots consistency,
            // so do not use it unless we can make sure there is no reader of the destroyed peer anymore.
            if *stale_sequence < oldest_sequence {
                df_ranges.push(Range::new(start_key, end_key));
            } else {
                SNAP_COUNTER_VEC
                    .with_label_values(&["overlap", "not_delete_files"])
                    .inc();
            }
            info!("delete data in range because of overlap"; "region_id" => region_id,
                  "start_key" => log_wrappers::Value::key(start_key),
                  "end_key" => log_wrappers::Value::key(end_key));
            ranges.push(Range::new(start_key, end_key));
        }
        self.engine
            .delete_all_in_range(DeleteStrategy::DeleteFiles, &df_ranges)
            .unwrap_or_else(|e| {
                error!("failed to delete files in range"; "err" => %e);
            });

        self.delete_all_in_range(&ranges)
    }

    /// Inserts a new pending range, and it will be cleaned up with some delay.
    fn insert_pending_delete_range(&mut self, region_id: u64, start_key: &[u8], end_key: &[u8]) {
        let overlap_ranges = self
            .pending_delete_ranges
            .drain_overlap_ranges(start_key, end_key);
        if !overlap_ranges.is_empty() {
            CLEAN_COUNTER_VEC
                .with_label_values(&["overlap-with-destroy"])
                .inc();
            if let Err(e) = self.cleanup_overlap_regions(overlap_ranges) {
                warn!("cleanup_overlap_ranges failed";
                    "region_id" => region_id,
                    "start_key" => log_wrappers::Value::key(start_key),
                    "end_key" => log_wrappers::Value::key(end_key),
                    "err" => %e,
                );
            }
        }
        info!("register deleting data in range";
            "region_id" => region_id,
            "start_key" => log_wrappers::Value::key(start_key),
            "end_key" => log_wrappers::Value::key(end_key),
        );
        let seq = self.engine.get_latest_sequence_number();
        self.pending_delete_ranges
            .insert(region_id, start_key, end_key, seq);
    }

    /// Cleans up stale ranges.
    fn clean_stale_ranges(&mut self) {
        STALE_PEER_PENDING_DELETE_RANGE_GAUGE.set(self.pending_delete_ranges.len() as f64);
        if self.ingest_maybe_stall() {
            return;
        }
        let oldest_sequence = self
            .engine
            .get_oldest_snapshot_sequence_number()
            .unwrap_or(u64::MAX);
        let mut cleanup_ranges: Vec<(u64, Vec<u8>, Vec<u8>)> = self
            .pending_delete_ranges
            .stale_ranges(oldest_sequence)
            .map(|(region_id, s, e)| (region_id, s.to_vec(), e.to_vec()))
            .collect();
        if cleanup_ranges.is_empty() {
            return;
        }
        CLEAN_COUNTER_VEC.with_label_values(&["destroy"]).inc_by(1);
        cleanup_ranges.sort_by(|a, b| a.1.cmp(&b.1));
        while cleanup_ranges.len() > CLEANUP_MAX_REGION_COUNT {
            cleanup_ranges.pop();
        }
        let ranges: Vec<Range<'_>> = cleanup_ranges
            .iter()
            .map(|(region_id, start, end)| {
                info!("delete data in range because of stale"; "region_id" => region_id,
                  "start_key" => log_wrappers::Value::key(start),
                  "end_key" => log_wrappers::Value::key(end));
                Range::new(start, end)
            })
            .collect();
        if let Err(e) = self.cleanup_range(&ranges) {
            error!("failed to cleanup stale range"; "err" => %e);
            return;
        }
        for (_, key, _) in cleanup_ranges {
            assert!(
                self.pending_delete_ranges.remove(&key).is_some(),
                "cleanup pending_delete_ranges {} should exist",
                log_wrappers::Value::key(&key)
            );
        }
    }

    /// Checks the number of files at level 0 to avoid write stall after ingesting sst.
    /// Returns true if the ingestion causes write stall.
    fn ingest_maybe_stall(&self) -> bool {
        for cf in SNAPSHOT_CFS {
            // no need to check lock cf
            if plain_file_used(cf) {
                continue;
            }
            if self.engine.ingest_maybe_slowdown_writes(cf).expect("cf") {
                return true;
            }
        }
        false
    }

    fn delete_all_in_range(&self, ranges: &[Range<'_>]) -> Result<()> {
        for cf in self.engine.cf_names() {
            // CF_LOCK usually contains fewer keys than other CFs, so we delete them by key.
            let strategy = if cf == CF_LOCK {
                DeleteStrategy::DeleteByKey
            } else if self.use_delete_range {
                DeleteStrategy::DeleteByRange
            } else {
                DeleteStrategy::DeleteByWriter {
                    sst_path: self.mgr.get_temp_path_for_ingest(),
                }
            };
            box_try!(self.engine.delete_ranges_cf(cf, strategy, ranges));
        }

        Ok(())
    }
}

pub struct Runner<EK, R>
where
    EK: KvEngine,
{
    pool: ThreadPool<TaskCell>,
    ctx: SnapContext<EK, R>,
    // we may delay some apply tasks if level 0 files to write stall threshold,
    // pending_applies records all delayed apply task, and will check again later
    pending_applies: VecDeque<Task<EK::Snapshot>>,
    clean_stale_tick: usize,
    clean_stale_check_interval: Duration,
}

impl<EK, R> Runner<EK, R>
where
    EK: KvEngine,
    R: CasualRouter<EK>,
{
    pub fn new(
        engine: EK,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_range: bool,
        snap_generator_pool_size: usize,
        coprocessor_host: CoprocessorHost<EK>,
        router: R,
    ) -> Runner<EK, R> {
        Runner {
            pool: Builder::new(thd_name!("snap-generator"))
                .max_thread_count(snap_generator_pool_size)
                .build_future_pool(),
            ctx: SnapContext {
                engine,
                mgr,
                batch_size,
                use_delete_range,
                pending_delete_ranges: PendingDeleteRanges::default(),
                coprocessor_host,
                router,
            },
            pending_applies: VecDeque::new(),
            clean_stale_tick: 0,
            clean_stale_check_interval: Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL),
        }
    }

    /// Tries to apply pending tasks if there is some.
    fn handle_pending_applies(&mut self) {
        fail_point!("apply_pending_snapshot", |_| {});
        while !self.pending_applies.is_empty() {
            // should not handle too many applies than the number of files that can be ingested.
            // check level 0 every time because we can not make sure how does the number of level 0 files change.
            if self.ctx.ingest_maybe_stall() {
                break;
            }
            if let Some(Task::Apply { region_id, status }) = self.pending_applies.pop_front() {
                self.ctx.handle_apply(region_id, status);
            }
        }
    }
}

impl<EK, R> Runnable for Runner<EK, R>
where
    EK: KvEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    type Task = Task<EK::Snapshot>;

    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::Gen {
                region_id,
                last_applied_index_term,
                last_applied_state,
                kv_snap,
                canceled,
                notifier,
                for_balance,
            } => {
                // It is safe for now to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let ctx = self.ctx.clone();

                self.pool.spawn(async move {
                    tikv_alloc::add_thread_memory_accessor();
                    ctx.handle_gen(
                        region_id,
                        last_applied_index_term,
                        last_applied_state,
                        kv_snap,
                        canceled,
                        notifier,
                        for_balance,
                    );
                    tikv_alloc::remove_thread_memory_accessor();
                });
            }
            task @ Task::Apply { .. } => {
                fail_point!("on_region_worker_apply", true, |_| {});
                // to makes sure applying snapshots in order.
                self.pending_applies.push_back(task);
                self.handle_pending_applies();
                if !self.pending_applies.is_empty() {
                    // delay the apply and retry later
                    SNAP_COUNTER.apply.delay.inc()
                }
            }
            Task::Destroy {
                region_id,
                start_key,
                end_key,
            } => {
                fail_point!("on_region_worker_destroy", true, |_| {});
                // try to delay the range deletion because
                // there might be a coprocessor request related to this range
                self.ctx
                    .insert_pending_delete_range(region_id, &start_key, &end_key);
                self.ctx.clean_stale_ranges();
            }
        }
    }

    fn shutdown(&mut self) {
        self.pool.shutdown();
    }
}

impl<EK, R> RunnableWithTimer for Runner<EK, R>
where
    EK: KvEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    fn on_timeout(&mut self) {
        self.handle_pending_applies();
        self.clean_stale_tick += 1;
        if self.clean_stale_tick >= STALE_PEER_CHECK_TICK {
            self.ctx.clean_stale_ranges();
            self.clean_stale_tick = 0;
        }
    }

    fn get_interval(&self) -> Duration {
        self.clean_stale_check_interval
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        sync::{atomic::AtomicUsize, mpsc, Arc},
        thread,
        time::Duration,
    };

    use engine_test::{
        ctor::{CFOptions, ColumnFamilyOptions},
        kv::{KvTestEngine, KvTestSnapshot},
    };
    use engine_traits::{
        CompactExt, FlowControlFactorsExt, KvEngine, MiscExt, Mutable, Peekable,
        RaftEngineReadOnly, SyncMutable, WriteBatch, WriteBatchExt, CF_DEFAULT,
    };
    use keys::data_key;
    use kvproto::raft_serverpb::{PeerState, RaftApplyState, RegionLocalState};
    use tempfile::Builder;
    use tikv_util::worker::{LazyWorker, Worker};

    use super::*;
    use crate::{
        coprocessor::CoprocessorHost,
        store::{
            peer_storage::JOB_STATUS_PENDING, snap::tests::get_test_db_for_regions,
            worker::RegionRunner, CasualMessage, SnapKey, SnapManager,
        },
    };

    fn insert_range(
        pending_delete_ranges: &mut PendingDeleteRanges,
        id: u64,
        s: &str,
        e: &str,
        stale_sequence: u64,
    ) {
        pending_delete_ranges.insert(id, s.as_bytes(), e.as_bytes(), stale_sequence);
    }

    #[test]
    #[allow(clippy::string_lit_as_bytes)]
    fn test_pending_delete_ranges() {
        let mut pending_delete_ranges = PendingDeleteRanges::default();
        let id = 0;

        let timeout1 = 10;
        insert_range(&mut pending_delete_ranges, id, "a", "c", timeout1);
        insert_range(&mut pending_delete_ranges, id, "m", "n", timeout1);
        insert_range(&mut pending_delete_ranges, id, "x", "z", timeout1);
        insert_range(&mut pending_delete_ranges, id + 1, "f", "i", timeout1);
        insert_range(&mut pending_delete_ranges, id + 1, "p", "t", timeout1);
        assert_eq!(pending_delete_ranges.len(), 5);

        //  a____c    f____i    m____n    p____t    x____z
        //              g___________________q
        // when we want to insert [g, q), we first extract overlap ranges,
        // which are [f, i), [m, n), [p, t)
        let timeout2 = 12;
        let overlap_ranges = pending_delete_ranges.drain_overlap_ranges(b"g", b"q");
        assert_eq!(
            overlap_ranges,
            [
                (id + 1, b"f".to_vec(), b"i".to_vec(), timeout1),
                (id, b"m".to_vec(), b"n".to_vec(), timeout1),
                (id + 1, b"p".to_vec(), b"t".to_vec(), timeout1),
            ]
        );
        assert_eq!(pending_delete_ranges.len(), 2);
        insert_range(&mut pending_delete_ranges, id + 2, "g", "q", timeout2);
        assert_eq!(pending_delete_ranges.len(), 3);

        // at t1, [a, c) and [x, z) will timeout
        {
            let now = 11;
            let ranges: Vec<_> = pending_delete_ranges.stale_ranges(now).collect();
            assert_eq!(
                ranges,
                [
                    (id, "a".as_bytes(), "c".as_bytes()),
                    (id, "x".as_bytes(), "z".as_bytes()),
                ]
            );
            for start_key in ranges
                .into_iter()
                .map(|(_, start, _)| start.to_vec())
                .collect::<Vec<Vec<u8>>>()
            {
                pending_delete_ranges.remove(&start_key);
            }
            assert_eq!(pending_delete_ranges.len(), 1);
        }

        // at t2, [g, q) will timeout
        {
            let now = 14;
            let ranges: Vec<_> = pending_delete_ranges.stale_ranges(now).collect();
            assert_eq!(ranges, [(id + 2, "g".as_bytes(), "q".as_bytes())]);
            for start_key in ranges
                .into_iter()
                .map(|(_, start, _)| start.to_vec())
                .collect::<Vec<Vec<u8>>>()
            {
                pending_delete_ranges.remove(&start_key);
            }
            assert_eq!(pending_delete_ranges.len(), 0);
        }
    }

    #[test]
    fn test_stale_peer() {
        let temp_dir = Builder::new().prefix("test_stale_peer").tempdir().unwrap();
        let engine = get_test_db_for_regions(&temp_dir, None, None, None, &[1]).unwrap();

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("region-worker");
        let mut worker: LazyWorker<Task<KvTestSnapshot>> = bg_worker.lazy_build("region-worker");
        let sched = worker.scheduler();
        let (router, _) = mpsc::sync_channel(11);
        let mut runner = RegionRunner::new(
            engine.kv.clone(),
            mgr,
            0,
            false,
            2,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        runner.clean_stale_check_interval = Duration::from_millis(100);

        let mut ranges = vec![];
        for i in 0..10 {
            let mut key = b"k0".to_vec();
            key.extend_from_slice(i.to_string().as_bytes());
            engine.kv.put(&key, b"v1").unwrap();
            ranges.push(key);
        }
        engine.kv.put(b"k1", b"v1").unwrap();
        let snap = engine.kv.snapshot();
        engine.kv.put(b"k2", b"v2").unwrap();

        sched
            .schedule(Task::Destroy {
                region_id: 1,
                start_key: b"k1".to_vec(),
                end_key: b"k2".to_vec(),
            })
            .unwrap();
        for i in 0..9 {
            sched
                .schedule(Task::Destroy {
                    region_id: i as u64 + 2,
                    start_key: ranges[i].clone(),
                    end_key: ranges[i + 1].clone(),
                })
                .unwrap();
        }
        worker.start_with_timer(runner);
        thread::sleep(Duration::from_millis(20));
        drop(snap);
        thread::sleep(Duration::from_millis(200));
        assert!(engine.kv.get_value(b"k1").unwrap().is_none());
        assert_eq!(engine.kv.get_value(b"k2").unwrap().unwrap(), b"v2");
        for i in 0..9 {
            assert!(engine.kv.get_value(&ranges[i]).unwrap().is_none());
        }
    }

    #[test]
    fn test_pending_applies() {
        let temp_dir = Builder::new()
            .prefix("test_pending_applies")
            .tempdir()
            .unwrap();

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_slowdown_writes_trigger(5);
        cf_opts.set_disable_auto_compactions(true);
        let kv_cfs_opts = vec![
            CFOptions::new("default", cf_opts.clone()),
            CFOptions::new("write", cf_opts.clone()),
            CFOptions::new("lock", cf_opts.clone()),
            CFOptions::new("raft", cf_opts.clone()),
        ];
        let engine = get_test_db_for_regions(
            &temp_dir,
            None,
            None,
            Some(kv_cfs_opts),
            &[1, 2, 3, 4, 5, 6, 7],
        )
        .unwrap();

        for cf_name in &["default", "write", "lock"] {
            for i in 0..7 {
                engine
                    .kv
                    .put_cf(cf_name, &data_key(i.to_string().as_bytes()), &[i])
                    .unwrap();
                engine
                    .kv
                    .put_cf(cf_name, &data_key((i + 1).to_string().as_bytes()), &[i + 1])
                    .unwrap();
                engine.kv.flush_cf(cf_name, true).unwrap();
                // check level 0 files
                assert_eq!(
                    engine
                        .kv
                        .get_cf_num_files_at_level(cf_name, 0)
                        .unwrap()
                        .unwrap(),
                    u64::from(i) + 1
                );
            }
        }

        let snap_dir = Builder::new().prefix("snap_dir").tempdir().unwrap();
        let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
        let bg_worker = Worker::new("snap-manager");
        let mut worker = bg_worker.lazy_build("snap-manager");
        let sched = worker.scheduler();
        let (router, receiver) = mpsc::sync_channel(1);
        let runner = RegionRunner::new(
            engine.kv.clone(),
            mgr,
            0,
            true,
            2,
            CoprocessorHost::<KvTestEngine>::default(),
            router,
        );
        worker.start_with_timer(runner);

        let gen_and_apply_snap = |id: u64| {
            // construct snapshot
            let (tx, rx) = mpsc::sync_channel(1);
            let apply_state: RaftApplyState = engine
                .kv
                .get_msg_cf(CF_RAFT, &keys::apply_state_key(id))
                .unwrap()
                .unwrap();
            let idx = apply_state.get_applied_index();
            let entry = engine.raft.get_entry(id, idx).unwrap().unwrap();
            sched
                .schedule(Task::Gen {
                    region_id: id,
                    kv_snap: engine.kv.snapshot(),
                    last_applied_index_term: entry.get_term(),
                    last_applied_state: apply_state,
                    canceled: Arc::new(AtomicBool::new(false)),
                    notifier: tx,
                    for_balance: false,
                })
                .unwrap();
            let s1 = rx.recv().unwrap();
            match receiver.recv() {
                Ok((region_id, CasualMessage::SnapshotGenerated)) => {
                    assert_eq!(region_id, id);
                }
                msg => panic!("expected SnapshotGenerated, but got {:?}", msg),
            }
            let data = s1.get_data();
            let key = SnapKey::from_snap(&s1).unwrap();
            let mgr = SnapManager::new(snap_dir.path().to_str().unwrap());
            let mut s2 = mgr.get_snapshot_for_sending(&key).unwrap();
            let mut s3 = mgr.get_snapshot_for_receiving(&key, data).unwrap();
            io::copy(&mut s2, &mut s3).unwrap();
            s3.save().unwrap();

            // set applying state
            let mut wb = engine.kv.write_batch();
            let region_key = keys::region_state_key(id);
            let mut region_state = engine
                .kv
                .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_key)
                .unwrap()
                .unwrap();
            region_state.set_state(PeerState::Applying);
            wb.put_msg_cf(CF_RAFT, &region_key, &region_state).unwrap();
            wb.write().unwrap();

            // apply snapshot
            let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
            sched
                .schedule(Task::Apply {
                    region_id: id,
                    status,
                })
                .unwrap();
        };
        let destroy_region = |id: u64| {
            let start_key = data_key(id.to_string().as_bytes());
            let end_key = data_key((id + 1).to_string().as_bytes());
            // destroy region
            sched
                .schedule(Task::Destroy {
                    region_id: id,
                    start_key,
                    end_key,
                })
                .unwrap();
        };

        let check_region_exist = |id: u64| -> bool {
            let key = data_key(id.to_string().as_bytes());
            let v = engine.kv.get_value(&key).unwrap();
            v.is_some()
        };

        let wait_apply_finish = |ids: &[u64]| {
            for id in ids {
                match receiver.recv_timeout(Duration::from_secs(5)) {
                    Ok((region_id, CasualMessage::SnapshotApplied)) => {
                        assert_eq!(region_id, *id);
                    }
                    msg => panic!("expected {} SnapshotApplied, but got {:?}", id, msg),
                }
                let region_key = keys::region_state_key(*id);
                assert_eq!(
                    engine
                        .kv
                        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_key)
                        .unwrap()
                        .unwrap()
                        .get_state(),
                    PeerState::Normal
                )
            }
        };

        // snapshot will not ingest cause already write stall
        gen_and_apply_snap(1);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            7
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );

        wait_apply_finish(&[1]);

        // the pending apply task should be finished and snapshots are ingested.
        // note that when ingest sst, it may flush memtable if overlap,
        // so here will two level 0 files.
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            2
        );

        // no write stall, ingest without delay
        gen_and_apply_snap(2);
        wait_apply_finish(&[2]);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // snapshot will not ingest cause it may cause write stall
        gen_and_apply_snap(3);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );
        gen_and_apply_snap(4);
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );
        gen_and_apply_snap(5);
        destroy_region(6);
        thread::sleep(Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL * 2));
        assert!(check_region_exist(6));
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // compact all files to the bottomest level
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );

        // make sure have checked pending applies
        wait_apply_finish(&[3, 4]);

        // before two pending apply tasks should be finished and snapshots are ingested
        // and one still in pending.
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            4
        );

        // make sure have checked pending applies
        engine.kv.compact_files_in_range(None, None, None).unwrap();
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            0
        );
        wait_apply_finish(&[5]);

        // the last one pending task finished
        assert_eq!(
            engine
                .kv
                .get_cf_num_files_at_level(CF_DEFAULT, 0)
                .unwrap()
                .unwrap(),
            2
        );
        thread::sleep(Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL * 2));
        assert!(!check_region_exist(6));
    }
}
