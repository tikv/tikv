// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! TODO: document
use std::{
    collections::{HashMap, VecDeque},
    fmt, fs,
    path::{Path, PathBuf},
    slice,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::SyncSender,
        Arc,
    },
    time::Duration,
};

use engine_traits::{
    DeleteStrategy, KvEngine, OpenOptions, RaftEngine, Range, TabletFactory, CF_RAFT,
};
use file_system::{IoType, WithIoType};
use into_other::into_other;
use kvproto::raft_serverpb::{PeerState, RaftApplyState, RaftSnapshotData, RegionLocalState};
use protobuf::Message;
use raft::eraftpb::{Snapshot as RaftSnapshot, Snapshot};
use raftstore::{
    store::{
        check_abort, storage_error,
        transport::CasualRouter,
        util,
        worker_metrics::{SNAP_COUNTER, SNAP_HISTOGRAM},
        ApplyOptions, CasualMessage, EntryStorage, SnapEntry, SnapKey, SnapManager,
    },
    Result,
};
use tikv_util::{
    box_err, box_try, debug, defer, error, info, thd_name,
    time::Instant,
    warn,
    worker::{Runnable, RunnableWithTimer},
};
use yatp::{
    pool::{Builder, ThreadPool},
    task::future::TaskCell,
};

#[cfg(test)]
pub const STALE_PEER_CHECK_TICK: usize = 1; // 1000 milliseconds

#[cfg(not(test))]
pub const STALE_PEER_CHECK_TICK: usize = 10; // 10000 milliseconds

// used to periodically check whether schedule pending applies in region runner
#[cfg(not(test))]
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 1_000; // 1000 milliseconds
#[cfg(test)]
pub const PENDING_APPLY_CHECK_INTERVAL: u64 = 200; // 200 milliseconds

/// Region snapshot related task
#[derive(Debug)]
pub enum Task {
    Gen {
        region_id: u64,
        tablet_suffix: u64,
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
        tablet_suffix: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snapshot gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy {
                region_id,
                tablet_suffix,
                ref start_key,
                ref end_key,
            } => write!(
                f,
                "Destroy {}_{} [{}, {})",
                region_id,
                tablet_suffix,
                log_wrappers::Value::key(start_key),
                log_wrappers::Value::key(end_key)
            ),
        }
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine, R> {
    ctx: SnapContext<EK, ER, R>,
    pool: ThreadPool<TaskCell>,
    // we may delay some apply tasks if level 0 files to write stall threshold,
    // pending_applies records all delayed apply task, and will check again later
    pending_applies: VecDeque<Task>,
    clean_stale_tick: usize,
    clean_stale_check_interval: Duration,
}

impl<EK, ER, R> Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(
        raft_engine: ER,
        table_factory: Arc<dyn TabletFactory<EK>>,
        mgr: SnapManager,
        batch_size: usize,
        use_delete_range: bool,
        snap_generator_pool_size: usize,
        router: R,
    ) -> Self {
        Self {
            pool: Builder::new(thd_name!("snap-generator"))
                .max_thread_count(snap_generator_pool_size)
                .build_future_pool(),
            ctx: SnapContext {
                raft_engine,
                table_factory,
                mgr,
                batch_size,
                use_delete_range,
                pending_delete_ranges: PendingDeleteRanges::default(),
                router,
            },
            pending_applies: VecDeque::new(),
            clean_stale_tick: 0,
            clean_stale_check_interval: Duration::from_millis(PENDING_APPLY_CHECK_INTERVAL),
        }
    }
}

impl<EK: KvEngine, ER, R> Runnable for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    type Task = Task;
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen {
                region_id,
                tablet_suffix,
                canceled,
                notifier,
                for_balance,
            } => {
                // It is safe to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let ctx = self.ctx.clone();

                self.pool.spawn(async move {
                    tikv_alloc::add_thread_memory_accessor();
                    ctx.handle_gen(region_id, tablet_suffix, canceled, notifier, for_balance);
                    tikv_alloc::remove_thread_memory_accessor();
                });
            }
            task @ Task::Apply { .. } => {
                // to makes sure applying snapshots in order.
                unimplemented!();
            }
            Task::Destroy {
                region_id,
                tablet_suffix,
                start_key,
                end_key,
            } => {
                // try to delay the range deletion because
                // there might be a coprocessor request related to this range
                unimplemented!();
            }
        }
    }
    fn shutdown(&mut self) {
        unimplemented!()
    }
}

impl<EK, ER, R> RunnableWithTimer for Runner<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
{
    fn on_timeout(&mut self) {
        // self.handle_pending_applies();
        self.clean_stale_tick += 1;
        if self.clean_stale_tick >= STALE_PEER_CHECK_TICK {
            self.ctx.clean_stale_tablets();
            self.clean_stale_tick = 0;
        }
    }

    fn get_interval(&self) -> Duration {
        self.clean_stale_check_interval
    }
}

#[derive(Clone)]
struct StalePeerInfo {
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    // Once the oldest snapshot sequence exceeds this, it ensures that no one is
    // reading on this peer anymore. So we can safely call `delete_files_in_range`
    // , which may break the consistency of snapshot, of this peer range.
    pub stale_sequence: u64,
}

/// (region_id, suffix)
pub type Key = (u64, u64);

/// A structure records all ranges to be deleted with some delay.
/// The delay is because there may be some coprocessor requests related to these
/// ranges.
type PendingDeleteRanges = HashMap<Key, Vec<StalePeerInfo>>;

#[derive(Clone)]
struct SnapContext<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    raft_engine: ER,
    table_factory: Arc<dyn TabletFactory<EK>>,
    batch_size: usize,
    mgr: SnapManager,
    use_delete_range: bool,
    pending_delete_ranges: PendingDeleteRanges,
    router: R,
}

impl<EK, ER, R> SnapContext<EK, ER, R>
where
    EK: KvEngine,
    ER: RaftEngine,
    R: CasualRouter<EK>,
{
    /// Generates the snapshot of the Region.
    fn generate_snap(
        &self,
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) -> Result<()> {
        // do we need to check leader here?
        let snap = box_try!(do_snapshot(
            self.raft_engine.clone(),
            self.mgr.clone(),
            self.table_factory.clone(),
            region_id,
            tablet_suffix,
            for_balance,
            canceled,
        ));
        // Only enable the fail point when the region id is equal to 1, which is
        // the id of bootstrapped region in tests.

        if let Err(e) = notifier.try_send(snap) {
            info!(
                "failed to notify snap result, leadership may have changed, ignore error";
                "region_id" => region_id,
                "err" => %e,
            );
        }
        // The error can be ignored as snapshot will be sent in next heartbeat in the
        // end.
        let _ = self
            .router
            .send(region_id, CasualMessage::SnapshotGenerated);
        Ok(())
    }

    /// Handles the task of generating snapshot of the Region. It calls
    /// `generate_snap` to do the actual work.
    fn handle_gen(
        &self,
        region_id: u64,
        tablet_suffix: u64,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
    ) {
        SNAP_COUNTER.generate.all.inc();
        if canceled.load(Ordering::Relaxed) {
            info!("generate snap is canceled"; "region_id" => region_id);
            return;
        }

        let start = Instant::now();
        let _io_type_guard = WithIoType::new(if for_balance {
            IoType::LoadBalance
        } else {
            IoType::Replication
        });

        if let Err(e) =
            self.generate_snap(region_id, tablet_suffix, canceled, notifier, for_balance)
        {
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
        unimplemented!();
    }

    /// Tries to apply the snapshot of the specified Region. It calls
    /// `apply_snap` to do the actual work.
    fn handle_apply(&mut self, region_id: u64, status: Arc<AtomicUsize>) {
        unimplemented!();
    }

    /// Cleans up the data within the range.
    fn cleanup_range(
        tablet: &EK,
        ranges: &[Range],
        mgr: &SnapManager,
        use_delete_range: bool,
    ) -> Result<()> {
        unimplemented!()
    }

    /// Inserts a new pending range, and it will be cleaned up with some delay.
    fn insert_pending_delete_range(
        &mut self,
        tablet: &EK,
        region_id: u64,
        tablet_suffix: u64,
        start_key: &[u8],
        end_key: &[u8],
    ) {
        info!("register deleting data in range";
            "region_id" => region_id,
            "start_key" => log_wrappers::Value::key(start_key),
            "end_key" => log_wrappers::Value::key(end_key),
        );
        let seq = tablet.get_latest_sequence_number();
        self.pending_delete_ranges
            .entry((region_id, tablet_suffix))
            .or_insert_with(Default::default)
            .push(StalePeerInfo {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                stale_sequence: seq,
            });
    }

    fn clean_stale_tablets(&self) {
        let root = self.table_factory.tablets_path();
        let dir = match std::fs::read_dir(&root) {
            Ok(dir) => dir,
            Err(e) => {
                info!("skip cleaning stale tablets: {:?}", e);
                return;
            }
        };
        for path in dir.flatten() {
            let file_name = path.file_name().into_string().unwrap();
            let mut parts = file_name.split('_');
            let region_id = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            // let suffix = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            // if region_id == 0 && suffix == 0 {
            //     continue;
            // };
            let (region_id, suffix) = match (
                parts.next().map(|p| p.parse()),
                parts.next().map(|p| p.parse()),
            ) {
                (Some(Ok(r)), Some(Ok(s))) => (r, s),
                _ => continue,
            };
            if self
                .table_factory
                .open_tablet(
                    region_id,
                    Some(suffix),
                    OpenOptions::default().set_create_new(true),
                )
                .is_ok()
            {
                continue;
            }
            if self.table_factory.is_tombstoned(region_id, suffix) {
                if let Err(e) = self.table_factory.destroy_tablet(region_id, suffix) {
                    info!("failed to destroy tablet {} {}: {:?}", region_id, suffix, e);
                }
            }
        }
    }
}

pub fn do_snapshot<EK, ER>(
    raft_engine: ER,
    mgr: SnapManager,
    tablet_factory: Arc<dyn TabletFactory<EK>>,
    region_id: u64,
    tablet_suffix: u64,
    for_balance: bool,
    canceled: Arc<AtomicBool>,
) -> raft::Result<Snapshot>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    debug!(
        "begin to generate a snapshot";
        "region_id" => region_id,
    );

    // FIXME: remove me after snap flow control.
    if mgr.stats().sending_count >= 32 {
        canceled.store(true, Ordering::Relaxed);
        return Err(storage_error(format!(
            "{} too many sending snapshots",
            region_id
        )));
    }

    info!(
        "generating snapshot";
        "region_id" => region_id,
        "tablet_suffix" => tablet_suffix,
    );

    let path = mgr.get_temp_path_for_build(region_id);
    defer!({
        if path.exists() {
            fs::remove_dir_all(&path).unwrap();
        }
    });
    match tablet_factory.open_tablet(
        region_id,
        Some(tablet_suffix),
        OpenOptions::default().set_cache_only(true),
    ) {
        // TODO: flush is not necessary if atomic flush is enabled.
        Ok(tablet) => tablet.checkpoint_to(slice::from_ref(&path), 0).unwrap(),
        Err(e) => {
            return Err(storage_error(format!(
                "{} {} don't exist anymore, error {}",
                region_id, tablet_suffix, e
            )));
        }
    }
    let mut tablet = match tablet_factory.open_tablet_raw(
        Path::new(&path),
        region_id,
        tablet_suffix,
        OpenOptions::default().set_read_only(true),
    ) {
        Ok(t) => t,
        Err(e) => {
            return Err(storage_error(format!(
                "failed to open tablet at {}, error {}",
                path.display(),
                e
            )));
        }
    };

    let msg = tablet
        .get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))
        .map_err(into_other::<_, raft::Error>)?;
    let apply_state: RaftApplyState = match msg {
        None => {
            return Err(storage_error(format!(
                "could not load raft state of region {}",
                region_id
            )));
        }
        Some(state) => state,
    };

    let apply_index = apply_state.get_applied_index();
    let apply_term = if apply_index == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match raft_engine.get_entry(region_id, apply_index) {
            Ok(Some(e)) => e.term,
            Ok(None) => {
                return Err(storage_error(format!(
                    "could not load entry at {}",
                    apply_index
                )));
            }
            Err(e) => return Err(into_other::<_, raft::Error>(e)),
        }
    };

    let key = SnapKey::new(region_id, apply_term, apply_index);
    let final_path = mgr.get_final_name_for_build(&key);
    // TODO: perhaps should check if exists before creating checkpoint.
    let reused = tablet_factory.exists_raw(&final_path);
    if reused {
        drop(tablet);
        tablet = match tablet_factory.open_tablet_raw(
            &final_path,
            region_id,
            tablet_suffix,
            OpenOptions::default().set_read_only(true),
        ) {
            Ok(t) => t,
            Err(e) => {
                return Err(storage_error(format!(
                    "failed to open tablet at {}, error {}",
                    final_path.display(),
                    e
                )));
            }
        };
    }

    mgr.register(key.clone(), SnapEntry::Generating);
    defer!(mgr.deregister(&key, &SnapEntry::Generating));

    let state: RegionLocalState = tablet
        .get_msg_cf(CF_RAFT, &keys::region_state_key(key.region_id))
        .and_then(|res| match res {
            None => Err(box_err!("region {} could not find region info", region_id)),
            Some(state) => Ok(state),
        })
        .map_err(into_other::<_, raft::Error>)?;

    if state.get_state() != PeerState::Normal {
        return Err(storage_error(format!(
            "snap job for {} seems stale, skip.",
            region_id
        )));
    }

    let mut snapshot = Snapshot::default();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let conf_state = util::conf_state_from_region(state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(state.get_region().clone());
    snap_data.mut_meta().set_for_balance(for_balance);
    let v = snap_data.write_to_bytes()?;
    snapshot.set_data(v.into());

    drop(tablet);
    if !reused {
        fs::rename(&path, &final_path).unwrap();
    }

    Ok(snapshot)
}
