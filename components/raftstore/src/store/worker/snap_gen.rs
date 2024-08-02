// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::{self, Display, Formatter},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::SyncSender,
        Arc,
    },
    u64,
};

use collections::HashMap;
use engine_traits::KvEngine;
use fail::fail_point;
use file_system::{IoType, WithIoType};
use kvproto::raft_serverpb::RaftApplyState;
use pd_client::PdClient;
use raft::eraftpb::Snapshot as RaftSnapshot;
use tikv_util::{
    box_try,
    config::VersionTrack,
    error, info,
    time::{Instant, UnixSecs},
    worker::Runnable,
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
};

use super::metrics::*;
use crate::store::{
    self, snap::Result, transport::CasualRouter, CasualMessage, Config, SnapManager,
};

const SNAP_GENERATOR_MAX_POOL_SIZE: usize = 16;

const TIFLASH: &str = "tiflash";
const ENGINE: &str = "engine";

/// Defines the snapshot generation task.
#[derive(Debug)]
pub enum Task<S> {
    Gen {
        region_id: u64,
        last_applied_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: S,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
        to_store_id: u64,
    },
}

impl<S> Display for Task<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
        }
    }
}

struct SnapGenContext<EK, R> {
    engine: EK,
    mgr: SnapManager,
    router: R,
    start: UnixSecs,
}

impl<EK, R> SnapGenContext<EK, R>
where
    EK: KvEngine,
    R: CasualRouter<EK>,
{
    /// Generates the snapshot of the Region.
    fn generate_snap(
        &self,
        region_id: u64,
        last_applied_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: EK::Snapshot,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
        allow_multi_files_snapshot: bool,
    ) -> Result<()> {
        // do we need to check leader here?
        let snap = box_try!(store::do_snapshot::<EK>(
            self.mgr.clone(),
            &self.engine,
            kv_snap,
            region_id,
            last_applied_term,
            last_applied_state,
            for_balance,
            allow_multi_files_snapshot,
            self.start
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
        last_applied_term: u64,
        last_applied_state: RaftApplyState,
        kv_snap: EK::Snapshot,
        canceled: Arc<AtomicBool>,
        notifier: SyncSender<RaftSnapshot>,
        for_balance: bool,
        allow_multi_files_snapshot: bool,
    ) {
        fail_point!("before_region_gen_snap", |_| ());
        SNAP_COUNTER.generate.start.inc();
        if canceled.load(Ordering::Relaxed) {
            info!("generate snap is canceled"; "region_id" => region_id);
            SNAP_COUNTER.generate.abort.inc();
            return;
        }

        let start = Instant::now();
        let _io_type_guard = WithIoType::new(if for_balance {
            IoType::LoadBalance
        } else {
            IoType::Replication
        });

        if let Err(e) = self.generate_snap(
            region_id,
            last_applied_term,
            last_applied_state,
            kv_snap,
            notifier,
            for_balance,
            allow_multi_files_snapshot,
        ) {
            error!(%e; "failed to generate snap!!!"; "region_id" => region_id,);
            SNAP_COUNTER.generate.fail.inc();
            return;
        }

        SNAP_COUNTER.generate.success.inc();
        SNAP_HISTOGRAM
            .generate
            .observe(start.saturating_elapsed_secs());
    }
}

pub struct Runner<EK, R, T>
where
    EK: KvEngine,
    T: PdClient + 'static,
{
    tiflash_stores: HashMap<u64, bool>,

    engine: EK,
    mgr: SnapManager,
    router: R,
    pd_client: Option<Arc<T>>,
    pool: FuturePool,
}

impl<EK, R, T> Runner<EK, R, T>
where
    EK: KvEngine,
    R: CasualRouter<EK>,
    T: PdClient + 'static,
{
    pub fn new(
        engine: EK,
        mgr: SnapManager,
        cfg: Arc<VersionTrack<Config>>,
        router: R,
        pd_client: Option<Arc<T>>,
    ) -> Runner<EK, R, T> {
        Runner {
            tiflash_stores: HashMap::default(),
            engine,
            mgr,
            router,
            pd_client,
            pool: YatpPoolBuilder::new(DefaultTicker::default())
                .name_prefix("snap-generator")
                .thread_count(
                    1,
                    cfg.value().snap_generator_pool_size,
                    SNAP_GENERATOR_MAX_POOL_SIZE,
                )
                .build_future_pool(),
        }
    }

    pub fn snap_generator_pool(&self) -> FuturePool {
        self.pool.clone()
    }
}

impl<EK, R, T> Runnable for Runner<EK, R, T>
where
    EK: KvEngine,
    R: CasualRouter<EK> + Send + Clone + 'static,
    T: PdClient,
{
    type Task = Task<EK::Snapshot>;

    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::Gen {
                region_id,
                last_applied_term,
                last_applied_state,
                kv_snap,
                canceled,
                notifier,
                for_balance,
                to_store_id,
            } => {
                // It is safe for now to handle generating and applying snapshot concurrently,
                // but it may not when merge is implemented.
                let mut allow_multi_files_snapshot = false;
                // if to_store_id is 0, it means the to_store_id cannot be found
                if to_store_id != 0 {
                    if let Some(is_tiflash) = self.tiflash_stores.get(&to_store_id) {
                        allow_multi_files_snapshot = !is_tiflash;
                    } else {
                        let is_tiflash = self.pd_client.as_ref().map_or(false, |pd_client| {
                            if let Ok(s) = pd_client.get_store(to_store_id) {
                                return s.get_labels().iter().any(|label| {
                                    label.get_key().to_lowercase() == ENGINE
                                        && label.get_value().to_lowercase() == TIFLASH
                                });
                            }
                            true
                        });
                        self.tiflash_stores.insert(to_store_id, is_tiflash);
                        allow_multi_files_snapshot = !is_tiflash;
                    }
                }
                SNAP_COUNTER.generate.all.inc();
                let ctx = SnapGenContext {
                    engine: self.engine.clone(),
                    mgr: self.mgr.clone(),
                    router: self.router.clone(),
                    start: UnixSecs::now(),
                };
                let scheduled_time = Instant::now_coarse();
                self.pool.spawn(async move {
                    SNAP_GEN_WAIT_DURATION_HISTOGRAM
                        .observe(scheduled_time.saturating_elapsed_secs());

                    ctx.handle_gen(
                        region_id,
                        last_applied_term,
                        last_applied_state,
                        kv_snap,
                        canceled,
                        notifier,
                        for_balance,
                        allow_multi_files_snapshot,
                    );
                }).unwrap_or_else(
                    |e| {
                        error!("failed to generate snapshot"; "region_id" => region_id, "err" => ?e);
                        SNAP_COUNTER.generate.fail.inc();
                    },
                );
            }
        }
    }
}
