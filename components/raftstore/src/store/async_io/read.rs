// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::raft_serverpb::{PeerState, RaftSnapshotData, RegionLocalState};
use protobuf::Message;
use raft::{eraftpb::Snapshot, GetEntriesContext};
use tikv_util::{defer, error, info, time::Instant, worker::Runnable};

use crate::store::{
    util, RaftlogFetchResult, SnapEntry, SnapKey, SnapManager, MAX_INIT_ENTRY_COUNT,
    worker::{SNAP_COUNTER, SNAP_HISTOGRAM}
};

pub enum ReadTask<EK> {
    FetchLogs {
        region_id: u64,
        context: GetEntriesContext,
        low: u64,
        high: u64,
        max_size: usize,
        tried_cnt: usize,
        term: u64,
    },

    // GenTabletSnapshot is used to generate tablet snapshot.
    GenTabletSnapshot {
        region_id: u64,
        tablet: EK,
        region_state: RegionLocalState,
        last_applied_term: u64,
        last_applied_index: u64,
        canceled: Arc<AtomicBool>,
        for_balance: bool,
    },
}

impl<EK> fmt::Display for ReadTask<EK> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadTask::FetchLogs {
                region_id,
                context,
                low,
                high,
                max_size,
                tried_cnt,
                term,
            } => write!(
                f,
                "Fetch Raft Logs [region: {}, low: {}, high: {}, max_size: {}] for sending with context {:?}, tried: {}, term: {}",
                region_id, low, high, max_size, context, tried_cnt, term,
            ),
            ReadTask::GenTabletSnapshot { region_id, .. } => {
                write!(f, "Snapshot gen for {}", region_id)
            }
        }
    }
}

#[derive(Debug)]
pub struct FetchedLogs {
    pub context: GetEntriesContext,
    pub logs: Box<RaftlogFetchResult>,
}

#[derive(Debug)]
pub struct GenSnapRes {
    pub snapshot: Box<Snapshot>,
    pub success: bool,
}

/// A router for receiving fetched result.
pub trait AsyncReadNotifier: Send {
    fn notify_logs_fetched(&self, region_id: u64, fetched: FetchedLogs);
    fn notify_snapshot_generated(&self, region_id: u64, res: GenSnapRes);
}

pub struct ReadRunner<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: AsyncReadNotifier,
{
    notifier: N,
    raft_engine: ER,
    mgr: Option<SnapManager>,
    _phantom: PhantomData<EK>,
}

impl<EK: KvEngine, ER: RaftEngine, N: AsyncReadNotifier> ReadRunner<EK, ER, N> {
    pub fn new(notifier: N, raft_engine: ER) -> ReadRunner<EK, ER, N> {
        ReadRunner {
            notifier,
            raft_engine,
            mgr: None,
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn set_snap_mgr(&mut self, mgr: SnapManager) {
        self.mgr = Some(mgr);
    }

    #[inline]
    fn snap_mgr(&self) -> &SnapManager {
        self.mgr.as_ref().unwrap()
    }
}

impl<EK, ER, N> Runnable for ReadRunner<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: AsyncReadNotifier,
{
    type Task = ReadTask<EK>;
    fn run(&mut self, task: ReadTask<EK>) {
        match task {
            ReadTask::FetchLogs {
                region_id,
                low,
                high,
                max_size,
                context,
                tried_cnt,
                term,
            } => {
                let mut ents =
                    Vec::with_capacity(std::cmp::min((high - low) as usize, MAX_INIT_ENTRY_COUNT));
                let res = self.raft_engine.fetch_entries_to(
                    region_id,
                    low,
                    high,
                    Some(max_size),
                    &mut ents,
                );

                let hit_size_limit = res
                    .as_ref()
                    .map(|c| (*c as u64) != high - low)
                    .unwrap_or(false);
                fail_point!("worker_async_fetch_raft_log");
                self.notifier.notify_logs_fetched(
                    region_id,
                    FetchedLogs {
                        context,
                        logs: Box::new(RaftlogFetchResult {
                            ents: res.map(|_| ents).map_err(|e| e.into()),
                            low,
                            max_size: max_size as u64,
                            hit_size_limit,
                            tried_cnt,
                            term,
                        }),
                    },
                );
            }

            ReadTask::GenTabletSnapshot {
                region_id,
                tablet,
                region_state,
                last_applied_term,
                last_applied_index,
                canceled,
                for_balance,
            } => {
                if canceled.load(Ordering::Relaxed) {
                    info!("generate snap is canceled"; "region_id" => region_id);
                    return;
                }
                let start = Instant::now();
    
                // the state should already checked in apply workers.
                assert_ne!(region_state.get_state(), PeerState::Tombstone);
                let key = SnapKey::new(region_id, last_applied_term, last_applied_index);
                self.snap_mgr().register(key.clone(), SnapEntry::Generating);
                defer!(self.snap_mgr().deregister(&key, &SnapEntry::Generating));

                let mut success = true;
                let mut snapshot = Snapshot::default();
                // Set snapshot metadata.
                snapshot.mut_metadata().set_index(key.idx);
                snapshot.mut_metadata().set_term(key.term);

                let conf_state = util::conf_state_from_region(region_state.get_region());
                snapshot.mut_metadata().set_conf_state(conf_state);

                // Set snapshot data.
                let mut snap_data = RaftSnapshotData::default();
                snap_data.set_region(region_state.get_region().clone());
                snap_data.mut_meta().set_for_balance(for_balance);
                if let Ok(v) = snap_data.write_to_bytes().map_err(|e| {
                    error!("fail to encode snapshot data"; "region_id" => region_id,  "snap_key" => ?key, "error" => ?e);
                    success = false;
                    e
                }) {
                    snapshot.set_data(v.into());
                }

                let checkpoint_path = self.snap_mgr().get_path_for_tablet_checkpoint(&key);
                _ = tablet
                    .checkpoint_to(std::slice::from_ref(&checkpoint_path), 0)
                    .map_err(|e| {
                        error!("failed to create checkpoint"; "region_id" => region_id, "snap_key" => ?key, "error" => ?e); 
                        success = false;
                        e
                });
                if success {
                    SNAP_COUNTER.generate.success.inc();
                    SNAP_HISTOGRAM
                    .generate
                    .observe(start.saturating_elapsed_secs());
                }else{
                    SNAP_COUNTER.generate.fail.inc();
                }
                let res = GenSnapRes {
                    snapshot: Box::new(snapshot),
                    success,
                };
                self.notifier.notify_snapshot_generated(region_id, res);
            }
        }
    }
}
