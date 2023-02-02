// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use engine_traits::{Checkpointer, KvEngine, RaftEngine};
use fail::fail_point;
use file_system::{IoType, WithIoType};
use kvproto::raft_serverpb::{PeerState, RaftSnapshotData, RegionLocalState};
use protobuf::Message;
use raft::{eraftpb::Snapshot, GetEntriesContext};
use tikv_util::{error, info, time::Instant, worker::Runnable};

use crate::store::{
    snap::TABLET_SNAPSHOT_VERSION,
    util,
    worker::metrics::{SNAP_COUNTER, SNAP_HISTOGRAM},
    RaftlogFetchResult, TabletSnapKey, TabletSnapManager, MAX_INIT_ENTRY_COUNT,
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
        to_peer: u64,
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
            ReadTask::GenTabletSnapshot {
                region_id, to_peer, ..
            } => {
                write!(f, "Snapshot gen for {}, to peer {}", region_id, to_peer)
            }
        }
    }
}

#[derive(Debug)]
pub struct FetchedLogs {
    pub context: GetEntriesContext,
    pub logs: Box<RaftlogFetchResult>,
}

pub type GenSnapRes = Option<Box<(Snapshot, u64)>>;

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
    sanp_mgr: Option<TabletSnapManager>,
    _phantom: PhantomData<EK>,
}

impl<EK: KvEngine, ER: RaftEngine, N: AsyncReadNotifier> ReadRunner<EK, ER, N> {
    pub fn new(notifier: N, raft_engine: ER) -> ReadRunner<EK, ER, N> {
        ReadRunner {
            notifier,
            raft_engine,
            sanp_mgr: None,
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn set_snap_mgr(&mut self, mgr: TabletSnapManager) {
        self.sanp_mgr = Some(mgr);
    }

    #[inline]
    fn snap_mgr(&self) -> &TabletSnapManager {
        self.sanp_mgr.as_ref().unwrap()
    }

    fn generate_snap(&self, snap_key: &TabletSnapKey, tablet: EK) -> crate::Result<()> {
        let checkpointer_path = self.snap_mgr().tablet_gen_path(snap_key);
        if checkpointer_path.as_path().exists() {
            // Remove the old checkpoint directly.
            std::fs::remove_dir_all(checkpointer_path.as_path())?;
        }
        // Here not checkpoint to a temporary directory first, the temporary directory
        // logic already implemented in rocksdb.
        let mut checkpointer = tablet.new_checkpointer()?;

        checkpointer.create_at(checkpointer_path.as_path(), None, 0)?;
        Ok(())
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
                to_peer,
                tablet,
                region_state,
                last_applied_term,
                last_applied_index,
                canceled,
                for_balance,
            } => {
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
                // the state should already checked in apply workers.
                assert_ne!(region_state.get_state(), PeerState::Tombstone);
                let mut snapshot = Snapshot::default();
                // Set snapshot metadata.
                snapshot.mut_metadata().set_term(last_applied_term);
                snapshot.mut_metadata().set_index(last_applied_index);
                let conf_state = util::conf_state_from_region(region_state.get_region());
                snapshot.mut_metadata().set_conf_state(conf_state);
                // Set snapshot data.
                let mut snap_data = RaftSnapshotData::default();
                snap_data.set_region(region_state.get_region().clone());
                snap_data.set_version(TABLET_SNAPSHOT_VERSION);
                snap_data.mut_meta().set_for_balance(for_balance);
                snapshot.set_data(snap_data.write_to_bytes().unwrap().into());

                // create checkpointer.
                let snap_key = TabletSnapKey::from_region_snap(region_id, to_peer, &snapshot);
                let mut res = None;
                if let Err(e) = self.generate_snap(&snap_key, tablet) {
                    error!("failed to create checkpointer"; "region_id" => region_id, "error" => %e);
                    SNAP_COUNTER.generate.fail.inc();
                } else {
                    let elapsed = start.saturating_elapsed_secs();
                    SNAP_COUNTER.generate.success.inc();
                    SNAP_HISTOGRAM.generate.observe(elapsed);
                    info!("snapshot generated"; "region_id" => region_id, "elapsed" => elapsed, "key" => ?snap_key, "for_balance" => for_balance);
                    res = Some(Box::new((snapshot, to_peer)))
                }

                self.notifier.notify_snapshot_generated(region_id, res);
            }
        }
    }
}
