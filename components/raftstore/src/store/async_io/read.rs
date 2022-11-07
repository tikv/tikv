// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt,
    marker::PhantomData,
    sync::{atomic::AtomicBool, Arc},
};

use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use kvproto::raft_serverpb::RegionLocalState;
use raft::{eraftpb::Snapshot as RaftSnapshot, GetEntriesContext};
use tikv_util::worker::Runnable;

use crate::store::{RaftlogFetchResult, MAX_INIT_ENTRY_COUNT};

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

/// A router for receiving fetched result.
pub trait AsyncReadNotifier: Send {
    fn notify_logs_fetched(&self, region_id: u64, fetched: FetchedLogs);
    fn notify_snapshot_generated(&self, region_id: u64, snapshot: Box<RaftSnapshot>);
}

pub struct ReadRunner<EK, ER, N>
where
    EK: KvEngine,
    ER: RaftEngine,
    N: AsyncReadNotifier,
{
    notifier: N,
    raft_engine: ER,
    _phantom: PhantomData<EK>,
}

impl<EK: KvEngine, ER: RaftEngine, N: AsyncReadNotifier> ReadRunner<EK, ER, N> {
    pub fn new(notifier: N, raft_engine: ER) -> ReadRunner<EK, ER, N> {
        ReadRunner {
            notifier,
            raft_engine,
            _phantom: PhantomData,
        }
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
            ReadTask::GenTabletSnapshot { region_id, .. } => {
                // TODO: implement generate tablet snapshot for raftstore v2
                self.notifier
                    .notify_snapshot_generated(region_id, Box::new(RaftSnapshot::default()));
            }
        }
    }
}
