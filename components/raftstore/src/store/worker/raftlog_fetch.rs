// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

use engine_traits::RaftEngine;
use fail::fail_point;
use raft::GetEntriesContext;
use tikv_util::worker::Runnable;

use crate::store::{RaftlogFetchResult, MAX_INIT_ENTRY_COUNT};

pub enum Task {
    PeerStorage {
        region_id: u64,
        context: GetEntriesContext,
        low: u64,
        high: u64,
        max_size: usize,
        tried_cnt: usize,
        term: u64,
    },
    // More to support, suck as fetch entries ayschronously when apply and schedule merge
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Task::PeerStorage {
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
        }
    }
}

#[derive(Debug)]
pub struct FetchedLogs {
    pub context: GetEntriesContext,
    pub logs: Box<RaftlogFetchResult>,
}

/// A router for receiving fetched result.
pub trait LogFetchedNotifier: Send {
    fn notify(&self, region_id: u64, fetched: FetchedLogs);
}

pub struct Runner<ER, N>
where
    ER: RaftEngine,
    N: LogFetchedNotifier,
{
    notifier: N,
    raft_engine: ER,
}

impl<ER: RaftEngine, N: LogFetchedNotifier> Runner<ER, N> {
    pub fn new(notifier: N, raft_engine: ER) -> Runner<ER, N> {
        Runner {
            notifier,
            raft_engine,
        }
    }
}

impl<ER, N> Runnable for Runner<ER, N>
where
    ER: RaftEngine,
    N: LogFetchedNotifier,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::PeerStorage {
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
                self.notifier.notify(
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
        }
    }
}
