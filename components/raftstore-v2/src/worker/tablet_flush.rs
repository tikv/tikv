// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry, DATA_CFS};
use kvproto::raft_cmdpb::RaftCmdRequest;
use slog::{error, info, warn, Logger};
use tikv_util::{time::Instant, worker::Runnable};
use txn_types::WriteBatchFlags;

use crate::{
    router::{CmdResChannel, PeerMsg, RaftRequest},
    StoreRouter,
};

pub enum Task {
    TabletFlush {
        region_id: u64,
        req: Option<RaftCmdRequest>,
        is_leader: bool,
        applied_index: u64,
        ch: Option<CmdResChannel>,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::TabletFlush { region_id, .. } => {
                write!(f, "Flush tablet before split for region {}", region_id)
            }
        }
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    router: StoreRouter<EK, ER>,
    tablet_registry: TabletRegistry<EK>,
    logger: Logger,

    // region_id -> last applied index
    last_applied_indexes: HashMap<u64, u64>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        router: StoreRouter<EK, ER>,
        tablet_registry: TabletRegistry<EK>,
        logger: Logger,
    ) -> Self {
        Self {
            router,
            tablet_registry,
            logger,
            last_applied_indexes: HashMap::default(),
        }
    }

    fn flush_tablet(
        &mut self,
        region_id: u64,
        req: Option<RaftCmdRequest>,
        is_leader: bool,
        applied_index: u64,
        ch: Option<CmdResChannel>,
    ) {
        let prev_applied_index = *self
            .last_applied_indexes
            .get(&region_id)
            .unwrap_or_else(|| &0);
        if prev_applied_index < applied_index {
            warn!(
                self.logger,
                "Applied index is less than before";
                "prev_applied_index" => prev_applied_index,
                "current_applied_index" => applied_index,
                "is_leader" => is_leader,
            );
        }
        let mut need_flush = true;
        if applied_index > prev_applied_index && applied_index - prev_applied_index <= 100 {
            // We dont need to flush memtable if we just flushed before
            need_flush = false;
        } else {
            self.last_applied_indexes.insert(region_id, applied_index);
        }

        if let Some(mut cache) = self.tablet_registry.get(region_id) {
            if let Some(tablet) = cache.latest() {
                info!(
                    self.logger,
                    "Begin flush memtable time";
                    "region_id" =>  region_id,
                    "prev_applied_index" => prev_applied_index,
                    "current_applied_index" => applied_index,
                    "is_leader" => is_leader,
                );

                if need_flush {
                    let now = Instant::now();
                    tablet.flush_cfs(DATA_CFS, true).unwrap();
                    let elapsed = now.saturating_elapsed();
                    info!(
                        self.logger,
                        "Flush memtable time consumes";
                        "region_id" =>  region_id,
                        "duration" => ?elapsed,
                        "prev_applied_index" => prev_applied_index,
                        "current_applied_index" => applied_index,
                        "is_leader" => is_leader,
                    );
                } else {
                    info!(
                        self.logger,
                        "Skip flush memtable";
                    );
                }

                if is_leader {
                    let mut req = req.unwrap();
                    req.mut_header()
                        .set_flags(WriteBatchFlags::SPLIT_SECOND_PHASE.bits());
                    if let Err(e) = self.router.send(
                        region_id,
                        PeerMsg::AdminCommand(RaftRequest::new(req, ch.unwrap())),
                    ) {
                        error!(
                            self.logger,
                            "send split request fail in the second phase";
                            "region_id" => region_id, "err" => ?e,
                        );
                    }
                }
            }
        }
    }
}

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::TabletFlush {
                region_id,
                req,
                is_leader,
                applied_index,
                ch,
            } => self.flush_tablet(region_id, req, is_leader, applied_index, ch),
        }
    }
}
