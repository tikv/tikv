// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};

use collections::HashMap;
use engine_traits::{KvEngine, RaftEngine, TabletRegistry, DATA_CFS};
use kvproto::raft_cmdpb::RaftCmdRequest;
use slog::{error, info, Logger};
use tikv_util::{time::Instant, worker::Runnable};
use txn_types::WriteBatchFlags;

use crate::{
    router::{CmdResChannel, PeerMsg, RaftRequest},
    StoreRouter,
};

pub enum Task {
    TabletFlush {
        req: RaftCmdRequest,
        applied_index: u64,
        ch: CmdResChannel,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Task::TabletFlush {
                req,
                applied_index,
                ch,
            } => unimplemented!(),
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

    fn flush_tablet(&mut self, mut req: RaftCmdRequest, applied_index: u64, ch: CmdResChannel) {
        let region_id = req.get_header().get_region_id();
        let prev_applied_index = self
            .last_applied_indexes
            .get(&region_id)
            .unwrap_or_else(|| &0)
            .clone();
        assert!(prev_applied_index < applied_index);
        if applied_index - prev_applied_index <= 100 {
            // We dont need to flush memtable if we just flushed before
            // figure out an appropriate number
        }

        self.last_applied_indexes.insert(region_id, applied_index);
        if let Some(mut cache) = self.tablet_registry.get(req.get_header().get_region_id()) {
            if let Some(tablet) = cache.latest() {
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
                );
                req.mut_header()
                    .set_flags(WriteBatchFlags::SPLIT_SECOND_PHASE.bits());
                if let Err(e) = self
                    .router
                    .send(region_id, PeerMsg::AdminCommand(RaftRequest::new(req, ch)))
                {
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

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Self::Task) {
        match task {
            Task::TabletFlush {
                req,
                applied_index,
                ch,
            } => self.flush_tablet(req, applied_index, ch),
        }
    }
}
