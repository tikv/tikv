// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{Display, Formatter};

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
        region_id: u64,
        req: Option<RaftCmdRequest>,
        is_leader: bool,
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
        }
    }

    fn flush_tablet(
        &mut self,
        region_id: u64,
        req: Option<RaftCmdRequest>,
        is_leader: bool,
        ch: Option<CmdResChannel>,
    ) {
        let Some(Some(tablet)) = self
            .tablet_registry
            .get(region_id)
            .map(|mut cache| cache.latest().cloned()) else {return};
        let now = Instant::now();
        tablet.flush_cfs(DATA_CFS, true).unwrap();
        let elapsed = now.saturating_elapsed();
        // to be removed after when it's stable
        info!(
            self.logger,
            "flush memtable time consumes";
            "region_id" => region_id,
            "duration" => ?elapsed,
            "is_leader" => is_leader,
        );

        if !is_leader {
            return;
        }

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
                "region_id" => region_id,
                "err" => ?e,
            );
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
                ch,
            } => self.flush_tablet(region_id, req, is_leader, ch),
        }
    }
}
