// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::RocksEngine;
use futures::future::{lazy, Future};
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::IsolationLevel;
use kvproto::metapb::Region;
use pd_client::PdClient;
use tikv::raftstore::coprocessor::*;
use tikv::raftstore::store::fsm::{ApplyRouter, ApplyTask};
use tikv::raftstore::store::msg::{Callback, ReadResponse};
use tikv::raftstore::store::RegionSnapshot;
use tikv::storage::mvcc::ScannerBuilder;
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv_util::collections::HashMap;
use tikv_util::timer::SteadyTimer;
use tikv_util::worker::{Runnable, Scheduler};
use tokio_threadpool::{Builder, Sender as PoolSender, ThreadPool};
use txn_types::TimeStamp;

use crate::delegate::{Delegate, Downstream, DownstreamID};

pub enum Task {
    Register {
        request: ChangeDataRequest,
        downstream: Downstream,
    },
    Deregister {
        region_id: u64,
        downstream_id: Option<DownstreamID>,
        err: Option<Error>,
    },
    /// A test-only task.
    #[cfg(not(validate))]
    Validate(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::Register {
                ref request,
                ref downstream,
                ..
            } => de
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.id)
                .finish(),
            Task::Deregister {
                ref region_id,
                ref downstream_id,
                ref err,
            } => de
                .field("deregister", &"")
                .field("region_id", region_id)
                .field("err", err)
                .field("downstream_id", downstream_id)
                .finish(),
            #[cfg(not(validate))]
            Task::Validate(region_id, _) => de.field("region_id", &region_id).finish(),
        }
    }
}

pub struct Endpoint {
    capture_regions: HashMap<u64, Delegate>,
    scheduler: Scheduler<Task>,
    apply_router: ApplyRouter,

    pd_client: Arc<dyn PdClient>,
    timer: SteadyTimer,
    min_ts_interval: Duration,
    scan_batch_size: usize,

    workers: ThreadPool,
}

impl Endpoint {
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        scheduler: Scheduler<Task>,
        apply_router: ApplyRouter,
    ) -> Endpoint {
        let workers = Builder::new().name_prefix("cdcwkr").pool_size(4).build();
        Endpoint {
            capture_regions: HashMap::default(),
            scheduler,
            pd_client,
            timer: SteadyTimer::default(),
            workers,
            apply_router,
            scan_batch_size: 1024,
            min_ts_interval: Duration::from_secs(10),
        }
    }

    fn on_deregister(&mut self, region_id: u64, id: Option<DownstreamID>, err: Option<Error>) {
        info!("cdc deregister region";
            "region_id" => region_id,
            "id" => ?id,
            "error" => ?err);
        unimplemented!()
    }

    pub fn on_register(&mut self, request: ChangeDataRequest, _downstream: Downstream) {
        let region_id = request.region_id;
        info!("cdc register region"; "region_id" => region_id);
        unimplemented!()
    }
}

impl Runnable<Task> for Endpoint {
    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::Register {
                request,
                downstream,
            } => self.on_register(request, downstream),
            Task::Deregister {
                region_id,
                downstream_id,
                err,
            } => self.on_deregister(region_id, downstream_id, err),
            #[cfg(not(validate))]
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
    }
}
