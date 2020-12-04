// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;

use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::ObserveID;
use raftstore::store::util::RemoteLease;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use txn_types::{Key, Lock, TimeStamp};

use crate::resolver::Resolver;
use crate::scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool};

enum ResolverStatus {
    Pending {
        id: ObserveID,
        locks: Vec<(Key, Lock)>,
    },
    Ready,
}

struct ObserveRegion {
    meta: Region,
    observe_id: ObserveID,
    lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

pub struct Endpoint<T, E> {
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E> Endpoint<T, E> {
    fn create_region() {}

    fn destory_region() {}
}

#[derive(Debug)]
pub enum Task {
    // Schedule by observer after a region was created.
    CreateRegion(Region),
    // Schedule by observer after a region was destoryed.
    DestoryRegion(Region),
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog(Vec<CmdBatch>),
    ScanLocks {
        region_id: u64,
        observe_id: ObserveID,
        locks: Vec<(Key, Lock)>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: 'static + Send + RaftStoreRouter<E>, E: KvEngine> Runnable for Endpoint<T, E> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => ?task);
        match task {
            Task::CreateRegion(_) => {}
            Task::DestoryRegion(_) => {}
            Task::AdvanceResolvedTs { regions, ts } => {}
            Task::ChangeLog(_) => {}
            Task::ScanLocks {
                region_id,
                observe_id,
                locks,
            } => {}
        }
    }
}
