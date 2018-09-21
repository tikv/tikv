// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{
    register_raftstore_event_sender, AdminResponse, CoprocessorHost, EventSenderFilter,
    ObserverContext, RaftStoreEvent,
};
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::AdminCmdType;
use raft::StateRole;
use raftstore::store::msg::{SeekRegionCallback, SeekRegionFilter, SeekRegionResult};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::{Arc, Mutex};
use storage::engine::{RegionInfoProvider, Result as EngineResult};
use util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};

const CHANNEL_BUFFER_SIZE: usize = 256;

/// Some events are not related to region collection. Filter out them.
#[derive(Clone)]
struct RegionCollectionEventFilter {}

impl EventSenderFilter for RegionCollectionEventFilter {
    fn post_apply_admin_filter(&self, _: &ObserverContext, resp: &AdminResponse) -> bool {
        // Only listen split and batch split events
        // TODO: How to handle merging?
        // TODO: Track changes of epoch
        match resp.get_cmd_type() {
            AdminCmdType::Split | AdminCmdType::BatchSplit => true,
            _ => false,
        }
    }

    fn on_role_change_filter(&self, _: &ObserverContext, _: StateRole) -> bool {
        // Listen all role change events
        true
    }
}

/// `RegionCollection` has its own thread (namely RegionCollectionWorker). Queries and updates are
/// done by sending commands to the thread.
enum RegionCollectionMsg {
    RaftStoreEvent(RaftStoreEvent),
    SeekRegion {
        from: Vec<u8>,
        filter: SeekRegionFilter,
        limit: u32,
        callback: SeekRegionCallback,
    },
}

impl Display for RegionCollectionMsg {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RegionCollectionMsg(fmt unimplemented)")
    }
}

/// `RegionCollectionWorker` is the underlying runner of `RegionCollection`.
struct RegionCollectionWorker {
    self_store_id: u64,
    regions: BTreeMap<Vec<u8>, (Region, StateRole)>,
}

impl RegionCollectionWorker {
    fn handle_split(&mut self, left: &Region, right: &Region) {
        // Old region info should be updated after inserting.
        for region in &[left, right] {
            self.regions.insert(
                region.get_start_key().to_vec(),
                ((*region).clone(), StateRole::Candidate),
            );
        }
    }

    fn handle_batch_split(&mut self, regions: &[Region]) {
        // Old region info should be updated after inserting.
        for region in regions {
            self.regions.insert(
                region.get_start_key().to_vec(),
                (region.clone(), StateRole::Candidate),
            );
        }
    }

    fn handle_role_change(&mut self, region: Region, role: StateRole) {
        // TODO: Save role to the map
        self.regions
            .entry(region.get_start_key().to_vec())
            .and_modify(|v| v.1 = role)
            .or_insert((region, role));
    }

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        match event {
            RaftStoreEvent::PostApplyAdmin { response, .. } => match response.get_cmd_type() {
                AdminCmdType::Split => {
                    self.handle_split(
                        response.get_split().get_left(),
                        response.get_split().get_right(),
                    );
                }
                AdminCmdType::BatchSplit => {
                    self.handle_batch_split(response.get_splits().get_regions());
                }
                _ => unreachable!(),
            },
            RaftStoreEvent::RoleChange { region, role } => {
                self.handle_role_change(region, role);
            }
            _ => unreachable!(),
        }
    }

    fn handle_seek_region(
        &self,
        from: Vec<u8>,
        filter: SeekRegionFilter,
        limit: u32,
        callback: SeekRegionCallback,
    ) {
        unimplemented!();
    }
}

impl Runnable<RegionCollectionMsg> for RegionCollectionWorker {
    fn run(&mut self, task: RegionCollectionMsg) {
        match task {
            RegionCollectionMsg::RaftStoreEvent(event) => {
                self.handle_raftstore_event(event);
            }
            RegionCollectionMsg::SeekRegion {
                from,
                filter,
                limit,
                callback,
            } => {}
        }
    }
}

/// `RegionCollection` keeps all region information separately from raftstore itself.
#[derive(Clone)]
pub struct RegionCollection {
    self_store_id: u64,
    worker: Arc<Mutex<Worker<RegionCollectionMsg>>>,
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl RegionCollection {
    pub fn new(host: &mut CoprocessorHost, self_store_id: u64) -> Self {
        let worker = WorkerBuilder::new("region-collection-worker")
            .pending_capacity(CHANNEL_BUFFER_SIZE)
            .create();
        let scheduler = worker.scheduler();

        register_raftstore_event_sender(host, RegionCollectionEventFilter {}, move |event| {
            scheduler
                .schedule(RegionCollectionMsg::RaftStoreEvent(event))
                .unwrap()
        });

        let scheduler = worker.scheduler();
        Self {
            self_store_id,
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
        }
    }

    pub fn start(&mut self) {
        self.worker
            .lock()
            .unwrap()
            .start(RegionCollectionWorker {
                self_store_id: self.self_store_id,
                regions: BTreeMap::default(),
            })
            .unwrap();
    }
}

impl RegionInfoProvider for RegionCollection {
    fn seek_region(
        &self,
        from: &[u8],
        filter: SeekRegionFilter,
        limit: u32,
    ) -> EngineResult<SeekRegionResult> {
        unimplemented!();
    }
}
