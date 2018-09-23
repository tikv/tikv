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
    AdminObserver, AdminResponse, Coprocessor, CoprocessorHost, ObserverContext, RoleObserver,
};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::AdminCmdType;
use raft::StateRole;
use raftstore::store::keys::{data_end_key, data_key, origin_key, DATA_MAX_KEY};
use raftstore::store::msg::{SeekRegionCallback, SeekRegionFilter, SeekRegionResult};
use raftstore::store::util;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::{mpsc, Arc, Mutex};
use storage::engine::{RegionInfoProvider, Result as EngineResult};
use util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};

const CHANNEL_BUFFER_SIZE: usize = 256;

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor. Only Admin events and
/// RoleChange events are listened.
#[derive(Debug)]
enum RaftStoreEvent {
    //    PreProposeAdmin {
    //        region: Region,
    //        request: AdminRequest,
    //    },
    //    PreApplyAdmin {
    //        region: Region,
    //        request: AdminRequest,
    //    },
    PostApplyAdmin {
        region: Region,
        response: AdminResponse,
    },
    RoleChange {
        region: Region,
        role: StateRole,
    },
}

impl Display for RaftStoreEvent {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self, f)
    }
}

#[derive(Clone)]
struct EventSender {
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl Coprocessor for EventSender {}

impl AdminObserver for EventSender {
    fn post_apply_admin(&self, context: &mut ObserverContext, resp: &mut AdminResponse) {
        match resp.get_cmd_type() {
            AdminCmdType::Split | AdminCmdType::BatchSplit => {
                let region = context.region.clone();
                let response = resp.clone();
                let event = RaftStoreEvent::PostApplyAdmin { region, response };
                self.scheduler
                    .schedule(RegionCollectionMsg::RaftStoreEvent(event))
                    .unwrap();
            }
            _ => {}
        }
    }
}

impl RoleObserver for EventSender {
    fn on_role_change(&self, context: &mut ObserverContext, role: StateRole) {
        let region = context.region.clone();
        let event = RaftStoreEvent::RoleChange { region, role };
        self.scheduler
            .schedule(RegionCollectionMsg::RaftStoreEvent(event))
            .unwrap();
    }
}

fn register_raftstore_event_sender(
    host: &mut CoprocessorHost,
    scheduler: Scheduler<RegionCollectionMsg>,
) {
    let event_sender = EventSender { scheduler };

    host.registry
        .register_admin_observer(1, box event_sender.clone());
    host.registry.register_role_observer(1, box event_sender);
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
    // Prefixed end key ('z' + key) -> (Region, State)
    regions: BTreeMap<Vec<u8>, (Region, StateRole)>,
}

impl RegionCollectionWorker {
    fn handle_split(&mut self, left: &Region, right: &Region) {
        // Old region info should be updated after inserting.
        for region in &[left, right] {
            self.regions.insert(
                data_end_key(region.get_end_key()),
                ((*region).clone(), StateRole::Candidate),
            );
        }
    }

    fn handle_batch_split(&mut self, regions: &[Region]) {
        // Old region info should be updated after inserting.
        for region in regions {
            self.regions.insert(
                data_end_key(region.get_end_key()),
                (region.clone(), StateRole::Candidate),
            );
        }
    }

    fn handle_role_change(&mut self, region: Region, role: StateRole) {
        // TODO: Save role to the map
        self.regions
            .entry(data_end_key(region.get_end_key()))
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
        }
    }

    fn handle_seek_region(
        &self,
        from_key: Vec<u8>,
        filter: SeekRegionFilter,
        mut limit: u32,
        callback: SeekRegionCallback,
    ) {
        assert!(limit > 0);

        let from_key = data_key(&from_key);
        for (end_key, (region, role)) in self.regions.range((Excluded(from_key), Unbounded)) {
            if filter(region, role) {
                callback(SeekRegionResult::Found {
                    local_peer: util::find_peer(region, self.self_store_id)
                        .cloned()
                        .unwrap_or_else(Peer::default),
                    region: region.clone(),
                });
                return;
            }

            limit -= 1;
            if limit == 0 {
                // `origin_key` does not handle `DATA_MAX_KEY`, but we can return `Ended` rather
                // than `LimitExceeded`.
                if end_key.as_slice() >= DATA_MAX_KEY {
                    break;
                }

                callback(SeekRegionResult::LimitExceeded {
                    next_key: origin_key(end_key).to_vec(),
                });
                return;
            }
        }
        callback(SeekRegionResult::Ended);
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
            } => {
                self.handle_seek_region(from, filter, limit, callback);
            }
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

        register_raftstore_event_sender(host, scheduler.clone());

        let scheduler = worker.scheduler();
        Self {
            self_store_id,
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
        }
    }

    pub fn start(&self) {
        self.worker
            .lock()
            .unwrap()
            .start(RegionCollectionWorker {
                self_store_id: self.self_store_id,
                regions: BTreeMap::default(),
            })
            .unwrap();
    }

    pub fn stop(&self) {
        self.worker.lock().unwrap().stop().unwrap().join().unwrap();
    }
}

impl RegionInfoProvider for RegionCollection {
    fn seek_region(
        &self,
        from: &[u8],
        filter: SeekRegionFilter,
        limit: u32,
    ) -> EngineResult<SeekRegionResult> {
        let (tx, rx) = mpsc::channel();
        let msg = RegionCollectionMsg::SeekRegion {
            from: from.to_vec(),
            filter,
            limit,
            callback: box move |res| {
                tx.send(res).unwrap_or_else(|e| {
                    panic!(
                        "region collection failed to send result back to caller: {:?}",
                        e
                    )
                })
            },
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collection: {:?}", e))
            .and_then(|_| {
                rx.recv().map_err(|e| {
                    box_err!(
                        "failed to receive seek region result from region collection: {:?}",
                        e
                    )
                })
            })
    }
}
