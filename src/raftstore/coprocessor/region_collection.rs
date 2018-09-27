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
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use kvproto::metapb::{Peer, Region};
use raft::StateRole;
use raftstore::store::keys::{data_end_key, data_key, origin_key, DATA_MAX_KEY};
use raftstore::store::msg::{SeekRegionCallback, SeekRegionFilter, SeekRegionResult};
use raftstore::store::util;
use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};
use std::sync::{mpsc, Arc, Mutex};
use std::usize;
use storage::engine::{RegionInfoProvider, Result as EngineResult};
use util::collections::HashMap;
use util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};

const CHANNEL_BUFFER_SIZE: usize = usize::MAX; // Unbounded

/// `RegionCollection` is used to collect all regions on this TiKV into a collection so that other
/// parts of TiKV can get region information from it. It registers several observers to raftstore,
/// which is named `EventSender`, and it simply send some specific types of events throw a channel.
/// In the mean time, `RegionCollectionWorker` keeps fetching messages from the channel, and mutate
/// the collection according tho the messages. When an accessor method of `RegionCollection` is
/// called, it also simply send a message to `RegionCollectionWorker`, and the result will be send
/// back through as soon as it's finished.

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor.
#[derive(Debug)]
enum RaftStoreEvent {
    NewRegion { region: Region },
    UpdateRegion { region: Region },
    DestroyRegion { region: Region },
    RoleChange { region: Region, role: StateRole },
}

impl Display for RaftStoreEvent {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        Debug::fmt(self, f)
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

/// `EventSender` implements observer traits. It simply send the events that we are interested in
/// through the `scheduler`.
#[derive(Clone)]
struct EventSender {
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl Coprocessor for EventSender {}

impl RegionChangeObserver for EventSender {
    fn on_region_changed(&self, context: &mut ObserverContext, event: RegionChangeEvent) {
        let region = context.region().clone();
        let event = match event {
            RegionChangeEvent::New => RaftStoreEvent::NewRegion { region },
            RegionChangeEvent::Update => RaftStoreEvent::UpdateRegion { region },
            RegionChangeEvent::Destroy => RaftStoreEvent::DestroyRegion { region },
        };
        self.scheduler
            .schedule(RegionCollectionMsg::RaftStoreEvent(event))
            .unwrap();
    }
}

impl RoleObserver for EventSender {
    fn on_role_change(&self, context: &mut ObserverContext, role: StateRole) {
        let region = context.region().clone();
        let event = RaftStoreEvent::RoleChange { region, role };
        self.scheduler
            .schedule(RegionCollectionMsg::RaftStoreEvent(event))
            .unwrap();
    }
}

/// Create an `EventSender` and register it to given coprocessor host.
fn register_raftstore_event_sender(
    host: &mut CoprocessorHost,
    scheduler: Scheduler<RegionCollectionMsg>,
) {
    let event_sender = EventSender { scheduler };

    host.registry
        .register_role_observer(1, box event_sender.clone());
    host.registry
        .register_region_change_observer(1, box event_sender.clone());
}

/// `RegionCollectionWorker` is the underlying runner of `RegionCollection`.
struct RegionCollectionWorker {
    self_store_id: u64,
    // region_id -> (Region, State)
    regions: HashMap<u64, (Region, StateRole)>,
    // region_id -> prefixed end key ('z' + key)
    region_ranges: BTreeMap<Vec<u8>, u64>,
}

impl RegionCollectionWorker {
    fn handle_new_region(&mut self, region: Region) {
        self.region_ranges
            .insert(data_end_key(region.get_end_key()), region.get_id());
        // TODO: Should we set it follower?
        self.regions
            .insert(region.get_id(), (region, StateRole::Follower));
    }

    fn handle_update_region(&mut self, region: Region) {
        let mut is_new_region = true;
        if let Some((ref mut old_region, _)) = self.regions.get_mut(&region.get_id()) {
            is_new_region = false;
            assert_eq!(old_region.get_id(), region.get_id());

            // If the region with this region_id already existed in the collection, it must be
            // stale and need to be updated. There may be a corresponding item in
            // `region_ranges`. If the end_key changed, the old item in `region_ranges` should
            // be removed, unless it was already updated.
            if old_region.get_end_key() != region.get_end_key() {
                // The region's end_key has changed.
                // Remove the old entry in `self.region_ranges` if it haven't been updated by
                // other items in `regions`. The entry with key equals to old_region's end key
                // was updated in previous circles in this for-loop, if it maps to a id that is
                // different from `region.id`.
                let old_end_key = data_end_key(old_region.get_end_key());
                if let Some(old_id) = self.region_ranges.get(&old_end_key).cloned() {
                    if old_id == region.get_id() {
                        self.region_ranges.remove(&old_end_key);
                    }
                }
            }

            // If the region already exists, update it and keep the original role.
            *old_region = region.clone();
        }

        if is_new_region {
            // If it's a new region, set it to follower state.
            // TODO: Should we set it follower?
            self.regions
                .insert(region.get_id(), (region.clone(), StateRole::Follower));
        }

        // If the end_key changed or the region didn't exist previously, insert a new item;
        // otherwise, update the old item. All regions in param `regions` must have unique
        // end_keys, so it won't conflict with each other.
        self.region_ranges
            .insert(data_end_key(region.get_end_key()), region.get_id());
    }

    fn handle_destroy_region(&mut self, region: Region) {
        let end_key = data_end_key(region.get_end_key());

        // The entry may be updated by other regions.
        if let Some(id) = self.region_ranges.get(&end_key).cloned() {
            if id == region.get_id() {
                self.region_ranges.remove(&end_key);
            }
        }

        self.regions.remove(&region.get_id());
    }

    fn handle_role_change(&mut self, region: Region, role: StateRole) {
        self.regions
            .insert(region.get_id(), (region, role))
            .expect("region didn't exist in region collection");
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
        for (end_key, region_id) in self.region_ranges.range((Excluded(from_key), Unbounded)) {
            let (region, role) = &self.regions[region_id];
            if filter(region, *role) {
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

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        match event {
            RaftStoreEvent::NewRegion { region } => {
                self.handle_new_region(region);
            }
            RaftStoreEvent::UpdateRegion { region } => {
                self.handle_update_region(region);
            }
            RaftStoreEvent::DestroyRegion { region } => {
                self.handle_destroy_region(region);
            }
            RaftStoreEvent::RoleChange { region, role } => {
                self.handle_role_change(region, role);
            }
        }
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
                regions: HashMap::default(),
                region_ranges: BTreeMap::default(),
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
