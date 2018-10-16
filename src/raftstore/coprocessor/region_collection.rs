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

use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::{mpsc, Arc, Mutex};
use std::usize;

use super::{
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::store::keys::data_end_key;
use util::collections::HashMap;
use util::worker::{Builder as WorkerBuilder, Runnable, Scheduler, Worker};

const CHANNEL_BUFFER_SIZE: usize = usize::MAX; // Unbounded

/// `RegionCollection` is used to collect all regions on this TiKV into a collection so that other
/// parts of TiKV can get region information from it. It registers a observer to raftstore, which
/// is named `EventSender`, and it simply send some specific types of events through a channel.
/// In the mean time, `RegionCollectionWorker` keeps fetching messages from the channel, and mutate
/// the collection according tho the messages. When an accessor method of `RegionCollection` is
/// called, it also simply send a message to `RegionCollectionWorker`, and the result will be send
/// back through as soon as it's finished.
/// In fact, the channel mentioned above is actually a `util::worker::Worker`.
///
/// **Caution**: Note that the information in `RegionCollection` is not perfectly precise. Some
/// regions may be absent while merging or splitting is in progress. Also, `RegionCollection` may
/// slightly lag actual regions on the TiKV.

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor.
#[derive(Debug)]
enum RaftStoreEvent {
    CreateRegion { region: Region },
    UpdateRegion { region: Region },
    DestroyRegion { region: Region },
    RoleChange { region: Region, role: StateRole },
}

#[derive(Clone, Debug)]
pub struct RegionInfo {
    pub region: Region,
    pub role: StateRole,
}

impl RegionInfo {
    pub fn new(region: Region, role: StateRole) -> Self {
        Self { region, role }
    }
}

type RegionsMap = HashMap<u64, RegionInfo>;
type RegionRangesMap = BTreeMap<Vec<u8>, u64>;

/// `RegionCollection` has its own thread (namely RegionCollectionWorker). Queries and updates are
/// done by sending commands to the thread.
enum RegionCollectionMsg {
    RaftStoreEvent(RaftStoreEvent),
    /// Get all contents from the collection. Only used for testing.
    DebugDump(mpsc::Sender<(RegionsMap, RegionRangesMap)>),
}

impl Display for RegionCollectionMsg {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        match self {
            RegionCollectionMsg::RaftStoreEvent(e) => write!(f, "RaftStoreEvent({:?})", e),
            RegionCollectionMsg::DebugDump(_) => write!(f, "DebugDump"),
        }
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
            RegionChangeEvent::Create => RaftStoreEvent::CreateRegion { region },
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

/// `RegionCollectionWorker` is the underlying runner of `RegionCollection`. It listens on events
/// sent by the `EventSender` and maintains the collection of all regions. Role of each region
/// are also tracked.
struct RegionCollectionWorker {
    // region_id -> (Region, State)
    regions: HashMap<u64, RegionInfo>,
    // 'z' + end_key -> region_id
    region_ranges: BTreeMap<Vec<u8>, u64>,
}

impl RegionCollectionWorker {
    fn new() -> Self {
        Self {
            regions: HashMap::default(),
            region_ranges: BTreeMap::default(),
        }
    }

    /// Check if the end_key has collided with another region. If so, try to clean it.
    /// If the collided region has a greater version than the current one, it means that the current
    /// operation is caused by a stale message. Then this function should fail.
    ///
    /// Returns a bool value indicating whether checking succeeded.
    fn check_end_key_collision(&mut self, region_id: u64, version: u64, end_key: &[u8]) -> bool {
        if let Some(collided_region_id) = self.region_ranges.get(end_key).cloned() {
            // There is already another region with the same end_key
            assert_ne!(collided_region_id, region_id);

            let collided_region_version = self.regions[&collided_region_id]
                .region
                .get_region_epoch()
                .get_version();
            if version < collided_region_version {
                // Another region is the actual new region. Now we are trying to create a region
                // because of a old message. Fail.
                return false;
            }
            // Another region is stale. Remove it.
            self.regions.remove(&collided_region_id);
        }
        true
    }

    fn handle_create_region(&mut self, region: Region) {
        let end_key = data_end_key(region.get_end_key());
        let region_id = region.get_id();

        if !self.check_end_key_collision(
            region_id,
            region.get_region_epoch().get_version(),
            &end_key,
        ) {
            return;
        }

        // Create new region
        self.region_ranges.insert(end_key, region.get_id());

        // TODO: Should we set it follower?
        if self
            .regions
            .insert(
                region.get_id(),
                RegionInfo::new(region, StateRole::Follower),
            )
            .is_some()
        {
            panic!(
                "region_collection: trying to create new region {} but it already exists.",
                region_id,
            );
        }
    }

    fn handle_update_region(&mut self, region: Region) {
        let mut need_create_new_region = true;
        let mut end_key_updated = false;

        if let Some(ref mut old_region_info) = self.regions.get_mut(&region.get_id()) {
            need_create_new_region = false;

            let old_region = &mut old_region_info.region;
            assert_eq!(old_region.get_id(), region.get_id());

            // If the end_key changed, the old item in `region_ranges` should be removed.
            if old_region.get_end_key() != region.get_end_key() {
                end_key_updated = true;
                // The region's end_key has changed.
                // Remove the old entry in `self.region_ranges`.
                let old_end_key = data_end_key(old_region.get_end_key());

                let old_id = self.region_ranges.remove(&old_end_key).unwrap();
                assert_eq!(old_id, region.get_id());
            }

            // If the region already exists, update it and keep the original role.
            *old_region = region.clone();
        }

        if end_key_updated {
            // If the region already exists, then `region_ranges` need to be updated.
            // However, if the new_end_key has already mapped to another region, we should only
            // keep the one with higher `version`.
            let end_key = data_end_key(region.get_end_key());
            if !self.check_end_key_collision(
                region.get_id(),
                region.get_region_epoch().get_version(),
                &end_key,
            ) {
                self.regions.remove(&region.get_id());
                return;
            }
            // Insert new entry to `region_ranges`.
            self.region_ranges.insert(end_key, region.get_id());
        }

        if need_create_new_region {
            warn!(
                "region_collection: trying to update region {} but it doesn't exist. create it.",
                region.get_id()
            );
            // It's a new region. Create it.
            self.handle_create_region(region);
        }
    }

    fn handle_destroy_region(&mut self, region: Region) {
        if let Some(removed_region_info) = self.regions.remove(&region.get_id()) {
            let removed_region = removed_region_info.region;
            assert_eq!(removed_region.get_id(), region.get_id());
            let end_key = data_end_key(removed_region.get_end_key());

            let removed_id = self.region_ranges.remove(&end_key).unwrap();
            assert_eq!(removed_id, region.get_id());
        } else {
            // It's possible that the region is already removed because it's end_key is used by
            // another newer region.
            debug!(
                "region_collection: destroying region {} but it doesn't exist",
                region.get_id()
            )
        }
    }

    fn handle_role_change(&mut self, region: Region, new_role: StateRole) {
        let region_id = region.get_id();
        if self.regions.get(&region_id).is_none() {
            warn!("region_collection: role change on region {} but the region doesn't exist. create it.", region_id);
            self.handle_create_region(region);
        }

        if let Some(r) = self.regions.get_mut(&region_id) {
            r.role = new_role;
        }
    }

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        match event {
            RaftStoreEvent::CreateRegion { region } => {
                self.handle_create_region(region);
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
            RegionCollectionMsg::DebugDump(tx) => {
                tx.send((self.regions.clone(), self.region_ranges.clone()))
                    .unwrap();
            }
        }
    }
}

/// `RegionCollection` keeps all region information separately from raftstore itself.
#[derive(Clone)]
pub struct RegionCollection {
    worker: Arc<Mutex<Worker<RegionCollectionMsg>>>,
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl RegionCollection {
    /// Create a new `RegionCollection` and register to `host`.
    /// `RegionCollection` doesn't need, and should not be created more than once. If it's needed
    /// in different places, just clone it, and their contents are shared.
    pub fn new(host: &mut CoprocessorHost) -> Self {
        let worker = WorkerBuilder::new("region-collection-worker")
            .pending_capacity(CHANNEL_BUFFER_SIZE)
            .create();
        let scheduler = worker.scheduler();

        register_raftstore_event_sender(host, scheduler.clone());

        Self {
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
        }
    }

    /// Start the `RegionCollection`. It should be started before raftstore.
    pub fn start(&self) {
        self.worker
            .lock()
            .unwrap()
            .start(RegionCollectionWorker::new())
            .unwrap();
    }

    /// Stop the `RegionCollection`. It should be stopped after raftstore.
    pub fn stop(&self) {
        self.worker.lock().unwrap().stop().unwrap().join().unwrap();
    }

    /// Get all content from the collection. Only used for testing.
    pub fn debug_dump(&self) -> (RegionsMap, RegionRangesMap) {
        let (tx, rx) = mpsc::channel();
        self.scheduler
            .schedule(RegionCollectionMsg::DebugDump(tx))
            .unwrap();
        rx.recv().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_region(id: u64, start_key: &[u8], end_key: &[u8], version: u64) -> Region {
        let mut region = Region::default();
        region.set_id(id);
        region.set_start_key(start_key.to_vec());
        region.set_end_key(end_key.to_vec());
        region.mut_region_epoch().set_version(version);
        region
    }

    fn check_collection(c: &RegionCollectionWorker, regions: &[(Region, StateRole)]) {
        let region_ranges: Vec<_> = regions
            .iter()
            .map(|(r, _)| (data_end_key(r.get_end_key()), r.get_id()))
            .collect();

        let mut is_regions_equal = c.regions.len() == regions.len();

        if is_regions_equal {
            for (expect_region, expect_role) in regions {
                is_regions_equal = is_regions_equal
                    && c.regions.get(&expect_region.get_id()).map_or(
                        false,
                        |RegionInfo { region, role }| {
                            expect_region == region && expect_role == role
                        },
                    );

                if !is_regions_equal {
                    break;
                }
            }
        }
        if !is_regions_equal {
            panic!("regions: expect {:?}, but got {:?}", regions, c.regions);
        }

        let mut is_ranges_equal = c.region_ranges.len() == region_ranges.len();
        is_ranges_equal = is_ranges_equal
            && c.region_ranges.iter().zip(region_ranges.iter()).all(
                |((actual_key, actual_id), (expect_key, expect_id))| {
                    actual_key == expect_key && actual_id == expect_id
                },
            );
        if !is_ranges_equal {
            panic!(
                "region_ranges: expect {:?}, but got {:?}",
                region_ranges, c.region_ranges
            );
        }
    }

    /// Add a set of regions to an empty collection and check if it's successfully loaded.
    fn must_load_regions(c: &mut RegionCollectionWorker, regions: &[Region]) {
        assert!(c.regions.is_empty());
        assert!(c.region_ranges.is_empty());

        for region in regions {
            must_create_region(c, &region);
        }

        let expected_regions: Vec<_> = regions
            .iter()
            .map(|r| (r.clone(), StateRole::Follower))
            .collect();
        check_collection(&c, &expected_regions);
    }

    fn must_create_region(c: &mut RegionCollectionWorker, region: &Region) {
        assert!(c.regions.get(&region.get_id()).is_none());

        c.handle_create_region(region.clone());

        assert_eq!(&c.regions[&region.get_id()].region, region);
        assert_eq!(
            c.region_ranges[&data_end_key(region.get_end_key())],
            region.get_id()
        );
    }

    fn must_update_region(c: &mut RegionCollectionWorker, region: &Region) {
        let old_end_key = c
            .regions
            .get(&region.get_id())
            .map(|r| r.region.get_end_key().to_vec());

        c.handle_update_region(region.clone());

        if let Some(r) = c.regions.get(&region.get_id()) {
            assert_eq!(r.region, *region);
            assert_eq!(
                c.region_ranges[&data_end_key(region.get_end_key())],
                region.get_id()
            );
        } else {
            let another_region_id = c.region_ranges[&data_end_key(region.get_end_key())];
            let version = c.regions[&another_region_id]
                .region
                .get_region_epoch()
                .get_version();
            assert!(region.get_region_epoch().get_version() < version);
        }
        // If end_key is updated and the region_id corresponding to the `old_end_key` doesn't equals
        // to `region_id`, it shouldn't be removed since it was used by another region.
        if let Some(old_end_key) = old_end_key {
            if old_end_key.as_slice() != region.get_end_key() {
                assert!(
                    c.region_ranges
                        .get(&data_end_key(&old_end_key))
                        .map_or(true, |id| *id != region.get_id())
                );
            }
        }
    }

    fn must_destroy_region(c: &mut RegionCollectionWorker, id: u64) {
        let end_key = c.regions.get(&id).map(|r| r.region.get_end_key().to_vec());

        c.handle_destroy_region(new_region(id, b"", b"", 1));

        assert!(c.regions.get(&id).is_none());
        // If the region_id corresponding to the end_key doesn't equals to `id`, it shouldn't be
        // removed since it was used by another region.
        if let Some(end_key) = end_key {
            assert!(
                c.region_ranges
                    .get(&data_end_key(&end_key))
                    .map_or(true, |r| *r != id)
            );
        }
    }

    fn must_change_role(c: &mut RegionCollectionWorker, region: &Region, role: StateRole) {
        c.handle_role_change(region.clone(), role);

        if let Some(r) = c.regions.get(&region.get_id()) {
            assert_eq!(r.role, role);
        }
    }

    #[test]
    fn test_basic_updating() {
        let mut c = RegionCollectionWorker::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];

        must_load_regions(&mut c, init_regions);

        // end_key changed
        must_update_region(&mut c, &new_region(2, b"k2", b"k8", 2));
        // end_key changed (previous end_key is empty)
        must_update_region(&mut c, &new_region(3, b"k9", b"k99", 2));
        // end_key not changed
        must_update_region(&mut c, &new_region(1, b"k0", b"k1", 2));
        check_collection(
            &c,
            &[
                (new_region(1, b"k0", b"k1", 2), StateRole::Follower),
                (new_region(2, b"k2", b"k8", 2), StateRole::Follower),
                (new_region(3, b"k9", b"k99", 2), StateRole::Follower),
            ],
        );

        must_change_role(
            &mut c,
            &new_region(1, b"k0", b"k1", 2),
            StateRole::Candidate,
        );
        must_create_region(&mut c, &new_region(5, b"k99", b"", 2));
        must_change_role(&mut c, &new_region(2, b"k2", b"k8", 2), StateRole::Leader);
        must_update_region(&mut c, &new_region(2, b"k3", b"k7", 3));
        must_create_region(&mut c, &new_region(4, b"k1", b"k3", 3));
        check_collection(
            &c,
            &[
                (new_region(1, b"k0", b"k1", 2), StateRole::Candidate),
                (new_region(4, b"k1", b"k3", 3), StateRole::Follower),
                (new_region(2, b"k3", b"k7", 3), StateRole::Leader),
                (new_region(3, b"k9", b"k99", 2), StateRole::Follower),
                (new_region(5, b"k99", b"", 2), StateRole::Follower),
            ],
        );

        must_destroy_region(&mut c, 4);
        must_destroy_region(&mut c, 3);
        check_collection(
            &c,
            &[
                (new_region(1, b"k0", b"k1", 2), StateRole::Candidate),
                (new_region(2, b"k3", b"k7", 3), StateRole::Leader),
                (new_region(5, b"k99", b"", 2), StateRole::Follower),
            ],
        );
    }

    /// Simulate splitting a region into 3 regions, and the region with old id will be the
    /// `derive_index`-th region of them. The events are triggered in order indicated by `seq`.
    /// This is to ensure the collection is correct, no matter what the events' order to happen is.
    /// Values in `seq` and of `derive_index` start from 1.
    fn test_split_impl(derive_index: usize, seq: &[usize]) {
        let mut c = RegionCollectionWorker::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        let mut final_regions = vec![
            new_region(1, b"", b"k1", 1),
            new_region(4, b"k1", b"k3", 2),
            new_region(5, b"k3", b"k6", 2),
            new_region(6, b"k6", b"k9", 2),
            new_region(3, b"k9", b"", 1),
        ];
        // `derive_index` starts from 1
        final_regions[derive_index].set_id(2);

        for idx in seq {
            if *idx == derive_index {
                must_update_region(&mut c, &final_regions[*idx]);
            } else {
                must_create_region(&mut c, &final_regions[*idx]);
            }
        }

        let final_regions = final_regions
            .into_iter()
            .map(|r| (r, StateRole::Follower))
            .collect::<Vec<_>>();
        check_collection(&c, &final_regions);
    }

    #[test]
    fn test_split() {
        let indices = &[1, 2, 3];
        let orders = &[
            &[1, 2, 3],
            &[1, 3, 2],
            &[2, 1, 3],
            &[2, 3, 1],
            &[3, 1, 2],
            &[3, 2, 1],
        ];

        for index in indices {
            for order in orders {
                test_split_impl(*index, *order);
            }
        }
    }

    fn test_merge_impl(to_left: bool, update_first: bool) {
        let mut c = RegionCollectionWorker::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k2", 1),
            new_region(3, b"k2", b"k3", 1),
            new_region(4, b"k3", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        let (mut updating_region, destroying_region_id) = if to_left {
            (init_regions[1].clone(), init_regions[2].get_id())
        } else {
            (init_regions[2].clone(), init_regions[1].get_id())
        };
        updating_region.set_start_key(b"k1".to_vec());
        updating_region.set_end_key(b"k3".to_vec());
        updating_region.mut_region_epoch().set_version(2);

        if update_first {
            must_update_region(&mut c, &updating_region);
            must_destroy_region(&mut c, destroying_region_id);
        } else {
            must_destroy_region(&mut c, destroying_region_id);
            must_update_region(&mut c, &updating_region);
        }

        let final_regions = &[
            (new_region(1, b"", b"k1", 1), StateRole::Follower),
            (updating_region, StateRole::Follower),
            (new_region(4, b"k3", b"", 1), StateRole::Follower),
        ];
        check_collection(&c, final_regions);
    }

    #[test]
    fn test_merge() {
        test_merge_impl(false, false);
        test_merge_impl(false, true);
        test_merge_impl(true, false);
        test_merge_impl(true, true);
    }

    #[test]
    fn test_extreme_cases() {
        let mut c = RegionCollectionWorker::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        // While splitting, region 4 created but region 2 still has an `update` event which haven't
        // been handled.
        must_create_region(&mut c, &new_region(4, b"k5", b"k9", 2));
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 1));
        must_change_role(&mut c, &new_region(2, b"k1", b"k9", 1), StateRole::Leader);
        must_update_region(&mut c, &new_region(2, b"k1", b"k5", 2));
        // TODO: In fact, region 2's role should be follower. However because it's revious state was
        // removed while creating updating region 4, it can't be successfully updated. Fortunately
        // this case may hardly happen so it can be fixed later.
        check_collection(
            &c,
            &[
                (new_region(1, b"", b"k1", 1), StateRole::Follower),
                (new_region(2, b"k1", b"k5", 2), StateRole::Follower),
                (new_region(4, b"k5", b"k9", 2), StateRole::Follower),
                (new_region(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );

        // While merging, region 2 expanded and covered region 4 but region 4 still has an `update`
        // event which haven't been handled.
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 3));
        must_update_region(&mut c, &new_region(4, b"k5", b"k9", 2));
        must_change_role(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Leader);
        must_destroy_region(&mut c, 4);
        check_collection(
            &c,
            &[
                (new_region(1, b"", b"k1", 1), StateRole::Follower),
                (new_region(2, b"k1", b"k9", 3), StateRole::Follower),
                (new_region(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );
    }
}
