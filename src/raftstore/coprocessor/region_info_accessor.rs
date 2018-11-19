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

/// `RegionInfoAccessor` is used to collect all regions on this TiKV into a collection so that other
/// parts of TiKV can get region information from it. It registers a observer to raftstore, which
/// is named `RegionEventListener`, and it simply send some specific types of events through a channel.
/// In the mean time, `RegionCollection` keeps fetching messages from the channel, and mutate
/// the collection according tho the messages. When an accessor method of `RegionInfoAccessor` is
/// called, it also simply send a message to `RegionCollection`, and the result will be send
/// back through as soon as it's finished.
/// In fact, the channel mentioned above is actually a `util::worker::Worker`.
///
/// **Caution**: Note that the information in `RegionInfoAccessor` is not perfectly precise. Some
/// regions may be absent while merging or splitting is in progress. Also, `RegionInfoAccessor` may
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

/// `RegionInfoAccessor` has its own thread. Queries and updates are done by sending commands to the
/// thread.
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

/// `RegionEventListener` implements observer traits. It simply send the events that we are interested in
/// through the `scheduler`.
#[derive(Clone)]
struct RegionEventListener {
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl Coprocessor for RegionEventListener {}

impl RegionChangeObserver for RegionEventListener {
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

impl RoleObserver for RegionEventListener {
    fn on_role_change(&self, context: &mut ObserverContext, role: StateRole) {
        let region = context.region().clone();
        let event = RaftStoreEvent::RoleChange { region, role };
        self.scheduler
            .schedule(RegionCollectionMsg::RaftStoreEvent(event))
            .unwrap();
    }
}

/// Create an `RegionEventListener` and register it to given coprocessor host.
fn register_region_event_listener(
    host: &mut CoprocessorHost,
    scheduler: Scheduler<RegionCollectionMsg>,
) {
    let listener = RegionEventListener { scheduler };

    host.registry
        .register_role_observer(1, box listener.clone());
    host.registry
        .register_region_change_observer(1, box listener);
}

/// `RegionCollection` is the place where we hold all region information we collected, and the
/// underlying runner of `RegionInfoAccessor`. It listens on events sent by the `RegionEventListener` and
/// keeps information of all regions. Role of each region are also tracked.
struct RegionCollection {
    // HashMap: region_id -> (Region, State)
    regions: RegionsMap,
    // BTreeMap: 'z' + end_key -> region_id
    region_ranges: RegionRangesMap,
}

impl RegionCollection {
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
    fn check_exist(&mut self, region_id: u64, version: u64, end_key: &[u8]) -> bool {
        if let Some(collided_region_id) = self.region_ranges.get(end_key).cloned() {
            // There is already another region with the same end_key
            assert_ne!(collided_region_id, region_id);

            let collided_region_version = self.regions[&collided_region_id]
                .region
                .get_region_epoch()
                .get_version();
            if version < collided_region_version {
                // Another region is the actual new region. Now we are trying to create a region
                // because of an old message. Fail.
                return false;
            }
            // Another region is older. Remove it.
            info!("region_collection: remove region {} because colliding with region {}", collided_region_id, region_id);
            self.regions.remove(&collided_region_id);
        }
        true
    }

    fn create_region(&mut self, region: Region) {
        let end_key = data_end_key(region.get_end_key());
        let region_id = region.get_id();

        if !self.check_exist(region_id, region.get_region_epoch().get_version(), &end_key) {
            warn!(
                "region_collection: creating region {} but it's end_key collides with another \
                 newer region.",
                region.get_id()
            );
            return;
        }

        // Create new region
        self.region_ranges.insert(end_key, region.get_id());

        // TODO: Should we set it follower?
        assert!(
            self.regions
                .insert(
                    region.get_id(),
                    RegionInfo::new(region, StateRole::Follower)
                )
                .is_none(),
            "region_collection: trying to create new region {} but it already exists.",
            region_id
        );
    }

    fn update_region(&mut self, region: Region) {
        let mut end_key_updated = false;

        {
            let existing_region_info = self.regions.get_mut(&region.get_id()).unwrap();
            // Ignore stale messages or messages with nothing updating
            {
                let epoch = region.get_region_epoch();
                let existing_epoch = existing_region_info.region.get_region_epoch();
                if epoch.get_version() <= existing_epoch.get_version()
                    && epoch.get_conf_ver() <= existing_epoch.get_conf_ver()
                {
                    return;
                }
            }

            let old_region = &mut existing_region_info.region;
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

        // The new region info has a different end_key, so we need to update it in the
        // `region_ranges` map.
        if end_key_updated {
            // If the region already exists, then `region_ranges` need to be updated.
            // However, if the new_end_key has already mapped to another region, we should only
            // keep the one with higher `version`.
            let end_key = data_end_key(region.get_end_key());
            if !self.check_exist(
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
    }

    fn handle_create_or_update(&mut self, region: Region) {
        // This function handles both create and update event.
        // During tests, we found that the `Create` event may arrive multiple times. And when we
        // receive an `Update` message, the region may have been deleted for some reason. So we
        // handle it according to whether the region exists in the collection.
        if self.regions.contains_key(&region.get_id()) {
            self.update_region(region);
        } else {
            self.create_region(region);
        }
    }

    fn handle_destroy_region(&mut self, region: Region) {
        if let Some(mut removed_region_info) = self.regions.remove(&region.get_id()) {
            let removed_region = removed_region_info.region;
            assert_eq!(removed_region.get_id(), region.get_id());

            // Ignore if it's a stale message
            let removed_version = removed_region.get_region_epoch().get_version();
            let removed_conf = removed_region.get_region_epoch().get_conf_ver();
            if removed_version > region.get_region_epoch().get_version()
                || removed_conf > region.get_region_epoch().get_conf_ver()
            {
                // It shouldn't be deleted because the message is stale. Put it back.
                removed_region_info.region = removed_region;
                self.regions.insert(region.get_id(), removed_region_info);
                return;
            }

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
            self.create_region(region);
        }

        if let Some(r) = self.regions.get_mut(&region_id) {
            r.role = new_role;
        }
    }

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        match event {
            RaftStoreEvent::CreateRegion { region } | RaftStoreEvent::UpdateRegion { region } => {
                self.handle_create_or_update(region);
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

impl Runnable<RegionCollectionMsg> for RegionCollection {
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

/// `RegionInfoAccessor` keeps all region information separately from raftstore itself.
#[derive(Clone)]
pub struct RegionInfoAccessor {
    worker: Arc<Mutex<Worker<RegionCollectionMsg>>>,
    scheduler: Scheduler<RegionCollectionMsg>,
}

impl RegionInfoAccessor {
    /// Create a new `RegionInfoAccessor` and register to `host`.
    /// `RegionInfoAccessor` doesn't need, and should not be created more than once. If it's needed
    /// in different places, just clone it, and their contents are shared.
    pub fn new(host: &mut CoprocessorHost) -> Self {
        let worker = WorkerBuilder::new("region-collection-worker")
            .pending_capacity(CHANNEL_BUFFER_SIZE)
            .create();
        let scheduler = worker.scheduler();

        register_region_event_listener(host, scheduler.clone());

        Self {
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
        }
    }

    /// Start the `RegionInfoAccessor`. It should be started before raftstore.
    pub fn start(&self) {
        self.worker
            .lock()
            .unwrap()
            .start(RegionCollection::new())
            .unwrap();
    }

    /// Stop the `RegionInfoAccessor`. It should be stopped after raftstore.
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

    fn check_collection(c: &RegionCollection, regions: &[(Region, StateRole)]) {
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
    fn must_load_regions(c: &mut RegionCollection, regions: &[Region]) {
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

    fn must_create_region(c: &mut RegionCollection, region: &Region) {
        assert!(c.regions.get(&region.get_id()).is_none());

        c.handle_create_or_update(region.clone());

        assert_eq!(&c.regions[&region.get_id()].region, region);
        assert_eq!(
            c.region_ranges[&data_end_key(region.get_end_key())],
            region.get_id()
        );
    }

    fn must_update_region(c: &mut RegionCollection, region: &Region) {
        let old_end_key = c
            .regions
            .get(&region.get_id())
            .map(|r| r.region.get_end_key().to_vec());

        c.handle_create_or_update(region.clone());

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

    fn must_destroy_region(c: &mut RegionCollection, region: Region) {
        let id = region.get_id();
        let end_key = c.regions.get(&id).map(|r| r.region.get_end_key().to_vec());

        c.handle_destroy_region(region);

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

    fn must_change_role(c: &mut RegionCollection, region: &Region, role: StateRole) {
        c.handle_role_change(region.clone(), role);

        if let Some(r) = c.regions.get(&region.get_id()) {
            assert_eq!(r.role, role);
        }
    }

    #[test]
    fn test_basic_updating() {
        let mut c = RegionCollection::new();
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

        must_destroy_region(&mut c, new_region(4, b"k1", b"k3", 3));
        must_destroy_region(&mut c, new_region(3, b"k9", b"k99", 2));
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
        let mut c = RegionCollection::new();
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
        let mut c = RegionCollection::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k2", 1),
            new_region(3, b"k2", b"k3", 1),
            new_region(4, b"k3", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        let (mut updating_region, destroying_region) = if to_left {
            (init_regions[1].clone(), init_regions[2].clone())
        } else {
            (init_regions[2].clone(), init_regions[1].clone())
        };
        updating_region.set_start_key(b"k1".to_vec());
        updating_region.set_end_key(b"k3".to_vec());
        updating_region.mut_region_epoch().set_version(2);

        if update_first {
            must_update_region(&mut c, &updating_region);
            must_destroy_region(&mut c, destroying_region);
        } else {
            must_destroy_region(&mut c, destroying_region);
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
        let mut c = RegionCollection::new();
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
        // TODO: In fact, region 2's role should be follower. However because it's previous state was
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

        // While merging, region 2 expanded and covered region 4 (and their end key become the same)
        // but region 4 still has an `update` event which haven't been handled.
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 3));
        must_update_region(&mut c, &new_region(4, b"k5", b"k9", 2));
        must_change_role(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Leader);
        must_destroy_region(&mut c, new_region(4, b"k5", b"k9", 2));
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
