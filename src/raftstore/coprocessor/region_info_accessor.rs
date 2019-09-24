// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeMap;
use std::collections::Bound::{Excluded, Unbounded};
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use super::metrics::*;
use super::{
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RoleObserver,
};
use crate::raftstore::store::keys::{data_end_key, data_key};
use crate::storage::kv::{RegionInfoProvider, RipResult};
use kvproto::metapb::Region;
use raft::StateRole;
use tikv_util::collections::HashMap;
use tikv_util::timer::Timer;
use tikv_util::worker::{Builder as WorkerBuilder, Runnable, RunnableWithTimer, Scheduler, Worker};

/// `RegionInfoAccessor` is used to collect all regions' information on this TiKV into a collection
/// so that other parts of TiKV can get region information from it. It registers a observer to
/// raftstore, which is named `RegionEventListener`. When the events that we are interested in
/// happen (such as creating and deleting regions), `RegionEventListener` simply sends the events
/// through a channel.
/// In the mean time, `RegionCollector` keeps fetching messages from the channel, and mutates
/// the collection according to the messages. When an accessor method of `RegionInfoAccessor` is
/// called, it also simply sends a message to `RegionCollector`, and the result will be sent
/// back through as soon as it's finished.
/// In fact, the channel mentioned above is actually a `util::worker::Worker`.
///
/// **Caution**: Note that the information in `RegionInfoAccessor` is not perfectly precise. Some
/// regions may be temporarily absent while merging or splitting is in progress. Also,
/// `RegionInfoAccessor`'s information may slightly lag the actual regions on the TiKV.

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor.
#[derive(Debug)]
enum RaftStoreEvent {
    CreateRegion { region: Region, role: StateRole },
    UpdateRegion { region: Region, role: StateRole },
    DestroyRegion { region: Region },
    RoleChange { region: Region, role: StateRole },
}

impl RaftStoreEvent {
    pub fn get_region(&self) -> &Region {
        match self {
            RaftStoreEvent::CreateRegion { region, .. }
            | RaftStoreEvent::UpdateRegion { region, .. }
            | RaftStoreEvent::DestroyRegion { region, .. }
            | RaftStoreEvent::RoleChange { region, .. } => region,
        }
    }
}

pub use storage_types::region_info::{RegionInfo, SeekRegionCallback};

type RegionsMap = HashMap<u64, RegionInfo>;
type RegionRangesMap = BTreeMap<Vec<u8>, u64>;

/// `RegionInfoAccessor` has its own thread. Queries and updates are done by sending commands to the
/// thread.
enum RegionInfoQuery {
    RaftStoreEvent(RaftStoreEvent),
    SeekRegion {
        from: Vec<u8>,
        callback: SeekRegionCallback,
    },
    /// Gets all contents from the collection. Only used for testing.
    DebugDump(mpsc::Sender<(RegionsMap, RegionRangesMap)>),
}

impl Display for RegionInfoQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            RegionInfoQuery::RaftStoreEvent(e) => write!(f, "RaftStoreEvent({:?})", e),
            RegionInfoQuery::SeekRegion { from, .. } => {
                write!(f, "SeekRegion(from: {})", hex::encode_upper(from))
            }
            RegionInfoQuery::DebugDump(_) => write!(f, "DebugDump"),
        }
    }
}

/// `RegionEventListener` implements observer traits. It simply send the events that we are interested in
/// through the `scheduler`.
#[derive(Clone)]
struct RegionEventListener {
    scheduler: Scheduler<RegionInfoQuery>,
}

impl Coprocessor for RegionEventListener {}

impl RegionChangeObserver for RegionEventListener {
    fn on_region_changed(
        &self,
        context: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        role: StateRole,
    ) {
        let region = context.region().clone();
        let event = match event {
            RegionChangeEvent::Create => RaftStoreEvent::CreateRegion { region, role },
            RegionChangeEvent::Update => RaftStoreEvent::UpdateRegion { region, role },
            RegionChangeEvent::Destroy => RaftStoreEvent::DestroyRegion { region },
        };
        self.scheduler
            .schedule(RegionInfoQuery::RaftStoreEvent(event))
            .unwrap();
    }
}

impl RoleObserver for RegionEventListener {
    fn on_role_change(&self, context: &mut ObserverContext<'_>, role: StateRole) {
        let region = context.region().clone();
        let event = RaftStoreEvent::RoleChange { region, role };
        self.scheduler
            .schedule(RegionInfoQuery::RaftStoreEvent(event))
            .unwrap();
    }
}

/// Creates an `RegionEventListener` and register it to given coprocessor host.
fn register_region_event_listener(
    host: &mut CoprocessorHost,
    scheduler: Scheduler<RegionInfoQuery>,
) {
    let listener = RegionEventListener { scheduler };

    host.registry
        .register_role_observer(1, Box::new(listener.clone()));
    host.registry
        .register_region_change_observer(1, Box::new(listener));
}

/// `RegionCollector` is the place where we hold all region information we collected, and the
/// underlying runner of `RegionInfoAccessor`. It listens on events sent by the `RegionEventListener` and
/// keeps information of all regions. Role of each region are also tracked.
pub struct RegionCollector {
    // HashMap: region_id -> (Region, State)
    regions: RegionsMap,
    // BTreeMap: data_end_key -> region_id
    region_ranges: RegionRangesMap,
}

impl RegionCollector {
    pub fn new() -> Self {
        Self {
            regions: HashMap::default(),
            region_ranges: BTreeMap::default(),
        }
    }

    pub fn create_region(&mut self, region: Region, role: StateRole) {
        let end_key = data_end_key(region.get_end_key());
        let region_id = region.get_id();

        // Create new region
        self.region_ranges.insert(end_key, region_id);

        // TODO: Should we set it follower?
        assert!(
            self.regions
                .insert(region_id, RegionInfo::new(region, role))
                .is_none(),
            "trying to create new region {} but it already exists.",
            region_id
        );
    }

    fn update_region(&mut self, region: Region) {
        let existing_region_info = self.regions.get_mut(&region.get_id()).unwrap();

        let old_region = &mut existing_region_info.region;
        assert_eq!(old_region.get_id(), region.get_id());

        // If the end_key changed, the old entry in `region_ranges` should be removed.
        if old_region.get_end_key() != region.get_end_key() {
            // The region's end_key has changed.
            // Remove the old entry in `self.region_ranges`.
            let old_end_key = data_end_key(old_region.get_end_key());

            let old_id = self.region_ranges.remove(&old_end_key).unwrap();
            assert_eq!(old_id, region.get_id());

            // Insert new entry to `region_ranges`.
            let end_key = data_end_key(region.get_end_key());
            assert!(self
                .region_ranges
                .insert(end_key, region.get_id())
                .is_none());
        }

        // If the region already exists, update it and keep the original role.
        *old_region = region.clone();
    }

    fn handle_create_region(&mut self, region: Region, role: StateRole) {
        // During tests, we found that the `Create` event may arrive multiple times. And when we
        // receive an `Update` message, the region may have been deleted for some reason. So we
        // handle it according to whether the region exists in the collection.
        if self.regions.contains_key(&region.get_id()) {
            info!(
                "trying to create region but it already exists, try to update it";
                "region_id" => region.get_id(),
            );
            self.update_region(region);
        } else {
            self.create_region(region, role);
        }
    }

    fn handle_update_region(&mut self, region: Region, role: StateRole) {
        if self.regions.contains_key(&region.get_id()) {
            self.update_region(region);
        } else {
            info!(
                "trying to update region but it doesn't exist, try to create it";
                "region_id" => region.get_id(),
            );
            self.create_region(region, role);
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
                "destroying region but it doesn't exist";
                "region_id" => region.get_id(),
            )
        }
    }

    fn handle_role_change(&mut self, region: Region, new_role: StateRole) {
        let region_id = region.get_id();

        if let Some(r) = self.regions.get_mut(&region_id) {
            r.role = new_role;
            return;
        }

        warn!(
            "role change on region but the region doesn't exist. create it.";
            "region_id" => region_id,
        );
        self.create_region(region, new_role);
    }

    /// Determines whether `region_to_check`'s epoch is stale compared to `current`'s epoch
    #[inline]
    fn is_region_epoch_stale(&self, region_to_check: &Region, current: &Region) -> bool {
        let epoch = region_to_check.get_region_epoch();
        let current_epoch = current.get_region_epoch();

        // Only compare conf_ver when they have the same version.
        // When a region A merges region B, region B may have a greater conf_ver. Then, the new
        // merged region meta has larger version but smaller conf_ver than the original B's. In this
        // case, the incoming region meta has a smaller conf_ver but is not stale.
        epoch.get_version() < current_epoch.get_version()
            || (epoch.get_version() == current_epoch.get_version()
                && epoch.get_conf_ver() < current_epoch.get_conf_ver())
    }

    /// For all regions whose range overlaps with the given `region` or region_id is the same as
    /// `region`'s, checks whether the given `region`'s epoch is not older than theirs.
    ///
    /// Returns false if the given `region` is stale, which means, at least one region above has
    /// newer epoch.
    /// If the given `region` is not stale, all other regions in the collection that overlaps with
    /// the given `region` must be stale. Returns true in this case, and if `clear_regions_in_range`
    /// is true, those out-of-date regions will be removed from the collection.
    fn check_region_range(&mut self, region: &Region, clear_regions_in_range: bool) -> bool {
        if let Some(region_with_same_id) = self.regions.get(&region.get_id()) {
            if self.is_region_epoch_stale(region, &region_with_same_id.region) {
                return false;
            }
        }

        let mut stale_regions_in_range = vec![];

        for (key, id) in self
            .region_ranges
            .range((Excluded(data_key(region.get_start_key())), Unbounded))
        {
            if *id == region.get_id() {
                continue;
            }

            let current_region = &self.regions[id].region;
            if !region.get_end_key().is_empty()
                && current_region.get_start_key() >= region.get_end_key()
            {
                // This and following regions are not overlapping with `region`.
                break;
            }

            if self.is_region_epoch_stale(region, current_region) {
                return false;
            }
            // They are impossible to equal, or they cannot overlap.
            assert_ne!(
                region.get_region_epoch().get_version(),
                current_region.get_region_epoch().get_version()
            );
            // Remove it since it's a out-of-date region info.
            if clear_regions_in_range {
                stale_regions_in_range.push((key.clone(), *id));
            }
        }

        // Remove all pending-remove regions
        for (key, id) in stale_regions_in_range {
            self.regions.remove(&id).unwrap();
            self.region_ranges.remove(&key).unwrap();
        }

        true
    }

    pub fn handle_seek_region(&self, from_key: Vec<u8>, callback: SeekRegionCallback) {
        let from_key = data_key(&from_key);
        let mut iter = self
            .region_ranges
            .range((Excluded(from_key), Unbounded))
            .map(|(_, region_id)| &self.regions[region_id]);
        callback(&mut iter)
    }

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        {
            let region = event.get_region();
            if region.get_region_epoch().get_version() == 0 {
                // Ignore messages with version 0.
                // In raftstore `Peer::replicate`, the region meta's fields are all initialized with
                // default value except region_id. So if there is more than one region replicating
                // when the TiKV just starts, the assertion "Any two region with different ids and
                // overlapping ranges must have different version" fails.
                //
                // Since 0 is actually an invalid value of version, we can simply ignore the
                // messages with version 0. The region will be created later when the region's epoch
                // is properly set and an Update message was sent.
                return;
            }
            if !self.check_region_range(region, true) {
                debug!(
                    "Received stale event";
                    "event" => ?event,
                );
                return;
            }
        }

        match event {
            RaftStoreEvent::CreateRegion { region, role } => {
                self.handle_create_region(region, role);
            }
            RaftStoreEvent::UpdateRegion { region, role } => {
                self.handle_update_region(region, role);
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

impl Runnable<RegionInfoQuery> for RegionCollector {
    fn run(&mut self, task: RegionInfoQuery) {
        match task {
            RegionInfoQuery::RaftStoreEvent(event) => {
                self.handle_raftstore_event(event);
            }
            RegionInfoQuery::SeekRegion { from, callback } => {
                self.handle_seek_region(from, callback);
            }
            RegionInfoQuery::DebugDump(tx) => {
                tx.send((self.regions.clone(), self.region_ranges.clone()))
                    .unwrap();
            }
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl RunnableWithTimer<RegionInfoQuery, ()> for RegionCollector {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let mut count = 0;
        let mut leader = 0;
        for r in self.regions.values() {
            count += 1;
            if r.role == StateRole::Leader {
                leader += 1;
            }
        }
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["region"])
            .set(count);
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["leader"])
            .set(leader);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
    }
}

/// `RegionInfoAccessor` keeps all region information separately from raftstore itself.
#[derive(Clone)]
pub struct RegionInfoAccessor {
    worker: Arc<Mutex<Worker<RegionInfoQuery>>>,
    scheduler: Scheduler<RegionInfoQuery>,
}

impl RegionInfoAccessor {
    /// Creates a new `RegionInfoAccessor` and register to `host`.
    /// `RegionInfoAccessor` doesn't need, and should not be created more than once. If it's needed
    /// in different places, just clone it, and their contents are shared.
    pub fn new(host: &mut CoprocessorHost) -> Self {
        let worker = WorkerBuilder::new("region-collector-worker").create();
        let scheduler = worker.scheduler();

        register_region_event_listener(host, scheduler.clone());

        Self {
            worker: Arc::new(Mutex::new(worker)),
            scheduler,
        }
    }

    /// Starts the `RegionInfoAccessor`. It should be started before raftstore.
    pub fn start(&self) {
        let mut timer = Timer::new(1);
        timer.add_task(Duration::from_millis(METRICS_FLUSH_INTERVAL), ());
        self.worker
            .lock()
            .unwrap()
            .start_with_timer(RegionCollector::new(), timer)
            .unwrap();
    }

    /// Stops the `RegionInfoAccessor`. It should be stopped after raftstore.
    pub fn stop(&self) {
        self.worker.lock().unwrap().stop().unwrap().join().unwrap();
    }

    /// Gets all content from the collection. Only used for testing.
    pub fn debug_dump(&self) -> (RegionsMap, RegionRangesMap) {
        let (tx, rx) = mpsc::channel();
        self.scheduler
            .schedule(RegionInfoQuery::DebugDump(tx))
            .unwrap();
        rx.recv().unwrap()
    }
}

impl RegionInfoProvider for RegionInfoAccessor {
    fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> RipResult<()> {
        let msg = RegionInfoQuery::SeekRegion {
            from: from.to_vec(),
            callback,
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collector: {:?}", e))
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

    fn region_with_conf(
        id: u64,
        start_key: &[u8],
        end_key: &[u8],
        version: u64,
        conf_ver: u64,
    ) -> Region {
        let mut region = new_region(id, start_key, end_key, version);
        region.mut_region_epoch().set_conf_ver(conf_ver);
        region
    }

    fn check_collection(c: &RegionCollector, regions: &[(Region, StateRole)]) {
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

    /// Adds a set of regions to an empty collection and check if it's successfully loaded.
    fn must_load_regions(c: &mut RegionCollector, regions: &[Region]) {
        assert!(c.regions.is_empty());
        assert!(c.region_ranges.is_empty());

        for region in regions {
            must_create_region(c, &region, StateRole::Follower);
        }

        let expected_regions: Vec<_> = regions
            .iter()
            .map(|r| (r.clone(), StateRole::Follower))
            .collect();
        check_collection(&c, &expected_regions);
    }

    fn must_create_region(c: &mut RegionCollector, region: &Region, role: StateRole) {
        assert!(c.regions.get(&region.get_id()).is_none());

        c.handle_raftstore_event(RaftStoreEvent::CreateRegion {
            region: region.clone(),
            role,
        });

        assert_eq!(&c.regions[&region.get_id()].region, region);
        assert_eq!(
            c.region_ranges[&data_end_key(region.get_end_key())],
            region.get_id()
        );
    }

    fn must_update_region(c: &mut RegionCollector, region: &Region, role: StateRole) {
        let old_end_key = c
            .regions
            .get(&region.get_id())
            .map(|r| r.region.get_end_key().to_vec());

        c.handle_raftstore_event(RaftStoreEvent::UpdateRegion {
            region: region.clone(),
            role,
        });

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
                assert!(c
                    .region_ranges
                    .get(&data_end_key(&old_end_key))
                    .map_or(true, |id| *id != region.get_id()));
            }
        }
    }

    fn must_destroy_region(c: &mut RegionCollector, region: Region) {
        let id = region.get_id();
        let end_key = c.regions.get(&id).map(|r| r.region.get_end_key().to_vec());

        c.handle_raftstore_event(RaftStoreEvent::DestroyRegion { region });

        assert!(c.regions.get(&id).is_none());
        // If the region_id corresponding to the end_key doesn't equals to `id`, it shouldn't be
        // removed since it was used by another region.
        if let Some(end_key) = end_key {
            assert!(c
                .region_ranges
                .get(&data_end_key(&end_key))
                .map_or(true, |r| *r != id));
        }
    }

    fn must_change_role(c: &mut RegionCollector, region: &Region, role: StateRole) {
        c.handle_raftstore_event(RaftStoreEvent::RoleChange {
            region: region.clone(),
            role,
        });

        if let Some(r) = c.regions.get(&region.get_id()) {
            assert_eq!(r.role, role);
        }
    }

    #[test]
    fn test_ignore_invalid_version() {
        let mut c = RegionCollector::new();

        c.handle_raftstore_event(RaftStoreEvent::CreateRegion {
            region: new_region(1, b"k1", b"k3", 0),
            role: StateRole::Follower,
        });
        c.handle_raftstore_event(RaftStoreEvent::UpdateRegion {
            region: new_region(2, b"k2", b"k4", 0),
            role: StateRole::Follower,
        });
        c.handle_raftstore_event(RaftStoreEvent::RoleChange {
            region: new_region(1, b"k1", b"k2", 0),
            role: StateRole::Leader,
        });

        check_collection(&c, &[]);
    }

    #[test]
    fn test_epoch_stale_check() {
        let regions = &[
            region_with_conf(1, b"", b"k1", 10, 10),
            region_with_conf(2, b"k1", b"k2", 20, 10),
            region_with_conf(3, b"k3", b"k4", 10, 20),
            region_with_conf(4, b"k4", b"k5", 20, 20),
            region_with_conf(5, b"k6", b"k7", 10, 20),
            region_with_conf(6, b"k7", b"", 20, 10),
        ];

        let mut c = RegionCollector::new();
        must_load_regions(&mut c, regions);

        assert!(c.check_region_range(&region_with_conf(1, b"", b"k1", 10, 10), false));
        assert!(c.check_region_range(&region_with_conf(1, b"", b"k1", 12, 10), false));
        assert!(c.check_region_range(&region_with_conf(1, b"", b"k1", 10, 12), false));
        assert!(!c.check_region_range(&region_with_conf(1, b"", b"k1", 8, 10), false));
        assert!(!c.check_region_range(&region_with_conf(1, b"", b"k1", 10, 8), false));

        assert!(c.check_region_range(&region_with_conf(3, b"k3", b"k4", 12, 20), false));
        assert!(!c.check_region_range(&region_with_conf(3, b"k3", b"k4", 8, 20), false));

        assert!(c.check_region_range(&region_with_conf(6, b"k7", b"", 20, 12), false));
        assert!(!c.check_region_range(&region_with_conf(6, b"k7", b"", 20, 8), false));

        assert!(!c.check_region_range(&region_with_conf(1, b"k7", b"", 15, 10), false));
        assert!(!c.check_region_range(&region_with_conf(1, b"", b"", 19, 10), false));

        assert!(c.check_region_range(&region_with_conf(7, b"k2", b"k3", 1, 1), false));

        must_update_region(
            &mut c,
            &region_with_conf(1, b"", b"k1", 100, 100),
            StateRole::Follower,
        );
        must_update_region(
            &mut c,
            &region_with_conf(6, b"k7", b"", 100, 100),
            StateRole::Follower,
        );
        assert!(c.check_region_range(&region_with_conf(2, b"k1", b"k7", 30, 30), false));
        assert!(c.check_region_range(&region_with_conf(2, b"k11", b"k61", 30, 30), false));
        assert!(!c.check_region_range(&region_with_conf(2, b"k0", b"k7", 30, 30), false));
        assert!(!c.check_region_range(&region_with_conf(2, b"k1", b"k8", 30, 30), false));

        must_update_region(
            &mut c,
            &region_with_conf(2, b"k1", b"k2", 100, 100),
            StateRole::Follower,
        );
        must_update_region(
            &mut c,
            &region_with_conf(5, b"k6", b"k7", 100, 100),
            StateRole::Follower,
        );
        assert!(c.check_region_range(&region_with_conf(3, b"k2", b"k6", 30, 30), false));
        assert!(c.check_region_range(&region_with_conf(3, b"k21", b"k51", 30, 30), false));
        assert!(c.check_region_range(&region_with_conf(3, b"k3", b"k5", 30, 30), false));
        assert!(!c.check_region_range(&region_with_conf(3, b"k11", b"k6", 30, 30), false));
        assert!(!c.check_region_range(&region_with_conf(3, b"k2", b"k61", 30, 30), false));
    }

    #[test]
    fn test_clear_overlapped_regions() {
        let init_regions = vec![
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k2", 1),
            new_region(3, b"k3", b"k4", 1),
            new_region(4, b"k4", b"k5", 1),
            new_region(5, b"k6", b"k7", 1),
            new_region(6, b"k7", b"", 1),
        ];

        let mut c = RegionCollector::new();
        must_load_regions(&mut c, &init_regions);
        let mut regions: Vec<_> = init_regions
            .iter()
            .map(|region| (region.clone(), StateRole::Follower))
            .collect();

        c.check_region_range(&new_region(7, b"k2", b"k3", 2), true);
        check_collection(&c, &regions);

        c.check_region_range(&new_region(7, b"k31", b"k32", 2), true);
        // Remove region 3
        regions.remove(2);
        check_collection(&c, &regions);

        c.check_region_range(&new_region(7, b"k3", b"k5", 2), true);
        // Remove region 4
        regions.remove(2);
        check_collection(&c, &regions);

        c.check_region_range(&new_region(7, b"k11", b"k61", 2), true);
        // Remove region 2 and region 5
        regions.remove(1);
        regions.remove(1);
        check_collection(&c, &regions);

        c.check_region_range(&new_region(7, b"", b"", 2), true);
        // Remove all
        check_collection(&c, &[]);

        // Test that the region with the same id will be kept in the collection
        c = RegionCollector::new();
        must_load_regions(&mut c, &init_regions);

        c.check_region_range(&new_region(3, b"k1", b"k7", 2), true);
        check_collection(
            &c,
            &[
                (init_regions[0].clone(), StateRole::Follower),
                (init_regions[2].clone(), StateRole::Follower),
                (init_regions[5].clone(), StateRole::Follower),
            ],
        );

        c.check_region_range(&new_region(1, b"", b"", 2), true);
        check_collection(&c, &[(init_regions[0].clone(), StateRole::Follower)]);
    }

    #[test]
    fn test_basic_updating() {
        let mut c = RegionCollector::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];

        must_load_regions(&mut c, init_regions);

        // end_key changed
        must_update_region(&mut c, &new_region(2, b"k2", b"k8", 2), StateRole::Follower);
        // end_key changed (previous end_key is empty)
        must_update_region(
            &mut c,
            &new_region(3, b"k9", b"k99", 2),
            StateRole::Follower,
        );
        // end_key not changed
        must_update_region(&mut c, &new_region(1, b"k0", b"k1", 2), StateRole::Follower);
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
        must_create_region(&mut c, &new_region(5, b"k99", b"", 2), StateRole::Follower);
        must_change_role(&mut c, &new_region(2, b"k2", b"k8", 2), StateRole::Leader);
        must_update_region(&mut c, &new_region(2, b"k3", b"k7", 3), StateRole::Leader);
        must_create_region(&mut c, &new_region(4, b"k1", b"k3", 3), StateRole::Follower);
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

    /// Simulates splitting a region into 3 regions, and the region with old id will be the
    /// `derive_index`-th region of them. The events are triggered in order indicated by `seq`.
    /// This is to ensure the collection is correct, no matter what the events' order to happen is.
    /// Values in `seq` and of `derive_index` start from 1.
    fn test_split_impl(derive_index: usize, seq: &[usize]) {
        let mut c = RegionCollector::new();
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
                must_update_region(&mut c, &final_regions[*idx], StateRole::Follower);
            } else {
                must_create_region(&mut c, &final_regions[*idx], StateRole::Follower);
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
        let mut c = RegionCollector::new();
        let init_regions = &[
            region_with_conf(1, b"", b"k1", 1, 1),
            region_with_conf(2, b"k1", b"k2", 1, 100),
            region_with_conf(3, b"k2", b"k3", 1, 1),
            region_with_conf(4, b"k3", b"", 1, 100),
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
            must_update_region(&mut c, &updating_region, StateRole::Follower);
            must_destroy_region(&mut c, destroying_region);
        } else {
            must_destroy_region(&mut c, destroying_region);
            must_update_region(&mut c, &updating_region, StateRole::Follower);
        }

        let final_regions = &[
            (region_with_conf(1, b"", b"k1", 1, 1), StateRole::Follower),
            (updating_region, StateRole::Follower),
            (region_with_conf(4, b"k3", b"", 1, 100), StateRole::Follower),
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
        let mut c = RegionCollector::new();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        // While splitting, region 4 created but region 2 still has an `update` event which haven't
        // been handled.
        must_create_region(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Follower);
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 1), StateRole::Follower);
        must_change_role(&mut c, &new_region(2, b"k1", b"k9", 1), StateRole::Leader);
        must_update_region(&mut c, &new_region(2, b"k1", b"k5", 2), StateRole::Leader);
        // TODO: In fact, region 2's role should be follower. However because it's previous state was
        // removed while creating updating region 4, it can't be successfully updated. Fortunately
        // this case may hardly happen so it can be fixed later.
        check_collection(
            &c,
            &[
                (new_region(1, b"", b"k1", 1), StateRole::Follower),
                (new_region(2, b"k1", b"k5", 2), StateRole::Leader),
                (new_region(4, b"k5", b"k9", 2), StateRole::Follower),
                (new_region(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );

        // While merging, region 2 expanded and covered region 4 (and their end key become the same)
        // but region 4 still has an `update` event which haven't been handled.
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 3), StateRole::Leader);
        must_update_region(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Follower);
        must_change_role(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Leader);
        must_destroy_region(&mut c, new_region(4, b"k5", b"k9", 2));
        check_collection(
            &c,
            &[
                (new_region(1, b"", b"k1", 1), StateRole::Follower),
                (new_region(2, b"k1", b"k9", 3), StateRole::Leader),
                (new_region(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );
    }
}
