// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    collections::{
        BTreeMap,
        Bound::{Excluded, Unbounded},
    },
    fmt::{Display, Formatter, Result as FmtResult},
    sync::{mpsc, Arc, Mutex, RwLock},
    time::Duration,
};

use collections::{HashMap, HashSet};
use engine_traits::KvEngine;
use itertools::Itertools;
use kvproto::metapb::Region;
use pd_client::RegionStat;
use raft::StateRole;
use tikv_util::{
    box_err, debug, info, warn,
    worker::{Builder as WorkerBuilder, Runnable, RunnableWithTimer, Scheduler, Worker},
};

use super::{
    dispatcher::BoxRegionHeartbeatObserver, metrics::*, BoxRegionChangeObserver, BoxRoleObserver,
    Coprocessor, CoprocessorHost, ObserverContext, RegionChangeEvent, RegionChangeObserver,
    RegionHeartbeatObserver, Result, RoleChange, RoleObserver,
};

/// `RegionInfoAccessor` is used to collect all regions' information on this
/// TiKV into a collection so that other parts of TiKV can get region
/// information from it. It registers a observer to raftstore, which is named
/// `RegionEventListener`. When the events that we are interested in happen
/// (such as creating and deleting regions), `RegionEventListener` simply
/// sends the events through a channel.
/// In the mean time, `RegionCollector` keeps fetching messages from the
/// channel, and mutates the collection according to the messages. When an
/// accessor method of `RegionInfoAccessor` is called, it also simply sends a
/// message to `RegionCollector`, and the result will be sent back through as
/// soon as it's finished. In fact, the channel mentioned above is actually a
/// `util::worker::Worker`.
///
/// **Caution**: Note that the information in `RegionInfoAccessor` is not
/// perfectly precise. Some regions may be temporarily absent while merging or
/// splitting is in progress. Also, `RegionInfoAccessor`'s information may
/// slightly lag the actual regions on the TiKV.

/// `RaftStoreEvent` Represents events dispatched from raftstore coprocessor.
#[derive(Debug)]
pub enum RaftStoreEvent {
    CreateRegion {
        region: Region,
        role: StateRole,
    },
    UpdateRegion {
        region: Region,
        role: StateRole,
    },
    DestroyRegion {
        region: Region,
    },
    RoleChange {
        region: Region,
        role: StateRole,
        initialized: bool,
    },
    UpdateRegionBuckets {
        region: Region,
        buckets: usize,
    },
    UpdateRegionActivity {
        region: Region,
        activity: RegionActivity,
    },
}

impl RaftStoreEvent {
    pub fn get_region(&self) -> &Region {
        match self {
            RaftStoreEvent::CreateRegion { region, .. }
            | RaftStoreEvent::UpdateRegion { region, .. }
            | RaftStoreEvent::DestroyRegion { region, .. }
            | RaftStoreEvent::UpdateRegionBuckets { region, .. }
            | RaftStoreEvent::UpdateRegionActivity { region, .. }
            | RaftStoreEvent::RoleChange { region, .. } => region,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegionInfo {
    pub region: Region,
    pub role: StateRole,
    pub buckets: usize,
}

impl RegionInfo {
    pub fn new(region: Region, role: StateRole) -> Self {
        Self {
            region,
            role,
            buckets: 1,
        }
    }
}

/// Region activity data. Used by in-memory cache.
#[derive(Clone, Debug)]
pub struct RegionActivity {
    pub region_stat: RegionStat,
    // TODO: add region's MVCC version/tombstone count to measure effectiveness of the in-memory
    // cache for that region's data. This information could be collected from rocksdb, see:
    // collection_regions_to_compact.
}

type RegionsMap = HashMap<u64, RegionInfo>;
type RegionRangesMap = BTreeMap<RangeKey, u64>;
type RegionActivityMap = HashMap<u64, RegionActivity>;

// RangeKey is a wrapper used to unify the comparison between region start key
// and region end key. Region end key is special as empty stands for the
// infinite, so we need to take special care for cases where the end key is
// empty.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RangeKey {
    Finite(Vec<u8>),
    Infinite,
}

impl RangeKey {
    pub fn from_start_key(key: Vec<u8>) -> Self {
        RangeKey::Finite(key)
    }

    pub fn from_end_key(key: Vec<u8>) -> Self {
        if key.is_empty() {
            RangeKey::Infinite
        } else {
            RangeKey::Finite(key)
        }
    }
}

pub type Callback<T> = Box<dyn FnOnce(T) + Send>;
pub type SeekRegionCallback = Box<dyn FnOnce(&mut dyn Iterator<Item = &RegionInfo>) + Send>;

/// `RegionInfoAccessor` has its own thread. Queries and updates are done by
/// sending commands to the thread.
pub enum RegionInfoQuery {
    RaftStoreEvent(RaftStoreEvent),
    SeekRegion {
        from: Vec<u8>,
        callback: SeekRegionCallback,
    },
    FindRegionById {
        region_id: u64,
        callback: Callback<Option<RegionInfo>>,
    },
    GetRegionsInRange {
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<Vec<Region>>,
    },
    GetTopRegions {
        count: usize,
        callback: Callback<Vec<Region>>,
    },
    /// Gets all contents from the collection. Only used for testing.
    DebugDump(mpsc::Sender<(RegionsMap, RegionRangesMap)>),
}

impl Display for RegionInfoQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            RegionInfoQuery::RaftStoreEvent(e) => write!(f, "RaftStoreEvent({:?})", e),
            RegionInfoQuery::SeekRegion { from, .. } => {
                write!(f, "SeekRegion(from: {})", log_wrappers::Value::key(from))
            }
            RegionInfoQuery::FindRegionById { region_id, .. } => {
                write!(f, "FindRegionById(region_id: {})", region_id)
            }
            RegionInfoQuery::GetRegionsInRange {
                start_key, end_key, ..
            } => write!(
                f,
                "GetRegionsInRange(start_key: {}, end_key: {})",
                &log_wrappers::Value::key(start_key),
                &log_wrappers::Value::key(end_key)
            ),
            RegionInfoQuery::GetTopRegions { count, .. } => {
                write!(f, "GetTopRegions(count: {})", count)
            }
            RegionInfoQuery::DebugDump(_) => write!(f, "DebugDump"),
        }
    }
}

/// `RegionEventListener` implements observer traits. It simply send the events
/// that we are interested in through the `scheduler`.
#[derive(Clone)]
struct RegionEventListener {
    scheduler: Scheduler<RegionInfoQuery>,
    region_stats_manager_enabled_cb: RegionStatsManagerEnabledCb,
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
            RegionChangeEvent::Update(_) => RaftStoreEvent::UpdateRegion { region, role },
            RegionChangeEvent::Destroy => RaftStoreEvent::DestroyRegion { region },
            RegionChangeEvent::UpdateBuckets(buckets) => {
                RaftStoreEvent::UpdateRegionBuckets { region, buckets }
            }
        };
        self.scheduler
            .schedule(RegionInfoQuery::RaftStoreEvent(event))
            .unwrap();
    }
}

impl RoleObserver for RegionEventListener {
    fn on_role_change(&self, context: &mut ObserverContext<'_>, role_change: &RoleChange) {
        let region = context.region().clone();
        let role = role_change.state;
        let event = RaftStoreEvent::RoleChange {
            region,
            role,
            initialized: role_change.initialized,
        };
        self.scheduler
            .schedule(RegionInfoQuery::RaftStoreEvent(event))
            .unwrap();
    }
}

impl RegionHeartbeatObserver for RegionEventListener {
    fn on_region_heartbeat(&self, context: &mut ObserverContext<'_>, region_stat: &RegionStat) {
        if !(self.region_stats_manager_enabled_cb)() {
            // Region stats manager is disabled, return early.
            return;
        }
        let region = context.region().clone();
        let region_stat = region_stat.clone();
        let event = RaftStoreEvent::UpdateRegionActivity {
            region,
            activity: RegionActivity { region_stat },
        };

        self.scheduler
            .schedule(RegionInfoQuery::RaftStoreEvent(event))
            .unwrap();
    }
}

/// Creates an `RegionEventListener` and register it to given coprocessor host.
fn register_region_event_listener(
    host: &mut CoprocessorHost<impl KvEngine>,
    scheduler: Scheduler<RegionInfoQuery>,
    region_stats_manager_enabled_cb: RegionStatsManagerEnabledCb,
) {
    let listener = RegionEventListener {
        scheduler,
        region_stats_manager_enabled_cb,
    };

    host.registry
        .register_role_observer(1, BoxRoleObserver::new(listener.clone()));
    host.registry
        .register_region_change_observer(1, BoxRegionChangeObserver::new(listener.clone()));

    host.registry
        .register_region_heartbeat_observer(1, BoxRegionHeartbeatObserver::new(listener))
}

/// `RegionCollector` is the place where we hold all region information we
/// collected, and the underlying runner of `RegionInfoAccessor`. It listens on
/// events sent by the `RegionEventListener` and keeps information of all
/// regions. Role of each region are also tracked.
pub struct RegionCollector {
    // HashMap: region_id -> (Region, State)
    regions: RegionsMap,
    // BTreeMap: data_end_key -> region_id
    region_ranges: RegionRangesMap,
    // HashMap: region_id -> RegionActivity
    // TODO: add BinaryHeap to keep track of top N regions. Wrap the HashMap and BinaryHeap
    // together in a struct exposing add, delete, and get_top_regions methods.
    region_activity: RegionActivityMap,
    region_leaders: Arc<RwLock<HashSet<u64>>>,
}

impl RegionCollector {
    pub fn new(region_leaders: Arc<RwLock<HashSet<u64>>>) -> Self {
        Self {
            region_leaders,
            regions: HashMap::default(),
            region_activity: HashMap::default(),
            region_ranges: BTreeMap::default(),
        }
    }

    pub fn create_region(&mut self, region: Region, role: StateRole) {
        let end_key = RangeKey::from_end_key(region.get_end_key().to_vec());
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
            let old_end_key = RangeKey::from_end_key(old_region.get_end_key().to_vec());

            let old_id = self.region_ranges.remove(&old_end_key).unwrap();
            assert_eq!(old_id, region.get_id());

            // Insert new entry to `region_ranges`.
            let end_key = RangeKey::from_end_key(region.get_end_key().to_vec());
            assert!(
                self.region_ranges
                    .insert(end_key, region.get_id())
                    .is_none()
            );
        }

        // If the region already exists, update it and keep the original role.
        *old_region = region;
    }

    fn update_region_buckets(&mut self, region: Region, buckets: usize) {
        let existing_region_info = self.regions.get_mut(&region.get_id()).unwrap();
        let old_region = &mut existing_region_info.region;
        assert_eq!(old_region.get_id(), region.get_id());
        existing_region_info.buckets = buckets;
    }

    fn handle_create_region(&mut self, region: Region, role: StateRole) {
        // During tests, we found that the `Create` event may arrive multiple times. And
        // when we receive an `Update` message, the region may have been deleted for
        // some reason. So we handle it according to whether the region exists in the
        // collection.
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

    fn handle_update_region_activity(&mut self, region_id: u64, region_activity: &RegionActivity) {
        _ = self
            .region_activity
            .insert(region_id, region_activity.clone())
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

    fn handle_update_region_buckets(&mut self, region: Region, buckets: usize) {
        if self.regions.contains_key(&region.get_id()) {
            self.update_region_buckets(region, buckets);
        } else {
            warn!(
                "trying to update region buckets but the region doesn't exist, ignore";
                "region_id" => region.get_id(),
            );
        }
    }

    fn handle_destroy_region(&mut self, region: Region) {
        if let Some(removed_region_info) = self.regions.remove(&region.get_id()) {
            let removed_region = removed_region_info.region;
            assert_eq!(removed_region.get_id(), region.get_id());

            let end_key = RangeKey::from_end_key(removed_region.get_end_key().to_vec());

            let removed_id = self.region_ranges.remove(&end_key).unwrap();
            assert_eq!(removed_id, region.get_id());
            // Remove any activity associated with this id.
            self.region_activity.remove(&removed_id);
        } else {
            // It's possible that the region is already removed because it's end_key is used
            // by another newer region.
            debug!(
                "destroying region but it doesn't exist";
                "region_id" => region.get_id(),
            )
        }
        self.region_leaders
            .write()
            .unwrap()
            .remove(&region.get_id());
    }

    fn handle_role_change(&mut self, region: Region, new_role: StateRole) {
        let region_id = region.get_id();

        if new_role == StateRole::Leader {
            self.region_leaders.write().unwrap().insert(region_id);
        } else {
            self.region_leaders.write().unwrap().remove(&region_id);
        }

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

    /// Determines whether `region_to_check`'s epoch is stale compared to
    /// `current`'s epoch
    #[inline]
    fn is_region_epoch_stale(&self, region_to_check: &Region, current: &Region) -> bool {
        let epoch = region_to_check.get_region_epoch();
        let current_epoch = current.get_region_epoch();

        // Only compare conf_ver when they have the same version.
        // When a region A merges region B, region B may have a greater conf_ver. Then,
        // the new merged region meta has larger version but smaller conf_ver than the
        // original B's. In this case, the incoming region meta has a smaller conf_ver
        // but is not stale.
        epoch.get_version() < current_epoch.get_version()
            || (epoch.get_version() == current_epoch.get_version()
                && epoch.get_conf_ver() < current_epoch.get_conf_ver())
    }

    /// For all regions whose range overlaps with the given `region` or
    /// region_id is the same as `region`'s, checks whether the given
    /// `region`'s epoch is not older than theirs.
    ///
    /// Returns false if the given `region` is stale, which means, at least one
    /// region above has newer epoch.
    /// If the given `region` is not stale, all other regions in the collection
    /// that overlaps with the given `region` must be stale. Returns true in
    /// this case, and if `clear_regions_in_range` is true, those out-of-date
    /// regions will be removed from the collection.
    fn check_region_range(&mut self, region: &Region, clear_regions_in_range: bool) -> bool {
        if let Some(region_with_same_id) = self.regions.get(&region.get_id()) {
            if self.is_region_epoch_stale(region, &region_with_same_id.region) {
                return false;
            }
        }

        let mut stale_regions_in_range = vec![];

        for (key, id) in self.region_ranges.range((
            Excluded(RangeKey::from_start_key(region.get_start_key().to_vec())),
            Unbounded,
        )) {
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
                current_region.get_region_epoch().get_version(),
                "{:?} vs {:?}",
                region,
                current_region,
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
        let mut iter = self
            .region_ranges
            .range((Excluded(RangeKey::from_start_key(from_key)), Unbounded))
            .map(|(_, region_id)| &self.regions[region_id]);
        callback(&mut iter)
    }

    pub fn handle_find_region_by_id(&self, region_id: u64, callback: Callback<Option<RegionInfo>>) {
        callback(self.regions.get(&region_id).cloned());
    }

    // It returns the regions covered by [start_key, end_key]
    pub fn handle_get_regions_in_range(
        &self,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        callback: Callback<Vec<Region>>,
    ) {
        let end_key = RangeKey::from_end_key(end_key);
        let mut regions = vec![];
        for (_, region_id) in self
            .region_ranges
            .range((Excluded(RangeKey::from_start_key(start_key)), Unbounded))
        {
            let region_info = &self.regions[region_id];
            if RangeKey::from_start_key(region_info.region.get_start_key().to_vec()) > end_key {
                break;
            }
            regions.push(region_info.region.clone());
        }
        callback(regions);
    }

    /// Used by the in-memory engine (if enabled.)
    /// If `count` is 0, return all the regions for which this node is the
    /// leader. Otherwise, return the top `count` regions from
    /// `self.region_activity`. Top regions are determined by comparing
    /// `read_keys + written_keys` in each region's most recent region stat.
    ///
    /// Note: this function is `O(N log(N))` with respect to size of
    /// region_activity. This is acceptable, as region_activity is populated
    /// by heartbeats for this node's region, so N cannot be greater than
    /// approximately `300_000``.
    pub fn handle_get_top_regions(&mut self, count: usize, callback: Callback<Vec<Region>>) {
        let compare_fn = |a: &RegionActivity, b: &RegionActivity| {
            let a = a.region_stat.read_keys + a.region_stat.written_keys;
            let b = b.region_stat.read_keys + b.region_stat.written_keys;
            b.cmp(&a)
        };
        let top_regions = if count == 0 {
            self.regions
                .values()
                .filter(|ri| ri.role == StateRole::Leader)
                .map(|ri| ri.region.clone())
                .sorted_by(|a, b| {
                    let a = self.region_activity.get(&a.get_id());
                    let b = self.region_activity.get(&b.get_id());
                    match (a, b) {
                        (None, None) => Ordering::Equal,
                        (None, Some(_)) => Ordering::Greater,
                        (Some(_), None) => Ordering::Less,
                        (Some(a), Some(b)) => compare_fn(a, b),
                    }
                })
                .collect::<Vec<_>>()
        } else {
            let count = usize::max(count, self.region_activity.len());
            self.region_activity
                .iter()
                .sorted_by(|(_, activity_0), (_, activity_1)| compare_fn(activity_0, activity_1))
                .take(count)
                .flat_map(|(id, _)| self.regions.get(id).map(|ri| ri.region.clone()))
                .collect::<Vec<_>>()
        };
        callback(top_regions)
    }

    fn handle_raftstore_event(&mut self, event: RaftStoreEvent) {
        {
            let region = event.get_region();
            if region.get_region_epoch().get_version() == 0 {
                // Ignore messages with version 0.
                // In raftstore `Peer::replicate`, the region meta's fields are all initialized
                // with default value except region_id. So if there is more than one region
                // replicating when the TiKV just starts, the assertion "Any two region with
                // different ids and overlapping ranges must have different version" fails.
                //
                // Since 0 is actually an invalid value of version, we can simply ignore the
                // messages with version 0. The region will be created later when the region's
                // epoch is properly set and an Update message was sent.
                return;
            }
            if let RaftStoreEvent::RoleChange { initialized, .. } = &event
                && !initialized
            {
                // Ignore uninitialized peers.
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
            RaftStoreEvent::RoleChange { region, role, .. } => {
                self.handle_role_change(region, role);
            }
            RaftStoreEvent::UpdateRegionBuckets { region, buckets } => {
                self.handle_update_region_buckets(region, buckets);
            }
            RaftStoreEvent::UpdateRegionActivity { region, activity } => {
                self.handle_update_region_activity(region.get_id(), &activity)
            }
        }
    }
}

impl Runnable for RegionCollector {
    type Task = RegionInfoQuery;

    fn run(&mut self, task: RegionInfoQuery) {
        match task {
            RegionInfoQuery::RaftStoreEvent(event) => {
                self.handle_raftstore_event(event);
            }
            RegionInfoQuery::SeekRegion { from, callback } => {
                self.handle_seek_region(from, callback);
            }
            RegionInfoQuery::FindRegionById {
                region_id,
                callback,
            } => {
                self.handle_find_region_by_id(region_id, callback);
            }
            RegionInfoQuery::GetRegionsInRange {
                start_key,
                end_key,
                callback,
            } => {
                self.handle_get_regions_in_range(start_key, end_key, callback);
            }
            RegionInfoQuery::GetTopRegions { count, callback } => {
                self.handle_get_top_regions(count, callback);
            }
            RegionInfoQuery::DebugDump(tx) => {
                tx.send((self.regions.clone(), self.region_ranges.clone()))
                    .unwrap();
            }
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl RunnableWithTimer for RegionCollector {
    fn on_timeout(&mut self) {
        let mut count = 0;
        let mut leader = 0;
        let mut buckets_count = 0;
        for r in self.regions.values() {
            count += 1;
            if r.role == StateRole::Leader {
                leader += 1;
            }
            buckets_count += r.buckets;
        }
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["region"])
            .set(count);
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["leader"])
            .set(leader);
        REGION_COUNT_GAUGE_VEC
            .with_label_values(&["buckets"])
            .set(buckets_count as i64);
    }
    fn get_interval(&self) -> Duration {
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}

pub type RegionStatsManagerEnabledCb = Arc<dyn Fn() -> bool + Send + Sync>;

/// `RegionInfoAccessor` keeps all region information separately from raftstore
/// itself.
#[derive(Clone)]
pub struct RegionInfoAccessor {
    // We use a dedicated worker for region info accessor. If we later want to share a worker with
    // other tasks, we must make sure the other tasks don't block on flush or compaction, which
    // may cause a deadlock between the flush or compaction task, and the region info accessor task
    // fired by compaction guard from the RocksDB compaction thread.
    // https://github.com/tikv/tikv/issues/9044
    worker: Worker,
    scheduler: Scheduler<RegionInfoQuery>,

    /// Region leader ids set on the store.
    ///
    /// Others can access this info directly, such as RaftKV.
    region_leaders: Arc<RwLock<HashSet<u64>>>,
}

impl RegionInfoAccessor {
    /// Creates a new `RegionInfoAccessor` and register to `host`.
    /// `RegionInfoAccessor` doesn't need, and should not be created more than
    /// once. If it's needed in different places, just clone it, and their
    /// contents are shared.
    pub fn new(
        host: &mut CoprocessorHost<impl KvEngine>,
        region_stats_manager_enabled_cb: RegionStatsManagerEnabledCb,
    ) -> Self {
        let region_leaders = Arc::new(RwLock::new(HashSet::default()));
        let worker = WorkerBuilder::new("region-collector-worker").create();
        let scheduler = worker.start_with_timer(
            "region-collector-worker",
            RegionCollector::new(region_leaders.clone()),
        );
        register_region_event_listener(host, scheduler.clone(), region_stats_manager_enabled_cb);

        Self {
            worker,
            scheduler,
            region_leaders,
        }
    }

    /// Get a set of region leader ids.
    pub fn region_leaders(&self) -> Arc<RwLock<HashSet<u64>>> {
        self.region_leaders.clone()
    }

    /// Stops the `RegionInfoAccessor`. It should be stopped after raftstore.
    pub fn stop(&self) {
        self.scheduler.stop();
        self.worker.stop();
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

pub trait RegionInfoProvider: Send + Sync {
    /// Get a iterator of regions that contains `from` or have keys larger than
    /// `from`, and invoke the callback to process the result.
    fn seek_region(&self, _from: &[u8], _callback: SeekRegionCallback) -> Result<()> {
        unimplemented!()
    }

    fn find_region_by_id(
        &self,
        _reigon_id: u64,
        _callback: Callback<Option<RegionInfo>>,
    ) -> Result<()> {
        unimplemented!()
    }

    fn find_region_by_key(&self, _key: &[u8]) -> Result<Region> {
        unimplemented!()
    }

    fn get_regions_in_range(&self, _start_key: &[u8], _end_key: &[u8]) -> Result<Vec<Region>> {
        unimplemented!()
    }
    fn get_top_regions(&self, _count: usize) -> Result<Vec<Region>> {
        unimplemented!()
    }
}

impl RegionInfoProvider for RegionInfoAccessor {
    fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> Result<()> {
        let msg = RegionInfoQuery::SeekRegion {
            from: from.to_vec(),
            callback,
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collector: {:?}", e))
    }

    fn find_region_by_id(
        &self,
        region_id: u64,
        callback: Callback<Option<RegionInfo>>,
    ) -> Result<()> {
        let msg = RegionInfoQuery::FindRegionById {
            region_id,
            callback,
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collector: {:?}", e))
    }

    fn find_region_by_key(&self, key: &[u8]) -> Result<Region> {
        let key_in_vec = key.to_vec();
        let (tx, rx) = mpsc::channel();
        self.seek_region(
            key,
            Box::new(move |iter| {
                if let Some(info) = iter.next()
                    && info.region.get_start_key() <= key_in_vec.as_slice()
                {
                    if let Err(e) = tx.send(info.region.clone()) {
                        warn!("failed to send find_region_by_key result: {:?}", e);
                    }
                }
            }),
        )?;
        rx.recv().map_err(|e| {
            box_err!(
                "failed to receive find_region_by_key result from region collector: {:?}",
                e
            )
        })
    }
    fn get_regions_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<Region>> {
        let (tx, rx) = mpsc::channel();
        let msg = RegionInfoQuery::GetRegionsInRange {
            start_key: start_key.to_vec(),
            end_key: end_key.to_vec(),
            callback: Box::new(move |regions| {
                if let Err(e) = tx.send(regions) {
                    warn!("failed to send get_regions_in_range result: {:?}", e);
                }
            }),
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collector: {:?}", e))
            .and_then(|_| {
                rx.recv().map_err(|e| {
                    box_err!(
                        "failed to receive get_regions_in_range result from region collector: {:?}",
                        e
                    )
                })
            })
    }
    fn get_top_regions(&self, count: usize) -> Result<Vec<Region>> {
        let (tx, rx) = mpsc::channel();
        let msg = RegionInfoQuery::GetTopRegions {
            count,
            callback: Box::new(move |regions| {
                if let Err(e) = tx.send(regions) {
                    warn!("failed to send get_top_regions result: {:?}", e);
                }
            }),
        };
        self.scheduler
            .schedule(msg)
            .map_err(|e| box_err!("failed to send request to region collector: {:?}", e))
            .and_then(|_| {
                rx.recv().map_err(|e| {
                    box_err!(
                        "failed to receive get_top_regions result from region_collector: {:?}",
                        e
                    )
                })
            })
    }
}

// Use in tests only.
// Note: The `StateRole` in RegionInfo here should not be used
pub struct MockRegionInfoProvider(Mutex<Vec<RegionInfo>>);

impl MockRegionInfoProvider {
    pub fn new(regions: Vec<Region>) -> Self {
        MockRegionInfoProvider(Mutex::new(
            regions
                .into_iter()
                .map(|region| RegionInfo::new(region, StateRole::Leader))
                .collect_vec(),
        ))
    }
}

impl Clone for MockRegionInfoProvider {
    fn clone(&self) -> Self {
        MockRegionInfoProvider::new(
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|region_info| region_info.region.clone())
                .collect_vec(),
        )
    }
}

impl RegionInfoProvider for MockRegionInfoProvider {
    fn get_regions_in_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<Region>> {
        let mut regions = Vec::new();
        let (tx, rx) = mpsc::channel();
        let end_key = RangeKey::from_end_key(end_key.to_vec());

        self.seek_region(
            start_key,
            Box::new(move |iter| {
                for region_info in iter {
                    if RangeKey::from_start_key(region_info.region.get_start_key().to_vec())
                        > end_key
                    {
                        continue;
                    }
                    tx.send(region_info.region.clone()).unwrap();
                }
            }),
        )?;

        for region in rx {
            regions.push(region);
        }
        Ok(regions)
    }

    fn seek_region(&self, from: &[u8], callback: SeekRegionCallback) -> Result<()> {
        let region_infos = self.0.lock().unwrap();
        let mut iter = region_infos.iter().filter(|&region_info| {
            RangeKey::from_end_key(region_info.region.get_end_key().to_vec())
                > RangeKey::from_start_key(from.to_vec())
        });
        callback(&mut iter);
        Ok(())
    }

    fn find_region_by_key(&self, key: &[u8]) -> Result<Region> {
        let region_infos = self.0.lock().unwrap();
        let key = RangeKey::from_start_key(key.to_vec());
        region_infos
            .iter()
            .find(|region_info| {
                RangeKey::from_start_key(region_info.region.get_start_key().to_vec()) <= key
                    && key < RangeKey::from_end_key(region_info.region.get_end_key().to_vec())
            })
            .map(|region_info| region_info.region.clone())
            .ok_or(box_err!("Not found region containing {:?}", key))
    }

    fn get_top_regions(&self, _count: usize) -> Result<Vec<Region>> {
        let mut regions = Vec::new();
        let (tx, rx) = mpsc::channel();

        self.seek_region(
            b"",
            Box::new(move |iter| {
                for region_info in iter {
                    tx.send(region_info.region.clone()).unwrap();
                }
            }),
        )?;

        for region in rx {
            regions.push(region);
        }
        Ok(regions)
    }
}

#[cfg(test)]
mod tests {
    use txn_types::Key;

    use super::*;

    fn new_region_collector() -> RegionCollector {
        RegionCollector::new(Arc::new(RwLock::new(HashSet::default())))
    }

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
            .map(|(r, _)| (RangeKey::from_end_key(r.get_end_key().to_vec()), r.get_id()))
            .collect();

        let mut is_regions_equal = c.regions.len() == regions.len();

        if is_regions_equal {
            for (expect_region, expect_role) in regions {
                is_regions_equal = is_regions_equal
                    && c.regions.get(&expect_region.get_id()).map_or(
                        false,
                        |RegionInfo { region, role, .. }| {
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

    /// Adds a set of regions to an empty collection and check if it's
    /// successfully loaded.
    fn must_load_regions(c: &mut RegionCollector, regions: &[Region]) {
        assert!(c.regions.is_empty());
        assert!(c.region_ranges.is_empty());

        for region in regions {
            must_create_region(c, region, StateRole::Follower);
        }

        let expected_regions: Vec<_> = regions
            .iter()
            .map(|r| (r.clone(), StateRole::Follower))
            .collect();
        check_collection(c, &expected_regions);
    }

    fn must_create_region(c: &mut RegionCollector, region: &Region, role: StateRole) {
        assert!(c.regions.get(&region.get_id()).is_none());

        c.handle_raftstore_event(RaftStoreEvent::CreateRegion {
            region: region.clone(),
            role,
        });

        assert_eq!(&c.regions[&region.get_id()].region, region);
        assert_eq!(
            c.region_ranges[&RangeKey::from_end_key(region.get_end_key().to_vec())],
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
                c.region_ranges[&RangeKey::from_end_key(region.get_end_key().to_vec())],
                region.get_id()
            );
        } else {
            let another_region_id =
                c.region_ranges[&RangeKey::from_end_key(region.get_end_key().to_vec())];
            let version = c.regions[&another_region_id]
                .region
                .get_region_epoch()
                .get_version();
            assert!(region.get_region_epoch().get_version() < version);
        }
        // If end_key is updated and the region_id corresponding to the `old_end_key`
        // doesn't equals to `region_id`, it shouldn't be removed since it was
        // used by another region.
        if let Some(old_end_key) = old_end_key {
            if old_end_key.as_slice() != region.get_end_key() {
                assert!(
                    c.region_ranges
                        .get(&RangeKey::from_end_key(old_end_key))
                        .map_or(true, |id| *id != region.get_id())
                );
            }
        }
    }

    fn must_update_region_buckets(c: &mut RegionCollector, region: &Region, buckets: usize) {
        c.handle_raftstore_event(RaftStoreEvent::UpdateRegionBuckets {
            region: region.clone(),
            buckets,
        });
        let r = c.regions.get(&region.get_id()).unwrap();
        assert_eq!(r.region, *region);
        assert_eq!(r.buckets, buckets);
    }

    fn must_destroy_region(c: &mut RegionCollector, region: Region) {
        let id = region.get_id();
        let end_key = c.regions.get(&id).map(|r| r.region.get_end_key().to_vec());

        c.handle_raftstore_event(RaftStoreEvent::DestroyRegion { region });

        assert!(c.regions.get(&id).is_none());
        // If the region_id corresponding to the end_key doesn't equals to `id`, it
        // shouldn't be removed since it was used by another region.
        if let Some(end_key) = end_key {
            assert!(
                c.region_ranges
                    .get(&RangeKey::from_end_key(end_key))
                    .map_or(true, |r| *r != id)
            );
        }
    }

    fn must_change_role(
        c: &mut RegionCollector,
        region: &Region,
        role: StateRole,
        initialized: bool,
    ) {
        c.handle_raftstore_event(RaftStoreEvent::RoleChange {
            region: region.clone(),
            role,
            initialized,
        });

        if let Some(r) = c.regions.get(&region.get_id()) {
            assert_eq!(r.role, role);
        }
    }

    #[test]
    #[allow(clippy::many_single_char_names)]
    fn test_range_key() {
        let a = RangeKey::from_start_key(b"".to_vec());
        let b = RangeKey::from_start_key(b"".to_vec());
        let c = RangeKey::from_end_key(b"a".to_vec());
        let d = RangeKey::from_start_key(b"a".to_vec());
        let e = RangeKey::from_start_key(b"d".to_vec());
        let f = RangeKey::from_end_key(b"f".to_vec());
        let g = RangeKey::from_end_key(b"u".to_vec());
        let h = RangeKey::from_end_key(b"".to_vec());

        assert!(a == b);
        assert!(a < c);
        assert!(a != h);
        assert!(c == d);
        assert!(d < e);
        assert!(e < f);
        assert!(f < g);
        assert!(g < h);
        assert!(h > g);
    }

    #[test]
    fn test_ignore_invalid_version() {
        let mut c = new_region_collector();

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
            initialized: true,
        });
        c.handle_raftstore_event(RaftStoreEvent::RoleChange {
            region: new_region(1, b"", b"", 3),
            role: StateRole::Leader,
            initialized: false,
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

        let mut c = new_region_collector();
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

        let mut c = new_region_collector();
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
        c = new_region_collector();
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
        let mut c = new_region_collector();
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
            true,
        );
        must_create_region(&mut c, &new_region(5, b"k99", b"", 2), StateRole::Follower);
        must_change_role(
            &mut c,
            &new_region(2, b"k2", b"k8", 2),
            StateRole::Leader,
            true,
        );
        must_update_region(&mut c, &new_region(2, b"k3", b"k7", 3), StateRole::Leader);
        // test region buckets update
        must_update_region_buckets(&mut c, &new_region(2, b"k3", b"k7", 3), 4);
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

    /// Simulates splitting a region into 3 regions, and the region with old id
    /// will be the `derive_index`-th region of them. The events are triggered
    /// in order indicated by `seq`. This is to ensure the collection is
    /// correct, no matter what the events' order to happen is.
    /// Values in `seq` and of `derive_index` start from 1.
    fn test_split_impl(derive_index: usize, seq: &[usize]) {
        let mut c = new_region_collector();
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
                test_split_impl(*index, order.as_slice());
            }
        }
    }

    fn test_merge_impl(to_left: bool, update_first: bool) {
        let mut c = new_region_collector();
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
        let mut c = new_region_collector();
        let init_regions = &[
            new_region(1, b"", b"k1", 1),
            new_region(2, b"k1", b"k9", 1),
            new_region(3, b"k9", b"", 1),
        ];
        must_load_regions(&mut c, init_regions);

        // While splitting, region 4 created but region 2 still has an `update` event
        // which haven't been handled.
        must_create_region(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Follower);
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 1), StateRole::Follower);
        must_change_role(
            &mut c,
            &new_region(2, b"k1", b"k9", 1),
            StateRole::Leader,
            true,
        );
        must_update_region(&mut c, &new_region(2, b"k1", b"k5", 2), StateRole::Leader);
        // TODO: In fact, region 2's role should be follower. However because it's
        // previous state was removed while creating updating region 4, it can't be
        // successfully updated. Fortunately this case may hardly happen so it can be
        // fixed later.
        check_collection(
            &c,
            &[
                (new_region(1, b"", b"k1", 1), StateRole::Follower),
                (new_region(2, b"k1", b"k5", 2), StateRole::Leader),
                (new_region(4, b"k5", b"k9", 2), StateRole::Follower),
                (new_region(3, b"k9", b"", 1), StateRole::Follower),
            ],
        );

        // While merging, region 2 expanded and covered region 4 (and their end key
        // become the same) but region 4 still has an `update` event which haven't been
        // handled.
        must_update_region(&mut c, &new_region(2, b"k1", b"k9", 3), StateRole::Leader);
        must_update_region(&mut c, &new_region(4, b"k5", b"k9", 2), StateRole::Follower);
        must_change_role(
            &mut c,
            &new_region(4, b"k5", b"k9", 2),
            StateRole::Leader,
            true,
        );
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

    #[test]
    fn test_mock_region_info_provider() {
        fn init_region(start_key: &[u8], end_key: &[u8], region_id: u64) -> Region {
            let start_key = Key::from_encoded(start_key.to_vec());
            let end_key = Key::from_encoded(end_key.to_vec());
            let mut region = Region::default();
            region.set_start_key(start_key.as_encoded().clone());
            region.set_end_key(end_key.as_encoded().clone());
            region.id = region_id;
            region
        }

        let regions = vec![
            init_region(b"k01", b"k03", 1),
            init_region(b"k05", b"k10", 2),
            init_region(b"k10", b"k15", 3),
        ];

        let provider = MockRegionInfoProvider::new(regions);

        // Test ranges covering all regions
        let regions = provider.get_regions_in_range(b"k01", b"k15").unwrap();
        assert!(regions.len() == 3);
        assert!(regions[0].id == 1);
        assert!(regions[1].id == 2);
        assert!(regions[2].id == 3);

        // Test ranges covering partial regions
        let regions = provider.get_regions_in_range(b"k04", b"k10").unwrap();
        assert!(regions.len() == 2);
        assert!(regions[0].id == 2);
        assert!(regions[1].id == 3);

        // Test seek for all regions
        provider
            .seek_region(
                b"k02",
                Box::new(|iter| {
                    assert!(iter.next().unwrap().region.id == 1);
                    assert!(iter.next().unwrap().region.id == 2);
                    assert!(iter.next().unwrap().region.id == 3);
                    assert!(iter.next().is_none());
                }),
            )
            .unwrap();

        // Test seek for partial regions
        provider
            .seek_region(
                b"k04",
                Box::new(|iter| {
                    assert!(iter.next().unwrap().region.id == 2);
                    assert!(iter.next().unwrap().region.id == 3);
                    assert!(iter.next().is_none());
                }),
            )
            .unwrap();
    }
}
