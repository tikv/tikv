// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    assert_matches::assert_matches,
    collections::{
        BTreeMap,
        Bound::{self, Excluded, Unbounded},
    },
    fmt::Debug,
    future::Future,
    pin::Pin,
    result,
    sync::atomic::{AtomicBool, Ordering},
};

use collections::HashMap;
use engine_traits::{CacheRegion, EvictReason, FailedReason};
use tikv_util::{info, time::Instant, warn};
use tokio::runtime::Runtime;

use crate::{metrics::observe_eviction_duration, read::RangeCacheSnapshotMeta};

pub(crate) trait AsyncFnOnce = FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(PartialEq, Eq, Debug, Clone, Copy, Default, Hash)]
pub enum RegionState {
    // waiting to be load.
    // NOTE: in this state, the region's epoch may be older than
    // target region in raftstore.
    #[default]
    Pending,
    // Region is handling batch loading from rocksdb snapshot.
    Loading,
    // Region is cached, ready to handle foreground read.
    Active,
    // region should be evicted, but batch loading is possible still running.
    LoadingCanceled,
    // region should be evicted, but there are possible active snapshot or gc task.
    PendingEvict,
    // evicting event is running, the region will be removed after the evict task finished.
    Evicting,
}

impl RegionState {
    pub fn as_str(&self) -> &'static str {
        use RegionState::*;
        match *self {
            Pending => "pending",
            Loading => "loading",
            Active => "cached",
            LoadingCanceled => "loading_canceled",
            PendingEvict => "pending_evict",
            Evicting => "evicting",
        }
    }

    pub fn is_evict(&self) -> bool {
        use RegionState::*;
        matches!(*self, LoadingCanceled | PendingEvict | Evicting)
    }
}

// read_ts -> ref_count
#[derive(Default, Debug)]
pub(crate) struct SnapshotList(pub(crate) BTreeMap<u64, u64>);

impl SnapshotList {
    pub(crate) fn new_snapshot(&mut self, read_ts: u64) {
        // snapshot with this ts may be granted before
        *self.0.entry(read_ts).or_default() += 1;
    }

    pub(crate) fn remove_snapshot(&mut self, read_ts: u64) {
        let count = self.0.get_mut(&read_ts).unwrap();
        assert!(*count >= 1);
        if *count == 1 {
            self.0.remove(&read_ts).unwrap();
        } else {
            *count -= 1;
        }
    }

    // returns the min snapshot_ts (read_ts) if there's any
    pub fn min_snapshot_ts(&self) -> Option<u64> {
        self.0.first_key_value().map(|(ts, _)| *ts)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug)]
pub struct CacheRegionMeta {
    // the cached region meta.
    region: CacheRegion,
    // active region snapshots.
    region_snapshot_list: SnapshotList,
    // the gc safe point.
    safe_point: u64,
    state: RegionState,
    // whether a gc task is running with this region.
    in_gc: bool,
    // region eviction triggers info and callback when eviction finishes.
    evict_info: Option<EvictInfo>,
}

struct EvictInfo {
    start: Instant,
    reason: EvictReason,
    // called when eviction finishes
    cb: Option<Box<dyn AsyncFnOnce + Send + Sync>>,
}

impl Debug for EvictInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EvictInfo")
            .field("reason", &self.reason)
            .finish()
    }
}

impl CacheRegionMeta {
    fn new(region: CacheRegion) -> Self {
        Self {
            region,
            region_snapshot_list: SnapshotList::default(),
            safe_point: 0,
            state: RegionState::Pending,
            in_gc: false,
            evict_info: None,
        }
    }

    #[inline]
    pub fn get_region(&self) -> &CacheRegion {
        &self.region
    }

    // check whether we can replace the current outdated pending region with the new
    // one.
    fn can_be_updated_to(&self, region: &CacheRegion) -> bool {
        assert!(
            self.region.id == region.id && self.region.epoch_version < region.epoch_version,
            "current: {:?}, new: {:?}",
            &self.region,
            region
        );
        // if the new region's range is contained by the current region, we can directly
        // update to the new one.
        self.region.contains_range(region)
    }

    pub(crate) fn safe_point(&self) -> u64 {
        self.safe_point
    }

    pub(crate) fn set_safe_point(&mut self, safe_point: u64) {
        assert!(self.safe_point <= safe_point);
        self.safe_point = safe_point;
    }

    pub fn get_state(&self) -> RegionState {
        self.state
    }

    // each state can only be updated to some specific new state.
    fn validate_update_region_state(&self, new_state: RegionState) -> bool {
        use RegionState::*;
        let valid_new_states: &[RegionState] = match self.state {
            Pending => &[Loading],
            Loading => &[Active, LoadingCanceled, Evicting],
            Active => &[PendingEvict],
            LoadingCanceled => &[PendingEvict, Evicting],
            PendingEvict => &[Evicting],
            Evicting => &[],
        };
        valid_new_states.contains(&new_state)
    }

    pub(crate) fn set_state(&mut self, new_state: RegionState) {
        assert!(self.validate_update_region_state(new_state));
        info!(
            "ime update region meta state";
            "region_id" => self.region.id,
            "epoch" => self.region.epoch_version,
            "curr_state" => ?self.state,
            "new_state" => ?new_state);
        self.state = new_state;
    }

    pub(crate) fn mark_evict(
        &mut self,
        state: RegionState,
        reason: EvictReason,
        cb: Option<Box<dyn AsyncFnOnce + Send + Sync>>,
    ) {
        use RegionState::*;
        assert_matches!(self.state, Loading | Active | LoadingCanceled);
        assert_matches!(state, PendingEvict | Evicting);
        self.set_state(state);
        self.evict_info = Some(EvictInfo {
            start: Instant::now_coarse(),
            reason,
            cb,
        });
    }

    pub(crate) fn set_in_gc(&mut self, in_gc: bool) {
        assert!(self.in_gc != in_gc);
        self.in_gc = in_gc;
    }

    pub(crate) fn is_in_gc(&self) -> bool {
        self.in_gc
    }

    // Build a new RangeMeta from a existing meta, the new meta should inherit
    // the safe_point, state, in_gc and evict_info.
    // This method is currently only used for handling region split.
    pub(crate) fn derive_from(region: CacheRegion, source_meta: &Self) -> Self {
        assert!(source_meta.region.contains_range(&region));
        Self {
            region,
            region_snapshot_list: SnapshotList::default(),
            safe_point: source_meta.safe_point,
            state: source_meta.state,
            in_gc: source_meta.in_gc,
            evict_info: None,
        }
    }

    pub(crate) fn region_snapshot_list(&self) -> &SnapshotList {
        &self.region_snapshot_list
    }
}

// TODO: it's currently impossible to implement a `Borrow`ed instance
// for `KeyAndVersion` from a borrowed key without clone.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
struct KeyAndVersion(Vec<u8>, u64);

// RegionManger manges the regions for RangeCacheMemoryEngine. Every new region
// (whether created by new_region/load_region or by split)'s range is unique and
// is not overlap with any other regions.
//
// Each region is first added with `pending` state. Because `pending` can be
// added by the background workers, it is possible the pending region is added
// with an outdated epoch. We handle this outdated epoch in the raft apply
// thread, before handling a region, the apply worker will check the region in
// RegionManager, if its epoch is outdated(only possible in `pending` state), if
// the old region's range contains new region's range, then we update it to the
// new version, else we just drop the outdated region.
//
// In RegionCacheEngine, we only keep region's epoch version updated with
// raftstore, but not the conf version for simplicity because conf version
// change doesn't affect the correctness of data. In order to always keep the
// region epoch version updated, we handle region epoch we use a ApplyObserver
// to watch following event:
// - PrepareMerge/CommitMerge. We evict target region currently for simplicity.
// - Leader Resign. evict the region.
// - SST Ingestion. evict the region.
// - Split/BatchSplit. For split event, we just replace the source region with
//   the split new regions. The new regions should inherit the state of the
//   source region including(state, safe_point, in_gc). If there are ongoing
//   snapshot in the source region, the source region meta should be put in
//   `historical_regions`.
#[derive(Default)]
pub struct RegionManager {
    // ranges that are cached now
    // data_end_key --> region_id
    regions_by_range: BTreeMap<Vec<u8>, u64>,
    // region_id --> region_meta
    regions: HashMap<u64, CacheRegionMeta>,
    // we use this flag to ensure there is only 1 running gc task.
    is_gc_task_running: AtomicBool,
    // Outdated regions that are split but still hold some on going snapshots.
    // These on going snapshot should block regions fell in this range from gc or eviction.
    // It's possible that multi region with the same end key are in `historical_regions`,
    // so we add epoch version into the key to ensure the uniqueness.
    // (data_end_key, epoch_version) --> region_id
    historical_regions: BTreeMap<KeyAndVersion, CacheRegionMeta>,
    // Record the region ranges that are being written.
    //
    // It is used to avoid the conccurency issue between delete range and write to memory: after
    // the range is evicted or failed to load, the range is marked as `PendingEvict`
    // which means no further write of it is allowed, and a DeleteRegion task of the range will be
    // scheduled to cleanup the KVs of this region. However, it is possible that the apply thread
    // is writing data for this range. Therefore, we have to delay the DeleteRange task until
    // the range leaves the `ranges_being_written`.
    //
    // The key in this map is the id of the write batch, and the value is a collection
    // the ranges of this batch. So, when the write batch is consumed by the in-memory engine,
    // all ranges of it are cleared from `ranges_being_written`.
    // write_batch_id --> Vec<cached_range>
    regions_being_written: HashMap<u64, Vec<CacheRegion>>,
}

impl RegionManager {
    pub(crate) fn regions(&self) -> &HashMap<u64, CacheRegionMeta> {
        &self.regions
    }

    #[cfg(test)]
    pub(crate) fn region_meta_by_end_key(&self, key: &[u8]) -> Option<&CacheRegionMeta> {
        self.regions_by_range
            .get(key)
            .and_then(|id| self.regions.get(id))
    }

    // load a new region directly in the active state.
    // This fucntion is used for unit/integration tests only.
    pub fn new_region(&mut self, region: CacheRegion) {
        let mut range_meta = CacheRegionMeta::new(region);
        range_meta.state = RegionState::Active;
        self.new_region_meta(range_meta);
    }

    fn new_region_meta(&mut self, meta: CacheRegionMeta) {
        assert!(!self.overlaps_with(&meta.region));
        let id = meta.region.id;
        let data_end_key = meta.region.end.clone();
        self.regions.insert(id, meta);
        self.regions_by_range.insert(data_end_key, id);
    }

    pub fn region_meta(&self, id: u64) -> Option<&CacheRegionMeta> {
        self.regions.get(&id)
    }

    pub fn mut_region_meta(&mut self, id: u64) -> Option<&mut CacheRegionMeta> {
        self.regions.get_mut(&id)
    }

    pub fn cached_regions(&self) -> Vec<u64> {
        self.regions
            .iter()
            .filter_map(|(id, meta)| {
                if meta.state == RegionState::Active {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
    }

    pub fn iter_overlapped_regions(
        &self,
        region: &CacheRegion,
        mut f: impl FnMut(&CacheRegionMeta) -> bool,
    ) {
        for (_key, id) in self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&region.start), Unbounded))
        {
            let region_meta = &self.regions[id];
            if region_meta.region.start >= region.end {
                break;
            }
            if !f(region_meta) {
                break;
            }
        }
    }

    pub fn iter_overlapped_regions_mut(
        &mut self,
        region: &CacheRegion,
        mut f: impl FnMut(&mut CacheRegionMeta),
    ) {
        for (_key, id) in self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&region.start), Unbounded))
        {
            let region_meta = self.regions.get_mut(id).unwrap();
            if region_meta.region.start >= region.end {
                break;
            }
            f(region_meta);
        }
    }

    pub fn set_safe_point(&mut self, region_id: u64, safe_ts: u64) -> bool {
        if let Some(meta) = self.regions.get_mut(&region_id) {
            if meta.safe_point > safe_ts {
                return false;
            }
            meta.safe_point = safe_ts;
            true
        } else {
            false
        }
    }

    pub fn get_region_for_key(&self, key: &[u8]) -> Option<CacheRegion> {
        if let Some((key, id)) = self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(key), Unbounded))
            .next()
        {
            let meta = &self.regions[id];
            if &meta.region.start <= key {
                return Some(meta.region.clone());
            }
        }
        None
    }

    pub fn contains_region(&self, region_id: u64) -> bool {
        self.regions.contains_key(&region_id)
    }

    pub fn check_region_state(&mut self, region: &CacheRegion) -> Option<RegionState> {
        use RegionState::*;
        let Some(cached_meta) = self.regions.get_mut(&region.id) else {
            return None;
        };
        if cached_meta.state == Pending && cached_meta.region.epoch_version != region.epoch_version
        {
            let meta = self.remove_region(region.id);
            if meta.can_be_updated_to(region) {
                info!("ime update outdated pending region";
                    "current_meta" => ?meta,
                    "new_region" => ?region);
                // the new region's range is smaller than removed region, so it is impossible to
                // be overlapped with other existing regions.
                self.load_region(region.clone()).unwrap();

                return Some(RegionState::Pending);
            }

            info!("ime remove outdated pending region";
                "pending_region" => ?meta.region,
                "new_region" => ?region);
            return None;
        }
        Some(cached_meta.state)
    }

    pub fn update_region_state(&mut self, id: u64, state: RegionState) {
        self.regions.get_mut(&id).unwrap().state = state;
    }

    fn overlaps_with(&self, region: &CacheRegion) -> bool {
        let entry = self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&region.start), Unbounded))
            .next();
        if let Some((_key, id)) = entry {
            let meta = &self.regions[id];
            if meta.region.start < region.end {
                return true;
            }
        }
        false
    }

    /// `check_overlap_with_region` check whether there are regions overlap with
    /// target region. If there are regions with `pending` state and whose
    /// epoch version is smaller than target region, the pending regions will
    /// be removed first.
    fn check_overlap_with_region(&mut self, region: &CacheRegion) -> Option<RegionState> {
        let mut removed_regions = vec![];
        let mut overlapped_region_state = None;
        self.iter_overlapped_regions(region, |region_meta| {
            // pending region with out-dated epoch, should be removed.
            if region_meta.state == RegionState::Pending
                && region_meta.region.epoch_version < region.epoch_version
            {
                removed_regions.push(region_meta.region.id);
                return true;
            }
            warn!("ime load region overlaps with existing region";
                "region" => ?region,
                "exist_meta" => ?region_meta);
            overlapped_region_state = Some(region_meta.state);
            false
        });
        if !removed_regions.is_empty() {
            info!("ime load region meet pending region with stale epoch, removed";
                "region" => ?region, "stale_regions" => ?removed_regions);
        }
        for id in removed_regions {
            self.remove_region(id);
        }
        overlapped_region_state
    }

    // Acquire a snapshot of the `range` with `read_ts`. If the range is not
    // accessable, None will be returned. Otherwise, the range id will be returned.
    pub(crate) fn region_snapshot(
        &mut self,
        region_id: u64,
        region_epoch: u64,
        read_ts: u64,
    ) -> result::Result<(), FailedReason> {
        let Some(meta) = self.regions.get_mut(&region_id) else {
            return Err(FailedReason::NotCached);
        };

        if meta.state != RegionState::Active {
            return Err(FailedReason::NotCached);
        }

        if meta.region.epoch_version != region_epoch {
            return Err(FailedReason::EpochNotMatch);
        }

        if read_ts <= meta.safe_point {
            return Err(FailedReason::TooOldRead);
        }

        meta.region_snapshot_list.new_snapshot(read_ts);
        Ok(())
    }

    // If the snapshot is the last one in the snapshot list of one cache range in
    // historical_regions, it means one or some evicted_ranges may be ready to be
    // removed physically.
    // So, we return a vector of ranges to denote the ranges that are ready to be
    // removed.
    pub(crate) fn remove_region_snapshot(
        &mut self,
        snapshot_meta: &RangeCacheSnapshotMeta,
    ) -> Vec<CacheRegion> {
        // fast path: in most case, region is not changed.
        if let Some(region_meta) = self.regions.get_mut(&snapshot_meta.region.id)
            && region_meta.region.epoch_version == snapshot_meta.region.epoch_version
        {
            // epoch not changed
            region_meta
                .region_snapshot_list
                .remove_snapshot(snapshot_meta.snapshot_ts);
            if Self::region_ready_to_evict(region_meta, &self.historical_regions) {
                region_meta.set_state(RegionState::Evicting);
                return vec![region_meta.region.clone()];
            }
            return vec![];
        }

        // slow path: region not found or epoch version changes, must fell in the
        // history regions.
        let hist_key = KeyAndVersion(
            snapshot_meta.region.end.clone(),
            snapshot_meta.region.epoch_version,
        );
        let meta = self.historical_regions.get_mut(&hist_key).unwrap();
        meta.region_snapshot_list
            .remove_snapshot(snapshot_meta.snapshot_ts);

        let mut deletable_regions = vec![];
        if meta.region_snapshot_list.is_empty() {
            self.historical_regions.remove(&hist_key).unwrap();
            self.iter_overlapped_regions(&snapshot_meta.region, |meta| {
                if matches!(
                    meta.get_state(),
                    RegionState::PendingEvict | RegionState::Evicting
                ) {
                    assert_eq!(meta.get_state(), RegionState::PendingEvict);
                    if Self::region_ready_to_evict(meta, &self.historical_regions) {
                        deletable_regions.push(meta.region.clone());
                    }
                }
                true
            });
            for r in &deletable_regions {
                let meta = self.regions.get_mut(&r.id).unwrap();
                meta.set_state(RegionState::Evicting);
            }
        }
        deletable_regions
    }

    // whether target region is ready to be physically evicted.
    // NOTE: region in_gc or region is actively written will also block
    // evicting, but we check these two factor in the DeleteRange worker,
    // so we don't check these two factors here for simplicity.
    #[inline]
    fn region_ready_to_evict(
        meta: &CacheRegionMeta,
        historical_regions: &BTreeMap<KeyAndVersion, CacheRegionMeta>,
    ) -> bool {
        if meta.state != RegionState::PendingEvict {
            return false;
        }
        meta.region_snapshot_list.is_empty()
            && !Self::overlaps_with_historical_regions(&meta.region, historical_regions)
    }

    fn overlaps_with_historical_regions(
        region: &CacheRegion,
        historical_regions: &BTreeMap<KeyAndVersion, CacheRegionMeta>,
    ) -> bool {
        for (_, meta) in historical_regions.range((
            Excluded(KeyAndVersion(region.start.clone(), u64::MAX)),
            Unbounded,
        )) {
            if meta.region.start < region.end {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_history_regions_min_ts(&self, region: &CacheRegion) -> Option<u64> {
        self.historical_regions
            .range((
                Excluded(KeyAndVersion(region.start.clone(), u64::MAX)),
                Unbounded,
            ))
            .filter_map(|(_, meta)| {
                if meta.region.start < region.end {
                    meta.region_snapshot_list.min_snapshot_ts()
                } else {
                    None
                }
            })
            .min()
    }

    pub(crate) fn on_gc_region_finished(&mut self, region: &CacheRegion) {
        let region_meta = self.regions.get_mut(&region.id).unwrap();
        if region_meta.region.epoch_version == region.epoch_version {
            region_meta.set_in_gc(false);
        } else {
            let cache_region = region_meta.region.clone();
            self.iter_overlapped_regions_mut(&cache_region, |meta| {
                assert!(cache_region.contains_range(&meta.region));
                meta.set_in_gc(false);
            });
        }
    }

    /// Return ranges that can be deleted now (no ongoing snapshot).
    // If the region epoch has changed which means the region range may have
    // changed, evict the regions overlapped with the range of `evict_region`.
    // `cb` is called when the eviction of the region with id equaling to the id of
    // `evict_region` has done.
    // Note: `cb` should not do anything heavy.
    pub(crate) fn evict_region(
        &mut self,
        evict_region: &CacheRegion,
        evict_reason: EvictReason,
        mut cb: Option<Box<dyn AsyncFnOnce + Send + Sync>>,
    ) -> Vec<CacheRegion> {
        info!(
            "ime try to evict region";
            "evict_region" => ?evict_region,
            "reason" => ?evict_reason,
        );

        if let Some(meta) = self.regions.get(&evict_region.id) {
            // if epoch not changed, no need to do range scan.
            if meta.region.epoch_version == evict_region.epoch_version {
                if let Some(region) =
                    self.do_evict_region(evict_region.id, evict_region, evict_reason, cb)
                {
                    return vec![region];
                } else {
                    return vec![];
                }
            }
        }

        let mut deleteable_regions = vec![];
        let mut evict_ids = vec![];
        self.iter_overlapped_regions(evict_region, |meta| {
            if evict_region.start >= meta.region.end {
                return false;
            }
            evict_ids.push(meta.region.id);
            true
        });
        if evict_ids.is_empty() {
            info!("ime evict a region that is not cached";
                "reason" => ?evict_reason,
                "region" => ?evict_region);
        }
        for rid in evict_ids {
            if let Some(region) = self.do_evict_region(
                rid,
                evict_region,
                evict_reason,
                if rid == evict_region.id {
                    cb.take()
                } else {
                    None
                },
            ) {
                deleteable_regions.push(region);
            }
        }
        deleteable_regions
    }

    // return the region if it can be directly deleted.
    fn do_evict_region(
        &mut self,
        id: u64,
        evict_region: &CacheRegion,
        evict_reason: EvictReason,
        cb: Option<Box<dyn AsyncFnOnce + Send + Sync>>,
    ) -> Option<CacheRegion> {
        let meta = self.regions.get_mut(&id).unwrap();
        let prev_state = meta.state;
        assert!(
            meta.region.overlaps(evict_region),
            "meta: {:?}, evict_region: {:?}",
            meta,
            evict_region
        );
        if prev_state == RegionState::Pending {
            let meta = self.remove_region(id);
            info!(
                "ime evict overlap pending region in cache range engine";
                "reason" => ?evict_reason,
                "target_region" => ?evict_region,
                "overlap_region" => ?meta.region,
                "state" => ?prev_state,
            );
            return None;
        } else if prev_state.is_evict() {
            info!("ime region already evicted";
                "region" => ?meta.region, "state" => ?prev_state);
            return None;
        }

        if prev_state == RegionState::Active {
            meta.mark_evict(RegionState::PendingEvict, evict_reason, cb);
        } else {
            meta.set_state(RegionState::LoadingCanceled)
        };

        info!(
            "ime evict overlap region in cache range engine";
            "reason" => ?evict_reason,
            "target_region" => ?evict_region,
            "overlap_region" => ?meta.region,
            "state" => ?prev_state,
            "new_state" => ?meta.state,
        );

        if meta.state == RegionState::PendingEvict
            && Self::region_ready_to_evict(meta, &self.historical_regions)
        {
            meta.set_state(RegionState::Evicting);
            return Some(meta.region.clone());
        }
        None
    }

    fn remove_region(&mut self, id: u64) -> CacheRegionMeta {
        let meta = self.regions.remove(&id).unwrap();
        self.regions_by_range.remove(&meta.region.end).unwrap();
        meta
    }

    pub fn on_delete_regions(&mut self, regions: &[CacheRegion], rt: &Runtime) {
        fail::fail_point!("in_memory_engine_on_delete_regions");
        for r in regions {
            let meta = self.remove_region(r.id);
            assert_eq!(meta.region.epoch_version, r.epoch_version);

            let evict_info = meta.evict_info.unwrap();
            observe_eviction_duration(
                evict_info.start.saturating_elapsed_secs(),
                evict_info.reason,
            );
            if let Some(cb) = evict_info.cb {
                rt.block_on(async { cb().await });
            }

            info!(
                "ime range eviction done";
                "region" => ?r,
            );
        }
    }

    // return whether the operation is successful.
    pub fn try_set_regions_in_gc(&self, in_gc: bool) -> bool {
        self.is_gc_task_running
            .compare_exchange(!in_gc, in_gc, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    pub(crate) fn is_overlapped_with_regions_being_written(&self, region: &CacheRegion) -> bool {
        self.regions_being_written.iter().any(|(_, ranges)| {
            ranges
                .iter()
                .any(|range_being_written| range_being_written.overlaps(region))
        })
    }

    pub(crate) fn record_in_region_being_written(
        &mut self,
        write_batch_id: u64,
        region: CacheRegion,
    ) {
        self.regions_being_written
            .entry(write_batch_id)
            .or_default()
            .push(region);
    }

    pub(crate) fn clear_regions_in_being_written(
        &mut self,
        write_batch_id: u64,
        has_entry_applied: bool,
    ) {
        let regions = self.regions_being_written.remove(&write_batch_id);
        if has_entry_applied {
            assert!(!regions.unwrap().is_empty());
        }
    }

    pub fn load_region(&mut self, cache_region: CacheRegion) -> Result<(), LoadFailedReason> {
        use RegionState::*;
        if let Some(state) = self.check_overlap_with_region(&cache_region) {
            let reason = match state {
                Pending | Loading => LoadFailedReason::PendingRange,
                Active => LoadFailedReason::Overlapped,
                LoadingCanceled | PendingEvict | Evicting => LoadFailedReason::Evicting,
            };
            return Err(reason);
        }
        let meta = CacheRegionMeta::new(cache_region);
        self.new_region_meta(meta);
        Ok(())
    }

    // return `true` is the region is evicted.
    pub(crate) fn split_region(
        &mut self,
        source_region: &CacheRegion,
        mut new_regions: Vec<CacheRegion>,
    ) {
        if let Some(region_meta) = self.region_meta(source_region.id) {
            // if region is evicting, skip handling split for simplicity.
            if region_meta.state.is_evict() {
                info!("ime region is evicted, skip split";
                    "meta" => ?&region_meta, "new_regions" => ?new_regions);
                return;
            }
        } else {
            info!("ime split source region not cached"; "region_id" => source_region.id);
            return;
        }

        let region_meta = self.remove_region(source_region.id);
        assert!(!region_meta.state.is_evict());
        if region_meta.region.epoch_version != source_region.epoch_version {
            // for pending regions, we keep regions that still fall in this range if epoch
            // version changed.
            assert_eq!(region_meta.state, RegionState::Pending);
            assert!(region_meta.region.epoch_version < source_region.epoch_version);
            new_regions.retain(|r| region_meta.region.overlaps(r));
            info!("ime handle split region met pending region epoch stale";
                "cached" => ?region_meta,
                "split_source" => ?source_region,
                "cache_new_regions" => ?new_regions);
        }

        info!("ime handle region split";
            "region_id" => source_region.id,
            "meta" => ?region_meta,
            "new_regions" => ?new_regions);

        for r in new_regions {
            let meta = CacheRegionMeta::derive_from(r, &region_meta);
            self.new_region_meta(meta);
        }

        // if there are still active snapshot, we need to put the orginal region
        // into `historical_regions` to track these snapshots.
        if !region_meta.region_snapshot_list.is_empty() {
            self.historical_regions.insert(
                KeyAndVersion(
                    region_meta.region.end.clone(),
                    region_meta.region.epoch_version,
                ),
                region_meta,
            );
        }
    }
}

#[cfg(test)]
impl Drop for RegionManager {
    fn drop(&mut self) {
        // check regions and regions by range matches with each other.
        for (key, id) in &self.regions_by_range {
            let meta = self.regions.get(id).unwrap();
            assert_eq!(key, &meta.region.end);
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum LoadFailedReason {
    Overlapped,
    PendingRange,
    Evicting,
}

#[derive(PartialEq, Debug)]
pub enum RangeCacheStatus {
    NotInCache,
    Cached,
    Loading,
}

#[cfg(test)]
mod tests {
    use engine_traits::{CacheRegion, EvictReason, FailedReason};

    use super::*;
    use crate::range_manager::LoadFailedReason;

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RegionManager::default();
        let r1 = CacheRegion::new(1, 0, "k00", b"k10");

        range_mgr.new_region(r1.clone());
        range_mgr.set_safe_point(r1.id, 5);
        assert_eq!(
            range_mgr.region_snapshot(r1.id, 0, 5).unwrap_err(),
            FailedReason::TooOldRead
        );
        range_mgr.region_snapshot(r1.id, 0, 8).unwrap();
        let snapshot1 = RangeCacheSnapshotMeta::new(r1.clone(), 8, 1);
        range_mgr.region_snapshot(r1.id, 0, 10).unwrap();
        let snapshot2 = RangeCacheSnapshotMeta::new(r1.clone(), 10, 2);
        assert_eq!(
            range_mgr.region_snapshot(2, 0, 8).unwrap_err(),
            FailedReason::NotCached
        );

        let r_evict = CacheRegion::new(2, 2, b"k03", b"k06");
        let r_left = CacheRegion::new(1, 2, b"k00", b"k03");
        let r_right = CacheRegion::new(3, 2, b"k06", b"k10");
        range_mgr.split_region(&r1, vec![r_left.clone(), r_evict.clone(), r_right.clone()]);
        range_mgr.evict_region(&r_evict, EvictReason::AutoEvict, None);
        let meta1 = range_mgr
            .historical_regions
            .get(&KeyAndVersion(r1.end.clone(), 0))
            .unwrap();
        assert_eq!(
            range_mgr.regions.get(&r_evict.id).unwrap().state,
            RegionState::PendingEvict,
        );
        assert_eq!(
            range_mgr.regions_by_range.get(&r1.end).unwrap(),
            &r_right.id
        );
        let meta2 = range_mgr.regions.get(&r_left.id).unwrap();
        let meta3 = range_mgr.regions.get(&r_right.id).unwrap();
        assert!(meta1.safe_point == meta2.safe_point && meta1.safe_point == meta3.safe_point);

        // evict a range with accurate match
        range_mgr.region_snapshot(r_left.id, 2, 10).unwrap();
        let snapshot3 = RangeCacheSnapshotMeta::new(r_left.clone(), 10, 3);
        range_mgr.evict_region(&r_left, EvictReason::AutoEvict, None);
        assert_eq!(
            range_mgr.regions.get(&r_left.id).unwrap().state,
            RegionState::PendingEvict,
        );
        assert!(range_mgr.remove_region_snapshot(&snapshot1).is_empty());

        let regions = range_mgr.remove_region_snapshot(&snapshot2);
        assert_eq!(regions, vec![r_evict.clone()]);
        assert_eq!(
            range_mgr.region_meta(r_evict.id).unwrap().get_state(),
            RegionState::Evicting
        );

        let regions = range_mgr.remove_region_snapshot(&snapshot3);
        assert_eq!(regions, vec![r_left.clone()]);
        assert_eq!(
            range_mgr.region_meta(r_left.id).unwrap().get_state(),
            RegionState::Evicting
        );
    }

    #[test]
    fn test_range_load() {
        let mut range_mgr = RegionManager::default();
        let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
        let mut r2 = CacheRegion::new(2, 2, b"k10", b"k20");
        let r3 = CacheRegion::new(3, 0, b"k20", b"k30");
        let r4 = CacheRegion::new(4, 0, b"k25", b"k35");

        range_mgr.new_region(r1.clone());
        range_mgr.load_region(r2.clone()).unwrap();
        range_mgr.new_region(r3.clone());
        range_mgr.evict_region(&r1, EvictReason::AutoEvict, None);

        assert_eq!(
            range_mgr.load_region(r1).unwrap_err(),
            LoadFailedReason::Evicting
        );

        // load r2 with an outdated epoch.
        r2.epoch_version = 1;
        assert_eq!(
            range_mgr.load_region(r2).unwrap_err(),
            LoadFailedReason::PendingRange,
        );

        assert_eq!(
            range_mgr.load_region(r4).unwrap_err(),
            LoadFailedReason::Overlapped
        );
    }

    #[test]
    fn test_range_load_overlapped() {
        let mut range_mgr = RegionManager::default();
        let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
        let r3 = CacheRegion::new(3, 0, b"k40", b"k50");
        range_mgr.new_region(r1.clone());
        range_mgr.evict_region(&r1, EvictReason::AutoEvict, None);

        range_mgr.load_region(r3).unwrap();

        let r = CacheRegion::new(4, 0, b"k00", b"k05");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );
        let r = CacheRegion::new(4, 0, b"k05", b"k15");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );

        let r = CacheRegion::new(4, 0, b"k35", b"k45");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
        let r = CacheRegion::new(4, 0, b"k45", b"k55");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
    }

    #[test]
    fn test_evict_regions() {
        {
            let mut range_mgr = RegionManager::default();
            let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
            let r2 = CacheRegion::new(2, 0, b"k20", b"k30");
            let r3 = CacheRegion::new(3, 0, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            range_mgr.contains_region(r1.id);
            range_mgr.contains_region(r2.id);
            range_mgr.contains_region(r3.id);

            let r4 = CacheRegion::new(4, 2, b"k00", b"k05");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict, None),
                vec![r1]
            );
        }

        {
            let mut range_mgr = RegionManager::default();
            let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
            let r2 = CacheRegion::new(2, 0, b"k20", b"k30");
            let r3 = CacheRegion::new(3, 0, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            assert!(range_mgr.contains_region(r1.id));
            assert!(range_mgr.contains_region(r2.id));
            assert!(range_mgr.contains_region(r3.id));

            let r4 = CacheRegion::new(4, 0, b"k", b"k51");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict, None),
                vec![r1, r2, r3]
            );
            assert!(
                range_mgr
                    .regions()
                    .values()
                    .all(|m| m.get_state() == RegionState::Evicting)
            );
        }

        {
            let mut range_mgr = RegionManager::default();
            let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
            let r2 = CacheRegion::new(2, 0, b"k20", b"k30");
            let r3 = CacheRegion::new(3, 0, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = CacheRegion::new(4, 0, b"k25", b"k55");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict, None),
                vec![r2, r3]
            );
            assert_eq!(
                range_mgr
                    .regions()
                    .values()
                    .filter(|m| m.get_state() == RegionState::Active)
                    .count(),
                1
            );
        }

        {
            let mut range_mgr = RegionManager::default();
            let r1 = CacheRegion::new(1, 0, b"k00", b"k10");
            let r2 = CacheRegion::new(2, 0, b"k30", b"k40");
            let r3 = CacheRegion::new(3, 0, b"k50", b"k60");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = CacheRegion::new(4, 0, b"k25", b"k75");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict, None),
                vec![r2, r3]
            );
            assert_eq!(
                range_mgr
                    .regions()
                    .values()
                    .filter(|m| m.get_state() == RegionState::Active)
                    .count(),
                1
            );
        }
    }
}
