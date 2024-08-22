// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    assert_matches::assert_matches,
    collections::{
        BTreeMap, BTreeSet,
        Bound::{self, Excluded, Unbounded},
    },
    result,
    sync::atomic::{AtomicBool, Ordering},
};

use collections::HashMap;
use engine_traits::{CacheRange, EvictReason, FailedReason};
use kvproto::metapb::Region;
use tikv_util::{info, time::Instant};

use crate::{metrics::observe_eviction_duration, read::RangeCacheSnapshotMeta};

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
pub struct RangeMeta {
    region: Region,
    // the data range of this region.
    range: CacheRange,
    // active region snapshots.
    region_snapshot_list: SnapshotList,
    // the gc safe point.
    safe_point: u64,
    state: RegionState,
    // whether a gc task is running with this region.
    in_gc: bool,
    // region eviction triggers info, used for logging.
    evict_info: Option<EvictInfo>,
}

#[derive(Debug, Clone, Copy)]
struct EvictInfo {
    start: Instant,
    reason: EvictReason,
}

impl RangeMeta {
    fn new(region: Region) -> Self {
        let range = CacheRange::from_region(&region);
        Self {
            region,
            range,
            region_snapshot_list: SnapshotList::default(),
            safe_point: 0,
            state: RegionState::Pending,
            in_gc: false,
            evict_info: None,
        }
    }

    #[inline]
    pub fn region(&self) -> &Region {
        &self.region
    }

    // update region info due to epoch version changes. This can only
    // happen for pending region because otherwise we will always update
    // the region epoch with ApplyObserver(for loading/active regions) or
    // no need to update the epoch for evicting regions.
    fn amend_pending_region(&mut self, region: &Region) -> bool {
        assert!(
            self.region.id == region.id
                && self.region.get_region_epoch().version < region.get_region_epoch().version
        );
        let new_range = CacheRange::from_region(region);
        if !self.range.contains_range(&new_range) {
            return false;
        }

        self.region = region.clone();
        self.range = new_range;
        true
    }

    pub(crate) fn safe_point(&self) -> u64 {
        self.safe_point
    }

    pub(crate) fn set_safe_point(&mut self, safe_point: u64) {
        assert!(self.safe_point <= safe_point);
        self.safe_point = safe_point;
    }

    pub fn get_range(&self) -> &CacheRange {
        &self.range
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
            "update region meta state";
            "region_id" => self.region.id,
            "epoch" => self.region.get_region_epoch().version,
            "curr_state" => ?self.state,
            "new_state" => ?new_state);
        self.state = new_state;
    }

    pub(crate) fn mark_evict(&mut self, state: RegionState, reason: EvictReason) {
        use RegionState::*;
        assert_matches!(self.state, Loading | Active | LoadingCanceled);
        assert_matches!(state, PendingEvict | Evicting);
        self.set_state(state);
        self.evict_info = Some(EvictInfo {
            start: Instant::now_coarse(),
            reason,
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
    pub(crate) fn derive_from(region: Region, source_meta: &Self) -> Self {
        let range = CacheRange::from_region(&region);
        assert!(source_meta.range.contains_range(&range));
        Self {
            region,
            range,
            region_snapshot_list: SnapshotList::default(),
            safe_point: source_meta.safe_point,
            state: source_meta.state,
            in_gc: source_meta.in_gc,
            evict_info: source_meta.evict_info,
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
    regions: HashMap<u64, RangeMeta>,
    // we use this flag to ensure there is only 1 running gc task.
    is_gc_task_running: AtomicBool,
    // Outdated regions that are split but still hold some on going snapshots.
    // These on going snapshot should block regions fell in this range from gc or eviction.
    // It's possible that multi region with the same end key are in `historical_regions`,
    // so we add epoch version into the key to ensure the uniqueness.
    // (data_end_key, epoch_version) --> region_id
    historical_regions: BTreeMap<KeyAndVersion, RangeMeta>,
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
    regions_being_written: HashMap<u64, Vec<CacheRange>>,

    preferred_range: BTreeSet<CacheRange>,
}

impl RegionManager {
    pub(crate) fn regions(&self) -> &HashMap<u64, RangeMeta> {
        &self.regions
    }

    // load a new region directly in the active state.
    // This fucntion is used for unit/integration tests only.
    pub fn new_region(&mut self, region: Region) {
        let mut range_meta = RangeMeta::new(region);
        range_meta.state = RegionState::Active;
        self.new_region_meta(range_meta);
    }

    fn new_region_meta(&mut self, meta: RangeMeta) {
        assert!(!self.overlaps_with(&meta.range));
        let id = meta.region.id;
        let data_end_key = meta.range.end.clone();
        self.regions.insert(id, meta);
        self.regions_by_range.insert(data_end_key, id);
    }

    pub fn region_meta(&self, id: u64) -> Option<&RangeMeta> {
        self.regions.get(&id)
    }

    pub fn mut_region_meta(&mut self, id: u64) -> Option<&mut RangeMeta> {
        self.regions.get_mut(&id)
    }

    pub fn iter_overlapped_regions(
        &self,
        range: &CacheRange,
        mut f: impl FnMut(&RangeMeta) -> bool,
    ) {
        for (_key, id) in self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&range.start), Unbounded))
        {
            let region_meta = &self.regions[id];
            if region_meta.range.start >= range.end {
                break;
            }
            if !f(region_meta) {
                break;
            }
        }
    }

    pub fn iter_overlapped_regions_mut(
        &mut self,
        range: &CacheRange,
        mut f: impl FnMut(&mut RangeMeta),
    ) {
        for (_key, id) in self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&range.start), Unbounded))
        {
            let region_meta = self.regions.get_mut(id).unwrap();
            if region_meta.range.start >= range.end {
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

    pub fn get_region_for_key(&self, key: &[u8]) -> Option<Region> {
        if let Some((key, id)) = self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(key), Unbounded))
            .next()
        {
            let meta = &self.regions[id];
            if &meta.range.start <= key {
                return Some(meta.region.clone());
            }
        }
        None
    }

    pub fn contains_region(&self, region_id: u64) -> bool {
        self.regions.contains_key(&region_id)
    }

    pub fn check_region_state(&mut self, region: &Region) -> Option<RegionState> {
        use RegionState::*;
        let Some(cached_meta) = self.regions.get_mut(&region.id) else {
            return None;
        };
        if cached_meta.state == Pending
            && cached_meta.region.get_region_epoch().version != region.get_region_epoch().version
            && !cached_meta.amend_pending_region(region)
        {
            info!("remove outdated pending region"; "pending_region" => ?cached_meta.region, "new_region" => ?region);
            self.remove_region(region.id);
            return None;
        }
        Some(cached_meta.state)
    }

    pub fn update_region_state(&mut self, id: u64, state: RegionState) {
        self.regions.get_mut(&id).unwrap().state = state;
    }

    fn overlaps_with(&self, range: &CacheRange) -> bool {
        let entry = self
            .regions_by_range
            .range::<[u8], (Bound<&[u8]>, Bound<&[u8]>)>((Excluded(&range.start), Unbounded))
            .next();
        if let Some((_key, id)) = entry {
            let meta = &self.regions[id];
            if meta.range.start < range.end {
                return true;
            }
        }
        false
    }

    /// `check_overlap_with_region` check whether there are regions overlap with
    /// target region. If there are regions with `pending` state and whose
    /// epoch version is smaller than target region, the pending regions will
    /// be removed first.
    fn check_overlap_with_region(&mut self, region: &Region) -> Option<RegionState> {
        let region_range = CacheRange::from_region(region);
        let mut removed_regions = vec![];
        let mut overlapped_region_state = None;
        self.iter_overlapped_regions(&region_range, |region_meta| {
            // pending region with out-dated epoch, should be removed.
            if region_meta.state == RegionState::Pending
                && region_meta.region.get_region_epoch().version < region.get_region_epoch().version
            {
                removed_regions.push(region_meta.region.id);
                return true;
            }
            tikv_util::debug!("load region overlaps with existing region";
                "region" => ?region,
                "exist_meta" => ?region_meta);
            overlapped_region_state = Some(region_meta.state);
            false
        });
        if !removed_regions.is_empty() {
            info!("load region meet pending region with stale epoch, removed";
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

        if meta.region.get_region_epoch().version != region_epoch {
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
    ) -> Vec<Region> {
        // fast path: in most case, region is not changed.
        if let Some(region_meta) = self.regions.get_mut(&snapshot_meta.region_id)
            && region_meta.region.get_region_epoch().version == snapshot_meta.epoch_version
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
        let hist_key = KeyAndVersion(snapshot_meta.range.end.clone(), snapshot_meta.epoch_version);
        let meta = self.historical_regions.get_mut(&hist_key).unwrap();
        meta.region_snapshot_list
            .remove_snapshot(snapshot_meta.snapshot_ts);

        let mut deletable_regions = vec![];
        if meta.region_snapshot_list.is_empty() {
            self.historical_regions.remove(&hist_key).unwrap();
            self.iter_overlapped_regions(&snapshot_meta.range, |meta| {
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
        meta: &RangeMeta,
        historical_regions: &BTreeMap<KeyAndVersion, RangeMeta>,
    ) -> bool {
        if meta.state != RegionState::PendingEvict {
            return false;
        }
        meta.region_snapshot_list.is_empty()
            && !Self::overlaps_with_historical_regions(&meta.range, historical_regions)
    }

    fn overlaps_with_historical_regions(
        range: &CacheRange,
        historical_regions: &BTreeMap<KeyAndVersion, RangeMeta>,
    ) -> bool {
        for (_, meta) in historical_regions.range((
            Excluded(KeyAndVersion(range.start.clone(), u64::MAX)),
            Unbounded,
        )) {
            if meta.range.start < range.end {
                return true;
            }
        }
        false
    }

    pub(crate) fn get_history_regions_min_ts(&self, range: &CacheRange) -> Option<u64> {
        self.historical_regions
            .range((
                Excluded(KeyAndVersion(range.start.clone(), u64::MAX)),
                Unbounded,
            ))
            .filter_map(|(_, meta)| {
                if meta.range.start < range.end {
                    meta.region_snapshot_list.min_snapshot_ts()
                } else {
                    None
                }
            })
            .min()
    }

    pub(crate) fn on_gc_region_finished(&mut self, region: &Region) {
        let region_meta = self.regions.get_mut(&region.id).unwrap();
        if region_meta.region.get_region_epoch().version == region.get_region_epoch().version {
            region_meta.set_in_gc(false);
        } else {
            let range = region_meta.range.clone();
            self.iter_overlapped_regions_mut(&range, |meta| {
                assert!(range.contains_range(&meta.range));
                meta.set_in_gc(false);
            });
        }
    }

    /// Return ranges that can be deleted now (no ongoing snapshot).
    // There are two cases based on the relationship between `evict_range` and
    // cached ranges:
    // 1. `evict_range` is contained(including equals) by a cached range (at most
    //    one due to non-overlapping in cached ranges)
    // 2. `evict_range` is overlapped with (including contains but not be contained)
    //    one or more cached ranges
    //
    // For 1, if the `evict_range` is a proper subset of the cached_range, we will
    // split the cached_range so that only the `evict_range` part will be evicted
    // and deleted.
    //
    // For 2, this is caused by some special operations such as merge and delete
    // range. So, conservatively, we evict all ranges overlap with it.
    pub(crate) fn evict_region(
        &mut self,
        evict_region: &Region,
        evict_reason: EvictReason,
    ) -> Vec<Region> {
        info!(
            "try to evict region";
            "evict_region" => ?evict_region,
            "reason" => ?evict_reason,
        );

        if let Some(meta) = self.regions.get(&evict_region.id) {
            // if epoch not changed, no need to do range scan.
            if meta.region.get_region_epoch().version == evict_region.get_region_epoch().version {
                if let Some(region) =
                    self.do_evict_region(evict_region.id, evict_region, evict_reason)
                {
                    return vec![region];
                }
            }
        }

        let mut deleteable_regions = vec![];
        let mut evict_ids = vec![];
        let evict_range = CacheRange::from_region(evict_region);
        self.iter_overlapped_regions(&evict_range, |meta| {
            if evict_range.start >= meta.range.end {
                return false;
            }
            evict_ids.push(meta.region.id);
            true
        });
        if evict_ids.is_empty() {
            info!("evict a region that is not cached";
                "reason" => ?evict_reason,
                "region" => ?evict_region);
        }
        for rid in evict_ids {
            if let Some(region) = self.do_evict_region(rid, evict_region, evict_reason) {
                deleteable_regions.push(region);
            }
        }
        deleteable_regions
    }

    // return the region if it can be directly deleted.
    fn do_evict_region(
        &mut self,
        id: u64,
        evict_region: &Region,
        evict_reason: EvictReason,
    ) -> Option<Region> {
        let meta = self.regions.get_mut(&id).unwrap();
        let prev_state = meta.state;
        assert!(
            meta.range.overlaps(&CacheRange::from_region(evict_region)),
            "meta: {:?}, evict_region: {:?}",
            meta,
            evict_region
        );
        if prev_state == RegionState::Pending {
            let meta = self.remove_region(id);
            info!(
                "evict overlap pending region in cache range engine";
                "reason" => ?evict_reason,
                "target_region" => ?evict_region,
                "overlap_region" => ?meta.region,
                "state" => ?prev_state,
            );
            return None;
        } else if prev_state.is_evict() {
            info!("region already evicted"; "region" => ?meta.region, "state" => ?prev_state);
            return None;
        }

        if prev_state == RegionState::Active {
            meta.mark_evict(RegionState::PendingEvict, evict_reason);
        } else {
            meta.set_state(RegionState::LoadingCanceled)
        };

        info!(
            "evict overlap region in cache range engine";
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

    fn remove_region(&mut self, id: u64) -> RangeMeta {
        let meta = self.regions.remove(&id).unwrap();
        self.regions_by_range.remove(&meta.range.end);
        meta
    }

    pub fn on_delete_regions(&mut self, regions: &[Region]) {
        for r in regions {
            let meta = self.remove_region(r.id);
            assert_eq!(
                meta.region.get_region_epoch().version,
                r.get_region_epoch().version
            );

            let evict_info = meta.evict_info.unwrap();
            observe_eviction_duration(
                evict_info.start.saturating_elapsed_secs(),
                evict_info.reason,
            );
            info!(
                "range eviction done";
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

    pub(crate) fn is_overlapped_with_regions_being_written(&self, range: &CacheRange) -> bool {
        self.regions_being_written.iter().any(|(_, ranges)| {
            ranges
                .iter()
                .any(|range_being_written| range_being_written.overlaps(range))
        })
    }

    pub(crate) fn record_in_region_being_written(
        &mut self,
        write_batch_id: u64,
        range: CacheRange,
    ) {
        self.regions_being_written
            .entry(write_batch_id)
            .or_default()
            .push(range);
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

    pub fn load_region(&mut self, region: Region) -> Result<(), LoadFailedReason> {
        use RegionState::*;
        if let Some(state) = self.check_overlap_with_region(&region) {
            let reason = match state {
                Pending | Loading => LoadFailedReason::PendingRange,
                Active => LoadFailedReason::Overlapped,
                LoadingCanceled | PendingEvict | Evicting => LoadFailedReason::Evicting,
            };
            return Err(reason);
        }
        let meta = RangeMeta::new(region);
        self.new_region_meta(meta);
        Ok(())
    }

    // return `true` is the region is evicted.
    pub(crate) fn split_region(
        &mut self,
        source_region: &Region,
        mut new_regions: Vec<kvproto::metapb::Region>,
    ) {
        if let Some(region_meta) = self.region_meta(source_region.id) {
            // if region is evicting, skip handling split for simplicity.
            if region_meta.state.is_evict() {
                info!("region is evicted, skip split"; "meta" => ?&region_meta, "new_regions" => ?new_regions);
                return;
            }
        } else {
            info!("split source region not cached"; "region_id" => source_region.id);
            return;
        }

        let region_meta = self.remove_region(source_region.id);
        assert!(!region_meta.state.is_evict());
        if region_meta.region.get_region_epoch().version != source_region.get_region_epoch().version
        {
            // for pending regions, we keep regions that still fall in this range if epoch
            // version changed.
            assert_eq!(region_meta.state, RegionState::Pending);
            assert!(
                region_meta.region.get_region_epoch().version
                    < source_region.get_region_epoch().version
            );
            new_regions.retain(|r| {
                r.start_key <= source_region.start_key
                    && (source_region.end_key.is_empty()
                        || (source_region.end_key >= r.end_key && !r.end_key.as_slice().is_empty()))
            });
            info!("[IME] handle split region met pending region epoch stale";
                "cached" => ?region_meta,
                "split_source" => ?source_region,
                "cache_new_regions" => ?new_regions);
        }

        info!("handle region split";
            "region_id" => source_region.id,
            "meta" => ?region_meta,
            "new_regions" => ?new_regions);

        for r in new_regions {
            let meta = RangeMeta::derive_from(r, &region_meta);
            self.new_region_meta(meta);
        }

        // if there are still active snapshot, we need to put the orginal region
        // into `historical_regions` to track these snapshots.
        if !region_meta.region_snapshot_list.is_empty() {
            self.historical_regions.insert(
                KeyAndVersion(
                    region_meta.range.end.clone(),
                    region_meta.region.get_region_epoch().version,
                ),
                region_meta,
            );
        }
    }

    pub fn overlap_with_preferred_range(&self, range: &CacheRange) -> bool {
        self.preferred_range.iter().any(|r| r.overlaps(range))
    }

    pub fn add_preferred_range(&mut self, range: CacheRange) {
        let mut union = range;
        self.preferred_range.retain(|r| {
            let Some(u) = r.union(&union) else {
                return true;
            };
            info!(
                "remove preferred range that is overlapped with range";
                "union" => ?union,
                "preferred_range" => ?r,
            );
            union = u;
            // The intersected range need to be removed before updating
            // the union range.
            false
        });
        info!("add preferred range"; "range" => ?union);
        self.preferred_range.insert(union);
    }

    pub fn remove_preferred_range(&mut self, range: CacheRange) {
        let mut diffs = Vec::new();
        self.preferred_range.retain(|r| {
            match r.difference(&range) {
                (None, None) => {
                    // Remove the range if it is overlapped with the range.
                    if !r.overlaps(&range) {
                        return true;
                    }
                }
                others => diffs.push(others),
            };
            info!(
                "remove preferred range that is overlapped with range";
                "range" => ?range,
                "preferred_range" => ?r,
            );
            // The intersected range need to be removed before updating
            // the union range.
            false
        });
        assert!(diffs.len() <= 2, "{:?}", diffs);
        for (left, right) in diffs.into_iter() {
            if let Some(left) = left {
                info!("update preferred range"; "range" => ?left);
                self.preferred_range.insert(left);
            }
            if let Some(right) = right {
                info!("update preferred range"; "range" => ?right);
                self.preferred_range.insert(right);
            }
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
    use engine_traits::{CacheRange, EvictReason, FailedReason};

    use super::*;
    use crate::{range_manager::LoadFailedReason, test_util::new_region};

    #[test]
    fn test_range_manager() {
        let mut range_mgr = RegionManager::default();
        let r1 = new_region(1, "k00", b"k10");

        range_mgr.new_region(r1.clone());
        range_mgr.set_safe_point(r1.id, 5);
        assert_eq!(
            range_mgr.region_snapshot(r1.id, 0, 5).unwrap_err(),
            FailedReason::TooOldRead
        );
        range_mgr.region_snapshot(r1.id, 0, 8).unwrap();
        let snapshot1 = RangeCacheSnapshotMeta::new(1, 0, CacheRange::from_region(&r1), 8, 1);
        range_mgr.region_snapshot(r1.id, 0, 10).unwrap();
        let snapshot2 = RangeCacheSnapshotMeta::new(1, 0, CacheRange::from_region(&r1), 10, 2);
        assert_eq!(
            range_mgr.region_snapshot(2, 0, 8).unwrap_err(),
            FailedReason::NotCached
        );

        let mut r_evict = new_region(2, b"k03", b"k06");
        let mut r_left = new_region(1, b"k00", b"k03");
        let mut r_right = new_region(3, b"k06", b"k10");
        for r in [&mut r_evict, &mut r_left, &mut r_right] {
            r.mut_region_epoch().version = 2;
        }
        range_mgr.split_region(&r1, vec![r_left.clone(), r_evict.clone(), r_right.clone()]);
        range_mgr.evict_region(&r_evict, EvictReason::AutoEvict);
        let range1 = CacheRange::from_region(&r1);
        let meta1 = range_mgr
            .historical_regions
            .get(&KeyAndVersion(range1.end.clone(), 0))
            .unwrap();
        assert_eq!(
            range_mgr.regions.get(&r_evict.id).unwrap().state,
            RegionState::PendingEvict,
        );
        assert_eq!(
            range_mgr.regions_by_range.get(&range1.end).unwrap(),
            &r_right.id
        );
        let meta2 = range_mgr.regions.get(&r_left.id).unwrap();
        let meta3 = range_mgr.regions.get(&r_right.id).unwrap();
        assert!(meta1.safe_point == meta2.safe_point && meta1.safe_point == meta3.safe_point);

        // evict a range with accurate match
        range_mgr.region_snapshot(r_left.id, 2, 10).unwrap();
        let snapshot3 =
            RangeCacheSnapshotMeta::new(r_left.id, 2, CacheRange::from_region(&r1), 10, 3);
        range_mgr.evict_region(&r_left, EvictReason::AutoEvict);
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
        let r1 = new_region(1, b"k00", b"k10");
        let mut r2 = new_region(2, b"k10", b"k20");
        r2.mut_region_epoch().version = 2;
        let r3 = new_region(3, b"k20", b"k30");
        let r4 = new_region(4, b"k25", b"k35");

        range_mgr.new_region(r1.clone());
        range_mgr.load_region(r2.clone()).unwrap();
        range_mgr.new_region(r3.clone());
        range_mgr.evict_region(&r1, EvictReason::AutoEvict);

        assert_eq!(
            range_mgr.load_region(r1).unwrap_err(),
            LoadFailedReason::Evicting
        );

        // load r2 with an outdated epoch.
        r2.mut_region_epoch().version = 1;
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
        let r1 = new_region(1, b"k00", b"k10");
        // let r2 = new_region(2, b"k20", b"k30");
        let r3 = new_region(3, b"k40", b"k50");
        range_mgr.new_region(r1.clone());
        range_mgr.evict_region(&r1, EvictReason::AutoEvict);

        range_mgr.load_region(r3).unwrap();

        let r = new_region(4, b"k00", b"k05");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );
        let r = new_region(4, b"k05", b"k15");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::Evicting
        );

        let r = new_region(4, b"k35", b"k45");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
        let r = new_region(4, b"k45", b"k55");
        assert_eq!(
            range_mgr.load_region(r).unwrap_err(),
            LoadFailedReason::PendingRange
        );
    }

    #[test]
    fn test_evict_regions() {
        {
            let mut range_mgr = RegionManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            range_mgr.contains_region(r1.id);
            range_mgr.contains_region(r2.id);
            range_mgr.contains_region(r3.id);

            let mut r4 = new_region(4, b"k00", b"k05");
            r4.mut_region_epoch().version = 2;
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict),
                vec![r1]
            );
        }

        {
            let mut range_mgr = RegionManager::default();
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());
            assert!(range_mgr.contains_region(r1.id));
            assert!(range_mgr.contains_region(r2.id));
            assert!(range_mgr.contains_region(r3.id));

            let r4 = new_region(4, b"k", b"k51");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict),
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
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k20", b"k30");
            let r3 = new_region(3, b"k40", b"k50");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = new_region(4, b"k25", b"k55");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict),
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
            let r1 = new_region(1, b"k00", b"k10");
            let r2 = new_region(2, b"k30", b"k40");
            let r3 = new_region(3, b"k50", b"k60");
            range_mgr.new_region(r1.clone());
            range_mgr.new_region(r2.clone());
            range_mgr.new_region(r3.clone());

            let r4 = new_region(4, b"k25", b"k75");
            assert_eq!(
                range_mgr.evict_region(&r4, EvictReason::AutoEvict),
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

    #[test]
    fn test_overlap_with_preferred_range() {
        let mut range_mgr = RegionManager::default();
        range_mgr.add_preferred_range(CacheRange::new(b"k00".to_vec(), b"k10".to_vec()));
        range_mgr.add_preferred_range(CacheRange::new(b"k20".to_vec(), b"k30".to_vec()));
        range_mgr.add_preferred_range(CacheRange::new(b"k30".to_vec(), b"k50".to_vec()));

        struct Case {
            name: &'static str,
            range: (&'static [u8], &'static [u8]),
            expected: bool,
        }
        let cases = [
            Case {
                name: "left intersect 1",
                range: (b"k00", b"k05"),
                expected: true,
            },
            Case {
                name: "left intersect 2",
                range: (b"k15", b"k25"),
                expected: true,
            },
            Case {
                name: "cover all",
                range: (b"k00", b"k60"),
                expected: true,
            },
            Case {
                name: "right intersect",
                range: (b"k05", b"k15"),
                expected: true,
            },
            Case {
                name: "left and right intersect",
                range: (b"k25", b"k45"),
                expected: true,
            },
            Case {
                name: "not overlap 1",
                range: (b"k15", b"k20"),
                expected: false,
            },
            Case {
                name: "not overlap 2",
                range: (b"k", b"k0"),
                expected: false,
            },
            Case {
                name: "not overlap 3",
                range: (b"k60", b"k70"),
                expected: false,
            },
        ];

        for case in cases {
            let range = CacheRange::new(case.range.0.to_vec(), case.range.1.to_vec());
            assert_eq!(
                range_mgr.overlap_with_preferred_range(&range),
                case.expected,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn test_preferred_range_add_remove() {
        struct Case {
            name: &'static str,
            build: Vec<(&'static [u8], &'static [u8])>,
            remove: Vec<(&'static [u8], &'static [u8])>,
            add: Vec<(&'static [u8], &'static [u8])>,
            result: Vec<(&'static [u8], &'static [u8])>,
        }
        let cases = [
            Case {
                name: "remove empty",
                build: vec![],
                remove: vec![(b"k00", b"k10")],
                add: vec![],
                result: vec![],
            },
            Case {
                name: "add empty",
                build: vec![],
                remove: vec![],
                add: vec![(b"k00", b"k10")],
                result: vec![(b"k00", b"k10")],
            },
            // Test remove
            Case {
                name: "remove one range",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30"), (b"k40", b"k50")],
                remove: vec![(b"k20", b"k30")],
                add: vec![],
                result: vec![(b"k00", b"k10"), (b"k40", b"k50")],
            },
            Case {
                name: "remove left intersected ranges",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30"), (b"k40", b"k50")],
                remove: vec![(b"k", b"k05")],
                add: vec![],
                result: vec![(b"k05", b"k10"), (b"k20", b"k30"), (b"k40", b"k50")],
            },
            Case {
                name: "remove left and right intersected ranges",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30"), (b"k40", b"k50")],
                remove: vec![(b"k05", b"k45")],
                add: vec![],
                result: vec![(b"k00", b"k05"), (b"k45", b"k50")],
            },
            Case {
                name: "remove right intersected ranges",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30"), (b"k40", b"k50")],
                remove: vec![(b"k45", b"k60")],
                add: vec![],
                result: vec![(b"k00", b"k10"), (b"k20", b"k30"), (b"k40", b"k45")],
            },
            // Test add
            Case {
                name: "add left intersected ranges 1",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30")],
                add: vec![(b"k", b"k05")],
                remove: vec![],
                result: vec![(b"k", b"k10"), (b"k20", b"k30")],
            },
            Case {
                name: "add left intersected ranges 2",
                build: vec![(b"k00", b"k10"), (b"k20", b"k30")],
                add: vec![(b"k", b"k25")],
                remove: vec![],
                result: vec![(b"k", b"k30")],
            },
            Case {
                name: "add right intersected ranges 1",
                build: vec![(b"k20", b"k30"), (b"k40", b"k50")],
                add: vec![(b"k45", b"k55")],
                remove: vec![],
                result: vec![(b"k20", b"k30"), (b"k40", b"k55")],
            },
            Case {
                name: "add right intersected ranges 2",
                build: vec![(b"k20", b"k30"), (b"k40", b"k50")],
                add: vec![(b"k25", b"k55")],
                remove: vec![],
                result: vec![(b"k20", b"k55")],
            },
            Case {
                name: "add left and right intersected ranges 1",
                build: vec![(b"k20", b"k30")],
                add: vec![(b"k10", b"k50")],
                remove: vec![],
                result: vec![(b"k10", b"k50")],
            },
            Case {
                name: "add left and right intersected ranges 2",
                build: vec![(b"k10", b"k20"), (b"k20", b"k30")],
                add: vec![(b"k10", b"k50")],
                remove: vec![],
                result: vec![(b"k10", b"k50")],
            },
        ];

        for case in cases {
            // Build
            let mut range_mgr = RegionManager::default();
            for (start, end) in case.build {
                let r = CacheRange::new(start.to_vec(), end.to_vec());
                range_mgr.add_preferred_range(r);
            }

            // Remove
            for (start, end) in case.remove {
                let r = CacheRange::new(start.to_vec(), end.to_vec());
                range_mgr.remove_preferred_range(r);
            }

            // Add
            for (start, end) in case.add {
                let r = CacheRange::new(start.to_vec(), end.to_vec());
                range_mgr.add_preferred_range(r);
            }

            // Check
            let result = range_mgr
                .preferred_range
                .iter()
                .map(|r| (r.start.as_slice(), r.end.as_slice()))
                .collect::<Vec<_>>();
            assert_eq!(result, case.result, "case: {}", case.name);
        }
    }
}
