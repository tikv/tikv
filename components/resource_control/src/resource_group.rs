// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    cmp::{max, min},
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

// Duration of each bucket in the historical RU sliding window.
const BUCKET_DURATION_SECS: u64 = 60;
// Scale factor applied to reservation_tag increments so that integer division
// of (vt_delta / cached_reservation) retains enough precision even for small
// per-task deltas. All reservation_tag values and last_min_r_tag are in these
// scaled units, so comparisons remain consistent.
const RESERVATION_TAG_SCALE: u64 = 1_000_000;

use collections::HashMap;
use dashmap::{DashMap, mapref::one::Ref};
use fail::fail_point;
use kvproto::{
    kvrpcpb::{CommandPri, ResourceControlContext},
    resource_manager::{GroupMode, ResourceGroup as PbResourceGroup},
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use tikv_util::{
    config::VersionTrack,
    info,
    resource_control::{DEFAULT_RESOURCE_GROUP_NAME, TaskMetadata, TaskPriority},
    time::Instant,
};
use yatp::queue::priority::TaskPriorityProvider;

use crate::{config::Config, metrics::deregister_metrics, resource_limiter::ResourceLimiter};

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default value of max RU quota.
const DEFAULT_MAX_RU_QUOTA: u64 = 10_000;
/// The maximum RU quota that can be configured.
const MAX_RU_QUOTA: u64 = i32::MAX as u64;

#[cfg(test)]
const LOW_PRIORITY: u32 = 1;
const MEDIUM_PRIORITY: u32 = 8;
#[cfg(test)]
const HIGH_PRIORITY: u32 = 16;

// the global maximum of virtual time is u64::MAX / 16, so when the virtual
// time of all groups are bigger than half of this value, we rest them to avoid
// virtual time overflow.
const RESET_VT_THRESHOLD: u64 = (u64::MAX >> 4) / 2;

/// Sliding-window tracker for VT consumption history.
///
/// Tracks per-group VT consumption in 1-minute buckets over a configurable
/// window. Used to compute the historical VT rate for dynamic reservation.
pub(crate) struct RuTracker {
    // Ring buffer of per-bucket VT totals.
    buckets: Vec<u64>,
    current_bucket: usize,
    last_rotation: Instant,
}

impl RuTracker {
    fn new(window_secs: u64) -> Self {
        let num_buckets = (window_secs / BUCKET_DURATION_SECS).max(1) as usize;
        Self {
            buckets: vec![0; num_buckets],
            current_bucket: 0,
            last_rotation: Instant::now_coarse(),
        }
    }

    /// Record `vt_delta` (consumed * weight) into the current bucket,
    /// rotating stale buckets first if a minute or more has elapsed.
    fn record(&mut self, vt_delta: u64) {
        let elapsed_secs = self
            .last_rotation
            .saturating_elapsed()
            .as_secs();
        if elapsed_secs >= BUCKET_DURATION_SECS {
            let rotations =
                (elapsed_secs / BUCKET_DURATION_SECS) as usize;
            let n = self.buckets.len();
            let clear = rotations.min(n);
            for i in 0..clear {
                let idx = (self.current_bucket + 1 + i) % n;
                self.buckets[idx] = 0;
            }
            self.current_bucket = (self.current_bucket + rotations) % n;
            // Advance last_rotation by the number of full buckets elapsed so
            // sub-minute remainder is preserved.
            self.last_rotation = Instant::now_coarse();
        }
        self.buckets[self.current_bucket] =
            self.buckets[self.current_bucket].saturating_add(vt_delta);
    }

    /// Returns the average VT consumption rate in VT-units per second over
    /// the full window.
    pub(crate) fn get_vt_rate_per_second(&self) -> u64 {
        let total: u64 = self.buckets.iter().sum();
        let window_secs = self.buckets.len() as u64 * BUCKET_DURATION_SECS;
        total / window_secs
    }
}

/// Encodes a two-phase scheduling priority.
///
/// Format: `[4-bit group_priority | 4-bit phase | 56-bit tag]`
///
/// Phase 0 (reservation) always sorts before phase 1 (weight) within the
/// same group priority tier because phase bits contribute 0 vs 1<<56.
fn encode_two_phase_priority(group_priority: u32, phase: u8, tag: u64) -> u64 {
    assert!((1..=16).contains(&group_priority));
    let tag = tag & ((1u64 << 56) - 1);
    let phase_bits = ((phase & 0xf) as u64) << 56;
    (!((group_priority - 1) as u64) << 60) | phase_bits | tag
}

pub enum ResourceConsumeType {
    CpuTime(Duration),
    IoBytes(u64),
}

/// ResourceGroupManager manages the metadata of each resource group.
pub struct ResourceGroupManager {
    pub(crate) resource_groups: DashMap<String, ResourceGroup>,
    // the count of all groups, a fast path because call `DashMap::len` is a little slower.
    group_count: AtomicU64,
    registry: RwLock<Vec<Arc<ResourceController>>>,
    // auto incremental version generator used for mark the background
    // resource limiter has changed.
    version_generator: AtomicU64,
    // the shared resource limiter of each priority
    priority_limiters: [Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT],
    // lastest config.
    config: Arc<VersionTrack<Config>>,
}

impl Default for ResourceGroupManager {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

impl ResourceGroupManager {
    pub fn new(config: Config) -> Self {
        let priority_limiters = TaskPriority::priorities().map(|p| {
            Arc::new(ResourceLimiter::new(
                p.as_str().to_owned(),
                f64::INFINITY,
                f64::INFINITY,
                0,
                false,
            ))
        });
        let manager = Self {
            resource_groups: Default::default(),
            group_count: AtomicU64::new(0),
            registry: Default::default(),
            version_generator: AtomicU64::new(0),
            priority_limiters,
            config: Arc::new(VersionTrack::new(config)),
        };

        // init the default resource group by default.
        let mut default_group = PbResourceGroup::new();
        default_group.name = DEFAULT_RESOURCE_GROUP_NAME.into();
        default_group.priority = MEDIUM_PRIORITY;
        default_group.mode = GroupMode::RuMode;
        default_group
            .mut_r_u_settings()
            .mut_r_u()
            .mut_settings()
            .fill_rate = MAX_RU_QUOTA;
        manager.add_resource_group(default_group);

        manager
    }

    #[inline]
    pub fn get_group_count(&self) -> u64 {
        self.group_count.load(Ordering::Relaxed)
    }

    fn get_ru_setting(rg: &PbResourceGroup, is_read: bool) -> u64 {
        match (rg.get_mode(), is_read) {
            // RU mode, read and write use the same setting.
            (GroupMode::RuMode, _) => rg
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            // TODO: currently we only consider the cpu usage in the read path, we may also take
            // io read bytes into account later.
            (GroupMode::RawMode, true) => rg
                .get_raw_resource_settings()
                .get_cpu()
                .get_settings()
                .get_fill_rate(),
            (GroupMode::RawMode, false) => rg
                .get_raw_resource_settings()
                .get_io_write()
                .get_settings()
                .get_fill_rate(),
            // return a default value for unsupported config.
            (GroupMode::Unknown, _) => 1,
        }
    }

    pub fn add_resource_group(&self, rg: PbResourceGroup) {
        let group_name = rg.get_name().to_ascii_lowercase();
        self.registry.read().iter().for_each(|controller| {
            let ru_quota = Self::get_ru_setting(&rg, controller.is_read);
            controller.add_resource_group(group_name.clone().into_bytes(), ru_quota, rg.priority);
        });
        info!("add resource group"; "name"=> &rg.name, "ru" => rg.get_r_u_settings().get_r_u().get_settings().get_fill_rate());
        // try to reuse the quota limit when update resource group settings.
        let prev_limiter = self
            .resource_groups
            .get(&rg.name)
            .and_then(|g| g.limiter.clone());
        let limiter = self.build_resource_limiter(&rg, prev_limiter);

        if self
            .resource_groups
            .insert(group_name, ResourceGroup::new(rg, limiter))
            .is_none()
        {
            self.group_count.fetch_add(1, Ordering::Relaxed);
        }
        self.update_has_background();
    }

    fn update_has_background(&self) {
        let any_has_bg = self
            .resource_groups
            .iter()
            .any(|g| !g.background_source_types.is_empty());
        self.registry.read().iter().for_each(|controller| {
            controller.set_has_background(any_has_bg);
        });
    }

    fn build_resource_limiter(
        &self,
        rg: &PbResourceGroup,
        old_limiter: Option<Arc<ResourceLimiter>>,
    ) -> Option<Arc<ResourceLimiter>> {
        if !rg.get_background_settings().get_job_types().is_empty() {
            old_limiter.or_else(|| {
                let version = self.version_generator.fetch_add(1, Ordering::Relaxed);
                Some(Arc::new(ResourceLimiter::new(
                    rg.name.clone(),
                    f64::INFINITY,
                    f64::INFINITY,
                    version,
                    true,
                )))
            })
        } else {
            None
        }
    }

    pub fn remove_resource_group(&self, name: &str) {
        let group_name = name.to_ascii_lowercase();
        self.registry.read().iter().for_each(|controller| {
            controller.remove_resource_group(group_name.as_bytes());
        });
        if self.resource_groups.remove(&group_name).is_some() {
            deregister_metrics(name);
            info!("remove resource group"; "name"=> name);
            self.group_count.fetch_sub(1, Ordering::Relaxed);
        }
        self.update_has_background();
    }

    pub fn retain(&self, mut f: impl FnMut(&String, &PbResourceGroup) -> bool) {
        let mut removed_names = vec![];
        self.resource_groups.retain(|k, v| {
            // avoid remove default group.
            if k == DEFAULT_RESOURCE_GROUP_NAME {
                return true;
            }
            let ret = f(k, &v.group);
            if !ret {
                removed_names.push(k.clone());
                deregister_metrics(k);
            }
            ret
        });
        if !removed_names.is_empty() {
            self.registry.read().iter().for_each(|controller| {
                for name in &removed_names {
                    controller.remove_resource_group(name.as_bytes());
                }
            });
            self.group_count
                .fetch_sub(removed_names.len() as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, ResourceGroup>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_config(&self) -> &Arc<VersionTrack<Config>> {
        &self.config
    }

    pub fn get_all_resource_groups(&self) -> Vec<PbResourceGroup> {
        self.resource_groups
            .iter()
            .map(|g| g.group.clone())
            .collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read, self.config.clone()));
        self.registry.write().push(controller.clone());
        for g in &self.resource_groups {
            let ru_quota = Self::get_ru_setting(&g.value().group, controller.is_read);
            controller.add_resource_group(g.key().clone().into_bytes(), ru_quota, g.group.priority);
        }
        controller
    }

    pub fn advance_min_virtual_time(&self) {
        for controller in self.registry.read().iter() {
            controller.update_min_virtual_time();
        }
    }

    pub fn consume_penalty(&self, ctx: &ResourceControlContext) {
        for controller in self.registry.read().iter() {
            // FIXME: Should consume CPU time for read controller and write bytes for write
            // controller, once CPU process time of scheduler worker is tracked. Currently,
            // we consume write bytes for read controller as the
            // order of magnitude of CPU time and write bytes is similar.
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::CpuTime(Duration::from_nanos(
                    (ctx.get_penalty().total_cpu_time_ms * 1_000_000.0) as u64,
                )),
            );
            controller.consume(
                ctx.resource_group_name.as_bytes(),
                ResourceConsumeType::IoBytes(ctx.get_penalty().write_bytes as u64),
            );
        }
    }

    // only enable priority quota limiter when there is at least 1 user-defined
    // resource group.
    #[inline]
    fn enable_priority_limiter(&self) -> bool {
        // TODO: reenable it once when we fix https://github.com/tikv/tikv/issues/18939
        // self.get_group_count() > 1
        false
    }

    // Always return the background resource limiter if any;
    // Only return the foregroup limiter when priority is enabled.
    pub fn get_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
        override_priority: u64,
    ) -> Option<Arc<ResourceLimiter>> {
        let (limiter, group_priority) =
            self.get_background_resource_limiter_with_priority(rg, request_source);
        if limiter.is_some() {
            return limiter;
        }

        // if there is only 1 resource group, priority quota limiter is useless so just
        // return None for better performance.
        if !self.enable_priority_limiter() {
            return None;
        }

        // request priority has higher priority, 0 means priority is not set.
        let mut task_priority = override_priority as u32;
        if task_priority == 0 {
            task_priority = group_priority;
        }
        Some(self.priority_limiters[TaskPriority::from(task_priority) as usize].clone())
    }

    // return a ResourceLimiter for background tasks only.
    pub fn get_background_resource_limiter(
        &self,
        rg: &str,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        self.get_background_resource_limiter_with_priority(rg, request_source)
            .0
    }

    fn get_background_resource_limiter_with_priority(
        &self,
        rg: &str,
        request_source: &str,
    ) -> (Option<Arc<ResourceLimiter>>, u32) {
        fail_point!("only_check_source_task_name", |name| {
            assert_eq!(&name.unwrap(), request_source);
            (None, 8)
        });
        let mut group_priority = None;
        if let Some(group) = self.resource_groups.get(rg) {
            group_priority = Some(group.group.priority);
            if !group.fallback_default {
                return (
                    group.get_background_resource_limiter(request_source),
                    group.group.priority,
                );
            }
        }

        let default_group = self
            .resource_groups
            .get(DEFAULT_RESOURCE_GROUP_NAME)
            .unwrap();
        (
            default_group.get_background_resource_limiter(request_source),
            group_priority.unwrap_or(default_group.group.priority),
        )
    }

    #[inline]
    pub fn get_priority_resource_limiters(
        &self,
    ) -> &[Arc<ResourceLimiter>; TaskPriority::PRIORITY_COUNT] {
        &self.priority_limiters
    }
}

pub(crate) struct ResourceGroup {
    pub group: PbResourceGroup,
    pub limiter: Option<Arc<ResourceLimiter>>,
    background_source_types: HashSet<String>,
    // whether to fallback background resource control to `default` group.
    fallback_default: bool,
}

impl ResourceGroup {
    fn new(group: PbResourceGroup, limiter: Option<Arc<ResourceLimiter>>) -> Self {
        let background_source_types =
            HashSet::from_iter(group.get_background_settings().get_job_types().to_owned());
        let fallback_default =
            !group.has_background_settings() && group.name != DEFAULT_RESOURCE_GROUP_NAME;
        Self {
            group,
            limiter,
            background_source_types,
            fallback_default,
        }
    }

    pub(crate) fn get_ru_quota(&self) -> u64 {
        assert!(self.group.has_r_u_settings());
        self.group
            .get_r_u_settings()
            .get_r_u()
            .get_settings()
            .get_fill_rate()
    }

    fn get_background_resource_limiter(
        &self,
        request_source: &str,
    ) -> Option<Arc<ResourceLimiter>> {
        self.limiter.as_ref().and_then(|limiter| {
            // the source task name is the last part of `request_source` separated by "_"
            // the request_source is
            // {extrenal|internal}_{tidb_req_source}_{source_task_name}
            let source_task_name = request_source.rsplit('_').next().unwrap_or("");
            if !source_task_name.is_empty()
                && self.background_source_types.contains(source_task_name)
            {
                Some(limiter.clone())
            } else {
                None
            }
        })
    }
}

pub struct ResourceController {
    // resource controller name is not used currently.
    #[allow(dead_code)]
    name: String,
    // We handle the priority differently between read and write request:
    // 1. the priority factor is calculated based on read/write RU settings.
    // 2. for read request, we increase a constant virtual time delta at each `get_priority` call
    //    because the cost can't be calculated at start, so we only increase a constant delta and
    //    increase the real cost after task is executed; but don't increase it at write because the
    //    cost is known so we just pre-consume it.
    is_read: bool,
    // Track the maximum ru quota used to calculate the factor of each resource group.
    // factor = max_ru_quota / group_ru_quota * 10.0
    // We use mutex here to ensure when we need to change this value and do adjust all resource
    // groups' factors, it can't be changed concurrently.
    // NOTE: because the ru config for "default" group is very large, and it can cause very big
    // group weight, we will not count this value by default.
    max_ru_quota: Mutex<u64>,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: RwLock<HashMap<Vec<u8>, GroupPriorityTracker>>,
    // the latest min vt, this value is used to init new added group vt
    last_min_vt: AtomicU64,
    // the last time min vt is overflow
    last_rest_vt_time: Cell<Instant>,
    // whether the settings are customized by user
    customized: AtomicBool,
    // whether any resource group has background settings configured
    has_background: AtomicBool,
    // The minimum reservation_tag across all groups. Used as the phase-0
    // threshold: groups with reservation_tag <= last_min_r_tag are in phase 0.
    // Only meaningful for read controllers with enable_dynamic_reservation.
    last_min_r_tag: AtomicU64,
    // Shared config. Read on the hot path to check enable_dynamic_reservation.
    config: Arc<VersionTrack<Config>>,
}

// we are ensure to visit the `last_rest_vt_time` by only 1 thread so it's
// thread safe.
unsafe impl Send for ResourceController {}
unsafe impl Sync for ResourceController {}

impl ResourceController {
    fn new(name: String, is_read: bool, config: Arc<VersionTrack<Config>>) -> Self {
        Self {
            name,
            is_read,
            resource_consumptions: RwLock::new(HashMap::default()),
            last_min_vt: AtomicU64::new(0),
            max_ru_quota: Mutex::new(DEFAULT_MAX_RU_QUOTA),
            last_rest_vt_time: Cell::new(Instant::now_coarse()),
            customized: AtomicBool::new(false),
            has_background: AtomicBool::new(false),
            last_min_r_tag: AtomicU64::new(0),
            config,
        }
    }

    pub fn new_for_test(name: String, is_read: bool) -> Self {
        let controller =
            Self::new(name, is_read, Arc::new(VersionTrack::new(Config::default())));
        // add the "default" resource group.
        controller.add_resource_group(
            DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
            0,
            MEDIUM_PRIORITY,
        );
        controller
    }

    fn calculate_factor(max_quota: u64, quota: u64) -> u64 {
        // we don't adjust the max_quota if it's the "default" group's default
        // value(u32::MAX), so here it is possible that the quota is bigger than
        // the max quota
        if quota == 0 || quota > max_quota {
            1
        } else {
            // we use max_quota / quota as the resource group factor, but because we need to
            // cast the value to integer, so we times it by 10 to ensure the accuracy is
            // enough.
            let max_quota = min(max_quota * 10, MAX_RU_QUOTA);
            (max_quota as f64 / quota as f64).round() as u64
        }
    }

    fn add_resource_group(&self, name: Vec<u8>, mut ru_quota: u64, mut group_priority: u32) {
        if group_priority == 0 {
            // map 0 to medium priority(default priority)
            group_priority = MEDIUM_PRIORITY;
        }
        if ru_quota > MAX_RU_QUOTA {
            ru_quota = MAX_RU_QUOTA;
        }

        let mut max_ru_quota = self.max_ru_quota.lock().unwrap();
        // skip to adjust max ru if it is the "default" group and the ru config eq
        // MAX_RU_QUOTA
        if ru_quota > *max_ru_quota
            && (name != DEFAULT_RESOURCE_GROUP_NAME.as_bytes() || ru_quota < MAX_RU_QUOTA)
        {
            *max_ru_quota = ru_quota;
            // adjust all group weight because the current value is too small.
            self.adjust_all_resource_group_factors(ru_quota);
        }
        let weight = Self::calculate_factor(*max_ru_quota, ru_quota);

        let vt_delta_for_get = if self.is_read {
            DEFAULT_PRIORITY_PER_READ_TASK * weight
        } else {
            0
        };
        // static_reservation = ru_quota * weight, the VT rate at which a group
        // consuming exactly its configured quota would accumulate virtual time.
        let static_reservation = ru_quota.saturating_mul(weight).max(1);
        let window_secs = self
            .config
            .value()
            .ru_historical_window
            .as_secs();
        let (ru_tracker, reservation_tag_init) = if self.is_read {
            // Preserve reservation_tag from an existing entry so a group
            // re-registration doesn't reset its phase position.
            let prev_r_tag = self
                .resource_consumptions
                .read()
                .get(&name)
                .map(|g| g.reservation_tag.load(Ordering::Relaxed))
                .unwrap_or_else(|| self.last_min_r_tag.load(Ordering::Acquire));
            (
                Some(Mutex::new(RuTracker::new(window_secs))),
                prev_r_tag,
            )
        } else {
            (None, 0)
        };
        let group = GroupPriorityTracker {
            ru_quota,
            group_priority,
            weight,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
            reservation_tag: AtomicU64::new(reservation_tag_init),
            cached_reservation: AtomicU64::new(static_reservation),
            ru_tracker,
        };

        // maybe update existed group
        self.resource_consumptions.write().insert(name, group);
        self.check_customized();
    }

    fn check_customized(&self) {
        let groups = self.resource_consumptions.read();
        if groups.len() == 1 && groups.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).is_some() {
            self.customized.store(false, Ordering::Release);
            return;
        }
        self.customized.store(true, Ordering::Release);
    }

    // we calculate the weight of each resource group based on the currently maximum
    // ru quota, if a incoming resource group has a bigger quota, we need to
    // adjust all the existing groups. As we expect this won't happen very
    // often, and iterate 10k entry cost less than 5ms, so the performance is
    // acceptable.
    fn adjust_all_resource_group_factors(&self, max_ru_quota: u64) {
        self.resource_consumptions
            .write()
            .iter_mut()
            .for_each(|(_, tracker)| {
                tracker.weight = Self::calculate_factor(max_ru_quota, tracker.ru_quota);
                let static_reservation =
                    tracker.ru_quota.saturating_mul(tracker.weight).max(1);
                tracker
                    .cached_reservation
                    .store(static_reservation, Ordering::Relaxed);
            });
    }

    fn remove_resource_group(&self, name: &[u8]) {
        // do not remove the default resource group, reset to default setting instead.
        if DEFAULT_RESOURCE_GROUP_NAME.as_bytes() == name {
            self.add_resource_group(
                DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
                0,
                MEDIUM_PRIORITY,
            );
            self.check_customized();
            return;
        }
        self.resource_consumptions.write().remove(name);
        self.check_customized();
    }

    pub fn is_customized(&self) -> bool {
        self.customized.load(Ordering::Acquire) || self.has_background.load(Ordering::Acquire)
    }

    pub fn set_has_background(&self, has_background: bool) {
        self.has_background.store(has_background, Ordering::Release);
    }

    #[inline]
    fn resource_group(&self, name: &[u8]) -> MappedRwLockReadGuard<'_, GroupPriorityTracker> {
        let guard = self.resource_consumptions.read();
        RwLockReadGuard::map(guard, |m| {
            if let Some(g) = m.get(name) {
                g
            } else {
                m.get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes()).unwrap()
            }
        })
    }

    pub fn consume(&self, name: &[u8], resource: ResourceConsumeType) {
        self.resource_group(name).consume(resource)
    }

    pub fn update_min_virtual_time(&self) {
        // Reservation state must be refreshed every call regardless of VT
        // spread, so that last_min_r_tag and cached_reservation stay current
        // even when the system is idle.
        if self.is_read {
            self.update_reservation_state();
        }

        let start = Instant::now_coarse();
        let mut min_vt = u64::MAX;
        let mut max_vt = 0;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                min_vt = min(min_vt, vt);
                max_vt = max(max_vt, vt);
            });

        // TODO: use different threshold for different resource type
        // needn't do update if the virtual different is less than 100ms/100KB.
        if min_vt >= max_vt.saturating_sub(100_000) && max_vt < RESET_VT_THRESHOLD {
            return;
        }

        fail_point!("increase_vt_duration_update_min_vt");

        let near_overflow = min_vt > RESET_VT_THRESHOLD;
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                let vt = tracker.current_vt();
                // NOTE: this decrease vt is not atomic across all resource groups,
                // but it should be ok as this operation should be extremely rare
                // and the impact is not big.
                if near_overflow {
                    tracker.decrease_vt(RESET_VT_THRESHOLD);
                } else if vt < max_vt {
                    // TODO: is increase by half is a good choice.
                    tracker.increase_vt((max_vt - vt) / 2);
                }
            });
        if near_overflow {
            let end = Instant::now_coarse();
            info!("all resource groups' virtual time are near overflow, do reset";
                "min" => min_vt, "max" => max_vt, "dur" => ?end.duration_since(start),
                "reset_dur" => ?end.duration_since(self.last_rest_vt_time.get()));
            max_vt -= RESET_VT_THRESHOLD;
            self.last_rest_vt_time.set(end);
        }
        // max_vt is actually a little bigger than the current min vt, but we don't
        // need totally accurate here.
        self.last_min_vt.store(max_vt, Ordering::Relaxed);
    }

    /// Refreshes each group's `cached_reservation` from its `RuTracker` (when
    /// dynamic reservation is enabled) and updates `last_min_r_tag`.
    ///
    /// Called once per second from `update_min_virtual_time`.
    fn update_reservation_state(&self) {
        let dynamic = self.config.value().enable_dynamic_reservation;
        let mut min_r_tag = u64::MAX;

        // First pass: refresh cached_reservation, find global minimum R.
        self.resource_consumptions
            .read()
            .iter()
            .for_each(|(_, tracker)| {
                if let Some(ru_tracker) = &tracker.ru_tracker {
                    if dynamic {
                        let historical_rate = ru_tracker.lock().unwrap().get_vt_rate_per_second();
                        // When there is no history yet (e.g. a brand-new or
                        // idle group), use the cost of a single read task per
                        // second as the reservation baseline. This is
                        // intentionally small so that a sudden burst
                        // immediately pushes R above last_min_r_tag and lands
                        // the group in phase 1. Using ru_quota * weight as a
                        // fallback would be too large for unlimited-quota
                        // groups and would prevent phase transitions entirely.
                        let reservation = if historical_rate > 0 {
                            historical_rate
                        } else {
                            (DEFAULT_PRIORITY_PER_READ_TASK * tracker.weight).max(1)
                        };
                        tracker
                            .cached_reservation
                            .store(reservation, Ordering::Relaxed);
                    }
                    let r = tracker.reservation_tag.load(Ordering::Relaxed);
                    min_r_tag = min(min_r_tag, r);
                }
            });

        if min_r_tag == u64::MAX {
            return;
        }

        // Second pass: normalize all reservation_tags by subtracting the
        // global minimum. This prevents unbounded growth and, crucially,
        // avoids the idle-group problem: without normalization an idle group
        // (R=0) would drag last_min_r_tag to 0 and put every other group into
        // phase 1. After normalization the lagging group's tag stays at 0
        // while active groups accumulate relative excess above 0.
        if min_r_tag > 0 {
            self.resource_consumptions
                .read()
                .iter()
                .for_each(|(_, tracker)| {
                    if tracker.ru_tracker.is_some() {
                        tracker
                            .reservation_tag
                            .fetch_sub(min_r_tag, Ordering::Relaxed);
                    }
                });
        }

        // Phase-0 threshold: groups with normalized R <= RESERVATION_TAG_SCALE
        // are considered within their reservation. This gives a small grace
        // band so that a group consuming slightly above its historical rate
        // is not immediately penalized.
        self.last_min_r_tag
            .store(RESERVATION_TAG_SCALE, Ordering::Relaxed);
    }

    pub fn get_priority(&self, name: &[u8], pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        self.resource_group(name)
            .get_priority(level, None, true, self.two_phase_threshold())
    }

    /// Returns the priority for the given task metadata without incrementing
    /// virtual time. Used for pre-spawn eviction comparison.
    pub fn peek_priority_of(&self, metadata: &TaskMetadata<'_>, pri: CommandPri) -> u64 {
        let level = Self::command_pri_to_level(pri);
        let group = self.resource_group(metadata.group_name());
        let override_priority = if metadata.override_priority() == 0 {
            None
        } else {
            Some(metadata.override_priority())
        };
        group.get_priority(level, override_priority, false, self.two_phase_threshold())
    }

    /// Returns `Some(last_min_r_tag)` when two-phase scheduling is active
    /// (read controller with enable_dynamic_reservation), `None` otherwise.
    #[inline]
    fn two_phase_threshold(&self) -> Option<u64> {
        if self.is_read && self.config.value().enable_dynamic_reservation {
            Some(self.last_min_r_tag.load(Ordering::Relaxed))
        } else {
            None
        }
    }

    fn command_pri_to_level(pri: CommandPri) -> usize {
        match pri {
            CommandPri::High => 0,
            CommandPri::Normal => 1,
            CommandPri::Low => 2,
        }
    }
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        let metadata = TaskMetadata::from(extras.metadata());
        self.resource_group(metadata.group_name()).get_priority(
            extras.current_level() as usize,
            if metadata.override_priority() == 0 {
                None
            } else {
                Some(metadata.override_priority())
            },
            true,
            self.two_phase_threshold(),
        )
    }
}

fn concat_priority_vt(group_priority: u32, vt: u64) -> u64 {
    assert!((1..=16).contains(&group_priority));

    // map group_priority from [1, 16] to [0, 15] to limit it 4 bits and get bitwise
    // negation to replace leading 4 bits of vt. So that the priority is ordered in
    // the descending order by group_priority first, then by vt in ascending order.
    vt | (!((group_priority - 1) as u64) << 60)
}

struct GroupPriorityTracker {
    // the ru setting of this group.
    ru_quota: u64,
    group_priority: u32,
    weight: u64,
    virtual_time: AtomicU64,
    // the constant delta value for each `get_priority` call,
    vt_delta_for_get: u64,
    // --- Two-phase scheduling fields (populated for read controllers only) ---
    // Accumulated R tag: sum of (vt_delta / static_reservation) over all
    // consume() calls. Compared against last_min_r_tag to choose phase.
    reservation_tag: AtomicU64,
    // Cached static reservation = ru_quota * weight. Refreshed when the group
    // is (re-)registered. For dynamic mode this is overwritten each second
    // with the historical VT rate from ru_tracker.
    cached_reservation: AtomicU64,
    // Historical VT consumption tracker. None for write controllers.
    ru_tracker: Option<Mutex<RuTracker>>,
}

impl GroupPriorityTracker {
    /// Computes the scheduling priority for a task at the given level.
    ///
    /// When `advance_vt` is true, atomically increments the virtual time
    /// (used when actually scheduling a task). When false, reads virtual
    /// time without advancing it (used for priority comparison only).
    ///
    /// `last_min_r_tag`: when `Some`, enables two-phase scheduling for this
    /// read controller. The group is placed in phase 0 (reservation, high
    /// priority) if its reservation_tag is at or below the threshold, and
    /// phase 1 (weight-based) otherwise.
    fn get_priority(
        &self,
        level: usize,
        override_priority: Option<u32>,
        advance_vt: bool,
        last_min_r_tag: Option<u64>,
    ) -> u64 {
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        let priority = override_priority.unwrap_or(self.group_priority);

        if let Some(min_r_tag) = last_min_r_tag {
            // Two-phase path: advance both VT and reservation_tag.
            let vt_delta = self.vt_delta_for_get;
            let vt = if advance_vt && vt_delta > 0 {
                self.virtual_time
                    .fetch_add(vt_delta, Ordering::Relaxed)
                    + vt_delta
            } else {
                self.virtual_time.load(Ordering::Relaxed) + vt_delta
            } + task_extra_priority;

            // Advance reservation_tag by vt_delta * SCALE / cached_reservation.
            // The scale factor preserves precision when vt_delta << cached_reservation.
            let reservation = self.cached_reservation.load(Ordering::Relaxed);
            let r_delta = vt_delta
                .saturating_mul(RESERVATION_TAG_SCALE)
                / reservation.max(1);
            let r_tag = if advance_vt && r_delta > 0 {
                self.reservation_tag
                    .fetch_add(r_delta, Ordering::Relaxed)
                    + r_delta
            } else {
                self.reservation_tag.load(Ordering::Relaxed) + r_delta
            };

            if r_tag <= min_r_tag {
                // Phase 0: within reservation, highest priority.
                encode_two_phase_priority(priority, 0, r_tag)
            } else {
                // Phase 1: over reservation, fall back to weight-based VT.
                encode_two_phase_priority(priority, 1, vt)
            }
        } else {
            // Single-phase path: existing behaviour.
            let vt = (if advance_vt && self.vt_delta_for_get > 0 {
                self.virtual_time
                    .fetch_add(self.vt_delta_for_get, Ordering::Relaxed)
                    + self.vt_delta_for_get
            } else {
                self.virtual_time.load(Ordering::Relaxed) + self.vt_delta_for_get
            }) + task_extra_priority;
            concat_priority_vt(priority, vt)
        }
    }

    #[inline]
    fn current_vt(&self) -> u64 {
        self.virtual_time.load(Ordering::Relaxed)
    }

    #[inline]
    fn increase_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed);
    }

    #[inline]
    fn decrease_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_sub(vt_delta, Ordering::Relaxed);
    }

    // TODO: make it delta type as generic to avoid mixed consume different types.
    #[inline]
    fn consume(&self, resource: ResourceConsumeType) {
        let vt_delta = match resource {
            ResourceConsumeType::CpuTime(dur) => dur.as_micros() as u64,
            ResourceConsumeType::IoBytes(bytes) => bytes,
        } * self.weight;
        self.increase_vt(vt_delta);

        // Update reservation_tag and historical tracker for read controllers.
        if let Some(ru_tracker) = &self.ru_tracker {
            ru_tracker.lock().unwrap().record(vt_delta);
            let reservation = self.cached_reservation.load(Ordering::Relaxed);
            let r_delta = vt_delta
                .saturating_mul(RESERVATION_TAG_SCALE)
                / reservation.max(1);
            if r_delta > 0 {
                self.reservation_tag
                    .fetch_add(r_delta, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use yatp::queue::Extras;

    use super::*;
    use crate::resource_limiter::ResourceType::{Cpu, Io};

    pub fn new_resource_group_ru(name: String, ru: u64, group_priority: u32) -> PbResourceGroup {
        new_resource_group(name, true, ru, ru, group_priority)
    }

    pub fn new_background_resource_group_ru(
        name: String,
        ru: u64,
        group_priority: u32,
        task_types: Vec<String>,
    ) -> PbResourceGroup {
        let mut rg = new_resource_group(name, true, ru, ru, group_priority);
        rg.mut_background_settings()
            .set_job_types(task_types.into());
        rg
    }

    pub fn new_resource_group(
        name: String,
        is_ru_mode: bool,
        read_tokens: u64,
        write_tokens: u64,
        group_priority: u32,
    ) -> PbResourceGroup {
        use kvproto::resource_manager::{GroupRawResourceSettings, GroupRequestUnitSettings};

        let mut group = PbResourceGroup::new();
        group.set_name(name);
        let mode = if is_ru_mode {
            GroupMode::RuMode
        } else {
            GroupMode::RawMode
        };
        group.set_mode(mode);
        group.set_priority(group_priority);
        if is_ru_mode {
            assert!(read_tokens == write_tokens);
            let mut ru_setting = GroupRequestUnitSettings::new();
            ru_setting
                .mut_r_u()
                .mut_settings()
                .set_fill_rate(read_tokens);
            group.set_r_u_settings(ru_setting);
        } else {
            let mut resource_setting = GroupRawResourceSettings::new();
            resource_setting
                .mut_cpu()
                .mut_settings()
                .set_fill_rate(read_tokens);
            resource_setting
                .mut_io_write()
                .mut_settings()
                .set_fill_rate(write_tokens);
            group.set_raw_resource_settings(resource_setting);
        }
        group
    }

    #[test]
    fn test_resource_group() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group1 = new_resource_group_ru("TEST".into(), 100, 0);
        resource_manager.add_resource_group(group1);

        assert!(resource_manager.get_resource_group("test1").is_none());
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 100);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group1 = new_resource_group_ru("Test".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(group.get_ru_quota(), 200);
        assert_eq!(group.value().group.get_priority(), 1);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 3);

        let group1 = resource_ctl.resource_group(b"test");
        let group2 = resource_ctl.resource_group(b"test2");
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        resource_ctl.consume(
            b"test",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl.consume(
            b"test2",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );

        assert_eq!(group1.current_vt(), group1.weight * 10000);
        assert_eq!(group1.current_vt(), group2.current_vt() * 2);

        // test update all group vts
        resource_manager.advance_min_virtual_time();
        let group1_vt = group1.current_vt();
        let group1_weight = group1.weight;
        assert_eq!(group1_vt, group1.weight * 10000);
        assert!(group2.current_vt() >= group1.current_vt() * 3 / 4);
        assert!(resource_ctl.resource_group(b"default").current_vt() >= group1.current_vt() / 2);

        drop(group1);
        drop(group2);

        // test add 1 new resource group
        let new_group = new_resource_group_ru("new_group".into(), 600, HIGH_PRIORITY);
        resource_manager.add_resource_group(new_group);

        assert_eq!(resource_ctl.resource_consumptions.read().len(), 4);
        let group3 = resource_ctl.resource_group("new_group".as_bytes());
        assert!(group1_weight - 10 <= group3.weight * 3 && group3.weight * 3 <= group1_weight + 10);
        assert!(group3.current_vt() >= group1_vt / 2);
        drop(group3);

        // test resource group resource limiter.
        let group1 = resource_manager.get_resource_group("test").unwrap();
        assert!(group1.limiter.is_none());
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
        let new_default = new_background_resource_group_ru(
            "default".into(),
            10000,
            MEDIUM_PRIORITY,
            vec!["br".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        let limiter = default_group.limiter.as_ref().unwrap().clone();
        assert!(limiter.get_limiter(Cpu).get_rate_limit().is_infinite());
        assert!(limiter.get_limiter(Io).get_rate_limit().is_infinite());
        limiter.get_limiter(Cpu).set_rate_limit(100.0);
        limiter.get_limiter(Io).set_rate_limit(200.0);
        drop(group1);
        drop(default_group);

        let new_default = new_background_resource_group_ru(
            "default".into(),
            100,
            LOW_PRIORITY,
            vec!["lightning".into()],
        );
        resource_manager.add_resource_group(new_default);
        let default_group = resource_manager.get_resource_group("default").unwrap();
        assert_eq!(default_group.get_ru_quota(), 100);
        let new_limiter = default_group.limiter.as_ref().unwrap().clone();
        // check rate_limiter is not changed.
        assert_eq!(new_limiter.get_limiter(Cpu).get_rate_limit(), 100.0);
        assert_eq!(new_limiter.get_limiter(Io).get_rate_limit(), 200.0);
        assert_eq!(&*new_limiter as *const _, &*limiter as *const _);
        drop(default_group);

        // remove background setting, quota limiter should be none.
        let new_default = new_resource_group_ru("default".into(), 100, LOW_PRIORITY);
        resource_manager.add_resource_group(new_default);
        assert!(
            resource_manager
                .get_resource_group("default")
                .unwrap()
                .limiter
                .is_none()
        );
    }

    #[test]
    fn test_resource_group_crud() {
        let resource_manager = ResourceGroupManager::default();
        assert_eq!(resource_manager.get_group_count(), 1);

        let group1 = new_resource_group_ru("test1".into(), 100, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_manager.get_group_count(), 2);

        let group2 = new_resource_group_ru("test2".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.get_group_count(), 3);

        let group1 = new_resource_group_ru("test1".into(), 150, HIGH_PRIORITY);
        resource_manager.add_resource_group(group1.clone());
        assert_eq!(resource_manager.get_group_count(), 3);
        assert_eq!(
            resource_manager.get_resource_group("test1").unwrap().group,
            group1
        );

        resource_manager.remove_resource_group("test2");
        assert!(resource_manager.get_resource_group("test2").is_none());
        assert_eq!(resource_manager.get_group_count(), 2);

        resource_manager.remove_resource_group("test2");
        assert_eq!(resource_manager.get_group_count(), 2);
    }

    #[test]
    fn test_resource_group_priority() {
        let resource_manager = ResourceGroupManager::default();
        let group1 = new_resource_group_ru("test1".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 3);

        let resource_ctl = resource_manager.derive_controller("test".into(), true);

        let group1 = resource_ctl.resource_group("test1".as_bytes());
        let group2 = resource_ctl.resource_group("test2".as_bytes());
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        let mut extras1 = Extras::single_level();
        extras1.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test1".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras1),
            concat_priority_vt(LOW_PRIORITY, group1.weight * 50)
        );
        assert_eq!(group1.current_vt(), group1.weight * 50);

        let mut extras2 = Extras::single_level();
        extras2.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2),
            concat_priority_vt(MEDIUM_PRIORITY, group2.weight * 50)
        );
        assert_eq!(group2.current_vt(), group2.weight * 50);

        // test override priority
        let mut extras2_override = Extras::single_level();
        extras2_override.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "test2".to_string(),
                override_priority: LOW_PRIORITY as u64,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras2_override),
            concat_priority_vt(LOW_PRIORITY, group2.weight * 100)
        );
        assert_eq!(group2.current_vt(), group2.weight * 100);

        let mut extras3 = Extras::single_level();
        extras3.set_metadata(
            TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: "unknown_group".to_string(),
                override_priority: 0,
                ..Default::default()
            })
            .to_vec(),
        );
        assert_eq!(
            resource_ctl.priority_of(&extras3),
            concat_priority_vt(MEDIUM_PRIORITY, 50)
        );
        assert_eq!(
            resource_ctl
                .resource_group("default".as_bytes())
                .current_vt(),
            50
        );
    }

    #[test]
    fn test_reset_resource_group_vt() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let group1 = new_resource_group_ru("g1".into(), i32::MAX as u64, 1);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("g2".into(), 1, 16);
        resource_manager.add_resource_group(group2);

        let g1 = resource_ctl.resource_group(b"g1");
        let g2 = resource_ctl.resource_group(b"g2");
        let threshold = 1 << 59;
        let mut last_g2_vt = 0;
        for i in 0..8 {
            resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
            resource_manager.advance_min_virtual_time();
            if i < 7 {
                assert!(g2.current_vt() < threshold);
            }
            // after 8 round, g1's vt still under the threshold and is still increasing.
            assert!(g1.current_vt() < threshold && g1.current_vt() > last_g2_vt);
            last_g2_vt = g2.current_vt();
        }

        resource_ctl.consume(b"g2", ResourceConsumeType::IoBytes(1 << 25));
        resource_manager.advance_min_virtual_time();
        assert!(g1.current_vt() > threshold);

        // adjust again, the virtual time of each group should decrease
        resource_manager.advance_min_virtual_time();
        let g1_vt = g1.current_vt();
        let g2_vt = g2.current_vt();
        assert!(g2_vt < threshold / 2);
        assert!(g1_vt < threshold / 2 && g1_vt < g2_vt);
        assert_eq!(resource_ctl.last_min_vt.load(Ordering::Relaxed), g2_vt);
    }

    #[test]
    fn test_adjust_resource_group_weight() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);
        assert_eq!(resource_ctl.is_customized(), false);
        assert_eq!(resource_ctl_write.is_customized(), false);
        let group1 = new_resource_group_ru("test1".into(), 5000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 20);
        assert_eq!(resource_ctl.is_customized(), true);
        assert_eq!(resource_ctl_write.is_customized(), true);

        // add a resource group with big ru
        let group1 = new_resource_group_ru("test2".into(), 50000, 0);
        resource_manager.add_resource_group(group1);
        assert_eq!(*resource_ctl.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl.resource_group(b"test2").weight, 10);
        // resource_ctl_write should be unchanged.
        assert_eq!(*resource_ctl_write.max_ru_quota.lock().unwrap(), 50000);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"test2").weight, 10);

        // add the default "default" group, the ru weight should not change.
        // add a resource group with big ru
        let group = new_resource_group_ru("default".into(), u32::MAX as u64, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 100);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 1);

        // change the default group to another value, it can impact the ru then.
        let group = new_resource_group_ru("default".into(), 100000, 0);
        resource_manager.add_resource_group(group);
        assert_eq!(resource_ctl_write.resource_group(b"test1").weight, 200);
        assert_eq!(resource_ctl_write.resource_group(b"default").weight, 10);
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_reset_resource_group_vt_overflow() {
        use rand::{RngCore, thread_rng};
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);
        let mut rng = thread_rng();

        let mut min_delta = u64::MAX;
        let mut max_delta = 0;
        for i in 0..10 {
            let name = format!("g{}", i);
            let g = new_resource_group_ru(name.clone(), 100, 1);
            resource_manager.add_resource_group(g);
            let delta = rng.next_u64() % 10000 + 1;
            min_delta = delta.min(min_delta);
            max_delta = delta.max(max_delta);
            resource_ctl
                .resource_group(name.as_bytes())
                .increase_vt(RESET_VT_THRESHOLD + delta);
        }
        resource_ctl
            .resource_group(b"default")
            .increase_vt(RESET_VT_THRESHOLD + 1);

        let old_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        let resource_ctl_cloned = resource_ctl.clone();
        fail::cfg_callback("increase_vt_duration_update_min_vt", move || {
            resource_ctl_cloned
                .resource_consumptions
                .read()
                .iter()
                .enumerate()
                .for_each(|(i, (_, tracker))| {
                    if i % 2 == 0 {
                        tracker.increase_vt(max_delta - min_delta);
                    }
                });
        })
        .unwrap();
        resource_ctl.update_min_virtual_time();
        fail::remove("increase_vt_duration_update_min_vt");

        let new_max_vt = resource_ctl
            .resource_consumptions
            .read()
            .iter()
            .fold(0, |v, (_, g)| v.max(g.current_vt()));
        // check all vt has decreased by RESET_VT_THRESHOLD.
        assert!(new_max_vt < max_delta * 2);
        // check fail-point takes effect, the `new_max_vt` has increased.
        assert!(old_max_vt - RESET_VT_THRESHOLD < new_max_vt);
    }

    #[test]
    fn test_retain_resource_groups() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        let resource_ctl_write = resource_manager.derive_controller("test_write".into(), false);

        for i in 0..5 {
            let group1 = new_resource_group_ru(format!("test{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
            // add a resource group with big ru
            let group1 = new_resource_group_ru(format!("group{}", i), 100, 0);
            resource_manager.add_resource_group(group1);
        }
        // consume for default group
        resource_ctl.consume(
            b"default",
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl_write.consume(b"default", ResourceConsumeType::IoBytes(10000));

        // 10 + 1(default)
        assert_eq!(resource_manager.get_all_resource_groups().len(), 11);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 11);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 11);

        resource_manager.retain(|k, _v| k.starts_with("test"));
        assert_eq!(resource_manager.get_all_resource_groups().len(), 6);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 6);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 6);
        assert!(resource_manager.get_resource_group("group1").is_none());
        // should use the virtual time of default group for non-exist group
        assert_ne!(resource_ctl.resource_group(b"group2").current_vt(), 0);
        assert_ne!(resource_ctl_write.resource_group(b"group2").current_vt(), 0);
    }

    #[test]
    fn test_concat_priority_vt() {
        let v1 = concat_priority_vt(MEDIUM_PRIORITY, 1000);
        let v2 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert!(v1 < v2);

        let v3 = concat_priority_vt(LOW_PRIORITY, 1000);
        assert!(v1 < v3);

        let v4 = concat_priority_vt(MEDIUM_PRIORITY, 1111);
        assert_eq!(v2, v4);

        let v5 = concat_priority_vt(HIGH_PRIORITY, 10);
        assert!(v5 < v1);
    }

    #[test]
    fn test_ru_tracker_rate() {
        let window_secs = 120; // 2 buckets of 60s each
        let mut tracker = RuTracker::new(window_secs);

        // Record 6000 VT in the first bucket (rate = 6000/120 = 50/s).
        tracker.record(6000);
        assert_eq!(tracker.get_vt_rate_per_second(), 50);

        // Simulate bucket rotation by directly advancing last_rotation.
        tracker.last_rotation =
            Instant::now_coarse() - Duration::from_secs(BUCKET_DURATION_SECS);
        tracker.record(3000); // adds 3000 to the new bucket
        // total = 6000 + 3000 = 9000, window = 120s → 75/s
        assert_eq!(tracker.get_vt_rate_per_second(), 75);

        // Rotate past the whole window: old data should be cleared.
        tracker.last_rotation =
            Instant::now_coarse() - Duration::from_secs(window_secs + BUCKET_DURATION_SECS);
        tracker.record(1200); // only this bucket should count
        assert_eq!(tracker.get_vt_rate_per_second(), 10);
    }

    #[test]
    fn test_encode_two_phase_priority_ordering() {
        // Phase 0 (reservation) must sort before phase 1 (weight) for the
        // same group_priority and a larger tag value.
        let phase0 = encode_two_phase_priority(MEDIUM_PRIORITY, 0, 9999);
        let phase1 = encode_two_phase_priority(MEDIUM_PRIORITY, 1, 1);
        assert!(phase0 < phase1, "phase 0 must be higher priority than phase 1");

        // Within phase 0, lower tag = higher priority.
        let p0_low = encode_two_phase_priority(MEDIUM_PRIORITY, 0, 100);
        let p0_high = encode_two_phase_priority(MEDIUM_PRIORITY, 0, 200);
        assert!(p0_low < p0_high);

        // group_priority still dominates across phases.
        let high_phase1 = encode_two_phase_priority(HIGH_PRIORITY, 1, u64::MAX >> 8);
        let low_phase0 = encode_two_phase_priority(LOW_PRIORITY, 0, 0);
        assert!(high_phase1 < low_phase0);
    }

    #[test]
    fn test_two_phase_scheduling_spike_protection() {
        // Build a manager with dynamic reservation enabled.
        let mut cfg = Config::default();
        cfg.enable_dynamic_reservation = true;
        cfg.ru_historical_window = tikv_util::config::ReadableDuration::minutes(10);
        let mgr = ResourceGroupManager::new(cfg);

        // steady: quota=1000, has been consuming for a while.
        // spike:  quota=1000, brand-new (no history).
        let steady = new_resource_group_ru("steady".into(), 1000, MEDIUM_PRIORITY);
        let spike = new_resource_group_ru("spike".into(), 1000, MEDIUM_PRIORITY);
        mgr.add_resource_group(steady);
        mgr.add_resource_group(spike);

        let ctl = mgr.derive_controller("read".into(), true);

        // Simulate the steady group having a long consumption history.
        // Record a large VT total into its tracker so cached_reservation is high.
        {
            let groups = ctl.resource_consumptions.read();
            let g = groups.get(b"steady".as_ref()).unwrap();
            // Record 60_000 VT per bucket × 10 buckets → rate = 100/s.
            if let Some(t) = &g.ru_tracker {
                let mut tracker = t.lock().unwrap();
                for _ in 0..10 {
                    let idx = tracker.current_bucket;
                    tracker.buckets[idx] = 60_000;
                    let len = tracker.buckets.len();
                    tracker.current_bucket = (idx + 1) % len;
                }
            }
        }

        // Trigger reservation refresh so cached_reservation reflects history.
        mgr.advance_min_virtual_time();

        // Now simulate a burst: spike consumes a huge amount.
        for _ in 0..200 {
            ctl.consume(b"spike", ResourceConsumeType::CpuTime(Duration::from_micros(500)));
        }

        // After normalization last_min_r_tag is set to RESERVATION_TAG_SCALE.
        // steady has consumed nothing → normalized R = 0 ≤ threshold → phase 0.
        // spike burst adds large r_delta → normalized R >> threshold → phase 1.
        let threshold = ctl.last_min_r_tag.load(Ordering::Relaxed);
        assert_eq!(
            threshold, RESERVATION_TAG_SCALE,
            "last_min_r_tag should be RESERVATION_TAG_SCALE after normalization"
        );

        let spike_r_tag = ctl
            .resource_consumptions
            .read()
            .get(b"spike".as_ref())
            .unwrap()
            .reservation_tag
            .load(Ordering::Relaxed);
        let steady_r_tag = ctl
            .resource_consumptions
            .read()
            .get(b"steady".as_ref())
            .unwrap()
            .reservation_tag
            .load(Ordering::Relaxed);

        assert!(
            spike_r_tag > threshold,
            "spike should be in phase 1 (r_tag={} > threshold={})",
            spike_r_tag,
            threshold
        );
        assert!(
            steady_r_tag <= threshold,
            "steady should remain in phase 0 (r_tag={} <= threshold={})",
            steady_r_tag,
            threshold
        );

        // Confirm the encoded priority of steady < spike (steady is scheduled first).
        let two_phase = Some(threshold);
        let steady_pri = ctl
            .resource_consumptions
            .read()
            .get(b"steady".as_ref())
            .unwrap()
            .get_priority(1, None, false, two_phase);
        let spike_pri = ctl
            .resource_consumptions
            .read()
            .get(b"spike".as_ref())
            .unwrap()
            .get_priority(1, None, false, two_phase);
        assert!(
            steady_pri < spike_pri,
            "steady (phase 0) must be scheduled before spike (phase 1)"
        );
    }

    #[test]
    fn test_get_resource_limiter() {
        let mgr = ResourceGroupManager::default();

        let default_group = new_background_resource_group_ru(
            "default".into(),
            200,
            MEDIUM_PRIORITY,
            vec!["br".into(), "stats".into()],
        );
        mgr.add_resource_group(default_group);
        let default_limiter = mgr
            .get_resource_group("default")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        assert!(mgr.get_resource_limiter("default", "query", 0).is_none());
        assert!(
            mgr.get_resource_limiter("default", "query", HIGH_PRIORITY as u64)
                .is_none()
        );

        let group1 = new_resource_group("test1".into(), true, 100, 100, HIGH_PRIORITY);
        mgr.add_resource_group(group1);

        let bg_group = new_background_resource_group_ru(
            "bg".into(),
            50,
            LOW_PRIORITY,
            vec!["ddl".into(), "stats".into()],
        );
        mgr.add_resource_group(bg_group);
        let bg_limiter = mgr
            .get_resource_group("bg")
            .unwrap()
            .limiter
            .clone()
            .unwrap();

        assert!(
            mgr.get_background_resource_limiter("test1", "ddl")
                .is_none()
        );
        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("test1", "stats")
                .unwrap(),
            &default_limiter
        ));

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("bg", "stats").unwrap(),
            &bg_limiter
        ));
        assert!(mgr.get_background_resource_limiter("bg", "br").is_none());
        assert!(
            mgr.get_background_resource_limiter("bg", "invalid")
                .is_none()
        );

        assert!(Arc::ptr_eq(
            &mgr.get_background_resource_limiter("unknown", "stats")
                .unwrap(),
            &default_limiter
        ));

        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "stats", 0).unwrap(),
            &default_limiter
        ));
    }
}
