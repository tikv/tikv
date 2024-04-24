// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    cell::Cell,
    cmp::{max, min},
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use collections::HashMap;
#[cfg(test)]
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use fail::fail_point;
use kvproto::{
    kvrpcpb::{CommandPri, ResourceControlContext},
    resource_manager::{GroupMode, ResourceGroup as PbResourceGroup},
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use strum::{EnumCount, EnumIter, IntoEnumIterator};
use tikv_util::{info, time::Instant};
use yatp::queue::priority::TaskPriorityProvider;

use crate::{metrics::deregister_metrics, resource_limiter::ResourceLimiter};

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default resource group name
const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";
/// default value of max RU quota.
const DEFAULT_MAX_RU_QUOTA: u64 = 10_000;
/// The maximum RU quota that can be configured.
const MAX_RU_QUOTA: u64 = i32::MAX as u64;

const LOW_PRIORITY: u32 = 1;
const MEDIUM_PRIORITY: u32 = 8;
#[cfg(test)]
const HIGH_PRIORITY: u32 = 16;

// the global maxinum of virtual time is u64::MAX / 16, so when the virtual
// time of all groups are bigger than half of this value, we rest them to avoid
// virtual time overflow.
const RESET_VT_THRESHOLD: u64 = (u64::MAX >> 4) / 2;

pub enum ResourceConsumeType {
    CpuTime(Duration),
    IoBytes(u64),
}

#[derive(Copy, Clone, Eq, PartialEq, EnumCount, EnumIter, Debug)]
#[repr(usize)]
pub enum TaskPriority {
    High = 0,
    Medium = 1,
    Low = 2,
}

impl TaskPriority {
    pub fn as_str(&self) -> &'static str {
        match *self {
            TaskPriority::High => "high",
            TaskPriority::Medium => "medium",
            TaskPriority::Low => "low",
        }
    }
}

impl From<u32> for TaskPriority {
    fn from(value: u32) -> Self {
        // map the resource group priority value (1,8,16) to (Low,Medium,High)
        // 0 means the priority is not set, so map it to medium by default.
        if value == 0 {
            Self::Medium
        } else if value < 6 {
            Self::Low
        } else if value < 11 {
            Self::Medium
        } else {
            Self::High
        }
    }
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
    priority_limiters: [Arc<ResourceLimiter>; TaskPriority::COUNT],
}

impl Default for ResourceGroupManager {
    fn default() -> Self {
        let priority_limiters = TaskPriority::iter()
            .map(|p| {
                Arc::new(ResourceLimiter::new(
                    p.as_str().to_owned(),
                    f64::INFINITY,
                    f64::INFINITY,
                    0,
                    false,
                ))
            })
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let manager = Self {
            resource_groups: Default::default(),
            group_count: AtomicU64::new(0),
            registry: Default::default(),
            version_generator: AtomicU64::new(0),
            priority_limiters,
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
}

impl ResourceGroupManager {
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

    #[cfg(test)]
    pub(crate) fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, ResourceGroup>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_all_resource_groups(&self) -> Vec<PbResourceGroup> {
        self.resource_groups
            .iter()
            .map(|g| g.group.clone())
            .collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read));
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
        self.get_group_count() > 1
    }

    /// return the priority of target resource group.
    #[inline]
    pub fn get_resource_group_priority(&self, group: &str) -> u32 {
        self.resource_groups
            .get(group)
            .map_or(LOW_PRIORITY, |g| g.group.priority)
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
    pub fn get_priority_resource_limiters(&self) -> [Arc<ResourceLimiter>; 3] {
        self.priority_limiters.clone()
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
    // 1. the priority factor is calculate based on read/write RU settings.
    // 2. for read request, we increase a constant virtual time delta at each `get_priority` call
    //    because the cost can't be calculated at start, so we only increase a constant delta and
    //    increase the real cost after task is executed; but don't increase it at write because the
    //    cost is known so we just pre-consume it.
    is_read: bool,
    // Track the maximum ru quota used to calculate the factor of each resource group.
    // factor = max_ru_quota / group_ru_quota * 10.0
    // We use mutex here to ensure when we need to change this value and do adjust all resource
    // groups' factors, it can't be changed concurrently.
    // NOTE: becuase the ru config for "default" group is very large and it can cause very big
    // group weight, we will not count this value by default.
    max_ru_quota: Mutex<u64>,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: RwLock<HashMap<Vec<u8>, GroupPriorityTracker>>,
    // the latest min vt, this value is used to init new added group vt
    last_min_vt: AtomicU64,
    // the last time min vt is overflow
    last_rest_vt_time: Cell<Instant>,
    // whether the settings is customized by user
    customized: AtomicBool,
}

// we are ensure to visit the `last_rest_vt_time` by only 1 thread so it's
// thread safe.
unsafe impl Send for ResourceController {}
unsafe impl Sync for ResourceController {}

impl ResourceController {
    fn new(name: String, is_read: bool) -> Self {
        Self {
            name,
            is_read,
            resource_consumptions: RwLock::new(HashMap::default()),
            last_min_vt: AtomicU64::new(0),
            max_ru_quota: Mutex::new(DEFAULT_MAX_RU_QUOTA),
            last_rest_vt_time: Cell::new(Instant::now_coarse()),
            customized: AtomicBool::new(false),
        }
    }

    pub fn new_for_test(name: String, is_read: bool) -> Self {
        let controller = Self::new(name, is_read);
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
        if ru_quota > *max_ru_quota && (name != b"default" || ru_quota < MAX_RU_QUOTA) {
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
        let group = GroupPriorityTracker {
            ru_quota,
            group_priority,
            weight,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
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
        self.customized.load(Ordering::Acquire)
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

    pub fn get_priority(&self, name: &[u8], pri: CommandPri) -> u64 {
        let level = match pri {
            CommandPri::Low => 2,
            CommandPri::Normal => 1,
            CommandPri::High => 0,
        };
        self.resource_group(name).get_priority(level, None)
    }
}

const OVERRIDE_PRIORITY_MASK: u8 = 0b1000_0000;
const RESOURCE_GROUP_NAME_MASK: u8 = 0b0100_0000;

#[derive(Clone, Default)]
pub struct TaskMetadata<'a> {
    // The first byte is a bit map to indicate which field exists,
    // then append override priority if nonzero,
    // then append resource group name if not default
    metadata: Cow<'a, [u8]>,
}

impl<'a> TaskMetadata<'a> {
    pub fn deep_clone(&self) -> TaskMetadata<'static> {
        TaskMetadata {
            metadata: Cow::Owned(self.metadata.to_vec()),
        }
    }

    pub fn from_ctx(ctx: &ResourceControlContext) -> Self {
        let mut mask = 0;
        let mut buf = vec![];
        if ctx.override_priority != 0 {
            mask |= OVERRIDE_PRIORITY_MASK;
        }
        if !ctx.resource_group_name.is_empty()
            && ctx.resource_group_name != DEFAULT_RESOURCE_GROUP_NAME
        {
            mask |= RESOURCE_GROUP_NAME_MASK;
        }
        if mask == 0 {
            // if all are default value, no need to write anything to save copy cost
            return Self {
                metadata: Cow::Owned(buf),
            };
        }
        buf.push(mask);
        if mask & OVERRIDE_PRIORITY_MASK != 0 {
            buf.extend_from_slice(&(ctx.override_priority as u32).to_ne_bytes());
        }
        if mask & RESOURCE_GROUP_NAME_MASK != 0 {
            buf.extend_from_slice(ctx.resource_group_name.as_bytes());
        }
        Self {
            metadata: Cow::Owned(buf),
        }
    }

    fn from_bytes(bytes: &'a [u8]) -> Self {
        Self {
            metadata: Cow::Borrowed(bytes),
        }
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.metadata.into_owned()
    }

    pub fn override_priority(&self) -> u32 {
        if self.metadata.is_empty() {
            return 0;
        }
        if self.metadata[0] & OVERRIDE_PRIORITY_MASK == 0 {
            return 0;
        }
        u32::from_ne_bytes(self.metadata[1..5].try_into().unwrap())
    }

    pub fn group_name(&self) -> &[u8] {
        if self.metadata.is_empty() {
            return DEFAULT_RESOURCE_GROUP_NAME.as_bytes();
        }
        if self.metadata[0] & RESOURCE_GROUP_NAME_MASK == 0 {
            return DEFAULT_RESOURCE_GROUP_NAME.as_bytes();
        }
        let start = if self.metadata[0] & OVERRIDE_PRIORITY_MASK != 0 {
            5
        } else {
            1
        };
        &self.metadata[start..]
    }
}

// return the TaskPriority value from task metadata.
// This function is used for handling thread pool task waiting metrics.
pub fn priority_from_task_meta(meta: &[u8]) -> usize {
    let priority = TaskMetadata::from_bytes(meta).override_priority();
    // mapping (high(15), medium(8), low(1)) -> (0, 1, 2)
    debug_assert!(priority <= 16);
    TaskPriority::from(priority) as usize
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        let metadata = TaskMetadata::from_bytes(extras.metadata());
        self.resource_group(metadata.group_name()).get_priority(
            extras.current_level() as usize,
            if metadata.override_priority() == 0 {
                None
            } else {
                Some(metadata.override_priority())
            },
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
}

impl GroupPriorityTracker {
    fn get_priority(&self, level: usize, override_priority: Option<u32>) -> u64 {
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        let vt = (if self.vt_delta_for_get > 0 {
            self.virtual_time
                .fetch_add(self.vt_delta_for_get, Ordering::Relaxed)
                + self.vt_delta_for_get
        } else {
            self.virtual_time.load(Ordering::Relaxed)
        }) + task_extra_priority;
        let priority = override_priority.unwrap_or(self.group_priority);
        concat_priority_vt(priority, vt)
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

        // test resource gorup resource limiter.
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
        use rand::{thread_rng, RngCore};
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
    fn test_task_metadata() {
        let cases = [
            ("default", 0u32),
            ("default", 6u32),
            ("test", 0u32),
            ("test", 15u32),
        ];

        let metadata = TaskMetadata::from_ctx(&ResourceControlContext::default());
        assert_eq!(metadata.group_name(), b"default");
        for (group_name, priority) in cases {
            let metadata = TaskMetadata::from_ctx(&ResourceControlContext {
                resource_group_name: group_name.to_string(),
                override_priority: priority as u64,
                ..Default::default()
            });
            assert_eq!(metadata.override_priority(), priority);
            assert_eq!(metadata.group_name(), group_name.as_bytes());
            let vec = metadata.to_vec();
            let metadata1 = TaskMetadata::from_bytes(&vec);
            assert_eq!(metadata1.override_priority(), priority);
            assert_eq!(metadata1.group_name(), group_name.as_bytes());
        }
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
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", 0).unwrap(),
            &mgr.priority_limiters[0]
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("test1", "query", LOW_PRIORITY as u64)
                .unwrap(),
            &mgr.priority_limiters[2]
        ));

        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("default", "query", LOW_PRIORITY as u64)
                .unwrap(),
            &mgr.priority_limiters[2]
        ));
        assert!(Arc::ptr_eq(
            &mgr.get_resource_limiter("unknown", "query", 0).unwrap(),
            &mgr.priority_limiters[1]
        ));
    }

    #[test]
    fn test_task_priority() {
        use TaskPriority::*;
        let cases = [
            (0, Medium),
            (1, Low),
            (7, Medium),
            (8, Medium),
            (15, High),
            (16, High),
        ];
        for (value, priority) in cases {
            assert_eq!(TaskPriority::from(value), priority);
        }
    }
}
