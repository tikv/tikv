// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::{max, min},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use collections::HashMap;
use dashmap::{mapref::one::Ref, DashMap};
use kvproto::{
    kvrpcpb::CommandPri,
    resource_manager::{GroupMode, ResourceGroup},
};
use parking_lot::{MappedRwLockReadGuard, RwLock, RwLockReadGuard};
use tikv_util::info;
use yatp::queue::priority::TaskPriorityProvider;

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default resource group name
const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";
/// The maximum RU quota that can be configured.
const MAX_RU_QUOTA: u64 = i32::MAX as u64;

#[cfg(test)]
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

/// ResourceGroupManager manages the metadata of each resource group.
#[derive(Default)]
pub struct ResourceGroupManager {
    resource_groups: DashMap<String, ResourceGroup>,
    registry: Mutex<Vec<Arc<ResourceController>>>,
}

impl ResourceGroupManager {
    fn get_ru_setting(rg: &ResourceGroup, is_read: bool) -> u64 {
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

    pub fn add_resource_group(&self, rg: ResourceGroup) {
        let group_name = rg.get_name().to_ascii_lowercase();
        self.registry.lock().unwrap().iter().for_each(|controller| {
            let ru_quota = Self::get_ru_setting(&rg, controller.is_read);
            controller.add_resource_group(group_name.clone().into_bytes(), ru_quota, rg.priority);
        });
        info!("add resource group"; "name"=> &rg.name, "ru" => rg.get_r_u_settings().get_r_u().get_settings().get_fill_rate());
        self.resource_groups.insert(group_name, rg);
    }

    pub fn remove_resource_group(&self, name: &str) {
        let group_name = name.to_ascii_lowercase();
        self.registry.lock().unwrap().iter().for_each(|controller| {
            controller.remove_resource_group(group_name.as_bytes());
        });
        info!("remove resource group"; "name"=> name);
        self.resource_groups.remove(&group_name);
    }

    pub fn retain(&self, mut f: impl FnMut(&String, &ResourceGroup) -> bool) {
        let mut removed_names = vec![];
        self.resource_groups.retain(|k, v| {
            let ret = f(k, v);
            if !ret {
                removed_names.push(k.clone());
            }
            ret
        });
        if !removed_names.is_empty() {
            self.registry.lock().unwrap().iter().for_each(|controller| {
                for name in &removed_names {
                    controller.remove_resource_group(name.as_bytes());
                }
            });
        }
    }

    pub fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, ResourceGroup>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_all_resource_groups(&self) -> Vec<ResourceGroup> {
        self.resource_groups.iter().map(|g| g.clone()).collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read));
        self.registry.lock().unwrap().push(controller.clone());
        for g in &self.resource_groups {
            let ru_quota = Self::get_ru_setting(g.value(), controller.is_read);
            controller.add_resource_group(g.key().clone().into_bytes(), ru_quota, g.priority);
        }
        controller
    }

    pub fn advance_min_virtual_time(&self) {
        for controller in self.registry.lock().unwrap().iter() {
            controller.update_min_virtual_time();
        }
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
    //    increase the real cost after task is executed; but don't increase it at write because
    //    the cost is known so we just pre-consume it.
    is_read: bool,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: RwLock<HashMap<Vec<u8>, GroupPriorityTracker>>,

    last_min_vt: AtomicU64,
}

impl ResourceController {
    pub fn new(name: String, is_read: bool) -> Self {
        let controller = Self {
            name,
            is_read,
            resource_consumptions: RwLock::new(HashMap::default()),
            last_min_vt: AtomicU64::new(0),
        };
        // add the "default" resource group
        controller.add_resource_group(
            DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
            0,
            MEDIUM_PRIORITY,
        );
        controller
    }

    fn calculate_factor(mut quota: u64) -> u64 {
        quota = min(quota, MAX_RU_QUOTA);
        if quota > 0 {
            // the maxinum ru quota is very big, so the precision lost due to
            // integer division is very small.
            MAX_RU_QUOTA / quota
        } else {
            1
        }
    }

    fn add_resource_group(&self, name: Vec<u8>, ru_quota: u64, mut group_priority: u32) {
        if group_priority == 0 {
            // map 0 to medium priority(default priority)
            group_priority = MEDIUM_PRIORITY;
        }

        let weight = Self::calculate_factor(ru_quota);

        let vt_delta_for_get = if self.is_read {
            DEFAULT_PRIORITY_PER_READ_TASK * weight
        } else {
            0
        };
        let group = GroupPriorityTracker {
            group_priority,
            weight,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
        };

        // maybe update existed group
        self.resource_consumptions.write().insert(name, group);
    }

    fn remove_resource_group(&self, name: &[u8]) {
        // do not remove the default resource group, reset to default setting instead.
        if DEFAULT_RESOURCE_GROUP_NAME.as_bytes() == name {
            self.add_resource_group(
                DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(),
                0,
                MEDIUM_PRIORITY,
            );
            return;
        }
        self.resource_consumptions.write().remove(name);
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

    pub fn consume(&self, name: &[u8], delta: ResourceConsumeType) {
        self.resource_group(name).consume(delta)
    }

    pub fn update_min_virtual_time(&self) {
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
        if min_vt + 100_000 >= max_vt && max_vt < RESET_VT_THRESHOLD {
            return;
        }

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
                    tracker.decrease_vt(RESET_VT_THRESHOLD - (max_vt - vt) / 2);
                } else if vt < max_vt {
                    // TODO: is increase by half is a good choice.
                    tracker.increase_vt((max_vt - vt) / 2);
                }
            });
        if near_overflow {
            info!("all reset groups' virtual time are near overflow, do reset");
            max_vt -= RESET_VT_THRESHOLD;
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
        self.resource_group(name).get_priority(level)
    }
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        self.resource_group(extras.metadata())
            .get_priority(extras.current_level() as usize)
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
    group_priority: u32,
    weight: u64,
    virtual_time: AtomicU64,
    // the constant delta value for each `get_priority` call,
    vt_delta_for_get: u64,
}

impl GroupPriorityTracker {
    fn get_priority(&self, level: usize) -> u64 {
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        let vt = (if self.vt_delta_for_get > 0 {
            self.virtual_time
                .fetch_add(self.vt_delta_for_get, Ordering::Relaxed)
                + self.vt_delta_for_get
        } else {
            self.virtual_time.load(Ordering::Relaxed)
        }) + task_extra_priority;
        concat_priority_vt(self.group_priority, vt)
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
    fn consume(&self, delta: ResourceConsumeType) {
        let vt_delta = match delta {
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

    pub fn new_resource_group_ru(name: String, ru: u64, group_priority: u32) -> ResourceGroup {
        new_resource_group(name, true, ru, ru, group_priority)
    }

    pub fn new_resource_group(
        name: String,
        is_ru_mode: bool,
        read_tokens: u64,
        write_tokens: u64,
        group_priority: u32,
    ) -> ResourceGroup {
        use kvproto::resource_manager::{GroupRawResourceSettings, GroupRequestUnitSettings};

        let mut group = ResourceGroup::new();
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

        let group1 = new_resource_group_ru("TEST".into(), 100, 0);
        resource_manager.add_resource_group(group1);

        assert!(resource_manager.get_resource_group("test1").is_none());
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(
            group
                .value()
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            100
        );
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group1 = new_resource_group_ru("Test".into(), 200, LOW_PRIORITY);
        resource_manager.add_resource_group(group1);
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(
            group
                .value()
                .get_r_u_settings()
                .get_r_u()
                .get_settings()
                .get_fill_rate(),
            200
        );
        assert_eq!(group.value().get_priority(), 1);
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group2 = new_resource_group_ru("test2".into(), 400, 0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let resource_ctl = resource_manager.derive_controller("test_read".into(), true);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 3);

        let group1 = resource_ctl.resource_group("test".as_bytes());
        let group2 = resource_ctl.resource_group("test2".as_bytes());
        assert_eq!(group1.weight, group2.weight * 2);
        assert_eq!(group1.current_vt(), 0);

        let mut extras1 = Extras::single_level();
        extras1.set_metadata("test".as_bytes().to_owned());
        assert_eq!(
            resource_ctl.priority_of(&extras1),
            concat_priority_vt(LOW_PRIORITY, group1.weight * 50)
        );
        assert_eq!(group1.current_vt(), group1.weight * 50);

        let mut extras2 = Extras::single_level();
        extras2.set_metadata("test2".as_bytes().to_owned());
        assert_eq!(
            resource_ctl.priority_of(&extras2),
            concat_priority_vt(MEDIUM_PRIORITY, group2.weight * 50)
        );
        assert_eq!(group2.current_vt(), group2.weight * 50);

        let mut extras3 = Extras::single_level();
        extras3.set_metadata("unknown_group".as_bytes().to_owned());
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

        resource_ctl.consume(
            "test".as_bytes(),
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resource_ctl.consume(
            "test2".as_bytes(),
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );

        assert_eq!(group1.current_vt(), group1.weight * 10050);
        assert_eq!(group1.current_vt(), group2.current_vt() * 2);

        // test update all group vts
        resource_manager.advance_min_virtual_time();
        let group1_vt = group1.current_vt();
        let group1_weight = group1.weight;
        assert_eq!(group1_vt, group1.weight * 10050);
        assert!(group2.current_vt() >= group1.current_vt() * 3 / 4);
        assert!(
            resource_ctl
                .resource_group("default".as_bytes())
                .current_vt()
                >= group1.current_vt() / 2
        );

        drop(group1);
        drop(group2);

        // test add 1 new resource group
        let new_group = new_resource_group_ru("new_group".into(), 600, HIGH_PRIORITY);
        resource_manager.add_resource_group(new_group);

        assert_eq!(resource_ctl.resource_consumptions.read().len(), 4);
        let group3 = resource_ctl.resource_group("new_group".as_bytes());
        assert!(group1_weight - 10 <= group3.weight * 3 && group3.weight * 3 <= group1_weight + 10);
        assert!(group3.current_vt() >= group1_vt / 2);
    }

    #[test]
    fn test_reset_resource_group_vt() {
        let resource_manager = ResourceGroupManager::default();
        let resource_ctl = resource_manager.derive_controller("test_write".into(), false);

        let group1 = new_resource_group_ru("g1".into(), i32::MAX as u64, 1);
        resource_manager.add_resource_group(group1);
        let group2 = new_resource_group_ru("g2".into(), 1, 16);
        resource_manager.add_resource_group(group2);

        let g1 = resource_ctl.resource_group("g1".as_bytes());
        let g2 = resource_ctl.resource_group("g2".as_bytes());
        let threshold = 1 << 59;
        let mut last_g2_vt = 0;
        for i in 0..8 {
            resource_ctl.consume("g2".as_bytes(), ResourceConsumeType::IoBytes(1 << 25));
            resource_manager.advance_min_virtual_time();
            if i < 7 {
                assert!(g2.current_vt() < threshold);
            }
            // after 8 round, g1's vt still under the threshold and is still increasing.
            assert!(g1.current_vt() < threshold && g1.current_vt() > last_g2_vt);
            last_g2_vt = g2.current_vt();
        }

        resource_ctl.consume("g2".as_bytes(), ResourceConsumeType::IoBytes(1 << 25));
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

        assert_eq!(resource_manager.get_all_resource_groups().len(), 10);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 11); // 10 + 1(default)
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 11);

        resource_manager.retain(|k, _v| k.starts_with("test"));
        assert_eq!(resource_manager.get_all_resource_groups().len(), 5);
        assert_eq!(resource_ctl.resource_consumptions.read().len(), 6);
        assert_eq!(resource_ctl_write.resource_consumptions.read().len(), 6);
        assert!(resource_manager.get_resource_group("group1").is_none());
        // should use the virtual time of default group for non-exist group
        assert_ne!(
            resource_ctl
                .resource_group("group2".as_bytes())
                .current_vt(),
            0
        );
        assert_ne!(
            resource_ctl_write
                .resource_group("group2".as_bytes())
                .current_vt(),
            0
        );
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
}
