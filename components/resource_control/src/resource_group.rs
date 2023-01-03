// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use dashmap::{mapref::one::Ref, DashMap};
use kvproto::resource_manager::{GroupMode, GroupSettings, ResourceGroup};
use tikv_util::sys::SysQuota;
use yatp::queue::priority::TaskPriorityProvider;

// a read task cost at least 50us.
const DEFAULT_PRIORITY_PER_READ_TASK: u64 = 50;
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [0, 20, 100];
/// duration to update the minimal priority value of each resource group.
pub const MIN_PRIORITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
/// default resource group name
const DEFAULT_RESOURCE_GROUP_NAME: &str = "default";

pub enum ResourceConsumeType {
    CpuTime(Duration),
    IoBytes(u64),
}

/// ResourceGroupManager manages the metadata of each resource group.
pub struct ResourceGroupManager {
    resource_groups: DashMap<String, GroupSettings>,
    registry: Mutex<Vec<Arc<ResourceController>>>,
    /// total_ru_quota is an estimated upper bound of RU, used to calculate the
    /// weight of each resource.
    total_ru_quota: f64,
}

impl Default for ResourceGroupManager {
    fn default() -> Self {
        let total_ru_quota = SysQuota::cpu_cores_quota() * 10000.0;
        Self {
            resource_groups: DashMap::new(),
            registry: Mutex::new(vec![]),
            total_ru_quota,
        }
    }
}

impl ResourceGroupManager {
    fn get_ru_setting(setting: &GroupSettings, is_read: bool) -> f64 {
        match (setting.get_mode(), is_read) {
            (GroupMode::RuMode, true) => setting.get_r_u_settings().get_r_r_u().get_tokens(),
            (GroupMode::RuMode, false) => setting.get_r_u_settings().get_w_r_u().get_tokens(),
            // TODO: currently we only consider the cpu usage in the read path, we may also take
            // io read bytes into account later.
            (GroupMode::NativeMode, true) => setting.get_resource_settings().get_cpu().get_tokens(),
            (GroupMode::NativeMode, false) => {
                setting.get_resource_settings().get_io_write().get_tokens()
            }
        }
    }

    fn gen_group_priority_factor(&self, setting: &GroupSettings, is_read: bool) -> u64 {
        let ru_settings = Self::get_ru_setting(setting, is_read);
        // TODO: ensure the result is a valid positive integer
        (self.total_ru_quota / ru_settings * 10.0) as u64
    }

    pub fn add_resource_group(&self, config: ResourceGroup) {
        let group_name = config.get_name().to_ascii_lowercase();
        self.registry.lock().unwrap().iter().for_each(|controller| {
            let priority_factor =
                self.gen_group_priority_factor(config.get_settings(), controller.is_read);
            controller.add_resource_group(group_name.clone().into_bytes(), priority_factor);
        });
        self.resource_groups
            .insert(group_name, config.get_settings().clone());
    }

    pub fn remove_resource_group(&self, name: &str) {
        let group_name = name.to_ascii_lowercase();
        // do not remove the default resource group.
        if DEFAULT_RESOURCE_GROUP_NAME == group_name {
            return;
        }
        self.registry.lock().unwrap().iter().for_each(|controller| {
            controller.remove_resource_group(group_name.as_bytes());
        });
        self.resource_groups.remove(&group_name);
    }

    pub fn get_resource_group(&self, name: &str) -> Option<Ref<'_, String, GroupSettings>> {
        self.resource_groups.get(&name.to_ascii_lowercase())
    }

    pub fn get_all_resource_groups(&self) -> Vec<GroupSettings> {
        self.resource_groups.iter().map(|g| g.clone()).collect()
    }

    pub fn derive_controller(&self, name: String, is_read: bool) -> Arc<ResourceController> {
        let controller = Arc::new(ResourceController::new(name, is_read));
        self.registry.lock().unwrap().push(controller.clone());
        for g in &self.resource_groups {
            let priority_factor = self.gen_group_priority_factor(g.value(), is_read);
            controller.add_resource_group(g.key().clone().into_bytes(), priority_factor);
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
    // 1. the priority factor is calcualte based on read/write RU settings.
    // 2. for read request, we increase a constant virtual time delta at each `get_priority` call
    //    because the cost can't be calculated at start, so we only increase a constant delta and
    //    increase the real cost after task is executed; but don't increase it at write because
    //    the cost is known so we just pre-consume it.
    is_read: bool,
    // record consumption of each resource group, name --> resource_group
    resource_consumptions: DashMap<Vec<u8>, GroupPriorityTracker>,
    last_min_vt: AtomicU64,
}

impl ResourceController {
    pub fn new(name: String, is_read: bool) -> Self {
        let controller = Self {
            name,
            is_read,
            resource_consumptions: DashMap::new(),
            last_min_vt: AtomicU64::new(0),
        };
        // add the "default" resource group
        controller.add_resource_group(DEFAULT_RESOURCE_GROUP_NAME.as_bytes().to_owned(), 1);
        controller
    }

    fn add_resource_group(&self, name: Vec<u8>, priority_factor: u64) {
        let vt_delta_for_get = if self.is_read {
            DEFAULT_PRIORITY_PER_READ_TASK * priority_factor
        } else {
            0
        };
        let group = GroupPriorityTracker {
            weight: priority_factor,
            virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
            vt_delta_for_get,
        };
        // maybe update existed group
        self.resource_consumptions.insert(name, group);
    }

    fn remove_resource_group(&self, name: &[u8]) {
        self.resource_consumptions.remove(name);
    }

    #[inline]
    fn resource_group(&self, name: &[u8]) -> Ref<'_, Vec<u8>, GroupPriorityTracker> {
        if let Some(g) = self.resource_consumptions.get(name) {
            g
        } else {
            self.resource_consumptions
                .get(DEFAULT_RESOURCE_GROUP_NAME.as_bytes())
                .unwrap()
        }
    }

    pub fn consume(&self, name: &[u8], delta: ResourceConsumeType) {
        self.resource_group(name).consume(delta)
    }

    pub fn update_min_virtual_time(&self) {
        let mut min_vt = u64::MAX;
        let mut max_vt = 0;
        self.resource_consumptions.iter().for_each(|g| {
            let vt = g.current_vt();
            if min_vt > vt {
                min_vt = vt;
            }
            if max_vt < vt {
                max_vt = vt;
            }
        });

        // TODO: use different threshold for different resource type
        // needn't do update if the virtual different is less than 100ms/100KB.
        if min_vt + 100_000 >= max_vt {
            return;
        }

        self.resource_consumptions.iter().for_each(|g| {
            let vt = g.current_vt();
            if vt < max_vt {
                // TODO: is increase by half is a good choice.
                g.increase_vt((max_vt - vt) / 2);
            }
        });
        // max_vt is actually a little bigger than the current min vt, but we don't
        // need totally accurate here.
        self.last_min_vt.store(max_vt, Ordering::Relaxed);
    }
}

impl TaskPriorityProvider for ResourceController {
    fn priority_of(&self, extras: &yatp::queue::Extras) -> u64 {
        self.resource_group(extras.metadata())
            .get_priority(extras.current_level() as usize)
    }
}

struct GroupPriorityTracker {
    virtual_time: AtomicU64,
    weight: u64,
    // the constant delta value for each `get_priority` call,
    vt_delta_for_get: u64,
}

impl GroupPriorityTracker {
    fn get_priority(&self, level: usize) -> u64 {
        // let level = match priority {
        //     CommandPri::High => 0,
        //     CommandPri::Normal => 0,
        //     CommandPri::Low => 2,
        // };
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.weight;
        (if self.vt_delta_for_get > 0 {
            self.virtual_time
                .fetch_add(self.vt_delta_for_get, Ordering::Relaxed)
                + self.vt_delta_for_get
        } else {
            self.virtual_time.load(Ordering::Relaxed)
        }) + task_extra_priority
    }

    #[inline]
    fn current_vt(&self) -> u64 {
        self.virtual_time.load(Ordering::Relaxed)
    }

    #[inline]
    fn increase_vt(&self, vt_delta: u64) {
        self.virtual_time.fetch_add(vt_delta, Ordering::Relaxed);
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
mod tests {
    use kvproto::resource_manager::*;
    use yatp::queue::Extras;

    use super::*;

    fn new_resource_group(
        name: String,
        is_ru_mode: bool,
        read_tokens: f64,
        write_tokens: f64,
    ) -> ResourceGroup {
        let mut group = ResourceGroup::new();
        group.set_name(name);
        let mut settings = GroupSettings::new();
        let mode = if is_ru_mode {
            GroupMode::RuMode
        } else {
            GroupMode::NativeMode
        };
        settings.set_mode(mode);
        if is_ru_mode {
            let mut ru_setting = GroupRequestUnitSettings::new();
            ru_setting.mut_r_r_u().set_tokens(read_tokens);
            ru_setting.mut_w_r_u().set_tokens(write_tokens);
            settings.set_r_u_settings(ru_setting);
        } else {
            let mut resource_setting = GroupResourceSettings::new();
            resource_setting.mut_cpu().set_tokens(read_tokens);
            resource_setting.mut_io_write().set_tokens(write_tokens);
            settings.set_resource_settings(resource_setting);
        }

        group.set_settings(settings);
        group
    }

    #[test]
    fn test_resource_group() {
        let mut resource_manager = ResourceGroupManager::default();
        resource_manager.total_ru_quota = 10000.0;

        let group1 = new_resource_group("TEST".into(), true, 100.0, 100.0);
        resource_manager.add_resource_group(group1);

        assert!(resource_manager.get_resource_group("test1").is_none());

        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(
            group.value().get_r_u_settings().get_r_r_u().get_tokens(),
            100.0
        );
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group1 = new_resource_group("Test".into(), true, 200.0, 100.0);
        resource_manager.add_resource_group(group1);
        let group = resource_manager.get_resource_group("test").unwrap();
        assert_eq!(
            group.value().get_r_u_settings().get_r_r_u().get_tokens(),
            200.0
        );
        drop(group);
        assert_eq!(resource_manager.resource_groups.len(), 1);

        let group2 = new_resource_group("test2".into(), true, 400.0, 200.0);
        resource_manager.add_resource_group(group2);
        assert_eq!(resource_manager.resource_groups.len(), 2);

        let resouce_ctl = resource_manager.derive_controller("test_read".into(), true);
        assert_eq!(resouce_ctl.resource_consumptions.len(), 3);

        let group1 = resouce_ctl.resource_group("test".as_bytes());
        assert_eq!(group1.weight, 500);
        let group2 = resouce_ctl.resource_group("test2".as_bytes());
        assert_eq!(group2.weight, 250);
        assert_eq!(group1.current_vt(), 0);

        let mut extras1 = Extras::single_level();
        extras1.set_metadata("test".as_bytes().to_owned());
        assert_eq!(resouce_ctl.priority_of(&extras1), 25_000);
        assert_eq!(group1.current_vt(), 25_000);

        let mut extras2 = Extras::single_level();
        extras2.set_metadata("test2".as_bytes().to_owned());
        assert_eq!(resouce_ctl.priority_of(&extras2), 12_500);
        assert_eq!(group2.current_vt(), 12_500);

        let mut extras3 = Extras::single_level();
        extras3.set_metadata("unknown_group".as_bytes().to_owned());
        assert_eq!(resouce_ctl.priority_of(&extras3), 50);
        assert_eq!(
            resouce_ctl
                .resource_group("default".as_bytes())
                .current_vt(),
            50
        );

        resouce_ctl.consume(
            "test".as_bytes(),
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );
        resouce_ctl.consume(
            "test2".as_bytes(),
            ResourceConsumeType::CpuTime(Duration::from_micros(10000)),
        );

        assert_eq!(group1.current_vt(), 5_025_000);
        assert_eq!(group1.current_vt(), group2.current_vt() * 2);

        // test update all group vts
        resource_manager.advance_min_virtual_time();
        let group1_vt = group1.current_vt();
        assert_eq!(group1_vt, 5_025_000);
        assert!(group2.current_vt() >= group1.current_vt() * 3 / 4);
        assert!(
            resouce_ctl
                .resource_group("default".as_bytes())
                .current_vt()
                >= group1.current_vt() / 2
        );

        drop(group1);
        drop(group2);

        // test add 1 new resource group
        let new_group = new_resource_group("new_group".into(), true, 500.0, 500.0);
        resource_manager.add_resource_group(new_group);

        assert_eq!(resouce_ctl.resource_consumptions.len(), 4);
        let group3 = resouce_ctl.resource_group("new_group".as_bytes());
        assert_eq!(group3.weight, 200);
        assert!(group3.current_vt() >= group1_vt / 2);
    }
}
