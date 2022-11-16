// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::{collections::HashMap, sync::Mutex};

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use tikv_util::{config::ReadableSize, debug};

lazy_static! {
    pub static ref RESOURCE_GROUPS: Mutex<HashMap<usize, ResourceGroup>> =
        Mutex::new(HashMap::default());
}

/// Metadata for a resource group
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ResourceGroup {
    resource_group_id: usize,
    cpu_quota: usize,
    read_bandwidth: ReadableSize,
    write_bandwidth: ReadableSize,
}

impl ResourceGroup {
    pub fn new(
        resource_group_id: usize,
        cpu_quota: usize,
        read_bandwidth: ReadableSize,
        write_bandwidth: ReadableSize,
    ) -> Self {
        Self {
            resource_group_id,
            cpu_quota,
            read_bandwidth,
            write_bandwidth,
        }
    }

    #[inline]
    pub fn get_resource_group_id(&self) -> usize {
        self.resource_group_id
    }

    #[inline]
    pub fn get_cpu_quota(&self) -> usize {
        self.cpu_quota
    }

    #[inline]
    pub fn get_read_bandwidth(&self) -> ReadableSize {
        self.read_bandwidth
    }

    #[inline]
    pub fn get_write_bandwidth(&self) -> ReadableSize {
        self.write_bandwidth
    }

    #[inline]
    pub fn set_cpu_quota(&mut self, quota: usize) {
        self.cpu_quota = quota;
    }

    #[inline]
    pub fn set_read_bandwidth(&mut self, read_bandwidth: ReadableSize) {
        self.read_bandwidth = read_bandwidth;
    }

    #[inline]
    pub fn set_write_bandwidth(&mut self, write_bandwidth: ReadableSize) {
        self.write_bandwidth = write_bandwidth;
    }
}

pub fn delete_resource_group(rgid: usize) {
    debug!("delete resource group {}", rgid);
    let mut resource_groups = RESOURCE_GROUPS.lock().unwrap();
    resource_groups.remove(&rgid);
}

pub fn set_resource_group_by_attributes(
    rgid: usize,
    cpu_quota: usize,
    read_bandwidth: ReadableSize,
    write_bandwidth: ReadableSize,
) {
    debug!(
        "set meta for resource group {}, cpu => {}, read_bandwidth => {:?}, write_bandwidth=> {:?}",
        cpu_quota, rgid, read_bandwidth, write_bandwidth
    );
    let mut resource_groups = RESOURCE_GROUPS.lock().unwrap();
    let new_resource_group = ResourceGroup::new(rgid, cpu_quota, read_bandwidth, write_bandwidth);
    resource_groups.insert(rgid, new_resource_group);
}

pub fn set_resource_group(from: &ResourceGroup) {
    debug!(
        "copy meta for resource group {}, cpu => {}, read_bandwidth => {:?}, write_bandwidth=> {:?}",
        from.get_resource_group_id(),
        from.get_cpu_quota(),
        from.get_read_bandwidth(),
        from.get_write_bandwidth(),
    );

    let rgid = from.get_resource_group_id();

    let mut resource_groups = RESOURCE_GROUPS.lock().unwrap();
    let new_resource_group = ResourceGroup::new(
        rgid,
        from.get_cpu_quota(),
        from.get_read_bandwidth(),
        from.get_write_bandwidth(),
    );
    resource_groups.insert(rgid, new_resource_group);
}

pub fn get_resource_group(rgid: usize) -> Option<ResourceGroup> {
    debug!("retrieve resource group {}", rgid);
    let resource_groups = RESOURCE_GROUPS.lock().unwrap();
    resource_groups.get(&rgid).cloned()
}
