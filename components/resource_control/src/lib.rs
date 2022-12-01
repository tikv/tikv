// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use dashmap::{mapref::one::Ref, DashMap};
use kvproto::kvrpcpb::CommandPri;
use lazy_static::lazy_static;
use pin_project::pin_project;
use prometheus::*;
use serde::{Deserialize, Serialize};
use tikv_util::{sys::SysQuota, time::Instant};
use yatp::queue::priority::set_task_priority;

const DEFAULT_PRIORITY_PER_TASK: u64 = 100; // a task cost at least 100us.
// extra task schedule factor
const TASK_EXTRA_FACTOR_BY_LEVEL: [u64; 3] = [1, 20, 100];
const MIN_DURATION_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

lazy_static! {
    static ref GROUP_PRIORITY: GaugeVec = register_gauge_vec!(
        "tikv_rc_group_priority",
        "Current group prioitry",
        &["group"],
    )
    .unwrap();
}

pub struct ResourceGroupManager {
    resource_groups: DashMap<String, ResourceGroupConfig>,
    total_cpu_quota: f64,
}

impl ResourceGroupManager {
    pub fn new() -> Self {
        let total_cpu_quota = SysQuota::cpu_cores_quota() * 1000.0;
        let r = Self {
            resource_groups: DashMap::new(),
            total_cpu_quota,
        };
        r.init_default_group();
        r
    }

    fn init_default_group(&self) -> Option<ResourceGroupConfig> {
        // grant half of the resource to the default group.
        let cpu_quota = self.total_cpu_quota / 2.0;
        let default_group_cfg = ResourceGroupConfig {
            id: 0,
            name: "default".into(),
            cpu_quota,
            read_bandwidth: 0,
            write_bandwidth: 0,
        };
        self.add_resource_group(default_group_cfg)
    }

    pub fn add_resource_group(&self, config: ResourceGroupConfig) -> Option<ResourceGroupConfig> {
        self.resource_groups.insert(config.name.to_lowercase(), config)
    }

    pub fn remove_resource_group(&self, name: &str) -> Option<ResourceGroupConfig> {
        if name == "default" {
            self.init_default_group()
        } else {
            self.resource_groups.remove(name).map(|(_, v)| v)
        }
    }

    pub fn total_cpu_quota(&self) -> f64 {
        self.total_cpu_quota
    }

    pub fn get_resource_group(&self, name: &str) -> Option<Ref<String, ResourceGroupConfig>> {
        self.resource_groups.get(name)
    }

    pub fn get_all_resource_groups(&self) -> Vec<ResourceGroupConfig> {
        self.resource_groups
            .iter()
            .map(|g| g.clone())
            .collect()
    }
} 

pub struct ResourceController {
    manager: Arc<ResourceGroupManager>,
    // record consumption of each resource group
    resource_consumptions: DashMap<String, ResourceGroup>,
    last_min_vt: AtomicU64,
    start_ts: Instant,
    // the value is the duration delta(in ms) from start_ts
    last_vt_update_time: AtomicU64,
}

impl ResourceController {
    pub fn new(manager: Arc<ResourceGroupManager>) -> Self {
        Self {
            manager,
            resource_consumptions: DashMap::new(),
            last_min_vt: AtomicU64::new(0),
            start_ts: Instant::now_coarse(),
            last_vt_update_time: AtomicU64::new(0),
        }
    }

    #[inline]
    fn resource_group(&self, name: &str) -> Ref<String, ResourceGroup> {
        if let Some(g) = self.resource_consumptions.get(name) {
            return g;
        } else {
            // get the resource group config from the manager
            if let Some(g) = self.manager.get_resource_group(name) {
                let config = g.value();
                let priority_factor = (self.manager.total_cpu_quota() / config.cpu_quota * 100.0) as u64;
                let group = ResourceGroup {
                    priority_factor,
                    virtual_time: AtomicU64::new(self.last_min_vt.load(Ordering::Acquire)),
                };
                self.resource_consumptions.insert(config.name.to_lowercase(), group);
                return self.resource_consumptions.get(name).unwrap();
            } else {
                self.resource_consumptions.get("default").unwrap()
            }
        }
    }

    pub fn get_priority(&self, name: &str, priority: CommandPri) -> u64 {
        self.resource_group(name).get_priority(priority)
    }

    pub fn consume(&self, name: &str, cpu_duration: Duration) {
        self.resource_group(name).consume(cpu_duration)
    }

    pub fn maybe_update_min_virtual_time(&self) {
        thread_local! {
            static TASK_COUNTER: Cell<u64> = Cell::new(0);
        }
        if !TASK_COUNTER.with(|c| {
            let count = c.get() + 1;
            c.set(count);
            count % 1000 == 0
        }) {
            return;
        }
        let last_update_since = self.last_vt_update_time.load(Ordering::Acquire);
        let now = self.start_ts.saturating_elapsed().as_millis() as u64;
        if now < last_update_since + MIN_DURATION_UPDATE_INTERVAL.as_millis() as u64 {
            return;
        }
        // updated by other thread
        if self
            .last_vt_update_time
            .compare_exchange(last_update_since, now, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let mut min_vt = u64::MAX;
        let mut max_vt = 0;
        self.resource_consumptions.iter().for_each(|g| {
            let vt = g.current_vt();
            GROUP_PRIORITY
                .with_label_values(&[&g.key()])
                .set(vt as f64);
            if min_vt > vt {
                min_vt = vt;
            }
            if max_vt < vt {
                max_vt = vt;
            }
        });

        // needn't do update if the virtual different is less than 100ms.
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

#[derive(Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
pub struct ResourceGroupConfig {
    id: u64,
    name: String,
    cpu_quota: f64,
    read_bandwidth: u64,
    write_bandwidth: u64,
}

impl ResourceGroupConfig {
    pub fn new(
        id: u64,
        name: String,
        cpu_quota: f64,
        read_bandwidth: u64,
        write_bandwidth: u64,
    ) -> Self {
        Self {
            id,
            name,
            cpu_quota,
            read_bandwidth,
            write_bandwidth,
        }
    }
}

pub struct ResourceGroup {
    virtual_time: AtomicU64,
    priority_factor: u64,
}

impl ResourceGroup {
    fn get_priority(&self, priority: CommandPri) -> u64 {
        let level = match priority {
            CommandPri::High => 0,
            CommandPri::Normal => 0,
            CommandPri::Low => 2,
        };
        let task_extra_priority = TASK_EXTRA_FACTOR_BY_LEVEL[level] * 1000 * self.priority_factor;
        let base_priority_delta = DEFAULT_PRIORITY_PER_TASK * self.priority_factor;
        self.virtual_time
            .fetch_add(base_priority_delta, Ordering::Relaxed)
            + base_priority_delta
            + task_extra_priority
        // self.virtual_time.load(Ordering::Relaxed) + task_extra_priority
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
    fn consume(&self, cpu_duration: Duration) {
        let vt_delta = cpu_duration.as_micros() as u64 * self.priority_factor;
        self.increase_vt(vt_delta);
    }
}

#[pin_project]
pub struct ControlledFuture<F> {
    #[pin]
    future: F,
    controller: Arc<ResourceController>,
    group_name: String,
    priority: CommandPri,
}

impl<F> ControlledFuture<F> {
    pub fn new(
        future: F,
        controller: Arc<ResourceController>,
        group_name: String,
        priority: CommandPri,
    ) -> Self {
        Self {
            future,
            controller,
            group_name,
            priority,
        }
    }
}

impl<F: Future> Future for ControlledFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let now = Instant::now();
        let res = this.future.poll(cx);
        this.controller
            .consume(this.group_name, now.saturating_elapsed());
        if res.is_pending() {
            set_task_priority(
                this.controller
                    .get_priority(this.group_name, *this.priority),
            );
        }
        this.controller.maybe_update_min_virtual_time();
        res
    }
}
