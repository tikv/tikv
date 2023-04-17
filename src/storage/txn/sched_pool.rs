// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use collections::HashMap;
use file_system::{set_io_type, IoType};
use kvproto::{kvrpcpb::CommandPri, pdpb::QueryKind};
use pd_client::{Feature, FeatureGate};
use prometheus::local::*;
use raftstore::store::WriteStats;
use resource_control::{ControlledFuture, ResourceController};
use tikv_util::{
    sys::SysQuota,
    yatp_pool::{Full, FuturePool, PoolTicker, YatpPoolBuilder},
};
use yatp::queue::Extras;

use crate::storage::{
    kv::{destroy_tls_engine, set_tls_engine, Engine, FlowStatsReporter, Statistics},
    metrics::*,
    test_util::latest_feature_gate,
};

const DEFAULT_MIN_SCHED_POOL_SIZE: usize = 1;
pub struct SchedLocalMetrics {
    local_scan_details: HashMap<&'static str, Statistics>,
    command_keyread_histogram_vec: LocalHistogramVec,
    local_write_stats: WriteStats,
}

thread_local! {
    static TLS_SCHED_METRICS: RefCell<SchedLocalMetrics> = RefCell::new(
        SchedLocalMetrics {
            local_scan_details: HashMap::default(),
            command_keyread_histogram_vec: KV_COMMAND_KEYREAD_HISTOGRAM_VEC.local(),
            local_write_stats:WriteStats::default(),
        }
    );

    static TLS_FEATURE_GATE: RefCell<FeatureGate> = RefCell::new(latest_feature_gate());
}

#[derive(Clone)]
pub struct SchedTicker<R: FlowStatsReporter> {
    reporter: R,
}

impl<R: FlowStatsReporter> PoolTicker for SchedTicker<R> {
    fn on_tick(&mut self) {
        tls_flush(&self.reporter);
    }
}

#[derive(Clone)]
pub enum SchedPool {
    // separated thread pools for different priority commands
    Vanilla {
        high_worker_pool: FuturePool,
        worker_pool: FuturePool,
    },
    // one priority based thread pool to handle all commands
    Priority {
        worker_pool: FuturePool,
        resource_ctl: Arc<ResourceController>,
    },
    // automatically switch between the `single-queue pool` and `priority-queue pool` based on the
    // resource group settings only used when the resource control feature is enabled.
    Dynamic {
        vanilla: Arc<SchedPool>,
        priority: Arc<SchedPool>,
        expect_pool_size: Arc<AtomicUsize>,
    },
}

impl SchedPool {
    pub fn new<E: Engine, R: FlowStatsReporter>(
        engine: E,
        pool_size: usize,
        reporter: R,
        feature_gate: FeatureGate,
        resource_ctl: Option<Arc<ResourceController>>,
    ) -> Self {
        let builder = |pool_size: usize, name_prefix: &str| {
            let engine = Arc::new(Mutex::new(engine.clone()));
            let feature_gate = feature_gate.clone();
            let reporter = reporter.clone();
            // for low cpu quota env, set the max-thread-count as 4 to allow potential cases
            // that we need more thread than cpu num.
            let max_pool_size = std::cmp::max(
                pool_size,
                std::cmp::max(4, SysQuota::cpu_cores_quota() as usize),
            );
            YatpPoolBuilder::new(SchedTicker {reporter:reporter.clone()})
                .thread_count(1, pool_size, max_pool_size)
                .name_prefix(name_prefix)
                // Safety: by setting `after_start` and `before_stop`, `FuturePool` ensures
                // the tls_engine invariants.
                .after_start(move || {
                    set_tls_engine(engine.lock().unwrap().clone());
                    set_io_type(IoType::ForegroundWrite);
                    TLS_FEATURE_GATE.with(|c| *c.borrow_mut() = feature_gate.clone());
                })
                .before_stop(move || unsafe {
                    // Safety: we ensure the `set_` and `destroy_` calls use the same engine type.
                    destroy_tls_engine::<E>();
                    tls_flush(&reporter);
                })
        };
        let vanilla = SchedPool::Vanilla {
            worker_pool: builder(pool_size, "sched-worker-pool").build_future_pool(),
            high_worker_pool: builder(std::cmp::max(1, pool_size / 2), "sched-worker-high")
                .build_future_pool(),
        };
        if let Some(ref r) = resource_ctl {
            let priority = SchedPool::Priority {
                worker_pool: builder(pool_size, "sched-worker-priority")
                    .build_priority_future_pool(r.clone()),
                resource_ctl: r.clone(),
            };
            info!("sched-worker pool use dynamic mode");
            SchedPool::Dynamic {
                vanilla: Arc::new(vanilla),
                priority: Arc::new(priority),
                expect_pool_size: Arc::new(AtomicUsize::new(pool_size)),
            }
        } else {
            info!("sched-worker pool use vanilla mode");
            vanilla
        }
    }

    pub fn spawn(
        &self,
        group_name: &str,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                if priority_level == CommandPri::High {
                    high_worker_pool.spawn(f)
                } else {
                    worker_pool.spawn(f)
                }
            }
            SchedPool::Priority {
                worker_pool,
                resource_ctl,
            } => {
                let fixed_level = match priority_level {
                    CommandPri::High => Some(0),
                    CommandPri::Normal => None,
                    CommandPri::Low => Some(2),
                };
                // TODO: maybe use a better way to generate task_id
                let task_id = rand::random::<u64>();
                let mut extras = Extras::new_multilevel(task_id, fixed_level);
                extras.set_metadata(group_name.as_bytes().to_owned());
                worker_pool.spawn_with_extras(
                    ControlledFuture::new(
                        async move {
                            f.await;
                        },
                        resource_ctl.clone(),
                        group_name.as_bytes().to_owned(),
                    ),
                    extras,
                )
            }
            SchedPool::Dynamic {
                vanilla, priority, ..
            } => {
                if self.can_use_priority() {
                    fail_point!("priority_pool_task");
                    priority.spawn(group_name, priority_level, f)
                } else {
                    fail_point!("single_queue_pool_task");
                    vanilla.spawn(group_name, priority_level, f)
                }
            }
        }
    }

    pub fn scale_pool_size(&self, pool_size: usize) {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                high_worker_pool.scale_pool_size(std::cmp::max(1, pool_size / 2));
                worker_pool.scale_pool_size(pool_size);
            }
            SchedPool::Priority { worker_pool, .. } => {
                worker_pool.scale_pool_size(pool_size);
            }
            SchedPool::Dynamic {
                vanilla,
                priority,
                expect_pool_size,
            } => {
                priority.scale_pool_size(pool_size);
                vanilla.scale_pool_size(pool_size);
                expect_pool_size.store(pool_size, Ordering::Release)
            }
        }
    }

    // check if the pool size is correct, if not, adjust it.
    // it's only used in dynamic mode if the pool size is switched, it's may
    // influence the performance for a while.
    pub fn check_idle_pool(&self) {
        match self {
            SchedPool::Vanilla { .. } | SchedPool::Priority { .. } => {}
            SchedPool::Dynamic {
                vanilla,
                priority,
                expect_pool_size,
            } => {
                let pool_size = expect_pool_size.load(Ordering::Acquire);
                if self.can_use_priority() {
                    if priority.get_pool_size(CommandPri::Normal) != pool_size
                        || vanilla.get_pool_size(CommandPri::Normal) != DEFAULT_MIN_SCHED_POOL_SIZE
                    {
                        priority.scale_pool_size(pool_size);
                        vanilla.scale_pool_size(DEFAULT_MIN_SCHED_POOL_SIZE);
                    }
                } else if vanilla.get_pool_size(CommandPri::Normal) != pool_size
                    || priority.get_pool_size(CommandPri::Normal) != DEFAULT_MIN_SCHED_POOL_SIZE
                {
                    vanilla.scale_pool_size(pool_size);
                    priority.scale_pool_size(DEFAULT_MIN_SCHED_POOL_SIZE);
                }
            }
        }
    }

    fn can_use_priority(&self) -> bool {
        match self {
            SchedPool::Vanilla { .. } => false,
            SchedPool::Priority { .. } => true,
            SchedPool::Dynamic { priority, .. } => {
                if let SchedPool::Priority { resource_ctl, .. } = &**priority {
                    return resource_ctl.is_customized();
                }
                false
            }
        }
    }

    pub fn get_pool_size(&self, priority_level: CommandPri) -> usize {
        match self {
            SchedPool::Vanilla {
                high_worker_pool,
                worker_pool,
            } => {
                if priority_level == CommandPri::High {
                    high_worker_pool.get_pool_size()
                } else {
                    worker_pool.get_pool_size()
                }
            }
            SchedPool::Priority { worker_pool, .. } => worker_pool.get_pool_size(),
            SchedPool::Dynamic {
                vanilla, priority, ..
            } => {
                if self.can_use_priority() {
                    priority.get_pool_size(priority_level)
                } else {
                    vanilla.get_pool_size(priority_level)
                }
            }
        }
    }
}

pub fn tls_collect_scan_details(cmd: &'static str, stats: &Statistics) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .local_scan_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_flush<R: FlowStatsReporter>(reporter: &R) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        for (cmd, stat) in m.local_scan_details.drain() {
            for (cf, cf_details) in stat.details().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS
                        .with_label_values(&[cmd, *cf, *tag])
                        .inc_by(*count as u64);
                }
            }
        }
        m.command_keyread_histogram_vec.flush();

        // Report PD metrics
        if !m.local_write_stats.is_empty() {
            let mut write_stats = WriteStats::default();
            mem::swap(&mut write_stats, &mut m.local_write_stats);
            reporter.report_write_stats(write_stats);
        }
    });
}

pub fn tls_collect_query(region_id: u64, kind: QueryKind) {
    TLS_SCHED_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_write_stats.add_query_num(region_id, kind);
    });
}

pub fn tls_collect_keyread_histogram_vec(cmd: &str, count: f64) {
    TLS_SCHED_METRICS.with(|m| {
        m.borrow_mut()
            .command_keyread_histogram_vec
            .with_label_values(&[cmd])
            .observe(count);
    });
}

pub fn tls_can_enable(feature: Feature) -> bool {
    TLS_FEATURE_GATE.with(|feature_gate| feature_gate.borrow().can_enable(feature))
}

#[cfg(test)]
pub fn set_tls_feature_gate(feature_gate: FeatureGate) {
    TLS_FEATURE_GATE.with(|f| *f.borrow_mut() = feature_gate);
}
