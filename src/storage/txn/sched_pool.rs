// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cell::RefCell,
    mem,
    sync::{Arc, Mutex},
};

use collections::HashMap;
use file_system::{IoType, set_io_type};
use kvproto::{kvrpcpb::CommandPri, pdpb::QueryKind};
use lazy_static::lazy_static;
use pd_client::{Feature, FeatureGate};
use prometheus::local::*;
use raftstore::store::WriteStats;
use resource_control::{
    ControlledFuture, ResourceController, ResourceGroupManager, TaskMetadata, with_resource_limiter,
};
use resource_metering::ResourceMeteringTag;
use tikv_util::{
    sys::SysQuota,
    thread_name_prefix::{
        SCHEDULE_WORKER_HIGH_PRI_THREAD, SCHEDULE_WORKER_POOL_THREAD,
        SCHEDULE_WORKER_PRIORITY_THREAD,
    },
    yatp_pool::{Full, FuturePool, PoolTicker, YatpPoolBuilder},
};
use yatp::queue::Extras;

use crate::storage::{
    kv::{Engine, FlowStatsReporter, Statistics, destroy_tls_engine, set_tls_engine},
    metrics::*,
    test_util::latest_feature_gate,
};

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

    static TLS_OUTER_RESOURCE_TAG_GUARD: RefCell<Option<OuterTaskTagGuard>> = const { RefCell::new(None) };
}

lazy_static! {
    static ref OUTER_TASK_RESOURCE_TAGS: Mutex<HashMap<u64, ResourceMeteringTag>> =
        Mutex::new(HashMap::default());
}

struct OuterTaskTagGuard {
    task_id: u64,
    tag: ResourceMeteringTag,
    guard: resource_metering::Guard,
}

fn register_outer_task_resource_tag(task_id: u64, tag: ResourceMeteringTag) {
    OUTER_TASK_RESOURCE_TAGS.lock().unwrap().insert(task_id, tag);
}

fn before_outer_task_handle(task_id: u64) {
    if task_id == 0 {
        return;
    }
    let Some(tag) = OUTER_TASK_RESOURCE_TAGS.lock().unwrap().remove(&task_id) else {
        return;
    };
    let guard = tag.attach();
    TLS_OUTER_RESOURCE_TAG_GUARD.with(|slot| {
        let prev = slot.borrow_mut().replace(OuterTaskTagGuard { task_id, tag, guard });
        debug_assert!(prev.is_none(), "outer task tag guard should not be nested");
    });
}

fn after_outer_task_handle(task_id: u64, finished: bool) {
    if task_id == 0 {
        return;
    }
    TLS_OUTER_RESOURCE_TAG_GUARD.with(|slot| {
        let outer = slot.borrow_mut().take();
        let Some(outer) = outer else {
            return;
        };
        debug_assert_eq!(outer.task_id, task_id);
        let OuterTaskTagGuard {
            task_id,
            tag,
            guard,
        } = outer;
        drop(guard);
        if !finished {
            register_outer_task_resource_tag(task_id, tag);
        }
    });
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
pub enum QueueType {
    // separated thread pools for different priority commands
    Vanilla,
    // automatically switch between the `single-queue pool` and `priority-queue pool` based on the
    // resource group settings, only used when the resource control feature is enabled.
    Dynamic,
}

#[derive(Clone)]
struct VanillaQueue {
    high_worker_pool: FuturePool,
    worker_pool: FuturePool,
}

impl VanillaQueue {
    fn spawn(
        &self,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        if priority_level == CommandPri::High {
            self.high_worker_pool.spawn(f)
        } else {
            self.worker_pool.spawn(f)
        }
    }

    fn spawn_with_outer_resource_metering_tag(
        &self,
        priority_level: CommandPri,
        outer_tag: ResourceMeteringTag,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        let task_id = rand::random::<u64>();
        let extras = Extras::new_multilevel(task_id, None);
        register_outer_task_resource_tag(task_id, outer_tag);
        let result = if priority_level == CommandPri::High {
            self.high_worker_pool.spawn_with_extras(f, extras)
        } else {
            self.worker_pool.spawn_with_extras(f, extras)
        };
        if result.is_err() {
            OUTER_TASK_RESOURCE_TAGS.lock().unwrap().remove(&task_id);
        }
        result
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.high_worker_pool
            .scale_pool_size(std::cmp::max(1, pool_size / 2));
        self.worker_pool.scale_pool_size(pool_size);
    }

    fn get_pool_size(&self, priority_level: CommandPri) -> usize {
        if priority_level == CommandPri::High {
            self.high_worker_pool.get_pool_size()
        } else {
            self.worker_pool.get_pool_size()
        }
    }
}

#[derive(Clone)]
struct PriorityQueue {
    worker_pool: FuturePool,
    resource_ctl: Arc<ResourceController>,
    resource_mgr: Arc<ResourceGroupManager>,
}

impl PriorityQueue {
    fn spawn(
        &self,
        request_source: &str,
        metadata: TaskMetadata<'_>,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        let fixed_level = match priority_level {
            CommandPri::High => Some(0),
            CommandPri::Normal => None,
            CommandPri::Low => Some(2),
        };
        // TODO: maybe use a better way to generate task_id
        let task_id = rand::random::<u64>();
        let group_name = metadata.group_name().to_owned();
        let resource_limiter = self.resource_mgr.get_resource_limiter(
            unsafe { std::str::from_utf8_unchecked(&group_name) },
            request_source,
            metadata.override_priority() as u64,
        );
        let mut extras = Extras::new_multilevel(task_id, fixed_level);
        extras.set_metadata(metadata.to_vec());
        self.worker_pool.spawn_with_extras(
            with_resource_limiter(
                ControlledFuture::new(f, self.resource_ctl.clone(), group_name),
                resource_limiter,
            ),
            extras,
        )
    }

    fn spawn_with_outer_resource_metering_tag(
        &self,
        request_source: &str,
        metadata: TaskMetadata<'_>,
        priority_level: CommandPri,
        outer_tag: ResourceMeteringTag,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        let fixed_level = match priority_level {
            CommandPri::High => Some(0),
            CommandPri::Normal => None,
            CommandPri::Low => Some(2),
        };
        let task_id = rand::random::<u64>();
        let group_name = metadata.group_name().to_owned();
        let resource_limiter = self.resource_mgr.get_resource_limiter(
            unsafe { std::str::from_utf8_unchecked(&group_name) },
            request_source,
            metadata.override_priority() as u64,
        );
        let mut extras = Extras::new_multilevel(task_id, fixed_level);
        extras.set_metadata(metadata.to_vec());
        register_outer_task_resource_tag(task_id, outer_tag);
        let result = self.worker_pool.spawn_with_extras(
            with_resource_limiter(
                ControlledFuture::new(f, self.resource_ctl.clone(), group_name),
                resource_limiter,
            ),
            extras,
        );
        if result.is_err() {
            OUTER_TASK_RESOURCE_TAGS.lock().unwrap().remove(&task_id);
        }
        result
    }

    fn scale_pool_size(&self, pool_size: usize) {
        self.worker_pool.scale_pool_size(pool_size);
    }

    fn get_pool_size(&self) -> usize {
        self.worker_pool.get_pool_size()
    }
}

#[derive(Clone)]
pub struct SchedPool {
    vanilla: VanillaQueue,
    priority: Option<PriorityQueue>,
    queue_type: QueueType,
}

impl SchedPool {
    pub fn new<E: Engine, R: FlowStatsReporter>(
        engine: E,
        pool_size: usize,
        reporter: R,
        feature_gate: FeatureGate,
        resource_ctl: Option<Arc<ResourceController>>,
        resource_mgr: Option<Arc<ResourceGroupManager>>,
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
                .enable_task_wait_metrics(true)
                .enable_task_exec_metrics(true)
                .before_task_handle(before_outer_task_handle)
                .after_task_handle(after_outer_task_handle)
        };
        let vanilla = VanillaQueue {
            worker_pool: builder(pool_size, SCHEDULE_WORKER_POOL_THREAD).build_future_pool(),
            high_worker_pool: builder(
                std::cmp::max(1, pool_size / 2),
                SCHEDULE_WORKER_HIGH_PRI_THREAD,
            )
            .build_future_pool(),
        };
        let priority = resource_ctl.as_ref().map(|r| PriorityQueue {
            worker_pool: builder(pool_size, SCHEDULE_WORKER_PRIORITY_THREAD)
                .build_priority_future_pool(r.clone()),
            resource_ctl: r.clone(),
            resource_mgr: resource_mgr.unwrap(),
        });
        let queue_type = if resource_ctl.is_some() {
            QueueType::Dynamic
        } else {
            QueueType::Vanilla
        };

        SchedPool {
            vanilla,
            priority,
            queue_type,
        }
    }

    pub fn spawn(
        &self,
        request_source: &str,
        metadata: TaskMetadata<'_>,
        priority_level: CommandPri,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        match self.queue_type {
            QueueType::Vanilla => self.vanilla.spawn(priority_level, f),
            QueueType::Dynamic => {
                if self.can_use_priority() {
                    fail_point!("priority_pool_task");
                    self.priority.as_ref().unwrap().spawn(
                        request_source,
                        metadata,
                        priority_level,
                        f,
                    )
                } else {
                    fail_point!("single_queue_pool_task");
                    self.vanilla.spawn(priority_level, f)
                }
            }
        }
    }

    pub fn spawn_with_outer_resource_metering_tag(
        &self,
        request_source: &str,
        metadata: TaskMetadata<'_>,
        priority_level: CommandPri,
        outer_tag: ResourceMeteringTag,
        f: impl futures::Future<Output = ()> + Send + 'static,
    ) -> Result<(), Full> {
        match self.queue_type {
            QueueType::Vanilla => {
                self.vanilla
                    .spawn_with_outer_resource_metering_tag(priority_level, outer_tag, f)
            }
            QueueType::Dynamic => {
                if self.can_use_priority() {
                    fail_point!("priority_pool_task");
                    self.priority
                        .as_ref()
                        .unwrap()
                        .spawn_with_outer_resource_metering_tag(
                            request_source,
                            metadata,
                            priority_level,
                            outer_tag,
                            f,
                        )
                } else {
                    fail_point!("single_queue_pool_task");
                    self.vanilla
                        .spawn_with_outer_resource_metering_tag(priority_level, outer_tag, f)
                }
            }
        }
    }

    pub fn scale_pool_size(&self, pool_size: usize) {
        match self.queue_type {
            QueueType::Vanilla => {
                self.vanilla.scale_pool_size(pool_size);
            }
            QueueType::Dynamic => {
                let priority = self.priority.as_ref().unwrap();
                priority.scale_pool_size(pool_size);
                self.vanilla.scale_pool_size(pool_size);
            }
        }
    }

    fn can_use_priority(&self) -> bool {
        match self.queue_type {
            QueueType::Vanilla => false,
            QueueType::Dynamic => self.priority.as_ref().unwrap().resource_ctl.is_customized(),
        }
    }

    pub fn get_pool_size(&self, priority_level: CommandPri) -> usize {
        match self.queue_type {
            QueueType::Vanilla => self.vanilla.get_pool_size(priority_level),
            QueueType::Dynamic => {
                if self.can_use_priority() {
                    self.priority.as_ref().unwrap().get_pool_size()
                } else {
                    self.vanilla.get_pool_size(priority_level)
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
            .or_default()
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
mod tests {
    use std::{
        sync::{
            Arc,
            mpsc::{channel, RecvTimeoutError},
        },
        thread,
        time::Duration,
    };

    use collections::HashMap;
    use kvproto::kvrpcpb::Context;
    use raftstore::store::WriteStats;
    use resource_control::TaskMetadata;
    use resource_metering::{Collector, RawRecord, RawRecords, ResourceTagFactory, init_recorder};
    use tikv_util::sys::thread as sys_thread;

    use super::*;
    use crate::storage::{RocksEngine, TestEngineBuilder};

    #[derive(Clone)]
    struct DummyReporter;

    impl FlowStatsReporter for DummyReporter {
        fn report_read_stats(&self, _read_stats: raftstore::store::ReadStats) {}

        fn report_write_stats(&self, _write_stats: WriteStats) {}
    }

    #[derive(Default, Clone)]
    struct DummyCollector {
        records: Arc<Mutex<HashMap<Vec<u8>, RawRecord>>>,
    }

    impl Collector for DummyCollector {
        fn collect(&self, records: Arc<RawRecords>) {
            let mut aggregated = self.records.lock().unwrap();
            for (tag, record) in &records.records {
                aggregated
                    .entry(tag.extra_attachment.as_ref().clone())
                    .or_default()
                    .merge(record);
            }
        }
    }

    impl DummyCollector {
        fn wait_for_cpu_time(&self, tag: &[u8]) -> u32 {
            for _ in 0..20 {
                if let Some(record) = self.records.lock().unwrap().get(tag) {
                    if record.cpu_time > 0 {
                        return record.cpu_time;
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
            panic!(
                "resource metering cpu record for tag {} was not collected",
                String::from_utf8_lossy(tag)
            );
        }
    }

    fn burn_cpu_ms(target_ms: u32) {
        let begin = sys_thread::current_thread_stat().unwrap();
        loop {
            let m: u64 = rand::random();
            let n: u64 = rand::random();
            let _ = m
                .wrapping_mul(n)
                .wrapping_add(m ^ n)
                .wrapping_sub(m & n)
                .wrapping_add(m | n);
            let now = sys_thread::current_thread_stat().unwrap();
            if (now.total_cpu_time() - begin.total_cpu_time()) * 1_000. >= target_ms as f64 {
                return;
            }
        }
    }

    #[test]
    fn test_spawn_with_outer_resource_metering_tag_attributes_cpu() {
        let (_cfg, collector_reg_handle, resource_tag_factory, recorder_worker) =
            init_recorder(200, false);
        let collector = DummyCollector::default();
        let _collector_guard = collector_reg_handle.register(Box::new(collector.clone()), false);

        let engine: RocksEngine = TestEngineBuilder::new().build().unwrap();
        let pool = SchedPool::new(
            engine,
            1,
            DummyReporter,
            latest_feature_gate(),
            None,
            None,
        );

        let mut ctx = Context::default();
        ctx.set_region_id(42);
        ctx.set_resource_group_tag(b"sched-outer-tag".to_vec());
        ctx.mut_peer().set_store_id(1);
        ctx.mut_peer().set_id(1);
        let resource_tag = resource_tag_factory.new_tag(&ctx);

        let (tx, rx) = channel();
        pool.spawn_with_outer_resource_metering_tag(
            "",
            TaskMetadata::default(),
            CommandPri::Normal,
            resource_tag,
            async move {
                burn_cpu_ms(300);
                tx.send(()).unwrap();
            },
        )
        .unwrap();

        match rx.recv_timeout(Duration::from_secs(5)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => panic!("timed out waiting for outer-tag task"),
            Err(RecvTimeoutError::Disconnected) => {
                panic!("outer-tag task channel disconnected unexpectedly")
            }
        }

        assert!(
            collector.wait_for_cpu_time(b"sched-outer-tag") > 0,
            "outer sched-pool tag should attribute cpu without an inner metering wrapper"
        );

        recorder_worker.stop_worker();
    }
}

#[cfg(test)]
pub fn set_tls_feature_gate(feature_gate: FeatureGate) {
    TLS_FEATURE_GATE.with(|f| *f.borrow_mut() = feature_gate);
}
