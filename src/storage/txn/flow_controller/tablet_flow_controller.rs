// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
};

use collections::{HashMap, HashMapEntry};
use engine_rocks::FlowInfo;
use engine_traits::{CfNamesExt, FlowControlFactorsExt, TabletRegistry, DATA_CFS};
use rand::Rng;
use tikv_util::{sys::thread::StdThreadBuildWrapper, time::Limiter};

use super::singleton_flow_controller::{
    FlowChecker, FlowControlFactorStore, Msg, RATIO_SCALE_FACTOR, TICK_DURATION,
};
use crate::storage::{config::FlowControlConfig, metrics::*};

pub struct TabletFlowFactorStore<EK> {
    registry: TabletRegistry<EK>,
}

impl<EK: Clone> TabletFlowFactorStore<EK> {
    pub fn new(registry: TabletRegistry<EK>) -> Self {
        Self { registry }
    }

    fn query(&self, region_id: u64, f: impl Fn(&EK) -> engine_traits::Result<Option<u64>>) -> u64 {
        self.registry
            .get(region_id)
            .and_then(|mut c| c.latest().and_then(|t| f(t).ok().flatten()))
            .unwrap_or(0)
    }
}

impl<EK: CfNamesExt + FlowControlFactorsExt + Clone> FlowControlFactorStore
    for TabletFlowFactorStore<EK>
{
    fn cf_names(&self, _region_id: u64) -> Vec<String> {
        engine_traits::DATA_CFS
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
    fn num_files_at_level(&self, region_id: u64, cf: &str, level: usize) -> u64 {
        self.query(region_id, |t| t.get_cf_num_files_at_level(cf, level))
    }
    fn num_immutable_mem_table(&self, region_id: u64, cf: &str) -> u64 {
        self.query(region_id, |t| t.get_cf_num_immutable_mem_table(cf))
    }
    fn pending_compaction_bytes(&self, region_id: u64, cf: &str) -> u64 {
        self.query(region_id, |t| t.get_cf_pending_compaction_bytes(cf))
    }
}

type Limiters = Arc<RwLock<HashMap<u64, (Arc<Limiter>, Arc<AtomicU32>)>>>;
pub struct TabletFlowController {
    enabled: Arc<AtomicBool>,
    tx: Option<SyncSender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
    limiters: Limiters,
    global_discard_ratio: Arc<AtomicU32>,
}

impl Drop for TabletFlowController {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Some(Err(e)) = self.tx.as_ref().map(|tx| tx.send(Msg::Close)) {
            error!("send quit message for flow controller failed"; "err" => ?e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join flow controller failed"; "err" => ?e);
        }
    }
}

impl TabletFlowController {
    pub fn new<E: CfNamesExt + FlowControlFactorsExt + Clone + Send + Sync + 'static>(
        config: &FlowControlConfig,
        registry: TabletRegistry<E>,
        flow_info_receiver: Receiver<FlowInfo>,
    ) -> Self {
        let (tx, rx) = mpsc::sync_channel(5);
        tx.send(if config.enable {
            Msg::Enable
        } else {
            Msg::Disable
        })
        .unwrap();
        let flow_checkers = Arc::new(RwLock::new(HashMap::default()));
        let limiters: Limiters = Arc::new(RwLock::new(HashMap::default()));
        let global_discard_ratio = Arc::new(AtomicU32::new(0));
        Self {
            enabled: Arc::new(AtomicBool::new(config.enable)),
            tx: Some(tx),
            limiters: limiters.clone(),
            handle: Some(FlowInfoDispatcher::start(
                rx,
                flow_info_receiver,
                registry,
                flow_checkers,
                limiters,
                config.clone(),
                global_discard_ratio.clone(),
            )),
            global_discard_ratio,
        }
    }

    pub fn tablet_exist(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        limiters.get(&region_id).is_some()
    }
}

struct FlowInfoDispatcher;

impl FlowInfoDispatcher {
    fn start<E: CfNamesExt + FlowControlFactorsExt + Clone + Send + Sync + 'static>(
        rx: Receiver<Msg>,
        flow_info_receiver: Receiver<FlowInfo>,
        registry: TabletRegistry<E>,
        flow_checkers: Arc<RwLock<HashMap<u64, FlowChecker<TabletFlowFactorStore<E>>>>>,
        limiters: Limiters,
        config: FlowControlConfig,
        global_discard_ratio: Arc<AtomicU32>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn_wrapper(move || {
                let mut deadline = std::time::Instant::now();
                let mut enabled = config.enable;
                let engine = TabletFlowFactorStore::new(registry.clone());
                let mut pending_compaction_checker = CompactionPendingBytesChecker::new(
                    config.clone(),
                    global_discard_ratio,
                    engine,
                );
                loop {
                    match rx.try_recv() {
                        Ok(Msg::Close) => break,
                        Ok(Msg::Disable) => {
                            enabled = false;
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            for checker in (*checkers).values_mut() {
                                checker.reset_statistics();
                            }
                        }
                        Ok(Msg::Enable) => {
                            enabled = true;
                        }
                        Err(_) => {}
                    }

                    let msg = flow_info_receiver.recv_deadline(deadline);
                    match msg.clone() {
                        Ok(FlowInfo::L0(_cf, _, region_id))
                        | Ok(FlowInfo::L0Intra(_cf, _, region_id))
                        | Ok(FlowInfo::Flush(_cf, _, region_id)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                checker.on_flow_info_msg(enabled, msg);
                            }
                        }
                        Ok(FlowInfo::Compaction(cf, region_id)) => {
                            if !enabled {
                                continue;
                            }
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                let current_pending_bytes =
                                    checker.on_pending_compaction_bytes_change(cf.clone());
                                pending_compaction_checker.report_pending_compaction_bytes(
                                    region_id,
                                    cf.clone(),
                                    current_pending_bytes,
                                );
                                pending_compaction_checker.on_pending_compaction_bytes_change(cf);
                            }
                        }
                        Ok(FlowInfo::BeforeUnsafeDestroyRange(region_id))
                        | Ok(FlowInfo::AfterUnsafeDestroyRange(region_id)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                checker.on_flow_info_msg(enabled, msg);
                            }
                        }
                        Ok(FlowInfo::Created(region_id)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            let current_count = checkers.len();
                            match checkers.entry(region_id) {
                                HashMapEntry::Occupied(e) => {
                                    let val = e.into_mut();
                                    val.inc();
                                    val
                                }
                                HashMapEntry::Vacant(e) => {
                                    let engine = TabletFlowFactorStore::new(registry.clone());
                                    let mut v = limiters.as_ref().write().unwrap();
                                    let discard_ratio = Arc::new(AtomicU32::new(0));
                                    let limiter = v.entry(region_id).or_insert((
                                        Arc::new(
                                            <Limiter>::builder(f64::INFINITY)
                                                .refill(Duration::from_millis(1))
                                                .build(),
                                        ),
                                        discard_ratio,
                                    ));
                                    info!(
                                        "add FlowChecker";
                                        "region_id" => region_id,
                                        "current_count" => current_count,
                                    );
                                    e.insert(FlowChecker::new_with_region_id(
                                        region_id,
                                        &config,
                                        engine,
                                        limiter.1.clone(),
                                        limiter.0.clone(),
                                    ))
                                }
                            };
                        }
                        Ok(FlowInfo::Destroyed(region_id)) => {
                            let mut remove_limiter = false;
                            let current_count: usize;
                            {
                                let mut checkers = flow_checkers.as_ref().write().unwrap();
                                current_count = checkers.len();
                                if let Some(checker) = checkers.get(&region_id) {
                                    // if the previous value is 1, then the updated reference count
                                    // will be 0
                                    if checker.dec() == 1 {
                                        checkers.remove(&region_id);
                                        remove_limiter = true;
                                    }
                                }
                            }
                            if remove_limiter {
                                limiters.as_ref().write().unwrap().remove(&region_id);
                                pending_compaction_checker.on_region_destroy(&region_id);
                                info!(
                                    "remove FlowChecker";
                                    "region_id" => region_id,
                                    "current_count" => current_count,
                                );
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            let mut total_rate = 0.0;
                            let mut cf_throttle_flags = HashMap::default();
                            for checker in (*checkers).values_mut() {
                                let (rate, tablet_cf_throttle_flags) = checker.update_statistics();
                                total_rate += rate;
                                for (key, val) in tablet_cf_throttle_flags {
                                    if let Some(value) = cf_throttle_flags.get_mut(key) {
                                        *value += val;
                                    } else {
                                        cf_throttle_flags.insert(key, val);
                                    }
                                }
                            }
                            SCHED_WRITE_FLOW_GAUGE.set(total_rate as i64);
                            for (cf, val) in cf_throttle_flags {
                                SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(val);
                            }
                            deadline = std::time::Instant::now() + TICK_DURATION;
                        }
                        Err(e) => {
                            error!("failed to receive compaction info {:?}", e);
                        }
                    }
                }
            })
            .unwrap()
    }
}

impl TabletFlowController {
    pub fn should_drop(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            let ratio = std::cmp::max(
                limiter.1.load(Ordering::Relaxed),
                self.global_discard_ratio.load(Ordering::Relaxed),
            );
            let mut rng = rand::thread_rng();
            return rng.gen_ratio(ratio, RATIO_SCALE_FACTOR);
        }
        false
    }

    #[cfg(test)]
    pub fn discard_ratio(&self, region_id: u64) -> f64 {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            let ratio = std::cmp::max(
                limiter.1.load(Ordering::Relaxed),
                self.global_discard_ratio.load(Ordering::Relaxed),
            );
            return ratio as f64 / RATIO_SCALE_FACTOR as f64;
        }
        0.0
    }

    pub fn consume(&self, region_id: u64, bytes: usize) -> Duration {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.0.consume_duration(bytes);
        }
        Duration::ZERO
    }

    pub fn unconsume(&self, region_id: u64, bytes: usize) {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            limiter.0.unconsume(bytes);
        }
    }

    #[cfg(test)]
    pub fn total_bytes_consumed(&self, region_id: u64) -> usize {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.0.total_bytes_consumed();
        }
        0
    }

    pub fn enable(&self, enable: bool) {
        self.enabled.store(enable, Ordering::Relaxed);
        if let Some(tx) = &self.tx {
            if enable {
                tx.send(Msg::Enable).unwrap();
            } else {
                tx.send(Msg::Disable).unwrap();
            }
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, region_id: u64, speed_limit: f64) {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            limiter.0.set_speed_limit(speed_limit);
        }
    }

    pub fn is_unlimited(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.0.speed_limit() == f64::INFINITY;
        }
        true
    }
}

struct CompactionPendingBytesChecker<E: FlowControlFactorStore + Send + 'static> {
    pending_compaction_bytes: HashMap<u64, HashMap<String, u64>>,
    checker: FlowChecker<E>,
}

impl<E: FlowControlFactorStore + Send + 'static> CompactionPendingBytesChecker<E> {
    pub fn new(config: FlowControlConfig, discard_ratio: Arc<AtomicU32>, engine: E) -> Self {
        CompactionPendingBytesChecker {
            pending_compaction_bytes: HashMap::default(),
            checker: FlowChecker::new_with_region_id(
                0, // global checker
                &config,
                engine,
                discard_ratio,
                Arc::new(
                    <Limiter>::builder(f64::INFINITY)
                        .refill(Duration::from_millis(1))
                        .build(), // not used
                ),
            ),
        }
    }

    fn total_pending_compaction_bytes(&self, cf: &String) -> u64 {
        let mut total = 0;
        for pending_compaction_bytes_cf in self.pending_compaction_bytes.values() {
            if let Some(v) = pending_compaction_bytes_cf.get(cf) {
                total += v;
            }
        }
        total
    }

    /// Update region's pending compaction bytes on cf
    pub fn report_pending_compaction_bytes(
        &mut self,
        region_id: u64,
        cf: String,
        pending_bytes: u64,
    ) {
        match self.pending_compaction_bytes.entry(region_id) {
            HashMapEntry::Occupied(e) => {
                let val = e.into_mut();
                val.insert(cf, pending_bytes);
            }
            HashMapEntry::Vacant(e) => {
                let mut pending_bytes_cf = HashMap::default();
                pending_bytes_cf.insert(cf, pending_bytes);
                e.insert(pending_bytes_cf);
            }
        };
    }

    /// called when region is destroy
    pub fn on_region_destroy(&mut self, region_id: &u64) {
        self.pending_compaction_bytes.remove(region_id);
        for cf in DATA_CFS {
            self.on_pending_compaction_bytes_change(cf.to_string());
        }
    }

    /// called when a specific cf's pending compaction bytes is changed
    pub fn on_pending_compaction_bytes_change(&mut self, cf: String) {
        self.checker
            .on_pending_compaction_bytes_change_cf(self.total_pending_compaction_bytes(&cf), cf);
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::FlowInfo;
    use engine_traits::{SingletonFactory, TabletContext};
    use tempfile::TempDir;

    use super::{
        super::{singleton_flow_controller::tests::*, FlowController},
        *,
    };

    fn create_tablet_flow_controller() -> (
        TempDir,
        FlowController,
        mpsc::SyncSender<FlowInfo>,
        TabletRegistry<EngineStub>,
    ) {
        let (tx, rx) = mpsc::sync_channel(0);
        let temp_dir = tempfile::tempdir().unwrap();
        let stub = EngineStub::new();
        let factory = Box::new(SingletonFactory::new(stub));
        let registry = TabletRegistry::new(factory, temp_dir.path()).unwrap();
        (
            temp_dir,
            FlowController::Tablet(TabletFlowController::new(
                &FlowControlConfig::default(),
                registry.clone(),
                rx,
            )),
            tx,
            registry,
        )
    }

    #[test]
    fn test_tablet_flow_controller_basic() {
        let (_dir, flow_controller, tx, reg) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        let tablet_context = TabletContext::with_infinite_region(region_id, Some(tablet_suffix));
        reg.load(tablet_context, false).unwrap();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
        test_flow_controller_basic_impl(&flow_controller, region_id);
        tx.send(FlowInfo::Destroyed(region_id)).unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
    }

    #[test]
    fn test_tablet_flow_controller_life_cycle() {
        const WAIT_TICK: Duration = Duration::from_millis(100);
        let (_dir, flow_controller, tx, reg) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        let tablet_context = TabletContext::with_infinite_region(region_id, Some(tablet_suffix));
        reg.load(tablet_context, false).unwrap();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        for _ in 0..30 {
            std::thread::sleep(WAIT_TICK);
            flow_controller.set_speed_limit(region_id, 1000.0);
            if !flow_controller.is_unlimited(region_id) {
                break;
            }
        }
        tx.send(FlowInfo::Destroyed(region_id)).unwrap();
        for _ in 0..30 {
            std::thread::sleep(WAIT_TICK);
            if flow_controller.is_unlimited(region_id) {
                break;
            }
        }
        // the region's limiter is removed so it's unlimited
        assert!(flow_controller.is_unlimited(region_id));

        tx.send(FlowInfo::Created(region_id)).unwrap();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        for _ in 0..30 {
            std::thread::sleep(WAIT_TICK);
            flow_controller.set_speed_limit(region_id, 1000.0);
            if !flow_controller.is_unlimited(region_id) {
                break;
            }
        }
        tx.send(FlowInfo::Destroyed(region_id)).unwrap();
        std::thread::sleep(TICK_DURATION);
        // the region's limiter should not be removed as the reference count is still 1
        assert!(!flow_controller.is_unlimited(region_id));
        tx.send(FlowInfo::Destroyed(region_id)).unwrap();
        for _ in 0..30 {
            std::thread::sleep(WAIT_TICK);
            if flow_controller.is_unlimited(region_id) {
                break;
            }
        }
        // the region's limiter is removed so it's unlimited
        assert!(flow_controller.is_unlimited(region_id));
        // no-op it should not crash
        tx.send(FlowInfo::Destroyed(region_id)).unwrap();
    }

    #[test]
    fn test_tablet_flow_controller_memtable() {
        let (_dir, flow_controller, tx, reg) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        let tablet_context = TabletContext::with_infinite_region(region_id, Some(tablet_suffix));
        let mut cached = reg.load(tablet_context, false).unwrap();
        let stub = cached.latest().unwrap().clone();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
        test_flow_controller_memtable_impl(&flow_controller, &stub, &tx, region_id);
    }

    #[test]
    fn test_tablet_flow_controller_l0() {
        let (_dir, flow_controller, tx, reg) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        let tablet_context = TabletContext::with_infinite_region(region_id, Some(tablet_suffix));
        let mut cached = reg.load(tablet_context, false).unwrap();
        let stub = cached.latest().unwrap().clone();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
        test_flow_controller_l0_impl(&flow_controller, &stub, &tx, region_id);
    }

    pub fn test_tablet_flow_controller_pending_compaction_bytes_impl(
        flow_controller: &FlowController,
        stub: &EngineStub,
        tx: &mpsc::SyncSender<FlowInfo>,
        region_id: u64,
    ) {
        // exceeds the threshold
        stub.0
            .pending_compaction_bytes
            .store(1000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        // on start check forbids flow control
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);
        // once fall below the threshold, pass the on start check
        stub.0
            .pending_compaction_bytes
            .store(100 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);

        stub.0
            .pending_compaction_bytes
            .store(1000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) > f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // unfreeze the control
        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(1000000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) > f64::EPSILON);
    }

    #[test]
    fn test_tablet_flow_controller_pending_compaction_bytes() {
        let (_dir, flow_controller, tx, reg) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        let tablet_context = TabletContext::with_infinite_region(region_id, Some(tablet_suffix));
        let mut cached = reg.load(tablet_context, false).unwrap();
        let stub = cached.latest().unwrap().clone();
        tx.send(FlowInfo::Created(region_id)).unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();

        test_tablet_flow_controller_pending_compaction_bytes_impl(
            &flow_controller,
            &stub,
            &tx,
            region_id,
        );
    }
}
