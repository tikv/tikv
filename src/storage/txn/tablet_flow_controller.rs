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

use collections::HashMap;
use engine_rocks::FlowInfo;
use engine_traits::{KvEngine, TabletFactory};
use rand::Rng;
use tikv_util::time::Limiter;

use super::flow_controller::{FlowChecker, FlowController, Msg, RATIO_SCALE_FACTOR, TICK_DURATION};
use crate::storage::{config::FlowControlConfig, metrics::*};

pub struct TabletFlowController {
    discard_ratio: Arc<AtomicU32>,
    enabled: Arc<AtomicBool>,
    tx: Option<SyncSender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
    limiters: Arc<RwLock<HashMap<u64, Arc<Limiter>>>>,
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
    pub fn new<E: KvEngine + Send + 'static>(
        config: &FlowControlConfig,
        tablet_factory: Arc<dyn TabletFactory<E> + Send + Sync>,
        flow_info_receiver: Receiver<FlowInfo>,
    ) -> Self {
        let discard_ratio = Arc::new(AtomicU32::new(0));
        let (tx, rx) = mpsc::sync_channel(5);
        tx.send(if config.enable {
            Msg::Enable
        } else {
            Msg::Disable
        })
        .unwrap();
        let flow_checkers: Arc<RwLock<HashMap<u64, FlowChecker<E>>>> =
            Arc::new(RwLock::new(HashMap::default()));
        let limiters: Arc<RwLock<HashMap<u64, Arc<Limiter>>>> =
            Arc::new(RwLock::new(HashMap::default()));
        Self {
            discard_ratio: discard_ratio.clone(),
            enabled: Arc::new(AtomicBool::new(config.enable)),
            tx: Some(tx),
            limiters: limiters.clone(),
            handle: Some(FlowInfoDispatcher::start(
                rx,
                flow_info_receiver,
                tablet_factory,
                flow_checkers,
                limiters,
                config.clone(),
                discard_ratio,
            )),
        }
    }

    pub fn tablet_exist(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        limiters.get(&region_id).is_some()
    }
}

struct FlowInfoDispatcher;

impl FlowInfoDispatcher {
    fn start<E: KvEngine + Send + 'static>(
        rx: Receiver<Msg>,
        flow_info_receiver: Receiver<FlowInfo>,
        tablet_factory: Arc<dyn TabletFactory<E> + Send + Sync>,
        flow_checkers: Arc<RwLock<HashMap<u64, FlowChecker<E>>>>,
        limiters: Arc<RwLock<HashMap<u64, Arc<Limiter>>>>,
        config: FlowControlConfig,
        discard_ratio: Arc<AtomicU32>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut deadline = std::time::Instant::now();
                let mut enabled = true;
                loop {
                    match rx.try_recv() {
                        Ok(Msg::Close) => break,
                        Ok(Msg::Disable) => {
                            enabled = false;
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            for (_, checker) in &mut *checkers {
                                checker.reset_statistics();
                            }
                        }
                        Ok(Msg::Enable) => {
                            enabled = true;
                        }
                        Err(_) => {}
                    }

                    let insert_limiter_and_checker = |region_id, suffix| -> FlowChecker<E> {
                        println!("insert_limiter_and_checker {} {}", region_id, suffix);
                        let engine = tablet_factory.open_tablet_cache(region_id, suffix).unwrap();
                        let mut v = limiters.as_ref().write().unwrap();
                        let limiter = v.entry(region_id).or_insert(Arc::new(
                            <Limiter>::builder(f64::INFINITY)
                                .refill(Duration::from_millis(1))
                                .build(),
                        ));
                        println!("creating flow checker {}{}", region_id, suffix);
                        FlowChecker::new_with_tablet_suffix(
                            &config,
                            engine,
                            discard_ratio.clone(),
                            limiter.clone(),
                            suffix,
                        )
                    };
                    match flow_info_receiver.recv_deadline(deadline) {
                        Ok(FlowInfo::L0(cf, l0_bytes, region_id, suffix)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                if checker.get_tablet_suffix() != suffix {
                                    continue;
                                }
                                checker.collect_l0_consumption_stats(&cf, l0_bytes);
                                if enabled {
                                    checker.on_l0_change(cf)
                                }
                            }
                        }
                        Ok(FlowInfo::L0Intra(cf, diff_bytes, region_id, suffix)) => {
                            if diff_bytes > 0 {
                                let mut checkers = flow_checkers.as_ref().write().unwrap();
                                if let Some(checker) = checkers.get_mut(&region_id) {
                                    if checker.get_tablet_suffix() != suffix {
                                        continue;
                                    }
                                    // Intra L0 merges some deletion records, so regard it as a L0 compaction.
                                    checker.collect_l0_consumption_stats(&cf, diff_bytes);
                                    if enabled {
                                        checker.on_l0_change(cf);
                                    }
                                }
                            }
                        }
                        Ok(FlowInfo::Flush(cf, flush_bytes, region_id, suffix)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                if checker.get_tablet_suffix() != suffix {
                                    continue;
                                }
                                checker.collect_l0_production_stats(&cf, flush_bytes);
                                if enabled {
                                    checker.on_memtable_change(&cf);
                                    checker.on_l0_change(cf)
                                }
                            }
                        }
                        Ok(FlowInfo::Compaction(cf, region_id, suffix)) => {
                            if !enabled {
                                continue;
                            }
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                if checker.get_tablet_suffix() != suffix {
                                    continue;
                                }
                                checker.on_pending_compaction_bytes_change(cf);
                            }
                        }
                        Ok(FlowInfo::BeforeUnsafeDestroyRange(region_id)) => {
                            if !enabled {
                                continue;
                            }
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                checker.wait_for_destroy_range_finish = true;
                                let soft =
                                    (checker.soft_pending_compaction_bytes_limit as f64).log2();
                                for cf_checker in checker.cf_checkers.values_mut() {
                                    let v = cf_checker.long_term_pending_bytes.get_avg();
                                    if v <= soft {
                                        cf_checker.pending_bytes_before_unsafe_destroy_range =
                                            Some(v);
                                    }
                                }
                            }
                        }
                        Ok(FlowInfo::AfterUnsafeDestroyRange(region_id)) => {
                            if !enabled {
                                continue;
                            }
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                checker.wait_for_destroy_range_finish = false;
                                for (cf, cf_checker) in &mut checker.cf_checkers {
                                    if let Some(before) =
                                        cf_checker.pending_bytes_before_unsafe_destroy_range
                                    {
                                        let soft = (checker.soft_pending_compaction_bytes_limit
                                            as f64)
                                            .log2();
                                        let after = (checker
                                            .engine
                                            .get_cf_pending_compaction_bytes(cf)
                                            .unwrap_or(None)
                                            .unwrap_or(0)
                                            as f64)
                                            .log2();

                                        assert!(before < soft);
                                        if after >= soft {
                                            // there is a pending bytes jump
                                            SCHED_THROTTLE_ACTION_COUNTER
                                                .with_label_values(&[cf, "pending_bytes_jump"])
                                                .inc();
                                        } else {
                                            cf_checker.pending_bytes_before_unsafe_destroy_range =
                                                None;
                                        }
                                    }
                                }
                            }
                        }
                        Ok(FlowInfo::Created(region_id, suffix)) => {
                            println!("FlowInfo::Created {} {}", region_id, suffix);
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            println!("got the lock");
                            let checker = checkers
                                .entry(region_id)
                                .or_insert_with(|| insert_limiter_and_checker(region_id, suffix));
                            // check if the checker's engine is exactly (region_id, suffix)
                            // if  checker.suffix < suffix, it means its tablet is old and needs the refresh
                            println!("Got checker");
                            if checker.get_tablet_suffix() < suffix {
                                let engine =
                                    tablet_factory.open_tablet_cache(region_id, suffix).unwrap();
                                checker.set_engine(engine);
                                checker.set_tablet_suffix(suffix);
                            }
                        }
                        Ok(FlowInfo::Destroyed(region_id, suffix)) => {
                            let mut remove_limiter = false;
                            {
                                let mut checkers = flow_checkers.as_ref().write().unwrap();
                                if let Some(checker) = checkers.get_mut(&region_id) {
                                    if checker.get_tablet_suffix() == suffix {
                                        checkers.remove(&region_id);
                                        remove_limiter = true;
                                    }
                                }
                            }
                            if remove_limiter {
                                limiters.as_ref().write().unwrap().remove(&region_id);
                            }
                        }
                        Err(RecvTimeoutError::Timeout) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            for (_, checker) in &mut *checkers {
                                checker.update_statistics();
                            }
                            deadline = std::time::Instant::now() + TICK_DURATION;
                        }
                        Err(e) => {
                            error!("failed to receive compaction info {:?}", e);
                        }
                    }
                }
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap()
    }
}

impl FlowController for TabletFlowController {
    fn should_drop(&self) -> bool {
        let ratio = self.discard_ratio.load(Ordering::Relaxed);
        let mut rng = rand::thread_rng();
        rng.gen_ratio(ratio, RATIO_SCALE_FACTOR)
    }

    #[cfg(test)]
    fn discard_ratio(&self) -> f64 {
        self.discard_ratio.load(Ordering::Relaxed) as f64 / RATIO_SCALE_FACTOR as f64
    }

    fn consume(&self, region_id: u64, bytes: usize) -> Duration {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.consume_duration(bytes);
        }
        println!("consume {} is not found", region_id);
        Duration::ZERO
    }

    fn unconsume(&self, region_id: u64, bytes: usize) {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            limiter.unconsume(bytes);
        }
    }

    #[cfg(test)]
    fn total_bytes_consumed(&self, region_id: u64) -> usize {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.total_bytes_consumed();
        }
        println!("total_bytes_consumed {} is not found", region_id);
        0
    }

    fn enable(&self, enable: bool) {
        self.enabled.store(enable, Ordering::Relaxed);
        if let Some(tx) = &self.tx {
            if enable {
                tx.send(Msg::Enable).unwrap();
            } else {
                tx.send(Msg::Disable).unwrap();
            }
        }
    }

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn set_speed_limit(&self, region_id: u64, speed_limit: f64) {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            limiter.set_speed_limit(speed_limit);
        }
    }

    fn is_unlimited(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            return limiter.speed_limit() == f64::INFINITY;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use engine_rocks::FlowInfo;
    use engine_test::{ctor::KvEngineConstructorExt, kv::KvTestEngine};
    use engine_traits::DummyFactory;

    use super::{super::flow_controller::*, *};
    #[test]
    fn test_tablet_flow_controller_basic() {
        let (tx, rx) = mpsc::channel();
        let root_path = "/tmp";
        let engine = Some(
            KvTestEngine::new_kv_engine(root_path, None, engine_traits::ALL_CFS, None).unwrap(),
        );
        let factory = DummyFactory::<KvTestEngine>::new(engine, root_path.to_string());
        let tablet_factory = Arc::new(factory);
        let flow_controller =
            TabletFlowController::new(&FlowControlConfig::default(), tablet_factory, rx);
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        tx.send(FlowInfo::Created(region_id, tablet_suffix))
            .unwrap();
        for _i in 0..600 {
            std::thread::sleep(std::time::Duration::from_millis(100));
            if flow_controller.tablet_exist(region_id) {
                break;
            }
        }
        assert!(flow_controller.tablet_exist(region_id));
        // enable flow controller
        assert_eq!(flow_controller.enabled(), true);
        assert_eq!(flow_controller.should_drop(), false);
        assert_eq!(flow_controller.is_unlimited(0), true);
        assert_eq!(flow_controller.consume(region_id, 0), Duration::ZERO);
        assert_eq!(flow_controller.consume(region_id, 1000), Duration::ZERO);
        assert_eq!(flow_controller.total_bytes_consumed(region_id), 1000);
        assert_eq!(flow_controller.total_bytes_consumed(region_id + 1), 0);

        // disable flow controller
        flow_controller.enable(false);
        assert_eq!(flow_controller.enabled(), false);
        // re-enable flow controller
        flow_controller.enable(true);
        assert_eq!(flow_controller.enabled(), true);
        assert_eq!(flow_controller.should_drop(), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);
        assert_eq!(flow_controller.consume(region_id, 1), Duration::ZERO);
    }
}
