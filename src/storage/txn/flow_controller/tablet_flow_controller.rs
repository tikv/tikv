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
use engine_traits::{CFNamesExt, FlowControlFactorsExt, TabletFactory};
use rand::Rng;
use tikv_util::{sys::thread::StdThreadBuildWrapper, time::Limiter};

use super::singleton_flow_controller::{FlowChecker, Msg, RATIO_SCALE_FACTOR, TICK_DURATION};
use crate::storage::config::FlowControlConfig;

type Limiters = Arc<RwLock<HashMap<u64, (Arc<Limiter>, Arc<AtomicU32>)>>>;
pub struct TabletFlowController {
    enabled: Arc<AtomicBool>,
    tx: Option<SyncSender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
    limiters: Limiters,
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
    pub fn new<E: CFNamesExt + FlowControlFactorsExt + Send + Sync + 'static>(
        config: &FlowControlConfig,
        tablet_factory: Arc<dyn TabletFactory<E> + Send + Sync>,
        flow_info_receiver: Receiver<FlowInfo>,
    ) -> Self {
        let (tx, rx) = mpsc::sync_channel(5);
        tx.send(if config.enable {
            Msg::Enable
        } else {
            Msg::Disable
        })
        .unwrap();
        let flow_checkers: Arc<RwLock<HashMap<u64, FlowChecker<E>>>> =
            Arc::new(RwLock::new(HashMap::default()));
        let limiters: Limiters = Arc::new(RwLock::new(HashMap::default()));
        Self {
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
    fn start<E: CFNamesExt + FlowControlFactorsExt + Send + Sync + 'static>(
        rx: Receiver<Msg>,
        flow_info_receiver: Receiver<FlowInfo>,
        tablet_factory: Arc<dyn TabletFactory<E> + Send + Sync>,
        flow_checkers: Arc<RwLock<HashMap<u64, FlowChecker<E>>>>,
        limiters: Limiters,
        config: FlowControlConfig,
    ) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn_wrapper(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut deadline = std::time::Instant::now();
                let mut enabled = true;
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

                    let insert_limiter_and_checker = |region_id, suffix| -> FlowChecker<E> {
                        let engine = tablet_factory.open_tablet_cache(region_id, suffix).unwrap();
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
                        FlowChecker::new_with_tablet_suffix(
                            &config,
                            engine,
                            limiter.1.clone(),
                            limiter.0.clone(),
                            suffix,
                        )
                    };
                    let msg = flow_info_receiver.recv_deadline(deadline);
                    match msg.clone() {
                        Ok(FlowInfo::L0(_cf, _, region_id, suffix))
                        | Ok(FlowInfo::L0Intra(_cf, _, region_id, suffix))
                        | Ok(FlowInfo::Flush(_cf, _, region_id, suffix))
                        | Ok(FlowInfo::Compaction(_cf, region_id, suffix)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                if checker.tablet_suffix() != suffix {
                                    continue;
                                }
                                checker.on_flow_info_msg(enabled, msg);
                            }
                        }
                        Ok(FlowInfo::BeforeUnsafeDestroyRange(region_id))
                        | Ok(FlowInfo::AfterUnsafeDestroyRange(region_id)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            if let Some(checker) = checkers.get_mut(&region_id) {
                                checker.on_flow_info_msg(enabled, msg);
                            }
                        }
                        Ok(FlowInfo::Created(region_id, suffix)) => {
                            let mut checkers = flow_checkers.as_ref().write().unwrap();
                            let checker = checkers
                                .entry(region_id)
                                .or_insert_with(|| insert_limiter_and_checker(region_id, suffix));
                            // check if the checker's engine is exactly (region_id, suffix)
                            // if  checker.suffix < suffix, it means its tablet is old and needs the refresh
                            if checker.tablet_suffix() < suffix {
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
                                    if checker.tablet_suffix() == suffix {
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
                            for checker in (*checkers).values_mut() {
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

impl TabletFlowController {
    pub fn should_drop(&self, region_id: u64) -> bool {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            let ratio = limiter.1.load(Ordering::Relaxed);
            let mut rng = rand::thread_rng();
            return rng.gen_ratio(ratio, RATIO_SCALE_FACTOR);
        }
        false
    }

    #[cfg(test)]
    pub fn discard_ratio(&self, region_id: u64) -> f64 {
        let limiters = self.limiters.as_ref().read().unwrap();
        if let Some(limiter) = limiters.get(&region_id) {
            let ratio = limiter.1.load(Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use engine_rocks::FlowInfo;
    use engine_traits::DummyFactory;

    use super::{
        super::{singleton_flow_controller::tests::*, FlowController},
        *,
    };

    fn create_tablet_flow_controller() -> (FlowController, mpsc::SyncSender<FlowInfo>, EngineStub) {
        let (tx, rx) = mpsc::sync_channel(0);
        let root_path = "/tmp";
        let stub = EngineStub::new();
        let factory = DummyFactory::<EngineStub>::new(Some(stub.clone()), root_path.to_string());
        let tablet_factory = Arc::new(factory);
        (
            FlowController::Tablet(TabletFlowController::new(
                &FlowControlConfig::default(),
                tablet_factory,
                rx,
            )),
            tx,
            stub,
        )
    }

    #[test]
    fn test_tablet_flow_controller_basic() {
        let (flow_controller, tx, _) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        tx.send(FlowInfo::Created(region_id, tablet_suffix))
            .unwrap();
        tx.send(FlowInfo::L0Intra(
            "default".to_string(),
            0,
            region_id,
            tablet_suffix,
        ))
        .unwrap();
        test_flow_controller_basic_impl(&flow_controller, region_id);
        tx.send(FlowInfo::Destroyed(region_id, tablet_suffix))
            .unwrap();
        tx.send(FlowInfo::L0Intra(
            "default".to_string(),
            0,
            region_id,
            tablet_suffix,
        ))
        .unwrap();
        //assert!(!flow_controller.tablet_exist(region_id));
    }

    #[test]
    fn test_tablet_flow_controller_memtable() {
        let (flow_controller, tx, stub) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        tx.send(FlowInfo::Created(region_id, tablet_suffix))
            .unwrap();
        tx.send(FlowInfo::L0Intra(
            "default".to_string(),
            0,
            region_id,
            tablet_suffix,
        ))
        .unwrap();
        test_flow_controller_memtable_impl(&flow_controller, &stub, &tx, region_id, tablet_suffix);
    }

    #[test]
    fn test_tablet_flow_controller_l0() {
        let (flow_controller, tx, stub) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        tx.send(FlowInfo::Created(region_id, tablet_suffix))
            .unwrap();
        tx.send(FlowInfo::L0Intra(
            "default".to_string(),
            0,
            region_id,
            tablet_suffix,
        ))
        .unwrap();
        test_flow_controller_l0_impl(&flow_controller, &stub, &tx, region_id, tablet_suffix);
    }

    #[test]
    fn test_tablet_flow_controller_pending_compaction_bytes() {
        let (flow_controller, tx, stub) = create_tablet_flow_controller();
        let region_id = 5_u64;
        let tablet_suffix = 5_u64;
        tx.send(FlowInfo::Created(region_id, tablet_suffix))
            .unwrap();
        tx.send(FlowInfo::L0Intra(
            "default".to_string(),
            0,
            region_id,
            tablet_suffix,
        ))
        .unwrap();

        test_flow_controller_pending_compaction_bytes_impl(
            &flow_controller,
            &stub,
            &tx,
            region_id,
            tablet_suffix,
        );
    }
}
