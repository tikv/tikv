// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc,
    },
    thread::{Builder, JoinHandle},
    time::Duration,
    u64,
};

use collections::HashMap;
use engine_rocks::FlowInfo;
use engine_traits::{CfNamesExt, FlowControlFactorsExt};
use getset::{CopyGetters, Setters};
use online_config::{ConfigChange, OnlineConfig};
use rand::Rng;
use tikv_util::{
    config::VersionTrack,
    smoother::{Smoother, Trend, SMOOTHER_STALE_RECORD_THRESHOLD, SMOOTHER_TIME_RANGE_THRESHOLD},
    sys::thread::StdThreadBuildWrapper,
    time::{Instant, Limiter},
};

use crate::storage::{config::FlowControlConfig, metrics::*};

pub(super) const TICK_DURATION: Duration = Duration::from_millis(1000);

pub(super) const RATIO_SCALE_FACTOR: u32 = 10_000_000;
const K_INC_SLOWDOWN_RATIO: f64 = 0.8;
const K_DEC_SLOWDOWN_RATIO: f64 = 1.0 / K_INC_SLOWDOWN_RATIO;
const MIN_THROTTLE_SPEED: f64 = 16.0 * 1024.0; // 16KB
const MAX_THROTTLE_SPEED: f64 = 200.0 * 1024.0 * 1024.0; // 200MB

const EMA_FACTOR: f64 = 0.6; // EMA stands for Exponential Moving Average

/// Flow controller is used to throttle the write rate at scheduler level,
/// aiming to substitute the write stall mechanism of RocksDB. It features in
/// two points:
///   * throttle at scheduler, so raftstore and apply won't be blocked anymore
///   * better control on the throttle rate to avoid QPS drop under heavy write
///
/// When write stall happens, the max speed of write rate max_delayed_write_rate
/// is limited to 16MB/s by default which doesn't take real disk ability into
/// account. It may underestimate the disk's throughout that 16MB/s is too small
/// at once, causing a very large jitter on the write duration.
/// Also, it decreases the delayed write rate further if the factors still
/// exceed the threshold. So under heavy write load, the write rate may be
/// throttled to a very low rate from time to time, causing QPS drop eventually.

/// For compaction pending bytes, we use discardable ratio to do flow control
/// which is separated mechanism from throttle speed. Compaction pending bytes
/// is a approximate value, usually, changes up and down dramatically, so it's
/// unwise to map compaction pending bytes to a specified throttle speed.
/// Instead, mapping it from soft limit to hard limit as 0% to 100% discardable
/// ratio. With this, there must be a point that foreground write rate is equal
/// to the background compaction pending bytes consuming rate so that compaction
/// pending bytes is kept around a steady level.
///
/// Here is a brief flow showing where the mechanism works:
/// grpc -> check should drop(discardable ratio) -> limiter -> async write to
/// raftstore
pub struct EngineFlowController {
    discard_ratio: Arc<AtomicU32>,
    limiter: Arc<Limiter>,
    tx: Option<SyncSender<Msg>>,
    handle: Option<std::thread::JoinHandle<()>>,
    config_tracker: Arc<VersionTrack<FlowControlConfig>>,
}

pub(super) enum Msg {
    Close,
    Enable,
    Disable,
}

impl Drop for EngineFlowController {
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

impl EngineFlowController {
    // only for test
    pub fn empty() -> Self {
        Self {
            discard_ratio: Arc::new(AtomicU32::new(0)),
            limiter: Arc::new(Limiter::new(f64::INFINITY)),
            tx: None,
            handle: None,
            config_tracker: Arc::new(VersionTrack::default()),
        }
    }

    pub fn new<E: CfNamesExt + FlowControlFactorsExt + Send + 'static>(
        config: &FlowControlConfig,
        engine: E,
        flow_info_receiver: Receiver<FlowInfo>,
    ) -> Self {
        let limiter = Arc::new(
            <Limiter>::builder(f64::INFINITY)
                .refill(Duration::from_millis(1))
                .build(),
        );
        let discard_ratio = Arc::new(AtomicU32::new(0));
        let config_tracker = Arc::new(VersionTrack::new(config.clone()));
        let checker = FlowChecker::new(
            config_tracker.clone(),
            engine,
            discard_ratio.clone(),
            limiter.clone(),
        );
        let (tx, rx) = mpsc::sync_channel(5);

        Self {
            discard_ratio,
            limiter,
            tx: Some(tx),
            handle: Some(checker.start(rx, flow_info_receiver)),
            config_tracker,
        }
    }
}

impl EngineFlowController {
    pub fn should_drop(&self, _region_id: u64) -> bool {
        let ratio = self.discard_ratio.load(Ordering::Relaxed);
        let mut rng = rand::thread_rng();
        rng.gen_ratio(ratio, RATIO_SCALE_FACTOR)
    }

    #[cfg(test)]
    pub fn discard_ratio(&self, _region_id: u64) -> f64 {
        self.discard_ratio.load(Ordering::Relaxed) as f64 / RATIO_SCALE_FACTOR as f64
    }

    pub fn consume(&self, _region_id: u64, bytes: usize) -> Duration {
        self.limiter.consume_duration(bytes)
    }

    pub fn unconsume(&self, _region_id: u64, bytes: usize) {
        self.limiter.unconsume(bytes);
    }

    #[cfg(test)]
    pub fn total_bytes_consumed(&self, _region_id: u64) -> usize {
        self.limiter.total_bytes_consumed()
    }

    pub fn enable(&self, enable: bool) {
        if let Some(tx) = &self.tx {
            if enable {
                tx.send(Msg::Enable).unwrap();
            } else {
                tx.send(Msg::Disable).unwrap();
            }
        }
    }

    pub fn enabled(&self) -> bool {
        self.config_tracker.value().enable
    }

    pub fn update_config(&self, config_change: ConfigChange) -> online_config::Result<()> {
        self.config_tracker.update(|cfg| cfg.update(config_change))
    }

    #[cfg(test)]
    pub fn set_speed_limit(&self, _region_id: u64, speed_limit: f64) {
        self.limiter.set_speed_limit(speed_limit);
    }

    pub fn is_unlimited(&self, _region_id: u64) -> bool {
        self.limiter.speed_limit() == f64::INFINITY
    }
}

// CfFlowChecker records some statistics and states related to one CF.
// These statistics fall into five categories:
//   * memtable
//   * L0 files
//   * L0 production flow (flush flow)
//   * L0 consumption flow (compaction read flow of L0)
//   * pending compaction bytes
// And all of them are collected from the hook of RocksDB's event listener.
struct CfFlowChecker {
    // Memtable related
    last_num_memtables: Smoother<u64, 20, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,
    memtable_debt: f64,
    memtable_init_speed: bool,

    // L0 files related
    // a few records of number of L0 files right after flush or L0 compaction
    // As we know, after flush the number of L0 files must increase by 1,
    // whereas, after L0 compaction the number of L0 files must decrease a lot
    // considering L0 compactions nearly includes all L0 files in a round.
    // So to evaluate the accumulation of L0 files, here only records the number
    // of L0 files right after L0 compactions.
    long_term_num_l0_files: Smoother<u64, 20, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // L0 production flow related
    last_flush_bytes: u64,
    last_flush_bytes_time: Instant,
    short_term_l0_production_flow: Smoother<u64, 10, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // L0 consumption flow related
    last_l0_bytes: u64,
    last_l0_bytes_time: Instant,
    short_term_l0_consumption_flow: Smoother<u64, 3, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    // Pending compaction bytes related
    // When the write flow is about 100MB/s, we observed that the compaction ops
    // is about 2.5, it means there are 750 compaction events in 5 minutes.
    long_term_pending_bytes:
        Option<Smoother<f64, 1024, SMOOTHER_STALE_RECORD_THRESHOLD, SMOOTHER_TIME_RANGE_THRESHOLD>>,
    pending_bytes_before_unsafe_destroy_range: Option<f64>,

    // On start related markers. Because after restart, the memtable, l0 files
    // and compaction pending bytes may be high on start. If throttle on start
    // at once, it may get a low throttle speed as initialization cause it may
    // has no write flow after restart. So use the markers to make sure only
    // throttled after the the memtable, l0 files and compaction pending bytes
    // go beyond the threshold again.
    on_start_memtable: bool,
    on_start_l0_files: bool,
    on_start_pending_bytes: bool,
}

impl Default for CfFlowChecker {
    fn default() -> Self {
        CfFlowChecker::new(true)
    }
}

impl CfFlowChecker {
    pub fn new(include_pending_bytes: bool) -> Self {
        Self {
            last_num_memtables: Smoother::default(),
            memtable_debt: 0.0,
            memtable_init_speed: false,
            long_term_num_l0_files: Smoother::default(),
            last_flush_bytes: 0,
            last_flush_bytes_time: Instant::now_coarse(),
            short_term_l0_production_flow: Smoother::default(),
            last_l0_bytes: 0,
            last_l0_bytes_time: Instant::now_coarse(),
            short_term_l0_consumption_flow: Smoother::default(),
            long_term_pending_bytes: if include_pending_bytes {
                Some(Smoother::default())
            } else {
                None
            },
            pending_bytes_before_unsafe_destroy_range: None,
            on_start_memtable: true,
            on_start_l0_files: true,
            on_start_pending_bytes: true,
        }
    }
}

pub trait FlowControlFactorStore {
    fn num_files_at_level(&self, region_id: u64, cf: &str, level: usize) -> u64;
    fn num_immutable_mem_table(&self, region_id: u64, cf: &str) -> u64;
    fn pending_compaction_bytes(&self, region_id: u64, cf: &str) -> u64;
    fn cf_names(&self, region_id: u64) -> Vec<String>;
}

impl<E: FlowControlFactorsExt + CfNamesExt> FlowControlFactorStore for E {
    fn cf_names(&self, _region_id: u64) -> Vec<String> {
        CfNamesExt::cf_names(self)
            .iter()
            .map(|v| v.to_string())
            .collect()
    }

    fn num_files_at_level(&self, _region_id: u64, cf: &str, level: usize) -> u64 {
        match self.get_cf_num_files_at_level(cf, level) {
            Ok(Some(n)) => n,
            _ => 0,
        }
    }
    fn num_immutable_mem_table(&self, _region_id: u64, cf: &str) -> u64 {
        match self.get_cf_num_immutable_mem_table(cf) {
            Ok(Some(n)) => n,
            _ => 0,
        }
    }
    fn pending_compaction_bytes(&self, _region_id: u64, cf: &str) -> u64 {
        match self.get_cf_pending_compaction_bytes(cf) {
            Ok(Some(n)) => n,
            _ => 0,
        }
    }
}

#[derive(CopyGetters, Setters)]
pub(super) struct FlowChecker<E: FlowControlFactorStore + Send + 'static> {
    config_tracker: Arc<VersionTrack<FlowControlConfig>>,

    // CfFlowChecker for each CF.
    cf_checkers: HashMap<String, CfFlowChecker>,
    // Record which CF is taking control of throttling, the throttle speed is
    // decided based on the statistics of the throttle CF. If the multiple CFs
    // exceed the threshold, choose the larger one.
    throttle_cf: Option<String>,
    // Discard ratio is decided by pending compaction bytes, it's the ratio to
    // drop write requests(return ServerIsBusy to TiDB) randomly.
    discard_ratio: Arc<AtomicU32>,

    #[getset(set = "pub")]
    engine: E,
    limiter: Arc<Limiter>,
    // Records the foreground write flow at scheduler level of last few seconds.
    write_flow_recorder: Smoother<u64, 30, SMOOTHER_STALE_RECORD_THRESHOLD, 0>,

    last_record_time: Instant,
    last_speed: f64,
    wait_for_destroy_range_finish: bool,

    region_id: u64,
    rc: AtomicU32,
}

impl<E: FlowControlFactorStore + Send + 'static> FlowChecker<E> {
    pub fn new(
        config: Arc<VersionTrack<FlowControlConfig>>,
        engine: E,
        discard_ratio: Arc<AtomicU32>,
        limiter: Arc<Limiter>,
    ) -> Self {
        Self::new_with_region_id(0, config, engine, discard_ratio, limiter)
    }

    pub fn new_with_region_id(
        region_id: u64,
        config_tracker: Arc<VersionTrack<FlowControlConfig>>,
        engine: E,
        discard_ratio: Arc<AtomicU32>,
        limiter: Arc<Limiter>,
    ) -> Self {
        let include_pending_bytes = region_id == 0;
        let cf_checkers = engine
            .cf_names(region_id)
            .into_iter()
            .map(|cf_name| (cf_name, CfFlowChecker::new(include_pending_bytes)))
            .collect();

        Self {
            region_id,
            config_tracker,
            engine,
            discard_ratio,
            limiter,
            write_flow_recorder: Smoother::default(),
            cf_checkers,
            throttle_cf: None,
            last_record_time: Instant::now_coarse(),
            last_speed: 0.0,
            wait_for_destroy_range_finish: false,
            rc: AtomicU32::new(1),
        }
    }

    pub fn on_flow_info_msg(&mut self, flow_info: Result<FlowInfo, RecvTimeoutError>) {
        let current_cfg = self.config_tracker.value().clone();
        match flow_info {
            Ok(FlowInfo::L0(cf, l0_bytes, ..)) => {
                self.collect_l0_consumption_stats(&cf, l0_bytes);
                if current_cfg.enable {
                    self.on_l0_change(cf)
                }
            }
            Ok(FlowInfo::L0Intra(cf, diff_bytes, ..)) => {
                if diff_bytes > 0 {
                    // Intra L0 merges some deletion records, so regard it as a L0 compaction.
                    self.collect_l0_consumption_stats(&cf, diff_bytes);
                    if current_cfg.enable {
                        self.on_l0_change(cf);
                    }
                }
            }
            Ok(FlowInfo::Flush(cf, flush_bytes, ..)) => {
                self.collect_l0_production_stats(&cf, flush_bytes);
                if current_cfg.enable {
                    self.on_memtable_change(&cf);
                    self.on_l0_change(cf)
                }
            }
            Ok(FlowInfo::Compaction(cf, ..)) => {
                if current_cfg.enable {
                    self.on_pending_compaction_bytes_change(cf);
                }
            }
            Ok(FlowInfo::BeforeUnsafeDestroyRange(..)) => {
                if !current_cfg.enable {
                    return;
                }
                if self.wait_for_destroy_range_finish {
                    // Concurrent unsafe destroy range, ignore the second one
                    info!("concurrent unsafe destroy range, ignore");
                    return;
                }
                self.wait_for_destroy_range_finish = true;
                let soft = (current_cfg.soft_pending_compaction_bytes_limit.0 as f64).log2();
                for (cf, cf_checker) in &mut self.cf_checkers {
                    if let Some(long_term_pending_bytes) =
                        cf_checker.long_term_pending_bytes.as_ref()
                    {
                        let v = long_term_pending_bytes.get_avg();
                        if v <= soft {
                            info!(
                                "before unsafe destroy range";
                                "cf" => cf,
                                "pending_bytes" => v
                            );
                            cf_checker.pending_bytes_before_unsafe_destroy_range = Some(v);
                        }
                    }
                }
            }
            Ok(FlowInfo::AfterUnsafeDestroyRange(..)) => {
                if !current_cfg.enable {
                    return;
                }
                self.wait_for_destroy_range_finish = false;
                for (cf, cf_checker) in &mut self.cf_checkers {
                    if let Some(before) = cf_checker.pending_bytes_before_unsafe_destroy_range {
                        let soft =
                            (current_cfg.soft_pending_compaction_bytes_limit.0 as f64).log2();
                        let after = (self.engine.pending_compaction_bytes(self.region_id, cf)
                            as f64)
                            .log2();

                        assert!(before < soft);
                        if after >= soft {
                            // there is a pending bytes jump
                            SCHED_THROTTLE_ACTION_COUNTER
                                .with_label_values(&[cf, "pending_bytes_jump"])
                                .inc();
                        }
                        info!(
                            "after unsafe destroy range";
                            "cf" => cf,
                            "before" => before,
                            "after" => after
                        );
                    }
                }
            }
            Ok(FlowInfo::Created(..)) => {}
            Ok(FlowInfo::Destroyed(..)) => {}
            Err(e) => {
                error!("failed to receive compaction info {:?}", e);
            }
        }
    }

    fn start(self, rx: Receiver<Msg>, flow_info_receiver: Receiver<FlowInfo>) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn_wrapper(move || {
                let mut checker = self;
                let mut deadline = std::time::Instant::now();
                loop {
                    match rx.try_recv() {
                        Ok(Msg::Close) => break,
                        Ok(Msg::Disable) => {
                            checker.reset_statistics();
                        }
                        Ok(Msg::Enable) => {}
                        Err(_) => {}
                    }

                    let msg = flow_info_receiver.recv_deadline(deadline);
                    if let Err(RecvTimeoutError::Timeout) = msg {
                        let (rate, cf_throttle_flags) = checker.update_statistics();
                        for (cf, val) in cf_throttle_flags {
                            SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(val);
                        }
                        SCHED_WRITE_FLOW_GAUGE.set(rate as i64);
                        deadline = std::time::Instant::now() + TICK_DURATION;
                    } else {
                        checker.on_flow_info_msg(msg);
                    }
                }
            })
            .unwrap()
    }

    pub fn reset_statistics(&mut self) {
        SCHED_L0_TARGET_FLOW_GAUGE.set(0);
        for cf in self.cf_checkers.keys() {
            SCHED_THROTTLE_CF_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_PENDING_COMPACTION_BYTES_GAUGE
                .with_label_values(&[cf])
                .set(0);
            SCHED_MEMTABLE_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_AVG_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_L0_FLOW_GAUGE.with_label_values(&[cf]).set(0);
            SCHED_FLUSH_FLOW_GAUGE.with_label_values(&[cf]).set(0);
        }
        SCHED_WRITE_FLOW_GAUGE.set(0);
        SCHED_THROTTLE_FLOW_GAUGE.set(0);
        self.limiter.set_speed_limit(f64::INFINITY);
        SCHED_DISCARD_RATIO_GAUGE.set(0);
        self.discard_ratio.store(0, Ordering::Relaxed);
    }

    pub fn update_statistics(&mut self) -> (f64, HashMap<&str, i64>) {
        let mut cf_throttle_flags = HashMap::default();
        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            cf_throttle_flags.insert(throttle_cf.as_str(), 1);
            for cf in self.cf_checkers.keys() {
                if cf != throttle_cf {
                    cf_throttle_flags.insert(cf.as_str(), 0);
                }
            }
        } else {
            for cf in self.cf_checkers.keys() {
                cf_throttle_flags.insert(cf.as_str(), 0);
            }
        }

        // calculate foreground write flow
        let dur = self.last_record_time.saturating_elapsed_secs();
        if dur < f64::EPSILON {
            return (0.0, cf_throttle_flags);
        }
        let rate = self.limiter.total_bytes_consumed() as f64 / dur;
        // don't record those write rate of 0.
        // For closed loop system, if all the requests are delayed(assume > 1s),
        // then in the next second, the write rate would be 0. But it doesn't
        // reflect the real write rate, so just ignore it.
        if self.limiter.total_bytes_consumed() != 0 {
            self.write_flow_recorder.observe(rate as u64);
        }

        self.last_record_time = Instant::now_coarse();

        self.limiter.reset_statistics();
        (rate, cf_throttle_flags)
    }

    pub fn on_pending_compaction_bytes_change(&mut self, cf: String) -> u64 {
        let pending_compaction_bytes = self.engine.pending_compaction_bytes(self.region_id, &cf);
        self.on_pending_compaction_bytes_change_cf(pending_compaction_bytes, cf);
        pending_compaction_bytes
    }

    pub fn on_pending_compaction_bytes_change_cf(
        &mut self,
        pending_compaction_bytes: u64,
        cf: String,
    ) {
        let control_cfg = self.config_tracker.value().clone();
        let hard = (control_cfg.hard_pending_compaction_bytes_limit.0 as f64).log2();
        let soft = (control_cfg.soft_pending_compaction_bytes_limit.0 as f64).log2();
        // Because pending compaction bytes changes dramatically, take the
        // logarithm of pending compaction bytes to make the values fall into
        // a relative small range
        let mut num = (pending_compaction_bytes as f64).log2();
        if !num.is_finite() {
            // 0.log2() == -inf, which is not expected and may lead to sum always be NaN
            num = 0.0;
        }
        let checker = self.cf_checkers.get_mut(&cf).unwrap();

        // only be called by v1
        if let Some(long_term_pending_bytes) = checker.long_term_pending_bytes.as_mut() {
            long_term_pending_bytes.observe(num);
            SCHED_PENDING_COMPACTION_BYTES_GAUGE
                .with_label_values(&[&cf])
                .set((long_term_pending_bytes.get_avg() * RATIO_SCALE_FACTOR as f64) as i64);

            // do special check on start, see the comment of the variable definition for
            // detail.
            if checker.on_start_pending_bytes {
                if num < soft || long_term_pending_bytes.trend() == Trend::Increasing {
                    // the write is accumulating, still need to throttle
                    checker.on_start_pending_bytes = false;
                } else {
                    // still on start, should not throttle now
                    return;
                }
            }

            let pending_compaction_bytes = long_term_pending_bytes.get_avg();
            let ignore = if let Some(before) = checker.pending_bytes_before_unsafe_destroy_range {
                // It assumes that the long term average will eventually come down below the
                // soft limit. If the general traffic flow increases during destroy, the long
                // term average may never come down and the flow control will be turned off for
                // a long time, which would be a rather rare case, so just ignore it.
                if pending_compaction_bytes <= before && !self.wait_for_destroy_range_finish {
                    info!(
                        "pending compaction bytes is back to normal";
                        "cf" => &cf,
                        "pending_compaction_bytes" => pending_compaction_bytes,
                        "before" => before
                    );
                    checker.pending_bytes_before_unsafe_destroy_range = None;
                }
                true
            } else {
                false
            };

            for checker in self.cf_checkers.values() {
                if let Some(long_term_pending_bytes) = checker.long_term_pending_bytes.as_ref()
                    && num < long_term_pending_bytes.get_recent()
                {
                    return;
                }
            }

            let mut ratio = if pending_compaction_bytes < soft || ignore {
                0
            } else {
                let new_ratio = (pending_compaction_bytes - soft) / (hard - soft);
                let old_ratio = self.discard_ratio.load(Ordering::Relaxed);

                // Because pending compaction bytes changes up and down, so using
                // EMA(Exponential Moving Average) to smooth it.
                (if old_ratio != 0 {
                    EMA_FACTOR * (old_ratio as f64 / RATIO_SCALE_FACTOR as f64)
                        + (1.0 - EMA_FACTOR) * new_ratio
                } else if new_ratio > 0.01 {
                    0.01
                } else {
                    new_ratio
                } * RATIO_SCALE_FACTOR as f64) as u32
            };
            SCHED_DISCARD_RATIO_GAUGE.set(ratio as i64);
            if ratio > RATIO_SCALE_FACTOR {
                ratio = RATIO_SCALE_FACTOR;
            }
            self.discard_ratio.store(ratio, Ordering::Relaxed);
        }
    }

    fn on_memtable_change(&mut self, cf: &str) {
        let num_memtables = self.engine.num_immutable_mem_table(self.region_id, cf);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        SCHED_MEMTABLE_GAUGE
            .with_label_values(&[cf])
            .set(num_memtables as i64);
        let prev = checker.last_num_memtables.get_recent();
        checker.last_num_memtables.observe(num_memtables);

        let memtables_threshold = self.config_tracker.value().memtables_threshold;
        // do special check on start, see the comment of the variable definition for
        // detail.
        if checker.on_start_memtable {
            if num_memtables < memtables_threshold
                || checker.last_num_memtables.trend() == Trend::Increasing
            {
                // the write is accumulating, still need to throttle
                checker.on_start_memtable = false;
            } else {
                // still on start, should not throttle now
                return;
            }
        }

        for c in self.cf_checkers.values() {
            if num_memtables < c.last_num_memtables.get_recent() {
                return;
            }
        }

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        let is_throttled = self.limiter.speed_limit() != f64::INFINITY;
        let should_throttle = checker.last_num_memtables.get_avg() > memtables_threshold as f64;
        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[cf, "memtable_init"])
                .inc();
            let x = self.write_flow_recorder.get_percentile_90();
            if x == 0 {
                f64::INFINITY
            } else {
                checker.memtable_init_speed = true;
                self.throttle_cf = Some(cf.to_string());
                x as f64
            }
        } else if is_throttled && (!should_throttle || num_memtables < memtables_threshold) {
            // should not throttle memtable
            if checker.memtable_init_speed {
                checker.memtable_init_speed = false;
                f64::INFINITY
            } else {
                let speed = self.limiter.speed_limit() + checker.memtable_debt * 1024.0 * 1024.0;
                checker.memtable_debt = 0.0;
                speed
            }
        } else if is_throttled && should_throttle {
            // should throttle
            let diff = match num_memtables.cmp(&prev) {
                std::cmp::Ordering::Greater => {
                    checker.memtable_debt += 1.0;
                    -1.0
                }
                std::cmp::Ordering::Less => {
                    checker.memtable_debt -= 1.0;
                    1.0
                }
                std::cmp::Ordering::Equal => {
                    // keep, do nothing
                    0.0
                }
            };
            self.limiter.speed_limit() + diff * 1024.0 * 1024.0
        } else {
            f64::INFINITY
        };

        self.update_speed_limit(throttle);
    }

    fn collect_l0_consumption_stats(&mut self, cf: &str, l0_bytes: u64) {
        let num_l0_files = self.engine.num_files_at_level(self.region_id, cf, 0);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        checker.last_l0_bytes += l0_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        SCHED_L0_GAUGE
            .with_label_values(&[cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);
    }

    fn collect_l0_production_stats(&mut self, cf: &str, flush_bytes: u64) {
        let num_l0_files = self.engine.num_files_at_level(self.region_id, cf, 0);

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        checker.last_flush_bytes += flush_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        SCHED_L0_GAUGE
            .with_label_values(&[cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);

        if checker.last_flush_bytes_time.saturating_elapsed_secs() > 5.0 {
            // update flush flow
            let flush_flow = checker.last_flush_bytes as f64
                / checker.last_flush_bytes_time.saturating_elapsed_secs();
            checker
                .short_term_l0_production_flow
                .observe(flush_flow as u64);
            SCHED_FLUSH_FLOW_GAUGE
                .with_label_values(&[cf])
                .set(checker.short_term_l0_production_flow.get_avg() as i64);

            // update l0 flow
            if checker.last_l0_bytes != 0 {
                let l0_flow = checker.last_l0_bytes as f64
                    / checker.last_l0_bytes_time.saturating_elapsed_secs();
                checker.last_l0_bytes_time = Instant::now_coarse();
                checker
                    .short_term_l0_consumption_flow
                    .observe(l0_flow as u64);
                SCHED_L0_FLOW_GAUGE
                    .with_label_values(&[cf])
                    .set(checker.short_term_l0_consumption_flow.get_avg() as i64);
            }

            checker.last_flush_bytes_time = Instant::now_coarse();
            checker.last_l0_bytes = 0;
            checker.last_flush_bytes = 0;
        }
    }

    // Check the number of l0 files to decide whether need to adjust target flow
    fn on_l0_change(&mut self, cf: String) {
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        let num_l0_files = checker.long_term_num_l0_files.get_recent();
        let l0_files_threshold = self.config_tracker.value().l0_files_threshold;

        // do special check on start, see the comment of the variable definition for
        // detail.
        if checker.on_start_l0_files {
            if num_l0_files < l0_files_threshold
                || checker.long_term_num_l0_files.trend() == Trend::Increasing
            {
                // the write is accumulating, still need to throttle
                checker.on_start_l0_files = false;
            } else {
                // still on start, should not throttle now
                return;
            }
        }

        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            if &cf != throttle_cf {
                // to avoid throttle cf changes back and forth, only change it
                // when the other is much higher.
                if num_l0_files
                    > self.cf_checkers[throttle_cf]
                        .long_term_num_l0_files
                        .get_max()
                        + 4
                {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "change_throttle_cf"])
                        .inc();
                    self.throttle_cf = Some(cf.clone());
                } else {
                    return;
                }
            }
        }

        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        if checker.memtable_init_speed {
            return;
        }

        let is_throttled = self.limiter.speed_limit() != f64::INFINITY;
        let should_throttle = checker.long_term_num_l0_files.get_recent() > l0_files_threshold;

        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[&cf, "init"])
                .inc();
            self.throttle_cf = Some(cf.clone());
            let x = if self.last_speed < f64::EPSILON {
                self.write_flow_recorder.get_percentile_90() as f64
            } else {
                self.last_speed
            };
            if x < f64::EPSILON { f64::INFINITY } else { x }
        } else if is_throttled && should_throttle {
            self.limiter.speed_limit() * K_INC_SLOWDOWN_RATIO
        } else if is_throttled && !should_throttle {
            self.last_speed = self.limiter.speed_limit() * K_DEC_SLOWDOWN_RATIO;
            f64::INFINITY
        } else {
            f64::INFINITY
        };

        self.update_speed_limit(throttle)
    }

    fn update_speed_limit(&mut self, mut throttle: f64) {
        if throttle < MIN_THROTTLE_SPEED {
            throttle = MIN_THROTTLE_SPEED;
        }
        if throttle > MAX_THROTTLE_SPEED {
            self.throttle_cf = None;
            throttle = f64::INFINITY;
        }
        SCHED_THROTTLE_FLOW_GAUGE.set(if throttle == f64::INFINITY {
            0
        } else {
            throttle as i64
        });
        self.limiter.set_speed_limit(throttle)
    }

    pub fn inc(&self) -> u32 {
        self.rc.fetch_add(1, Ordering::SeqCst)
    }

    pub fn dec(&self) -> u32 {
        self.rc.fetch_sub(1, Ordering::SeqCst)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::atomic::AtomicU64;

    use engine_rocks::RocksCfOptions;
    use engine_traits::{CfOptionsExt, Result};
    use online_config::ConfigValue;

    use super::{super::FlowController, *};

    #[derive(Clone)]
    pub struct EngineStub(pub Arc<EngineStubInner>);

    pub struct EngineStubInner {
        pub pending_compaction_bytes: AtomicU64,
        pub num_l0_files: AtomicU64,
        pub num_memtables: AtomicU64,
    }

    impl EngineStub {
        pub fn new() -> Self {
            Self(Arc::new(EngineStubInner {
                pending_compaction_bytes: AtomicU64::new(0),
                num_l0_files: AtomicU64::new(0),
                num_memtables: AtomicU64::new(0),
            }))
        }
    }

    impl CfNamesExt for EngineStub {
        fn cf_names(&self) -> Vec<&str> {
            vec!["default"]
        }
    }

    impl CfOptionsExt for EngineStub {
        type CfOptions = RocksCfOptions;
        fn get_options_cf(&self, _cf: &str) -> Result<Self::CfOptions> {
            unimplemented!();
        }

        fn set_options_cf(&self, _cf: &str, _options: &[(&str, &str)]) -> Result<()> {
            unimplemented!();
        }
    }

    impl FlowControlFactorsExt for EngineStub {
        fn get_cf_num_files_at_level(&self, _cf: &str, _level: usize) -> Result<Option<u64>> {
            Ok(Some(self.0.num_l0_files.load(Ordering::Relaxed)))
        }

        fn get_cf_num_immutable_mem_table(&self, _cf: &str) -> Result<Option<u64>> {
            Ok(Some(self.0.num_memtables.load(Ordering::Relaxed)))
        }

        fn get_cf_pending_compaction_bytes(&self, _cf: &str) -> Result<Option<u64>> {
            Ok(Some(
                self.0.pending_compaction_bytes.load(Ordering::Relaxed),
            ))
        }
    }

    pub fn send_flow_info(tx: &mpsc::SyncSender<FlowInfo>, region_id: u64) {
        tx.send(FlowInfo::Flush("default".to_string(), 0, region_id))
            .unwrap();
        tx.send(FlowInfo::Compaction("default".to_string(), region_id))
            .unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
    }

    pub fn test_flow_controller_basic_impl(flow_controller: &FlowController, region_id: u64) {
        // enable flow controller
        assert_eq!(flow_controller.enabled(), true);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);
        assert_eq!(flow_controller.consume(region_id, 0), Duration::ZERO);
        assert_eq!(flow_controller.consume(region_id, 1000), Duration::ZERO);

        // disable flow controller
        let change = std::collections::HashMap::from_iter([(
            "enable".to_string(),
            ConfigValue::Bool(false),
        )]);
        flow_controller.update_config(change).unwrap();
        assert_eq!(flow_controller.enabled(), false);
        // re-enable flow controller
        let change =
            std::collections::HashMap::from_iter([("enable".to_string(), ConfigValue::Bool(true))]);
        flow_controller.update_config(change).unwrap();
        assert_eq!(flow_controller.enabled(), true);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);
        assert_eq!(flow_controller.consume(region_id, 1), Duration::ZERO);
    }

    #[test]
    fn test_flow_controller_basic() {
        let stub = EngineStub::new();
        let (_tx, rx) = mpsc::channel();
        let flow_controller = EngineFlowController::new(&FlowControlConfig::default(), stub, rx);
        let flow_controller = FlowController::Singleton(flow_controller);
        test_flow_controller_basic_impl(&flow_controller, 0);
    }

    pub fn test_flow_controller_memtable_impl(
        flow_controller: &FlowController,
        stub: &EngineStub,
        tx: &mpsc::SyncSender<FlowInfo>,
        region_id: u64,
    ) {
        assert_eq!(flow_controller.consume(0, 2000), Duration::ZERO);
        loop {
            if flow_controller.total_bytes_consumed(0) == 0 {
                break;
            }
            std::thread::sleep(TICK_DURATION);
        }

        assert_eq!(flow_controller.consume(region_id, 2000), Duration::ZERO);
        loop {
            if flow_controller.total_bytes_consumed(region_id) == 0 {
                break;
            }
            std::thread::sleep(TICK_DURATION);
        }

        // exceeds the threshold on start
        stub.0.num_memtables.store(8, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        // on start check forbids flow control
        assert_eq!(flow_controller.is_unlimited(region_id), true);
        // once falls below the threshold, pass the on start check
        stub.0.num_memtables.store(1, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        // not throttle when the average of the sliding window doesn't exceeds the
        // threshold
        stub.0.num_memtables.store(6, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);

        // the average of sliding window exceeds the threshold
        stub.0.num_memtables.store(6, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), false);
        assert_ne!(flow_controller.consume(region_id, 2000), Duration::ZERO);

        // increase the threshold.
        let change = std::collections::HashMap::from_iter([(
            "memtables_threshold".to_string(),
            ConfigValue::U64(10),
        )]);
        flow_controller.update_config(change).unwrap();
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);

        // decrease the threshold.
        let change = std::collections::HashMap::from_iter([(
            "memtables_threshold".to_string(),
            ConfigValue::U64(5),
        )]);
        flow_controller.update_config(change).unwrap();
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), false);

        // not throttle once the number of memtables falls below the threshold
        stub.0.num_memtables.store(1, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);
    }

    #[test]
    fn test_flow_controller_memtable() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let flow_controller =
            EngineFlowController::new(&FlowControlConfig::default(), stub.clone(), rx);
        let flow_controller = FlowController::Singleton(flow_controller);
        test_flow_controller_memtable_impl(&flow_controller, &stub, &tx, 0);
    }

    pub fn test_flow_controller_l0_impl(
        flow_controller: &FlowController,
        stub: &EngineStub,
        tx: &mpsc::SyncSender<FlowInfo>,
        region_id: u64,
    ) {
        assert_eq!(flow_controller.consume(region_id, 2000), Duration::ZERO);
        loop {
            if flow_controller.total_bytes_consumed(region_id) == 0 {
                break;
            }
            std::thread::sleep(TICK_DURATION);
        }

        // exceeds the threshold
        stub.0.num_l0_files.store(30, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        // on start check forbids flow control
        assert_eq!(flow_controller.is_unlimited(region_id), true);
        // once fall below the threshold, pass the on start check
        stub.0.num_l0_files.store(10, Ordering::Relaxed);
        send_flow_info(tx, region_id);

        // exceeds the threshold, throttle now
        stub.0.num_l0_files.store(30, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), false);
        assert_ne!(flow_controller.consume(region_id, 2000), Duration::ZERO);

        // increase the threshold.
        let change = std::collections::HashMap::from_iter([(
            "l0_files_threshold".to_string(),
            ConfigValue::U64(50),
        )]);
        flow_controller.update_config(change).unwrap();
        send_flow_info(tx, region_id);
        assert_eq!(flow_controller.should_drop(region_id), false);
        assert_eq!(flow_controller.is_unlimited(region_id), true);
    }

    #[test]
    fn test_flow_controller_l0() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let flow_controller =
            EngineFlowController::new(&FlowControlConfig::default(), stub.clone(), rx);
        let flow_controller = FlowController::Singleton(flow_controller);
        test_flow_controller_l0_impl(&flow_controller, &stub, &tx, 0);
    }

    pub fn test_flow_controller_pending_compaction_bytes_impl(
        flow_controller: &FlowController,
        stub: &EngineStub,
        tx: &mpsc::SyncSender<FlowInfo>,
        region_id: u64,
    ) {
        // exceeds the threshold
        stub.0
            .pending_compaction_bytes
            .store(500 * 1024 * 1024 * 1024, Ordering::Relaxed);
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
            .store(500 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) > f64::EPSILON);

        // increase the threshold.
        let change = std::collections::HashMap::from_iter([(
            "soft_pending_compaction_bytes_limit".to_string(),
            ConfigValue::Size(600 * 1024 * 1024 * 1024),
        )]);
        flow_controller.update_config(change).unwrap();
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // reset to default value.
        let change = std::collections::HashMap::from_iter([(
            "soft_pending_compaction_bytes_limit".to_string(),
            ConfigValue::Size(192 * 1024 * 1024 * 1024),
        )]);
        flow_controller.update_config(change).unwrap();

        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // pending compaction bytes jump after unsafe destroy range
        tx.send(FlowInfo::BeforeUnsafeDestroyRange(region_id))
            .unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // during unsafe destroy range, pending compaction bytes may change
        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        stub.0
            .pending_compaction_bytes
            .store(10000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // after unsafe destroy range, pending compaction bytes may jump back to a lower
        // value
        stub.0
            .pending_compaction_bytes
            .store(100 * 1024 * 1024 * 1024, Ordering::Relaxed);
        tx.send(FlowInfo::Compaction("default".to_string(), region_id))
            .unwrap();
        tx.send(FlowInfo::AfterUnsafeDestroyRange(region_id))
            .unwrap();
        tx.send(FlowInfo::L0Intra("default".to_string(), 0, region_id))
            .unwrap();
        assert!(
            flow_controller.discard_ratio(region_id) < f64::EPSILON,
            "discard_ratio {}",
            flow_controller.discard_ratio(region_id)
        );

        // the long term average pending compaction bytes is still high, shouldn't
        // unfreeze the jump control
        stub.0
            .pending_compaction_bytes
            .store(100 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // the long term average pending compaction bytes falls below the threshold,
        // should unfreeze the jump control
        stub.0
            .pending_compaction_bytes
            .store(1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);

        // exceeds the threshold, should perform throttle
        stub.0
            .pending_compaction_bytes
            .store(1000000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) > f64::EPSILON);
    }

    #[test]
    fn test_flow_controller_pending_compaction_bytes() {
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let flow_controller =
            EngineFlowController::new(&FlowControlConfig::default(), stub.clone(), rx);
        let flow_controller = FlowController::Singleton(flow_controller);
        test_flow_controller_pending_compaction_bytes_impl(&flow_controller, &stub, &tx, 0);
    }

    #[test]
    fn test_flow_controller_pending_compaction_bytes_of_zero() {
        let region_id = 0;
        let stub = EngineStub::new();
        let (tx, rx) = mpsc::sync_channel(0);
        let flow_controller =
            EngineFlowController::new(&FlowControlConfig::default(), stub.clone(), rx);
        let flow_controller = FlowController::Singleton(flow_controller);

        // should handle zero pending compaction bytes properly
        stub.0.pending_compaction_bytes.store(0, Ordering::Relaxed);
        send_flow_info(&tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) < f64::EPSILON);
        stub.0
            .pending_compaction_bytes
            .store(10000000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(&tx, region_id);
        stub.0
            .pending_compaction_bytes
            .store(10000000000 * 1024 * 1024 * 1024, Ordering::Relaxed);
        send_flow_info(&tx, region_id);
        assert!(flow_controller.discard_ratio(region_id) > f64::EPSILON);
    }
}
