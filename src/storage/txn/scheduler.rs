// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Scheduler which schedules the execution of `storage::Command`s.
//!
//! There is one scheduler for each store. It receives commands from clients, executes them against
//! the MVCC layer storage engine.
//!
//! Logically, the data organization hierarchy from bottom to top is row -> region -> store ->
//! database. But each region is replicated onto N stores for reliability, the replicas form a Raft
//! group, one of which acts as the leader. When the client read or write a row, the command is
//! sent to the scheduler which is on the region leader's store.
//!
//! Scheduler runs in a single-thread event loop, but command executions are delegated to a pool of
//! worker thread.
//!
//! Scheduler keeps track of all the running commands and uses latches to ensure serialized access
//! to the overlapping rows involved in concurrent commands. But note that scheduler only ensures
//! serialized access to the overlapping rows at command level, but a transaction may consist of
//! multiple commands, therefore conflicts may happen at transaction level. Transaction semantics
//! is ensured by the transaction protocol implemented in the client library, which is transparent
//! to the scheduler.

use crossbeam::utils::CachePadded;
use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::f64::INFINITY;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::u64;

use collections::HashMap;
use concurrency_manager::{ConcurrencyManager, KeyHandleGuard};
use engine_traits::{CFNamesExt, MiscExt};
use kvproto::kvrpcpb::{CommandPri, ExtraOp};
use rand::Rng;
use tikv_util::{
    callback::must_call,
    time::{duration_to_sec, Consume, Instant, Limiter},
};
use txn_types::TimeStamp;

use crate::storage::config::Config;
use crate::storage::kv::{
    drop_snapshot_callback, with_tls_engine, Engine, ExtCallback, Result as EngineResult,
    SnapContext, Statistics,
};
use crate::storage::lock_manager::{self, LockManager, WaitTimeout};
use crate::storage::metrics::{self, *};
use crate::storage::txn::commands::{ResponsePolicy, WriteContext, WriteResult};
use crate::storage::txn::{
    commands::Command,
    latch::{Latches, Lock},
    sched_pool::{tls_collect_read_duration, tls_collect_scan_details, SchedPool},
    Error, ProcessResult,
};
use crate::storage::{
    get_priority_tag, types::StorageCallback, Error as StorageError,
    ErrorInner as StorageErrorInner,
};

const TASKS_SLOTS_NUM: usize = 1 << 12; // 4096 slots.

/// Task is a running command.
pub(super) struct Task {
    pub(super) cid: u64,
    pub(super) cmd: Command,
    pub(super) extra_op: ExtraOp,
}

impl Task {
    /// Creates a task for a running command.
    pub(super) fn new(cid: u64, cmd: Command) -> Task {
        Task {
            cid,
            cmd,
            extra_op: ExtraOp::Noop,
        }
    }
}

struct CmdTimer {
    tag: metrics::CommandKind,
    begin: Instant,
}

impl Drop for CmdTimer {
    fn drop(&mut self) {
        SCHED_HISTOGRAM_VEC_STATIC
            .get(self.tag)
            .observe(self.begin.elapsed_secs());
    }
}

// It stores context of a task.
struct TaskContext {
    task: Option<Task>,

    lock: Lock,
    cb: Option<StorageCallback>,
    pr: Option<ProcessResult>,
    write_bytes: usize,
    tag: metrics::CommandKind,
    // How long it waits on latches.
    // latch_timer: Option<Instant>,
    latch_timer: Instant,
    // Total duration of a command.
    _cmd_timer: CmdTimer,
}

impl TaskContext {
    fn new(task: Task, latches: &Latches, cb: StorageCallback) -> TaskContext {
        let tag = task.cmd.tag();
        let lock = task.cmd.gen_lock(latches);
        // Write command should acquire write lock.
        if !task.cmd.readonly() && !lock.is_write_lock() {
            panic!("write lock is expected for command {}", task.cmd);
        }
        let write_bytes = if lock.is_write_lock() {
            task.cmd.write_bytes()
        } else {
            0
        };

        TaskContext {
            task: Some(task),
            lock,
            cb: Some(cb),
            pr: None,
            write_bytes,
            tag,
            latch_timer: Instant::now_coarse(),
            _cmd_timer: CmdTimer {
                tag,
                begin: Instant::now_coarse(),
            },
        }
    }

    fn on_schedule(&mut self) {
        SCHED_LATCH_HISTOGRAM_VEC
            .get(self.tag)
            .observe(self.latch_timer.elapsed_secs());
    }
}

const ADJUST_DURATION: u64 = 1000; // 1000ms
const RATIO_PRECISION: f64 = 10000000.0;
const EMA_FACTOR: f64 = 0.1;
const LIMIT_UP_PERCENT: f64 = 0.04; // 4%
const LIMIT_DOWN_PERCENT: f64 = 0.02; // 2%
const MIN_THROTTLE_SPEED: f64 = 16.0 * 1024.0; // 16KB

enum Trend {
    Increasing,
    Decreasing,
    NoTrend,
    OnlyOne,
}

struct Smoother<const CAP: usize> {
    records: VecDeque<(u64, Instant)>,
    total: u64,
    square_total: u64,
}

impl<const CAP: usize> Smoother<CAP> {
    pub fn new() -> Self {
        Self {
            records: VecDeque::with_capacity(CAP),
            total: 0,
            square_total: 0,
        }
    }

    pub fn observe(&mut self, record: u64) {
        if self.records.len() == CAP {
            let v = self.records.pop_front().unwrap().0;
            self.total -= v;
            self.square_total -= v * v;
        }

        self.total += record;
        self.square_total += record * record;

        self.records.push_back((record, Instant::now_coarse()));
        self.clean_timeout();
    }

    fn clean_timeout(&mut self) {
        while self.records.len() > 1 {
            if self.records.front().unwrap().1.elapsed_secs() > 300.0 {
                let v = self.records.pop_front().unwrap().0;
                self.total -= v;
                self.square_total -= v * v;
            } else {
                break;
            }
        }
    }

    pub fn get_recent(&self) -> u64 {
        if self.records.len() == 0 {
            return 0;
        }
        self.records.back().unwrap().0
    }

    pub fn get_avg(&self) -> f64 {
        if self.records.len() == 0 {
            return 0.0;
        }
        self.total as f64 / self.records.len() as f64
    }

    pub fn get_max(&mut self) -> u64 {
        if self.records.len() == 0 {
            return 0;
        }
        self.records
            .make_contiguous()
            .iter()
            .max_by_key(|(k, _)| k)
            .unwrap()
            .0
    }

    pub fn get_percentile_95(&mut self) -> u64 {
        let mut v = self.records.make_contiguous().to_vec();
        v.sort_by_key(|k| k.0);
        v[((self.records.len() - 1) as f64 * 0.90) as usize].0
    }

    // pub fn get_variance(&self) -> f64 {
    //     if self.size == 0 {
    //         return 0.0;
    //     }

    //     (self.square_total as f64 / self.size as f64 - self.get_avg().powi(2)).sqrt()
    // }

    // fn factorial(&self, n: u64) -> u64 {
    //     let mut res = 1;
    //     for i in 1..=n {
    //         res *= i;
    //     }
    //     res
    // }

    // fn binom_fact(&self, n: u64, k: u64) -> u64 {
    //     self.factorial(n) / self.factorial(k) / self.factorial(n - k)
    // }

    // fn binom_cdf(&self, x: u64, n: u64, p: f64) -> f64 {
    //     let mut cd = 0.0;
    //     for i in 0..=x {
    //         cd += self.binom_fact(n, i) as f64 * p.powi(i as i32) * (1.0 - p).powi((n - i) as i32);
    //     }
    //     cd
    // }

    pub fn slope(&self) -> f64 {
        if self.records.len() <= 1 {
            return 0.0;
        }

        let half = self.records.len() / 2;
        let mut left = 0.0;
        let mut right = 0.0;
        for (i, r) in self.records.iter().enumerate() {
            if i + 1 < half {
                left += r.0 as f64;
            } else if i + 1 > half {
                right += r.0 as f64;
            } else {
                if self.records.len() % 2 == 0 {
                    left += r.0 as f64;
                } else {
                    continue;
                }
            }
        }
        let elapsed = duration_to_sec(
            self.records
                .back()
                .unwrap()
                .1
                .duration_since(self.records.front().unwrap().1),
        );
        (right - left) / half as f64 / (elapsed / 2.0)
    }

    pub fn trend(&self) -> Trend {
        if self.records.len() == 0 {
            return Trend::NoTrend;
        } else if self.records.len() == 1 {
            return Trend::OnlyOne;
        }

        if self.records.back().unwrap().0 > self.records.front().unwrap().0 + 2 {
            Trend::Increasing
        } else if self.records.back().unwrap().0 < self.records.front().unwrap().0 - 2 {
            Trend::Decreasing
        } else {
            Trend::NoTrend
        }

        // follow the way of Cox-Stuart
        // let half = if self.size % 2 == 0 {
        //     self.size / 2
        // } else {
        //     (self.size - 1) / 2
        // };

        // use std::cmp::Ordering;
        // let mut num_pos = 0;
        // let mut num_neg = 0;
        // for i in 0..half {
        //     match self.records[(self.idx + i + half) % CAP].cmp(&self.records[(self.idx + i) % CAP])
        //     {
        //         Ordering::Greater => num_pos += 1,
        //         Ordering::Less => num_neg += 1,
        //         Ordering::Equal => {}
        //     }
        // }

        // let num = num_neg + num_pos;
        // let k = std::cmp::min(num_neg, num_pos);
        // let p_value = 2.0 * self.binom_cdf(k, num, 0.5);

        // if num_pos > num_neg && p_value < 0.05 {
        //     Trend::Increasing
        // } else if num_pos < num_neg && p_value < 0.05 {
        //     Trend::Decreasing
        // } else {
        //     Trend::NoTrend
        // }
    }
}

use engine_rocks::Info;

struct CFFlowChecker {
    last_num_memtables: Smoother<60>,
    last_num_l0_files: u64,
    last_num_l0_files_from_flush: u64,
    long_term_num_l0_files: Smoother<60>,
    long_term_pending_bytes: Smoother<120>,

    last_flush_bytes_time: Instant,
    last_flush_bytes: u64,
    short_term_flush_flow: Smoother<10>,
    last_l0_bytes: u64,
    last_l0_bytes_time: Instant,
    short_term_l0_flow: Smoother<3>,

    memtable_debt: f64,
    init_speed: bool,
}

struct FlowChecker<E: Engine> {
    pending_compaction_bytes_soft_limit: u64,
    pending_compaction_bytes_hard_limit: u64,
    memtables_threshold: u64,
    l0_files_threshold: u64,

    cf_checkers: HashMap<String, CFFlowChecker>,
    throttle_cf: Option<String>,
    target_flow: Option<f64>,
    last_target_file: u64,
    factor: f64,

    start_control_time: Instant,
    discard_ratio: Arc<AtomicU64>,

    engine: E,
    recorder: Smoother<30>,
    limiter: Arc<Limiter>,
    last_record_time: Instant,
}

impl<E: Engine> FlowChecker<E> {
    pub fn new(
        config: &Config,
        engine: E,
        discard_ratio: Arc<AtomicU64>,
        limiter: Arc<Limiter>,
    ) -> Self {
        let mut cf_checkers = map![];

        for cf in engine.kv_engine().cf_names() {
            cf_checkers.insert(
                cf.to_owned(),
                CFFlowChecker {
                    last_num_memtables: Smoother::new(),
                    long_term_pending_bytes: Smoother::new(),
                    long_term_num_l0_files: Smoother::new(),
                    last_num_l0_files: 0,
                    last_num_l0_files_from_flush: 0,
                    last_flush_bytes: 0,
                    last_flush_bytes_time: Instant::now_coarse(),
                    short_term_flush_flow: Smoother::new(),
                    last_l0_bytes: 0,
                    last_l0_bytes_time: Instant::now_coarse(),
                    short_term_l0_flow: Smoother::new(),
                    memtable_debt: 0.0,
                    init_speed: false,
                },
            );
        }
        Self {
            pending_compaction_bytes_soft_limit: config.pending_compaction_bytes_soft_limit,
            pending_compaction_bytes_hard_limit: config.pending_compaction_bytes_hard_limit,
            memtables_threshold: config.memtables_threshold,
            l0_files_threshold: config.l0_files_threshold,
            engine,
            factor: EMA_FACTOR,
            discard_ratio,
            start_control_time: Instant::now_coarse(),
            limiter,
            recorder: Smoother::new(),
            cf_checkers,
            throttle_cf: None,
            target_flow: None,
            last_target_file: 0,
            last_record_time: Instant::now_coarse(),
        }
    }

    fn start(self, rx: Receiver<bool>, l0_completed_receiver: Receiver<Info>) -> JoinHandle<()> {
        Builder::new()
            .name(thd_name!("flow-checker"))
            .spawn(move || {
                tikv_alloc::add_thread_memory_accessor();
                let mut checker = self;
                while rx.try_recv().is_err() {
                    match l0_completed_receiver.recv_timeout(Duration::from_millis(ADJUST_DURATION))
                    {
                        Ok(Info::L0(cf, l0_bytes)) => {
                            checker.adjust_memtables(&cf);
                            checker.check_long_term_l0_files(cf, l0_bytes)
                        }
                        Ok(Info::Flush(cf, flush_bytes)) => checker.check_l0_flow(cf, flush_bytes),
                        Ok(Info::Compaction(cf)) => checker.adjust_pending_compaction_bytes(cf),
                        Err(RecvTimeoutError::Timeout) => checker.update_statistics(),
                        Err(e) => {
                            error!("failed to receive compaction info {:?}", e);
                        }
                    }
                }
                tikv_alloc::remove_thread_memory_accessor();
            })
            .unwrap()
    }

    fn update_statistics(&mut self) {
        if let Some(target_flow) = self.target_flow {
            SCHED_TARGET_FLOW_GAUGE.set(target_flow as i64);
        } else {
            SCHED_TARGET_FLOW_GAUGE.set(0);
        }
        let rate =
            self.limiter.total_bytes_consumed() as f64 / self.last_record_time.elapsed_secs();
        if self.limiter.total_bytes_consumed() != 0 {
            self.recorder.observe(rate as u64);
        }
        SCHED_WRITE_FLOW_GAUGE.set(rate as i64);
        self.last_record_time = Instant::now_coarse();
        self.limiter.reset_statistics();
    }

    fn adjust_pending_compaction_bytes(&mut self, cf: String) {
        let num = (self
            .engine
            .kv_engine()
            .get_cf_pending_compaction_bytes(&cf)
            .unwrap_or(None)
            .unwrap_or(0) as f64)
            .log2();
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker
            .long_term_pending_bytes
            .observe((num * RATIO_PRECISION) as u64);
        SCHED_PENDING_BYTES_GAUGE
            .with_label_values(&[&cf])
            .set(checker.long_term_pending_bytes.get_avg() as i64);
        let pending_compaction_bytes = checker.long_term_pending_bytes.get_avg() / RATIO_PRECISION;
        drop(checker);

        for (_, checker) in &self.cf_checkers {
            if num < (checker.long_term_pending_bytes.get_recent() as f64) / RATIO_PRECISION {
                return;
            }
        }
        let hard = (self.pending_compaction_bytes_hard_limit as f64).log2();
        let soft = (self.pending_compaction_bytes_soft_limit as f64).log2();

        let ratio = if pending_compaction_bytes < soft {
            0
        } else {
            let checker = self.cf_checkers.get_mut(&cf).unwrap();
            // let x = 10.0
            //     - 10.0
            //         / ((self.pending_compaction_bytes_hard_limit as f64).log2()
            //             - (self.pending_compaction_bytes_soft_limit as f64).log2())
            //         * (pending_compaction_bytes - (self.pending_compaction_bytes_soft_limit as f64).log2());
            // let new_ratio = 1.0 / (1.0 + x.exp());
            let kp = (pending_compaction_bytes - soft) / (hard - soft);
            let mut kd = -10.0 * checker.long_term_pending_bytes.slope();

            SCHED_KD_GAUGE.set(kd as i64);
            SCHED_KP_GAUGE.set((kp * RATIO_PRECISION) as i64);

            if kd > 0.1 {
                kd = 0.1;
            } else if kd < -0.1 {
                kd = -0.1;
            }

            let new_ratio = kp + kd;
            let old_ratio = self.discard_ratio.load(Ordering::Relaxed);
            (if old_ratio != 0 {
                self.factor * (old_ratio as f64 / RATIO_PRECISION) + (1.0 - self.factor) * new_ratio
            } else {
                self.start_control_time = Instant::now_coarse();
                new_ratio
            } * RATIO_PRECISION) as u64
        };
        SCHED_DISCARD_RATIO_GAUGE.set(ratio as i64);
        self.discard_ratio.store(ratio, Ordering::Relaxed);
    }

    fn adjust_memtables(&mut self, cf: &String) {
        let num_memtables = self
            .engine
            .kv_engine()
            .get_cf_num_memtables(cf)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(cf).unwrap();
        SCHED_MEMTABLE_GAUGE
            .with_label_values(&[cf])
            .set(num_memtables as i64);
        let prev = checker.last_num_memtables.get_recent();
        checker.last_num_memtables.observe(num_memtables);
        drop(checker);

        for (_, c) in &self.cf_checkers {
            if num_memtables < c.last_num_memtables.get_recent() {
                return;
            }
        }

        let checker = self.cf_checkers.get_mut(cf).unwrap();
        let is_throttled = self.limiter.speed_limit() != INFINITY;
        let should_throttle =
            checker.last_num_memtables.get_avg() > self.memtables_threshold as f64;
        let throttle = if !is_throttled {
            if should_throttle {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[cf, "memtable_init"])
                    .inc();
                checker.init_speed = true;
                self.recorder.get_percentile_95() as f64
            } else {
                INFINITY
            }
        } else if !should_throttle
            || checker.last_num_memtables.get_recent() < self.memtables_threshold
        {
            // should not throttle_memtable
            checker.memtable_debt = 0.0;
            if checker.init_speed {
                INFINITY
            } else {
                self.limiter.speed_limit() + checker.memtable_debt
            }
        } else {
            // should throttle
            let diff = if checker.last_num_memtables.get_recent() > prev {
                checker.memtable_debt += 1.0;
                -1.0
            } else if checker.last_num_memtables.get_recent() < prev {
                checker.memtable_debt -= 1.0;
                1.0
            } else {
                // keep, do nothing
                0.0
            };
            self.limiter.speed_limit() + diff
        };

        self.update_speed_limit(throttle);
    }

    fn check_long_term_l0_files(&mut self, cf: String, l0_bytes: u64) {
        let num_l0_files = self
            .engine
            .kv_engine()
            .get_cf_num_files_at_level(&cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);
        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker.last_l0_bytes += l0_bytes;
        checker.long_term_num_l0_files.observe(num_l0_files);
        checker.last_num_l0_files = num_l0_files;
        SCHED_L0_GAUGE
            .with_label_values(&[&cf])
            .set(num_l0_files as i64);
        SCHED_L0_AVG_GAUGE
            .with_label_values(&[&cf])
            .set(checker.long_term_num_l0_files.get_avg() as i64);
        SCHED_THROTTLE_ACTION_COUNTER
            .with_label_values(&[&cf, "tick"])
            .inc();
        drop(checker);

        if let Some(throttle_cf) = self.throttle_cf.as_ref() {
            if &cf != throttle_cf {
                if num_l0_files > self.cf_checkers[throttle_cf].last_num_l0_files + 4 {
                    self.throttle_cf = Some(cf.clone());
                } else {
                    return;
                }
            }
        }

        self.adjust_l0_files(cf);
    }

    fn adjust_l0_files(&mut self, cf: String) {
        let checker = self.cf_checkers.get_mut(&cf).unwrap();

        let is_throttled = self.limiter.speed_limit() != INFINITY;
        let should_throttle = checker.last_num_l0_files > self.l0_files_threshold;

        let throttle = if !is_throttled && should_throttle {
            SCHED_THROTTLE_ACTION_COUNTER
                .with_label_values(&[&cf, "init"])
                .inc();
            self.throttle_cf = Some(cf.clone());
            self.recorder.get_percentile_95() as f64
        } else if is_throttled && should_throttle {
            match checker.long_term_num_l0_files.trend() {
                Trend::OnlyOne => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "down2"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
                }
                Trend::Increasing => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "down"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
                }
                Trend::Decreasing => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep_decr"])
                        .inc();
                    self.limiter.speed_limit()
                }
                Trend::NoTrend => {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep"])
                        .inc();
                    self.limiter.speed_limit()
                }
            }
        } else if is_throttled && !should_throttle {
            if checker.long_term_num_l0_files.get_avg() >= self.l0_files_threshold as f64 * 0.5 {
                SCHED_THROTTLE_ACTION_COUNTER
                    .with_label_values(&[&cf, "keep2"])
                    .inc();
                self.limiter.speed_limit()
            } else {
                if checker.long_term_num_l0_files.get_recent() as f64
                    >= self.l0_files_threshold as f64 * 0.5
                {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep3"])
                        .inc();
                    self.limiter.speed_limit()
                } else if checker.last_num_l0_files_from_flush >= self.l0_files_threshold {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "keep4"])
                        .inc();
                    self.limiter.speed_limit()
                } else {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "up"])
                        .inc();
                    self.limiter.speed_limit() * (1.0 + LIMIT_UP_PERCENT)
                }
            }
        } else {
            INFINITY
        };

        self.update_speed_limit(throttle)
    }

    fn update_speed_limit(&mut self, mut throttle: f64) {
        if throttle < MIN_THROTTLE_SPEED {
            throttle = MIN_THROTTLE_SPEED;
        }
        if throttle > 1.5 * self.recorder.get_max() as f64 {
            self.throttle_cf = None;
            self.target_flow = None;
            throttle = INFINITY;
        }
        SCHED_THROTTLE_FLOW_GAUGE.set(if throttle == INFINITY {
            0
        } else {
            throttle as i64
        });
        self.limiter.set_speed_limit(throttle)
    }

    fn check_l0_flow(&mut self, cf: String, flush_bytes: u64) {
        let num_l0_files = self
            .engine
            .kv_engine()
            .get_cf_num_files_at_level(&cf, 0)
            .unwrap_or(None)
            .unwrap_or(0);

        let checker = self.cf_checkers.get_mut(&cf).unwrap();
        checker.last_flush_bytes += flush_bytes;
        // no need to add it to long_term_num_l0_files, we want to exclude the
        checker.last_num_l0_files = num_l0_files;
        checker.last_num_l0_files_from_flush = num_l0_files;
        SCHED_FLUSH_L0_GAUGE
            .with_label_values(&[&cf])
            .set(num_l0_files as i64);

        if checker.last_flush_bytes_time.elapsed_secs() > 1.0 {
            let flush_flow =
                checker.last_flush_bytes as f64 / checker.last_flush_bytes_time.elapsed_secs();
            checker.short_term_flush_flow.observe(flush_flow as u64);
            SCHED_FLUSH_FLOW_GAUGE
                .with_label_values(&[&cf])
                .set(checker.short_term_flush_flow.get_avg() as i64);

            if checker.last_l0_bytes != 0 {
                let l0_flow =
                    checker.last_l0_bytes as f64 / checker.last_l0_bytes_time.elapsed_secs();
                checker.last_l0_bytes_time = Instant::now_coarse();
                checker.short_term_l0_flow.observe(l0_flow as u64);
                SCHED_L0_FLOW_GAUGE
                    .with_label_values(&[&cf])
                    .set(checker.short_term_l0_flow.get_avg() as i64);
            }

            checker.last_flush_bytes_time = Instant::now_coarse();
            checker.last_l0_bytes = 0;
            checker.last_flush_bytes = 0;

            if let Some(throttle_cf) = self.throttle_cf.as_ref() {
                if &cf != throttle_cf {
                    if num_l0_files > self.cf_checkers[throttle_cf].last_num_l0_files + 2 {
                        self.throttle_cf = Some(cf.clone());
                    } else {
                        return;
                    }
                }
            }

            if num_l0_files > self.l0_files_threshold {
                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_l0_flow.get_avg() > target_flow {
                        self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "refresh2_flow"])
                            .inc();
                    }
                }

                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_flush_flow.get_avg() > target_flow {
                        self.down_flow(cf);
                    } else if num_l0_files > self.last_target_file + 3 {
                        self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                        self.last_target_file = num_l0_files;
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "refresh1_flow"])
                            .inc();
                        self.down_flow(cf);
                    } else {
                        SCHED_THROTTLE_ACTION_COUNTER
                            .with_label_values(&[&cf, "keep_flow"])
                            .inc();
                    }
                } else if self.cf_checkers[&cf].short_term_flush_flow.get_avg()
                    > self.cf_checkers[&cf].short_term_l0_flow.get_avg()
                {
                    self.target_flow = Some(self.cf_checkers[&cf].short_term_l0_flow.get_avg());
                    self.last_target_file = num_l0_files;
                    self.down_flow(cf);
                } else {
                    SCHED_THROTTLE_ACTION_COUNTER
                        .with_label_values(&[&cf, "no_need_down_flow"])
                        .inc();
                }
            } else {
                if let Some(target_flow) = self.target_flow {
                    if self.cf_checkers[&cf].short_term_l0_flow.get_avg() > target_flow {
                        self.target_flow = None;
                    }
                }
            }
        }
    }

    fn down_flow(&mut self, cf: String) {
        SCHED_THROTTLE_ACTION_COUNTER
            .with_label_values(&[&cf, "down_flow"])
            .inc();
        let throttle = if self.limiter.speed_limit() == INFINITY {
            self.throttle_cf = Some(cf.clone());
            self.recorder.get_percentile_95() as f64
        } else {
            self.limiter.speed_limit() * (1.0 - LIMIT_DOWN_PERCENT)
        };
        self.update_speed_limit(throttle)
    }
}

struct FlowController {
    discard_ratio: Arc<AtomicU64>,
    limiter: Arc<Limiter>,
    tx: Sender<bool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for FlowController {
    fn drop(&mut self) {
        let h = self.handle.take();
        if h.is_none() {
            return;
        }

        if let Err(e) = self.tx.send(true) {
            error!("send quit message for time monitor worker failed"; "err" => ?e);
            return;
        }

        if let Err(e) = h.unwrap().join() {
            error!("join time monitor worker failed"; "err" => ?e);
            return;
        }
    }
}

impl FlowController {
    pub fn new<E: Engine>(
        config: &Config,
        engine: E,
        l0_completed_receiver: Option<Receiver<Info>>,
    ) -> Self {
        let limiter = Arc::new(Limiter::new(INFINITY));
        let discard_ratio = Arc::new(AtomicU64::new(0));
        let checker = FlowChecker::new(config, engine, discard_ratio.clone(), limiter.clone());
        let (tx, rx) = mpsc::channel();
        let handle = if config.disable_write_stall {
            Some(checker.start(rx, l0_completed_receiver.unwrap()))
        } else {
            None
        };
        Self {
            discard_ratio,
            limiter,
            tx,
            handle,
        }
    }

    pub fn should_drop(&self) -> bool {
        let ratio = self.discard_ratio.load(Ordering::Relaxed) as f64 / RATIO_PRECISION;
        let mut rng = rand::thread_rng();
        rng.gen::<f64>() < ratio
    }

    fn consume(&self, bytes: usize) -> Consume {
        self.limiter.consume(bytes)
    }
}

struct SchedulerInner<L: LockManager> {
    // slot_id -> { cid -> `TaskContext` } in the slot.
    task_slots: Vec<CachePadded<Mutex<HashMap<u64, TaskContext>>>>,

    // cmd id generator
    id_alloc: CachePadded<AtomicU64>,

    // write concurrency control
    latches: Latches,

    sched_pending_write_threshold: usize,

    // worker pool
    worker_pool: SchedPool,

    // high priority commands and system commands will be delivered to this pool
    high_priority_pool: SchedPool,

    // used to control write flow
    running_write_bytes: CachePadded<AtomicUsize>,

    flow_controller: FlowController,

    lock_mgr: L,

    concurrency_manager: ConcurrencyManager,

    pipelined_pessimistic_lock: Arc<AtomicBool>,

    enable_async_apply_prewrite: bool,
}

#[inline]
fn id_index(cid: u64) -> usize {
    cid as usize % TASKS_SLOTS_NUM
}

impl<L: LockManager> SchedulerInner<L> {
    /// Generates the next command ID.
    #[inline]
    fn gen_id(&self) -> u64 {
        let id = self.id_alloc.fetch_add(1, Ordering::Relaxed);
        id + 1
    }

    #[inline]
    fn get_task_slot(&self, cid: u64) -> MutexGuard<HashMap<u64, TaskContext>> {
        self.task_slots[id_index(cid)].lock()
    }

    fn new_task_context(&self, task: Task, callback: StorageCallback) -> TaskContext {
        let tctx = TaskContext::new(task, &self.latches, callback);
        let running_write_bytes = self
            .running_write_bytes
            .fetch_add(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes + tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.inc();
        tctx
    }

    fn dequeue_task_context(&self, cid: u64) -> TaskContext {
        let tctx = self.get_task_slot(cid).remove(&cid).unwrap();

        let running_write_bytes = self
            .running_write_bytes
            .fetch_sub(tctx.write_bytes, Ordering::AcqRel) as i64;
        SCHED_WRITING_BYTES_GAUGE.set(running_write_bytes - tctx.write_bytes as i64);
        SCHED_CONTEX_GAUGE.dec();

        tctx
    }

    fn take_task_cb_and_pr(&self, cid: u64) -> (Option<StorageCallback>, Option<ProcessResult>) {
        self.get_task_slot(cid)
            .get_mut(&cid)
            .map(|tctx| (tctx.cb.take(), tctx.pr.take()))
            .unwrap_or((None, None))
    }

    fn store_pr(&self, cid: u64, pr: ProcessResult) {
        self.get_task_slot(cid).get_mut(&cid).unwrap().pr = Some(pr);
    }

    fn too_busy(&self) -> bool {
        fail_point!("txn_scheduler_busy", |_| true);
        self.running_write_bytes.load(Ordering::Acquire) >= self.sched_pending_write_threshold
            || self.flow_controller.should_drop()
    }

    /// Tries to acquire all the required latches for a command.
    ///
    /// Returns the `Task` if successful; returns `None` otherwise.
    fn acquire_lock(&self, cid: u64) -> Option<Task> {
        let mut task_slot = self.get_task_slot(cid);
        let tctx = task_slot.get_mut(&cid).unwrap();
        if self.latches.acquire(&mut tctx.lock, cid) {
            tctx.on_schedule();
            return tctx.task.take();
        }
        None
    }
}

/// Scheduler which schedules the execution of `storage::Command`s.
#[derive(Clone)]
pub struct Scheduler<E: Engine, L: LockManager> {
    // `engine` is `None` means currently the program is in scheduler worker threads.
    engine: Option<E>,
    inner: Arc<SchedulerInner<L>>,
    control_mutex: Arc<tokio::sync::Mutex<bool>>,
}

unsafe impl<E: Engine, L: LockManager> Send for Scheduler<E, L> {}

impl<E: Engine, L: LockManager> Scheduler<E, L> {
    /// Creates a scheduler.
    pub(in crate::storage) fn new(
        engine: E,
        lock_mgr: L,

        concurrency_manager: ConcurrencyManager,
        config: &Config,
        pipelined_pessimistic_lock: Arc<AtomicBool>,
        l0_completed_receiver: Option<Receiver<Info>>,
    ) -> Self {
        let t = Instant::now_coarse();
        let mut task_slots = Vec::with_capacity(TASKS_SLOTS_NUM);
        for _ in 0..TASKS_SLOTS_NUM {
            task_slots.push(Mutex::new(Default::default()).into());
        }

        let inner = Arc::new(SchedulerInner {
            task_slots,
            id_alloc: AtomicU64::new(0).into(),
            latches: Latches::new(config.scheduler_concurrency),
            running_write_bytes: AtomicUsize::new(0).into(),
            sched_pending_write_threshold: config.scheduler_pending_write_threshold.0 as usize,
            worker_pool: SchedPool::new(
                engine.clone(),
                config.scheduler_worker_pool_size,
                "sched-worker-pool",
            ),
            high_priority_pool: SchedPool::new(
                engine.clone(),
                std::cmp::max(1, config.scheduler_worker_pool_size / 2),
                "sched-high-pri-pool",
            ),
            lock_mgr,
            concurrency_manager,
            pipelined_pessimistic_lock,
            enable_async_apply_prewrite: config.enable_async_apply_prewrite,
            flow_controller: FlowController::new(config, engine.clone(), l0_completed_receiver),
        });

        slow_log!(t.elapsed(), "initialized the transaction scheduler");
        Scheduler {
            engine: Some(engine),
            inner,
            control_mutex: Arc::new(tokio::sync::Mutex::new(false)),
        }
    }

    pub(in crate::storage) fn run_cmd(&self, cmd: Command, callback: StorageCallback) {
        // write flow control
        if cmd.need_flow_control() && self.inner.too_busy() {
            SCHED_TOO_BUSY_COUNTER_VEC.get(cmd.tag()).inc();
            callback.execute(ProcessResult::Failed {
                err: StorageError::from(StorageErrorInner::SchedTooBusy),
            });
            return;
        }
        self.schedule_command(cmd, callback);
    }

    /// Releases all the latches held by a command.
    fn release_lock(&self, lock: &Lock, cid: u64) {
        let wakeup_list = self.inner.latches.release(lock, cid);
        for wcid in wakeup_list {
            self.try_to_wake_up(wcid);
        }
    }

    fn schedule_command(&self, cmd: Command, callback: StorageCallback) {
        let cid = self.inner.gen_id();
        debug!("received new command"; "cid" => cid, "cmd" => ?cmd);

        let tag = cmd.tag();
        let priority_tag = get_priority_tag(cmd.priority());
        SCHED_STAGE_COUNTER_VEC.get(tag).new.inc();
        SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC
            .get(priority_tag)
            .inc();

        let mut task_slot = self.inner.get_task_slot(cid);
        let tctx = task_slot
            .entry(cid)
            .or_insert_with(|| self.inner.new_task_context(Task::new(cid, cmd), callback));
        if self.inner.latches.acquire(&mut tctx.lock, cid) {
            fail_point!("txn_scheduler_acquire_success");
            tctx.on_schedule();
            let task = tctx.task.take().unwrap();
            drop(task_slot);
            self.execute(task);
            return;
        }
        fail_point!("txn_scheduler_acquire_fail");
    }

    /// Tries to acquire all the necessary latches. If all the necessary latches are acquired,
    /// the method initiates a get snapshot operation for further processing.
    fn try_to_wake_up(&self, cid: u64) {
        if let Some(task) = self.inner.acquire_lock(cid) {
            fail_point!("txn_scheduler_try_to_wake_up");
            self.execute(task);
        }
    }

    fn get_sched_pool(&self, priority: CommandPri) -> &SchedPool {
        if priority == CommandPri::High {
            &self.inner.high_priority_pool
        } else {
            &self.inner.worker_pool
        }
    }

    /// Initiates an async operation to get a snapshot from the storage engine, then execute the
    /// task in the sched pool.
    fn execute(&self, mut task: Task) {
        let cid = task.cid;
        let tag = task.cmd.tag();
        let ctx = task.cmd.ctx().clone();
        let sched = self.clone();

        let cb = must_call(
            move |(cb_ctx, snapshot)| {
                debug!(
                    "receive snapshot finish msg";
                    "cid" => task.cid, "cb_ctx" => ?cb_ctx
                );

                match snapshot {
                    Ok(snapshot) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_ok.inc();

                        if let Some(term) = cb_ctx.term {
                            task.cmd.ctx_mut().set_term(term);
                        }
                        task.extra_op = cb_ctx.txn_extra_op;

                        debug!(
                            "process cmd with snapshot";
                            "cid" => task.cid, "cb_ctx" => ?cb_ctx
                        );
                        sched.process_by_worker(snapshot, task);
                    }
                    Err(err) => {
                        SCHED_STAGE_COUNTER_VEC.get(tag).snapshot_err.inc();

                        info!("get snapshot failed"; "cid" => task.cid, "err" => ?err);
                        sched
                            .get_sched_pool(task.cmd.priority())
                            .clone()
                            .pool
                            .spawn(async move {
                                sched.finish_with_err(task.cid, Error::from(err));
                            })
                            .unwrap();
                    }
                }
            },
            drop_snapshot_callback::<E>,
        );

        let f = |engine: &E| {
            let snap_ctx = SnapContext {
                pb_ctx: &ctx,
                ..Default::default()
            };
            if let Err(e) = engine.async_snapshot(snap_ctx, cb) {
                SCHED_STAGE_COUNTER_VEC.get(tag).async_snapshot_err.inc();

                info!("engine async_snapshot failed"; "err" => ?e);
                self.finish_with_err(cid, e.into());
            } else {
                SCHED_STAGE_COUNTER_VEC.get(tag).snapshot.inc();
            }
        };

        if let Some(engine) = self.engine.as_ref() {
            f(engine)
        } else {
            // The program is currently in scheduler worker threads.
            // Safety: `self.inner.worker_pool` should ensure that a TLS engine exists.
            unsafe { with_tls_engine(f) }
        }
    }

    /// Calls the callback with an error.
    fn finish_with_err(&self, cid: u64, err: Error) {
        debug!("write command finished with error"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);

        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).error.inc();

        let pr = ProcessResult::Failed {
            err: StorageError::from(err),
        };
        tctx.cb.unwrap().execute(pr);

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of read.
    ///
    /// If a next command is present, continues to execute; otherwise, delivers the result to the
    /// callback.
    fn on_read_finished(&self, cid: u64, pr: ProcessResult, tag: metrics::CommandKind) {
        SCHED_STAGE_COUNTER_VEC.get(tag).read_finish.inc();

        debug!("read command finished"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        if let ProcessResult::NextCommand { cmd } = pr {
            SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
            self.schedule_command(cmd, tctx.cb.unwrap());
        } else {
            tctx.cb.unwrap().execute(pr);
        }

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the success of write.
    fn on_write_finished(
        &self,
        cid: u64,
        pr: Option<ProcessResult>,
        result: EngineResult<()>,
        lock_guards: Vec<KeyHandleGuard>,
        pipelined: bool,
        async_apply_prewrite: bool,
        tag: metrics::CommandKind,
    ) {
        // TODO: Does async apply prewrite worth a special metric here?
        if pipelined {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .pipelined_write_finish
                .inc();
        } else if async_apply_prewrite {
            SCHED_STAGE_COUNTER_VEC
                .get(tag)
                .async_apply_prewrite_finish
                .inc();
        } else {
            SCHED_STAGE_COUNTER_VEC.get(tag).write_finish.inc();
        }

        debug!("write command finished";
            "cid" => cid, "pipelined" => pipelined, "async_apply_prewrite" => async_apply_prewrite);
        drop(lock_guards);
        let tctx = self.inner.dequeue_task_context(cid);

        // If pipelined pessimistic lock or async apply prewrite takes effect, it's not guaranteed
        // that the proposed or committed callback is surely invoked, which takes and invokes
        // `tctx.cb(tctx.pr)`.
        if let Some(cb) = tctx.cb {
            let pr = match result {
                Ok(()) => pr.or(tctx.pr).unwrap(),
                Err(e) => ProcessResult::Failed {
                    err: StorageError::from(e),
                },
            };
            if let ProcessResult::NextCommand { cmd } = pr {
                SCHED_STAGE_COUNTER_VEC.get(tag).next_cmd.inc();
                self.schedule_command(cmd, cb);
            } else {
                cb.execute(pr);
            }
        } else {
            assert!(pipelined || async_apply_prewrite);
        }

        self.release_lock(&tctx.lock, cid);
    }

    /// Event handler for the request of waiting for lock
    fn on_wait_for_lock(
        &self,
        cid: u64,
        start_ts: TimeStamp,
        pr: ProcessResult,
        lock: lock_manager::Lock,
        is_first_lock: bool,
        wait_timeout: Option<WaitTimeout>,
    ) {
        debug!("command waits for lock released"; "cid" => cid);
        let tctx = self.inner.dequeue_task_context(cid);
        SCHED_STAGE_COUNTER_VEC.get(tctx.tag).lock_wait.inc();
        self.inner.lock_mgr.wait_for(
            start_ts,
            tctx.cb.unwrap(),
            pr,
            lock,
            is_first_lock,
            wait_timeout,
        );
        self.release_lock(&tctx.lock, cid);
    }

    fn early_response(
        cid: u64,
        cb: StorageCallback,
        pr: ProcessResult,
        tag: metrics::CommandKind,
        stage: metrics::CommandStageKind,
    ) {
        debug!("early return response"; "cid" => cid);
        SCHED_STAGE_COUNTER_VEC.get(tag).get(stage).inc();
        cb.execute(pr);
        // It won't release locks here until write finished.
    }

    /// Delivers a command to a worker thread for processing.
    fn process_by_worker(self, snapshot: E::Snap, task: Task) {
        let tag = task.cmd.tag();

        self.get_sched_pool(task.cmd.priority())
            .clone()
            .pool
            .spawn(async move {
                fail_point!("scheduler_async_snapshot_finish");
                SCHED_STAGE_COUNTER_VEC.get(tag).process.inc();

                let read_duration = Instant::now_coarse();

                let region_id = task.cmd.ctx().get_region_id();
                let ts = task.cmd.ts();
                let timer = Instant::now_coarse();
                let mut statistics = Statistics::default();

                if task.cmd.readonly() {
                    self.process_read(snapshot, task, &mut statistics);
                } else {
                    self.process_write(snapshot, task, &mut statistics).await;
                };
                tls_collect_scan_details(tag.get_str(), &statistics);
                slow_log!(
                    timer.elapsed(),
                    "[region {}] scheduler handle command: {}, ts: {}",
                    region_id,
                    tag,
                    ts
                );

                tls_collect_read_duration(tag.get_str(), read_duration.elapsed());
            })
            .unwrap();
    }

    /// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
    /// `Scheduler`.
    fn process_read(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_read");
        debug!("process read cmd in worker pool"; "cid" => task.cid);

        let tag = task.cmd.tag();

        let pr = task
            .cmd
            .process_read(snapshot, statistics)
            .unwrap_or_else(|e| ProcessResult::Failed { err: e.into() });
        self.on_read_finished(task.cid, pr, tag);
    }

    /// Processes a write command within a worker thread, then posts either a `WriteFinished`
    /// message if successful or a `FinishedWithErr` message back to the `Scheduler`.
    async fn process_write(self, snapshot: E::Snap, task: Task, statistics: &mut Statistics) {
        fail_point!("txn_before_process_write");
        let tag = task.cmd.tag();
        let cid = task.cid;
        let priority = task.cmd.priority();
        let ts = task.cmd.ts();
        let scheduler = self.clone();
        let pipelined_pessimistic_lock = self
            .inner
            .pipelined_pessimistic_lock
            .load(Ordering::Relaxed);
        let pipelined = pipelined_pessimistic_lock && task.cmd.can_be_pipelined();

        let res = {
            let context = WriteContext {
                lock_mgr: &self.inner.lock_mgr,
                concurrency_manager: self.inner.concurrency_manager.clone(),
                extra_op: task.extra_op,
                statistics,
                async_apply_prewrite: self.inner.enable_async_apply_prewrite,
            };
            task.cmd.process_write(snapshot, context)
        };
        match res {
            // Initiates an async write operation on the storage engine, there'll be a `WriteFinished`
            // message when it finishes.
            Ok(WriteResult {
                ctx,
                to_be_write,
                rows,
                pr,
                lock_info,
                lock_guards,
                response_policy,
            }) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).write.inc();

                if let Some((lock, is_first_lock, wait_timeout)) = lock_info {
                    scheduler.on_wait_for_lock(cid, ts, pr, lock, is_first_lock, wait_timeout);
                } else if to_be_write.modifies.is_empty() {
                    scheduler.on_write_finished(
                        cid,
                        Some(pr),
                        Ok(()),
                        lock_guards,
                        false,
                        false,
                        tag,
                    );
                } else {
                    let mut pr = Some(pr);
                    let mut is_async_apply_prewrite = false;

                    let (proposed_cb, committed_cb): (Option<ExtCallback>, Option<ExtCallback>) =
                        match response_policy {
                            ResponsePolicy::OnApplied => (None, None),
                            ResponsePolicy::OnCommitted => {
                                self.inner.store_pr(cid, pr.take().unwrap());
                                let sched = scheduler.clone();
                                // Currently, the only case that response is returned after finishing
                                // commit is async applying prewrites for async commit transactions.
                                // The committed callback is not guaranteed to be invoked. So store
                                // the `pr` to the tctx instead of capturing it to the closure.
                                let committed_cb = Box::new(move || {
                                    fail_point!("before_async_apply_prewrite_finish", |_| {});
                                    let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                                    Self::early_response(
                                        cid,
                                        cb.unwrap(),
                                        pr.unwrap(),
                                        tag,
                                        metrics::CommandStageKind::async_apply_prewrite,
                                    );
                                });
                                is_async_apply_prewrite = true;
                                (None, Some(committed_cb))
                            }
                            ResponsePolicy::OnProposed => {
                                if pipelined {
                                    // The normal write process is respond to clients and release
                                    // latches after async write finished. If pipelined pessimistic
                                    // locking is enabled, the process becomes parallel and there are
                                    // two msgs for one command:
                                    //   1. Msg::PipelinedWrite: respond to clients
                                    //   2. Msg::WriteFinished: deque context and release latches
                                    // The proposed callback is not guaranteed to be invoked. So store
                                    // the `pr` to the tctx instead of capturing it to the closure.
                                    self.inner.store_pr(cid, pr.take().unwrap());
                                    let sched = scheduler.clone();
                                    // Currently, the only case that response is returned after finishing
                                    // proposed phase is pipelined pessimistic lock.
                                    // TODO: Unify the code structure of pipelined pessimistic lock and
                                    // async apply prewrite.
                                    let proposed_cb = Box::new(move || {
                                        fail_point!("before_pipelined_write_finish", |_| {});
                                        let (cb, pr) = sched.inner.take_task_cb_and_pr(cid);
                                        Self::early_response(
                                            cid,
                                            cb.unwrap(),
                                            pr.unwrap(),
                                            tag,
                                            metrics::CommandStageKind::pipelined_write,
                                        );
                                    });
                                    (Some(proposed_cb), None)
                                } else {
                                    (None, None)
                                }
                            }
                        };

                    let sched = scheduler.clone();
                    let sched_pool = scheduler.get_sched_pool(priority).pool.clone();
                    // The callback to receive async results of write prepare from the storage engine.
                    let engine_cb = Box::new(move |(_, result)| {
                        sched_pool
                            .spawn(async move {
                                fail_point!("scheduler_async_write_finish");

                                sched.on_write_finished(
                                    cid,
                                    pr,
                                    result,
                                    lock_guards,
                                    pipelined,
                                    is_async_apply_prewrite,
                                    tag,
                                );
                                KV_COMMAND_KEYWRITE_HISTOGRAM_VEC
                                    .get(tag)
                                    .observe(rows as f64);
                            })
                            .unwrap()
                    });

                    {
                        let _guard = self.control_mutex.lock().await;
                        let delay = self.inner.flow_controller.consume(to_be_write.size());
                        delay.await;
                    }
                    // Safety: `self.sched_pool` ensures a TLS engine exists.
                    unsafe {
                        with_tls_engine(|engine: &E| {
                            if let Err(e) = engine.async_write_ext(
                                &ctx,
                                to_be_write,
                                engine_cb,
                                proposed_cb,
                                committed_cb,
                            ) {
                                SCHED_STAGE_COUNTER_VEC.get(tag).async_write_err.inc();

                                info!("engine async_write failed"; "cid" => cid, "err" => ?e);
                                scheduler.finish_with_err(cid, e.into());
                            }
                        })
                    }
                }
            }
            // Write prepare failure typically means conflicting transactions are detected. Delivers the
            // error to the callback, and releases the latches.
            Err(err) => {
                SCHED_STAGE_COUNTER_VEC.get(tag).prepare_write_err.inc();
                debug!("write command failed at prewrite"; "cid" => cid, "err" => ?err);
                scheduler.finish_with_err(cid, err);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mvcc::{self, Mutation};
    use crate::storage::txn::{commands, latch::*};
    use kvproto::kvrpcpb::Context;
    use txn_types::Key;

    #[test]
    fn test_smoother() {
        let mut smoother = Smoother::<5>::new();
        smoother.observe(1);
        smoother.observe(6);
        smoother.observe(2);
        smoother.observe(3);
        smoother.observe(4);
        smoother.observe(5);
        smoother.observe(0);

        assert_eq!(smoother.get_avg(), 2.8);
        assert_eq!(smoother.get_recent(), 0);
        assert_eq!(smoother.get_max(), 5);
        assert_eq!(smoother.get_percentile_95(), 4);
    }

    #[test]
    fn test_command_latches() {
        let mut temp_map = HashMap::default();
        temp_map.insert(10.into(), 20.into());
        let readonly_cmds: Vec<Command> = vec![
            commands::ResolveLockReadPhase::new(temp_map.clone(), None, Context::default()).into(),
            commands::MvccByKey::new(Key::from_raw(b"k"), Context::default()).into(),
            commands::MvccByStartTs::new(25.into(), Context::default()).into(),
        ];
        let write_cmds: Vec<Command> = vec![
            commands::Prewrite::with_defaults(
                vec![Mutation::Put((Key::from_raw(b"k"), b"v".to_vec()))],
                b"k".to_vec(),
                10.into(),
            )
            .into(),
            commands::AcquirePessimisticLock::new(
                vec![(Key::from_raw(b"k"), false)],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                TimeStamp::default(),
                Some(WaitTimeout::Default),
                false,
                TimeStamp::default(),
                Context::default(),
            )
            .into(),
            commands::Commit::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Cleanup::new(
                Key::from_raw(b"k"),
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::Rollback::new(vec![Key::from_raw(b"k")], 10.into(), Context::default())
                .into(),
            commands::PessimisticRollback::new(
                vec![Key::from_raw(b"k")],
                10.into(),
                20.into(),
                Context::default(),
            )
            .into(),
            commands::ResolveLock::new(
                temp_map,
                None,
                vec![(
                    Key::from_raw(b"k"),
                    mvcc::Lock::new(
                        mvcc::LockType::Put,
                        b"k".to_vec(),
                        10.into(),
                        20,
                        None,
                        TimeStamp::zero(),
                        0,
                        TimeStamp::zero(),
                    ),
                )],
                Context::default(),
            )
            .into(),
            commands::ResolveLockLite::new(
                10.into(),
                TimeStamp::zero(),
                vec![Key::from_raw(b"k")],
                Context::default(),
            )
            .into(),
            commands::TxnHeartBeat::new(Key::from_raw(b"k"), 10.into(), 100, Context::default())
                .into(),
        ];

        let latches = Latches::new(1024);
        let write_locks: Vec<Lock> = write_cmds
            .into_iter()
            .enumerate()
            .map(|(id, cmd)| {
                let mut lock = cmd.gen_lock(&latches);
                assert_eq!(latches.acquire(&mut lock, id as u64), id == 0);
                lock
            })
            .collect();

        for (id, cmd) in readonly_cmds.iter().enumerate() {
            let mut lock = cmd.gen_lock(&latches);
            assert!(latches.acquire(&mut lock, id as u64));
        }

        // acquire/release locks one by one.
        let max_id = write_locks.len() as u64 - 1;
        for (id, mut lock) in write_locks.into_iter().enumerate() {
            let id = id as u64;
            if id != 0 {
                assert!(latches.acquire(&mut lock, id));
            }
            let unlocked = latches.release(&lock, id);
            if id as u64 == max_id {
                assert!(unlocked.is_empty());
            } else {
                assert_eq!(unlocked, vec![id + 1]);
            }
        }
    }
}
