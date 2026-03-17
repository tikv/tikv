// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use dashmap::DashMap;
use futures::compat::Future01CompatExt;
use tikv_util::{
    debug, info, resource_control::DEFAULT_RESOURCE_GROUP_NAME, sys::SysQuota,
    timer::GLOBAL_TIMER_HANDLE,
};

use crate::{
    cpu_config::CpuThrottleConfig,
    metrics::{
        deregister_cpu_throttle_metrics, CPU_THROTTLE_ALLOCATIONS,
        CPU_THROTTLE_GLOBAL_BUCKET_AVAILABLE, CPU_THROTTLE_GLOBAL_BUCKET_CAPACITY,
        CPU_THROTTLE_GLOBAL_REFILL_RATE, CPU_THROTTLE_GROUP_BUCKET_AVAILABLE,
        CPU_THROTTLE_GROUP_BUCKET_CAPACITY, CPU_THROTTLE_GROUP_REFILL_RATE,
        CPU_THROTTLE_REFILL_RATE_ADJUSTMENTS, CPU_THROTTLE_REQUEST_ACTUAL_TO_ESTIMATED_RATIO,
        CPU_THROTTLE_REQUEST_CPU_TIME, CPU_THROTTLE_RUNTIME_TOKEN_WAIT_DURATION,
        CPU_THROTTLE_TOKEN_WAIT_DURATION, CPU_THROTTLE_UNKNOWN_GROUP,
        CPU_USAGE_MONITOR_COLLECT_DURATION, CPU_USAGE_MONITOR_GLOBAL_RATIO,
        CPU_USAGE_MONITOR_RESOURCE_GROUP_DAG_RATIO,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThrottleError {
    GlobalCpuExhausted,
    ResourceGroupCpuExhausted,
    RequestTimeout,
}

impl fmt::Display for ThrottleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ThrottleError::GlobalCpuExhausted => {
                write!(f, "global coprocessor cpu tokens exhausted")
            }
            ThrottleError::ResourceGroupCpuExhausted => {
                write!(f, "resource group coprocessor cpu tokens exhausted")
            }
            ThrottleError::RequestTimeout => write!(f, "request timeout waiting for cpu tokens"),
        }
    }
}

impl std::error::Error for ThrottleError {}

impl ThrottleError {
    fn timeout_allocation_result_label(&self) -> &'static str {
        match self {
            ThrottleError::GlobalCpuExhausted => "global_exhausted",
            ThrottleError::ResourceGroupCpuExhausted => "resource_group_exhausted",
            ThrottleError::RequestTimeout => "timeout",
        }
    }
}

fn clamp_gauge_value(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value as i64
    }
}

async fn sleep_async(duration: Duration) {
    // The global timer can be dropped during shutdown. This sleep is best-effort,
    // so ignore the compat error instead of panicking.
    let _ = GLOBAL_TIMER_HANDLE
        .delay(std::time::Instant::now() + duration)
        .compat()
        .await;
}

fn inflight_add(inflight: &DashMap<String, AtomicU64>, resource_group: &str, delta_us: u64) -> u64 {
    let entry = inflight
        .entry(resource_group.to_owned())
        .or_insert_with(|| AtomicU64::new(0));
    entry.fetch_add(delta_us, Ordering::AcqRel) + delta_us
}

fn inflight_sub(inflight: &DashMap<String, AtomicU64>, resource_group: &str, delta_us: u64) -> u64 {
    let Some(entry) = inflight.get(resource_group) else {
        return 0;
    };
    loop {
        let current = entry.load(Ordering::Acquire);
        let new_value = current.saturating_sub(delta_us);
        if entry
            .compare_exchange(current, new_value, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            return new_value;
        }
    }
}

fn accumulate_dag_cpu_usage(
    accum: &DashMap<String, AtomicU64>,
    resource_group: &str,
    delta_us: u64,
) {
    let entry = accum
        .entry(resource_group.to_owned())
        .or_insert_with(|| AtomicU64::new(0));
    entry.fetch_add(delta_us, Ordering::AcqRel);
}

fn next_refill_rate(
    base_rate: u64,
    current_rate: u64,
    usage_ratio: f64,
    low_watermark: f64,
    high_watermark: f64,
) -> Option<(u64, &'static str)> {
    if !usage_ratio.is_finite() {
        return None;
    }

    if usage_ratio > high_watermark {
        let reduction = ((usage_ratio - high_watermark) / (1.0 - high_watermark)).clamp(0.0, 1.0);
        let new_rate = (base_rate as f64 * (1.0 - reduction * 0.5)) as u64;
        let new_rate = new_rate.max(1);
        (new_rate != current_rate).then_some((new_rate, "decrease"))
    } else if usage_ratio < low_watermark && current_rate < base_rate {
        let recovery =
            ((low_watermark - usage_ratio) / low_watermark.max(f64::EPSILON)).clamp(0.0, 1.0);
        let recovery_step = ((base_rate - current_rate) as f64 * recovery * 0.5).ceil() as u64;
        let new_rate = current_rate
            .saturating_add(recovery_step.max(1))
            .min(base_rate)
            .max(1);
        (new_rate != current_rate).then_some((new_rate, "increase"))
    } else {
        None
    }
}

#[derive(Debug, Clone, Copy)]
struct AdaptiveEstimateEntry {
    estimate_us: u64,
    window_id: u64,
    sum_us: u64,
    count: u64,
}

#[derive(Debug)]
struct AdaptiveEstimator {
    alpha: f64,
    default_estimate_us: u64,
    interval_ms: u64,
    start_instant: Instant,
    estimates: DashMap<String, AdaptiveEstimateEntry>,
}

impl AdaptiveEstimator {
    fn new(default_estimate_us: u64, interval_ms: u64, initial: HashMap<String, u64>) -> Self {
        let estimates = DashMap::new();
        for (resource_group, estimate_us) in initial {
            estimates.insert(
                resource_group,
                AdaptiveEstimateEntry {
                    estimate_us: estimate_us.max(1),
                    window_id: 0,
                    sum_us: 0,
                    count: 0,
                },
            );
        }
        Self {
            alpha: 0.2,
            default_estimate_us: default_estimate_us.max(1),
            interval_ms: interval_ms.max(1),
            start_instant: Instant::now(),
            estimates,
        }
    }

    fn get(&self, resource_group: &str) -> u64 {
        self.estimates
            .get(resource_group)
            .map(|value| value.estimate_us)
            .unwrap_or(self.default_estimate_us)
    }

    fn current_window_id(&self) -> u64 {
        let elapsed_ms = self.start_instant.elapsed().as_millis() as u64;
        elapsed_ms / self.interval_ms
    }

    fn observe(&self, resource_group: &str, actual_cpu_us: u64) {
        if actual_cpu_us == 0 {
            return;
        }
        let window_id = self.current_window_id();
        let mut entry =
            self.estimates
                .entry(resource_group.to_owned())
                .or_insert(AdaptiveEstimateEntry {
                    estimate_us: self.default_estimate_us,
                    window_id,
                    sum_us: 0,
                    count: 0,
                });
        if entry.window_id != window_id {
            if entry.count > 0 {
                let average_us = entry.sum_us / entry.count;
                let new_estimate = (entry.estimate_us as f64 * (1.0 - self.alpha)
                    + average_us as f64 * self.alpha) as u64;
                entry.estimate_us = new_estimate.max(1);
            }
            entry.window_id = window_id;
            entry.sum_us = 0;
            entry.count = 0;
        }
        entry.sum_us = entry.sum_us.saturating_add(actual_cpu_us);
        entry.count = entry.count.saturating_add(1);
    }
}

#[derive(Debug)]
struct CpuUsageState {
    global_ratio: f64,
    per_resource_group_dag_ratios: HashMap<String, f64>,
    last_update: Instant,
}

impl CpuUsageState {
    fn new() -> Self {
        Self {
            global_ratio: 0.0,
            per_resource_group_dag_ratios: HashMap::new(),
            last_update: Instant::now(),
        }
    }
}

#[derive(Debug)]
pub struct CpuTokenBucket {
    capacity_us: AtomicU64,
    available_tokens: AtomicU64,
    base_refill_rate_us: AtomicU64,
    current_refill_rate_us: AtomicU64,
    refill_interval_ms: AtomicU64,
    last_refill: RwLock<Instant>,
}

impl CpuTokenBucket {
    pub fn new(capacity_us: u64, refill_rate_us: u64, refill_interval: Duration) -> Self {
        Self {
            capacity_us: AtomicU64::new(capacity_us),
            available_tokens: AtomicU64::new(capacity_us),
            base_refill_rate_us: AtomicU64::new(refill_rate_us),
            current_refill_rate_us: AtomicU64::new(refill_rate_us),
            refill_interval_ms: AtomicU64::new((refill_interval.as_millis() as u64).max(1)),
            last_refill: RwLock::new(Instant::now()),
        }
    }

    pub fn try_allocate(&self, tokens: u64) -> bool {
        loop {
            let current = self.available_tokens.load(Ordering::Acquire);
            if current < tokens {
                return false;
            }
            if self
                .available_tokens
                .compare_exchange(
                    current,
                    current - tokens,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    pub fn release(&self, tokens: u64) {
        loop {
            let current = self.available_tokens.load(Ordering::Acquire);
            let capacity = self.capacity_us.load(Ordering::Acquire);
            let new_value = current.saturating_add(tokens).min(capacity);
            if self
                .available_tokens
                .compare_exchange(current, new_value, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn refill(&self) {
        let interval_ms = self.refill_interval_ms();
        if interval_ms == 0 {
            return;
        }

        let mut last = self.last_refill.write().unwrap();
        let now = Instant::now();
        let elapsed_ms = now.saturating_duration_since(*last).as_millis() as u64;
        if elapsed_ms < interval_ms {
            return;
        }

        let intervals = elapsed_ms / interval_ms;
        let refill_rate = self.current_refill_rate_us.load(Ordering::Acquire);
        let tokens_to_add = refill_rate.saturating_mul(intervals);
        let capacity = self.capacity_us.load(Ordering::Acquire);

        loop {
            let current = self.available_tokens.load(Ordering::Acquire);
            let new_value = current.saturating_add(tokens_to_add).min(capacity);
            if self
                .available_tokens
                .compare_exchange(current, new_value, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }

        *last = now;
    }

    pub fn available(&self) -> u64 {
        self.available_tokens.load(Ordering::Relaxed)
    }

    pub fn capacity(&self) -> u64 {
        self.capacity_us.load(Ordering::Acquire)
    }

    pub fn base_refill_rate(&self) -> u64 {
        self.base_refill_rate_us.load(Ordering::Acquire)
    }

    pub fn current_refill_rate(&self) -> u64 {
        self.current_refill_rate_us.load(Ordering::Acquire)
    }

    pub fn refill_interval_ms(&self) -> u64 {
        self.refill_interval_ms.load(Ordering::Acquire).max(1)
    }

    pub fn set_refill_rate(&self, new_refill_rate_us: u64) {
        self.current_refill_rate_us
            .store(new_refill_rate_us.max(1), Ordering::Release);
    }

    pub fn set_refill_interval_ms(&self, new_refill_interval_ms: u64) {
        self.refill_interval_ms
            .store(new_refill_interval_ms.max(1), Ordering::Release);
        self.reset_last_refill();
    }

    pub fn restore_base_rate(&self) {
        self.current_refill_rate_us
            .store(self.base_refill_rate(), Ordering::Release);
    }

    pub fn update_quota(&self, new_capacity_us: u64, new_refill_rate_us: u64) {
        let old_capacity = self.capacity();
        let old_base_refill_rate = self.base_refill_rate();
        let old_current_refill_rate = self.current_refill_rate();
        let current_available = self.available();
        let new_base_refill_rate = new_refill_rate_us.max(1);
        let new_available = if new_capacity_us >= old_capacity {
            current_available
                .saturating_add(new_capacity_us - old_capacity)
                .min(new_capacity_us)
        } else {
            current_available.saturating_sub(old_capacity - new_capacity_us)
        };
        // Preserve the dynamic refill-rate reduction across quota refreshes so a
        // config change does not immediately snap throttling back to the base
        // rate and create a transient CPU spike. If the bucket is currently at
        // base rate, this naturally keeps it at the new base rate as well.
        let new_current_refill_rate = if old_current_refill_rate >= old_base_refill_rate {
            new_base_refill_rate
        } else {
            ((old_current_refill_rate as u128 * new_base_refill_rate as u128
                + old_base_refill_rate as u128
                - 1)
                / old_base_refill_rate as u128) as u64
        }
        .clamp(1, new_base_refill_rate);
        self.capacity_us.store(new_capacity_us, Ordering::Release);
        self.base_refill_rate_us
            .store(new_base_refill_rate, Ordering::Release);
        self.current_refill_rate_us
            .store(new_current_refill_rate, Ordering::Release);
        self.available_tokens
            .store(new_available, Ordering::Release);
        self.reset_last_refill();
    }

    fn set_available(&self, available_tokens: u64) {
        self.available_tokens
            .store(available_tokens, Ordering::Release);
    }

    fn reset_last_refill(&self) {
        *self.last_refill.write().unwrap() = Instant::now();
    }
}

#[derive(Debug)]
pub struct CpuTokenHandle {
    pub(crate) resource_group: String,
    pub(crate) initial_estimated_us: u64,
    pub(crate) allocated_us: AtomicU64,
    pub(crate) resource_group_allocated_us: AtomicU64,
    pub(crate) global_allocated_us: AtomicU64,
    pub(crate) actual_used_us: AtomicU64,
    pub(crate) global_bucket: Arc<CpuTokenBucket>,
    pub(crate) resource_group_bucket: Option<Arc<CpuTokenBucket>>,
    pub(crate) resource_group_inflight_allocated_us: Option<Arc<DashMap<String, AtomicU64>>>,
    pub(crate) per_resource_group_dag_cpu_accum: Option<Arc<DashMap<String, AtomicU64>>>,
    pub(crate) request_deadline: Instant,
    pub(crate) config: Arc<CpuThrottleConfig>,
    adaptive_estimator: Option<Arc<AdaptiveEstimator>>,
    pub(crate) is_burst: AtomicBool,
}

impl CpuTokenHandle {
    pub fn allocate_more(&self, additional_us: u64) -> bool {
        self.global_bucket.refill();
        if let Some(bucket) = &self.resource_group_bucket {
            bucket.refill();
        }

        if !self.global_bucket.try_allocate(additional_us) {
            return false;
        }

        let mut group_allocated_delta = 0;
        if !self.is_burst.load(Ordering::Acquire) {
            if let Some(bucket) = &self.resource_group_bucket {
                if bucket.try_allocate(additional_us) {
                    group_allocated_delta = additional_us;
                } else if self.config.enable_burst {
                    self.is_burst.store(true, Ordering::Release);
                } else {
                    self.global_bucket.release(additional_us);
                    return false;
                }
            }
        }

        self.allocated_us.fetch_add(additional_us, Ordering::AcqRel);
        self.global_allocated_us
            .fetch_add(additional_us, Ordering::AcqRel);
        if group_allocated_delta > 0 {
            self.resource_group_allocated_us
                .fetch_add(group_allocated_delta, Ordering::AcqRel);
            self.inflight_add(group_allocated_delta);
        }
        self.update_available_metrics();
        true
    }

    pub async fn allocate_more_with_wait(&self, additional_us: u64) -> Result<u64, ThrottleError> {
        let start = Instant::now();
        let refill_interval = Duration::from_millis(self.config.refill_interval_ms.max(1));
        loop {
            if self.allocate_more(additional_us) {
                CPU_THROTTLE_RUNTIME_TOKEN_WAIT_DURATION
                    .with_label_values(&[self.resource_group.as_str(), "success"])
                    .observe(start.elapsed().as_secs_f64());
                return Ok(additional_us);
            }

            let now = Instant::now();
            if now >= self.request_deadline {
                CPU_THROTTLE_RUNTIME_TOKEN_WAIT_DURATION
                    .with_label_values(&[self.resource_group.as_str(), "timeout"])
                    .observe(start.elapsed().as_secs_f64());
                return Err(ThrottleError::RequestTimeout);
            }

            sleep_async(
                self.request_deadline
                    .saturating_duration_since(now)
                    .min(refill_interval),
            )
            .await;
        }
    }

    pub fn record_actual_usage(&self, actual_cpu_us: u64) {
        self.actual_used_us
            .fetch_add(actual_cpu_us, Ordering::AcqRel);
    }

    pub fn allocated(&self) -> u64 {
        self.allocated_us.load(Ordering::Acquire)
    }

    pub fn get_runtime_config(&self) -> Option<(Duration, f64, u64)> {
        if self.config.enable_runtime_token_management {
            Some((
                Duration::from_micros(self.config.runtime_check_interval_us.max(1)),
                self.config.additional_allocation_threshold,
                self.config.per_allocation_us.max(1),
            ))
        } else {
            None
        }
    }

    fn inflight_add(&self, delta_us: u64) {
        if let Some(inflight) = &self.resource_group_inflight_allocated_us {
            inflight_add(inflight, &self.resource_group, delta_us);
        }
    }

    fn inflight_sub(&self, delta_us: u64) {
        if let Some(inflight) = &self.resource_group_inflight_allocated_us {
            inflight_sub(inflight, &self.resource_group, delta_us);
        }
    }

    fn update_available_metrics(&self) {
        CPU_THROTTLE_GLOBAL_BUCKET_AVAILABLE.set(clamp_gauge_value(self.global_bucket.available()));
        if let Some(bucket) = &self.resource_group_bucket {
            CPU_THROTTLE_GROUP_BUCKET_AVAILABLE
                .with_label_values(&[self.resource_group.as_str()])
                .set(clamp_gauge_value(bucket.available()));
        }
    }

    fn allocation_result_label(&self) -> &'static str {
        if self.is_burst.load(Ordering::Acquire) {
            "burst"
        } else if self.resource_group_bucket.is_some() {
            "success"
        } else {
            "global_only"
        }
    }
}

impl Drop for CpuTokenHandle {
    fn drop(&mut self) {
        let actual_used = self.actual_used_us.load(Ordering::Acquire);
        let global_allocated = self.global_allocated_us.load(Ordering::Acquire);
        let resource_group_allocated = self.resource_group_allocated_us.load(Ordering::Acquire);

        if let Some(estimator) = &self.adaptive_estimator {
            estimator.observe(&self.resource_group, actual_used);
        }
        if actual_used > 0 {
            if let Some(accum) = &self.per_resource_group_dag_cpu_accum {
                accumulate_dag_cpu_usage(accum, &self.resource_group, actual_used);
            }
        }

        if actual_used < global_allocated {
            self.global_bucket.release(global_allocated - actual_used);
        }
        if let Some(bucket) = &self.resource_group_bucket {
            if actual_used < resource_group_allocated {
                bucket.release(resource_group_allocated - actual_used);
            }
        }

        self.inflight_sub(resource_group_allocated);
        self.update_available_metrics();

        CPU_THROTTLE_REQUEST_CPU_TIME
            .with_label_values(&[self.resource_group.as_str()])
            .observe(Duration::from_micros(actual_used).as_secs_f64());
        if self.initial_estimated_us > 0 {
            CPU_THROTTLE_REQUEST_ACTUAL_TO_ESTIMATED_RATIO
                .with_label_values(&[self.resource_group.as_str()])
                .observe(actual_used as f64 / self.initial_estimated_us as f64);
        }
    }
}

pub struct CpuThrottleManager {
    global_bucket: Arc<CpuTokenBucket>,
    resource_group_buckets: Arc<DashMap<String, Arc<CpuTokenBucket>>>,
    resource_group_weights: Arc<DashMap<String, u64>>,
    resource_group_inflight_allocated_us: Arc<DashMap<String, AtomicU64>>,
    config: RwLock<Arc<CpuThrottleConfig>>,
    global_capacity_us: AtomicU64,
    resource_group_estimated_cpu_per_request_us: RwLock<HashMap<String, u64>>,
    adaptive_estimator: RwLock<Option<Arc<AdaptiveEstimator>>>,
    resource_group_dag_cpu_accum: Arc<DashMap<String, AtomicU64>>,
    usage_state: RwLock<CpuUsageState>,
}

impl CpuThrottleManager {
    pub fn new(config: CpuThrottleConfig) -> Self {
        assert!(
            !config.throttle_default_group || config.default_group_weight.unwrap_or(0) > 0,
            "cpu throttle config invariant violated: default_group_weight must be set when throttle_default_group is enabled",
        );
        let refill_interval_ms = config.refill_interval_ms.max(1);
        let refill_interval = Duration::from_millis(refill_interval_ms);
        let global_capacity_us = Self::calculate_global_capacity_us(&config);

        let global_bucket = Arc::new(CpuTokenBucket::new(
            global_capacity_us,
            global_capacity_us,
            refill_interval,
        ));

        CPU_THROTTLE_GLOBAL_BUCKET_CAPACITY.set(clamp_gauge_value(global_capacity_us));
        CPU_THROTTLE_GLOBAL_BUCKET_AVAILABLE.set(clamp_gauge_value(global_bucket.available()));
        CPU_THROTTLE_GLOBAL_REFILL_RATE.set(clamp_gauge_value(global_bucket.current_refill_rate()));

        let estimated_overrides =
            CpuThrottleConfig::parse_resource_group_estimated_cpu_per_request_us(
                &config.resource_group_estimated_cpu_per_request_us,
            );
        let adaptive_estimator = if config.enable_adaptive_estimated_cpu_per_request_us {
            Some(Arc::new(AdaptiveEstimator::new(
                config.estimated_cpu_per_request_us,
                config.stats_interval_ms.max(1),
                estimated_overrides.clone(),
            )))
        } else {
            None
        };

        info!(
            "initialize cpu throttle manager";
            "global_capacity_us" => global_capacity_us,
            "refill_interval_ms" => refill_interval_ms,
            "max_read_cpu_ratio" => config.max_read_cpu_ratio,
        );

        Self {
            global_bucket,
            resource_group_buckets: Arc::new(DashMap::new()),
            resource_group_weights: Arc::new(DashMap::new()),
            resource_group_inflight_allocated_us: Arc::new(DashMap::new()),
            global_capacity_us: AtomicU64::new(global_capacity_us),
            resource_group_estimated_cpu_per_request_us: RwLock::new(estimated_overrides),
            adaptive_estimator: RwLock::new(adaptive_estimator),
            resource_group_dag_cpu_accum: Arc::new(DashMap::new()),
            usage_state: RwLock::new(CpuUsageState::new()),
            config: RwLock::new(Arc::new(config)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.current_config().enabled
    }

    pub fn canonicalize_group_name(name: &str) -> String {
        CpuThrottleConfig::canonicalize_group_name(name)
    }

    pub fn stats_interval(&self) -> Duration {
        Duration::from_millis(self.current_config().stats_interval_ms.max(1))
    }

    pub fn window_size(&self) -> Duration {
        let config = self.current_config();
        Duration::from_millis(config.window_size_ms.max(config.stats_interval_ms.max(1)))
    }

    pub fn max_read_cpu_ratio(&self) -> f64 {
        self.current_config().max_read_cpu_ratio
    }

    pub fn global_capacity_us(&self) -> u64 {
        self.global_capacity_us.load(Ordering::Acquire)
    }

    pub fn refill_interval_ms(&self) -> u64 {
        self.global_bucket.refill_interval_ms()
    }

    pub fn has_resource_group_bucket(&self, resource_group: &str) -> bool {
        self.resource_group_buckets
            .contains_key(Self::canonicalize_group_name(resource_group).as_str())
    }

    pub fn refresh_config(&self, config: CpuThrottleConfig) {
        assert!(
            !config.throttle_default_group || config.default_group_weight.unwrap_or(0) > 0,
            "cpu throttle config invariant violated: default_group_weight must be set when throttle_default_group is enabled",
        );

        let refill_interval_ms = config.refill_interval_ms.max(1);
        let new_global_capacity_us = Self::calculate_global_capacity_us(&config);
        let estimated_overrides =
            CpuThrottleConfig::parse_resource_group_estimated_cpu_per_request_us(
                &config.resource_group_estimated_cpu_per_request_us,
            );
        let adaptive_estimator = if config.enable_adaptive_estimated_cpu_per_request_us {
            Some(Arc::new(AdaptiveEstimator::new(
                config.estimated_cpu_per_request_us,
                config.stats_interval_ms.max(1),
                estimated_overrides.clone(),
            )))
        } else {
            None
        };

        *self.config.write().unwrap() = Arc::new(config);
        self.global_capacity_us
            .store(new_global_capacity_us, Ordering::Release);
        *self
            .resource_group_estimated_cpu_per_request_us
            .write()
            .unwrap() = estimated_overrides;
        *self.adaptive_estimator.write().unwrap() = adaptive_estimator;

        self.global_bucket
            .set_refill_interval_ms(refill_interval_ms);
        self.global_bucket
            .update_quota(new_global_capacity_us, new_global_capacity_us);
        for entry in self.resource_group_buckets.iter() {
            entry.value().set_refill_interval_ms(refill_interval_ms);
        }

        CPU_THROTTLE_GLOBAL_BUCKET_CAPACITY.set(clamp_gauge_value(new_global_capacity_us));
        CPU_THROTTLE_GLOBAL_BUCKET_AVAILABLE.set(clamp_gauge_value(self.global_bucket.available()));
        CPU_THROTTLE_GLOBAL_REFILL_RATE
            .set(clamp_gauge_value(self.global_bucket.current_refill_rate()));

        info!(
            "refresh cpu throttle config";
            "global_capacity_us" => new_global_capacity_us,
            "refill_interval_ms" => refill_interval_ms,
        );
    }

    pub fn sync_resource_groups(&self, resource_groups: Vec<(String, u64)>) {
        let mut next_weights = HashMap::new();
        for (resource_group, ru_quota) in resource_groups {
            let resource_group = Self::canonicalize_group_name(&resource_group);
            if self.should_throttle_group(&resource_group) {
                next_weights.insert(
                    resource_group.clone(),
                    self.weight_for_group(&resource_group, ru_quota),
                );
            }
        }

        let existing: Vec<String> = self
            .resource_group_weights
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        for resource_group in existing {
            if !next_weights.contains_key(&resource_group) {
                self.resource_group_weights.remove(&resource_group);
                self.remove_resource_group_bucket(&resource_group);
            }
        }

        for (resource_group, weight) in next_weights {
            self.resource_group_weights.insert(resource_group, weight);
        }

        self.recalculate_all_quotas();
    }

    pub fn take_per_resource_group_dag_cpu_deltas(&self) -> HashMap<String, u64> {
        let mut deltas = HashMap::new();
        let mut stale_resource_groups = Vec::new();
        for entry in self.resource_group_dag_cpu_accum.iter() {
            let resource_group = entry.key().clone();
            let delta = entry.value().swap(0, Ordering::AcqRel);
            if self
                .resource_group_buckets
                .contains_key(resource_group.as_str())
            {
                if delta > 0 {
                    deltas.insert(resource_group, delta);
                }
            } else {
                stale_resource_groups.push(resource_group);
            }
        }
        for resource_group in stale_resource_groups {
            self.resource_group_dag_cpu_accum.remove(&resource_group);
        }
        deltas
    }

    pub fn update_usage(
        &self,
        global_ratio: f64,
        per_resource_group_dag_ratios: HashMap<String, f64>,
    ) {
        let global_ratio = global_ratio.max(0.0);
        let per_resource_group_dag_ratios: HashMap<String, f64> = per_resource_group_dag_ratios
            .into_iter()
            .map(|(resource_group, ratio)| {
                (
                    Self::canonicalize_group_name(&resource_group),
                    ratio.max(0.0),
                )
            })
            .filter(|(resource_group, _)| self.resource_group_buckets.contains_key(resource_group))
            .collect();
        let previous_resource_groups = {
            let usage_state = self.usage_state.read().unwrap();
            usage_state
                .per_resource_group_dag_ratios
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        };
        {
            let mut usage_state = self.usage_state.write().unwrap();
            usage_state.global_ratio = global_ratio;
            usage_state.per_resource_group_dag_ratios = per_resource_group_dag_ratios.clone();
            usage_state.last_update = Instant::now();
        }

        CPU_USAGE_MONITOR_GLOBAL_RATIO.set(global_ratio);
        for entry in self.resource_group_buckets.iter() {
            let ratio = per_resource_group_dag_ratios
                .get(entry.key())
                .copied()
                .unwrap_or(0.0);
            CPU_USAGE_MONITOR_RESOURCE_GROUP_DAG_RATIO
                .with_label_values(&[entry.key().as_str()])
                .set(ratio);
        }
        for resource_group in previous_resource_groups {
            if !per_resource_group_dag_ratios.contains_key(&resource_group) {
                CPU_USAGE_MONITOR_RESOURCE_GROUP_DAG_RATIO
                    .with_label_values(&[resource_group.as_str()])
                    .set(0.0);
            }
        }
    }

    pub fn adjust_refill_rates(&self) {
        let config = self.current_config();
        if !config.enable_dynamic_adjustment {
            return;
        }

        let (global_ratio, per_resource_group_dag_ratios) = {
            let usage_state = self.usage_state.read().unwrap();
            (
                usage_state.global_ratio,
                usage_state.per_resource_group_dag_ratios.clone(),
            )
        };
        let high_watermark = config.high_watermark;
        let low_watermark = config.low_watermark;

        if let Some((new_rate, direction)) = next_refill_rate(
            self.global_bucket.base_refill_rate(),
            self.global_bucket.current_refill_rate(),
            global_ratio,
            low_watermark,
            high_watermark,
        ) {
            if new_rate == self.global_bucket.base_refill_rate() {
                self.global_bucket.restore_base_rate();
            } else {
                self.global_bucket.set_refill_rate(new_rate);
            }
            CPU_THROTTLE_REFILL_RATE_ADJUSTMENTS
                .with_label_values(&["global", direction])
                .inc();
        }
        CPU_THROTTLE_GLOBAL_REFILL_RATE
            .set(clamp_gauge_value(self.global_bucket.current_refill_rate()));

        for entry in self.resource_group_buckets.iter() {
            let resource_group = entry.key();
            let bucket = entry.value();
            let ratio = per_resource_group_dag_ratios
                .get(resource_group.as_str())
                .copied()
                .unwrap_or(0.0);
            let quota_ratio = bucket.capacity() as f64 / self.global_capacity_us() as f64;
            if quota_ratio <= 0.0 {
                continue;
            }
            let normalized_ratio = ratio / quota_ratio;
            if let Some((new_rate, direction)) = next_refill_rate(
                bucket.base_refill_rate(),
                bucket.current_refill_rate(),
                normalized_ratio,
                low_watermark,
                high_watermark,
            ) {
                if new_rate == bucket.base_refill_rate() {
                    bucket.restore_base_rate();
                } else {
                    bucket.set_refill_rate(new_rate);
                }
                CPU_THROTTLE_REFILL_RATE_ADJUSTMENTS
                    .with_label_values(&["resource_group", direction])
                    .inc();
            }
            CPU_THROTTLE_GROUP_REFILL_RATE
                .with_label_values(&[resource_group.as_str()])
                .set(clamp_gauge_value(bucket.current_refill_rate()));
        }
    }

    pub fn observe_cpu_monitor_collect_duration(&self, duration: Duration) {
        CPU_USAGE_MONITOR_COLLECT_DURATION.observe(duration.as_secs_f64());
    }

    pub fn recalculate_all_quotas(&self) {
        let weights: Vec<(String, u64)> = self
            .resource_group_weights
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect();
        let active_groups: HashSet<String> = weights
            .iter()
            .map(|(resource_group, _)| resource_group.clone())
            .collect();
        let sum_weights: u64 = weights.iter().map(|(_, weight)| *weight).sum();

        if sum_weights == 0 {
            let existing: Vec<String> = self
                .resource_group_buckets
                .iter()
                .map(|entry| entry.key().clone())
                .collect();
            for resource_group in existing {
                self.remove_resource_group_bucket(&resource_group);
            }
            return;
        }

        for (resource_group, weight) in weights {
            let quota_us = ((self.global_capacity_us() as u128 * weight as u128)
                / sum_weights as u128)
                .max(1) as u64;
            let bucket = if let Some(entry) = self.resource_group_buckets.get(&resource_group) {
                let bucket = entry.clone();
                bucket.update_quota(quota_us, quota_us);
                bucket
            } else {
                let bucket = Arc::new(CpuTokenBucket::new(
                    quota_us,
                    quota_us,
                    Duration::from_millis(self.global_bucket.refill_interval_ms()),
                ));
                self.resource_group_buckets
                    .insert(resource_group.clone(), bucket.clone());
                bucket
            };
            let inflight = self
                .resource_group_inflight_allocated_us
                .get(&resource_group)
                .map(|value| value.load(Ordering::Acquire))
                .unwrap_or(0);
            if inflight > quota_us {
                bucket.set_available(0);
            }
            CPU_THROTTLE_GROUP_BUCKET_CAPACITY
                .with_label_values(&[resource_group.as_str()])
                .set(clamp_gauge_value(quota_us));
            CPU_THROTTLE_GROUP_BUCKET_AVAILABLE
                .with_label_values(&[resource_group.as_str()])
                .set(clamp_gauge_value(bucket.available()));
            CPU_THROTTLE_GROUP_REFILL_RATE
                .with_label_values(&[resource_group.as_str()])
                .set(clamp_gauge_value(bucket.current_refill_rate()));
        }

        let stale_groups: Vec<String> = self
            .resource_group_buckets
            .iter()
            .filter_map(|entry| {
                if active_groups.contains(entry.key()) {
                    None
                } else {
                    Some(entry.key().clone())
                }
            })
            .collect();
        for resource_group in stale_groups {
            self.remove_resource_group_bucket(&resource_group);
        }
    }

    pub fn on_resource_group_changed(&self, resource_group: &str, ru_quota: u64) {
        let resource_group = Self::canonicalize_group_name(resource_group);
        if !self.should_throttle_group(&resource_group) {
            self.remove_resource_group_bucket(&resource_group);
            if self
                .resource_group_weights
                .remove(&resource_group)
                .is_some()
            {
                self.recalculate_all_quotas();
            }
            return;
        }
        let weight = self.weight_for_group(&resource_group, ru_quota);
        let should_recalculate = self
            .resource_group_weights
            .insert(resource_group.clone(), weight)
            .map_or(true, |previous_weight| previous_weight != weight);
        if should_recalculate || !self.resource_group_buckets.contains_key(&resource_group) {
            self.recalculate_all_quotas();
        }
    }

    pub fn on_resource_group_removed(&self, resource_group: &str) {
        let resource_group = Self::canonicalize_group_name(resource_group);
        self.remove_resource_group_bucket(&resource_group);
        self.resource_group_dag_cpu_accum.remove(&resource_group);
        self.usage_state
            .write()
            .unwrap()
            .per_resource_group_dag_ratios
            .remove(&resource_group);
        if self
            .resource_group_weights
            .remove(&resource_group)
            .is_some()
        {
            self.recalculate_all_quotas();
        }
    }

    pub async fn allocate_with_wait(
        &self,
        resource_group: &str,
        estimated_cpu_us: u64,
        request_deadline: Instant,
    ) -> Result<CpuTokenHandle, ThrottleError> {
        let resource_group = Self::canonicalize_group_name(resource_group);
        if self.is_unknown_group(&resource_group) {
            CPU_THROTTLE_UNKNOWN_GROUP.inc();
        }

        let start = Instant::now();
        loop {
            match self.try_allocate_canonicalized(
                &resource_group,
                estimated_cpu_us,
                request_deadline,
            ) {
                Ok(handle) => {
                    CPU_THROTTLE_TOKEN_WAIT_DURATION
                        .with_label_values(&[resource_group.as_str(), "success"])
                        .observe(start.elapsed().as_secs_f64());
                    CPU_THROTTLE_ALLOCATIONS
                        .with_label_values(&[
                            resource_group.as_str(),
                            handle.allocation_result_label(),
                        ])
                        .inc();
                    return Ok(handle);
                }
                Err(
                    err @ (ThrottleError::GlobalCpuExhausted
                    | ThrottleError::ResourceGroupCpuExhausted),
                ) => {
                    if Instant::now() >= request_deadline {
                        CPU_THROTTLE_TOKEN_WAIT_DURATION
                            .with_label_values(&[resource_group.as_str(), "timeout"])
                            .observe(start.elapsed().as_secs_f64());
                        CPU_THROTTLE_ALLOCATIONS
                            .with_label_values(&[
                                resource_group.as_str(),
                                err.timeout_allocation_result_label(),
                            ])
                            .inc();
                        debug!(
                            "cpu throttle token allocation timed out";
                            "resource_group" => resource_group.as_str(),
                            "estimated_cpu_us" => estimated_cpu_us,
                            "wait_duration" => ?start.elapsed(),
                            "last_exhaustion" => %err,
                        );
                        return Err(ThrottleError::RequestTimeout);
                    }
                    sleep_async(
                        request_deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(
                                self.global_bucket.refill_interval_ms(),
                            )),
                    )
                    .await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    pub fn get_estimated_cpu_per_request_us(&self, resource_group: &str) -> u64 {
        let resource_group = Self::canonicalize_group_name(resource_group);
        let config = self.current_config();
        let adaptive_estimator = self.adaptive_estimator.read().unwrap().clone();
        let estimated = if let Some(estimator) = adaptive_estimator {
            estimator.get(&resource_group)
        } else {
            self.resource_group_estimated_cpu_per_request_us
                .read()
                .unwrap()
                .get(&resource_group)
                .copied()
                .unwrap_or(config.estimated_cpu_per_request_us)
        };
        let cap = self.get_capacity_cap(&resource_group);
        estimated.min(cap).max(1)
    }

    fn try_allocate_canonicalized(
        &self,
        resource_group: &str,
        estimated_cpu_us: u64,
        request_deadline: Instant,
    ) -> Result<CpuTokenHandle, ThrottleError> {
        let config = self.current_config();
        self.global_bucket.refill();
        let resource_group_bucket = self
            .resource_group_buckets
            .get(resource_group)
            .map(|bucket| {
                bucket.refill();
                bucket.clone()
            });

        if !self.global_bucket.try_allocate(estimated_cpu_us) {
            return Err(ThrottleError::GlobalCpuExhausted);
        }

        let mut is_burst = false;
        let mut resource_group_allocated_us = 0;
        if let Some(bucket) = &resource_group_bucket {
            if bucket.try_allocate(estimated_cpu_us) {
                resource_group_allocated_us = estimated_cpu_us;
            } else if config.enable_burst {
                is_burst = true;
            } else {
                self.global_bucket.release(estimated_cpu_us);
                return Err(ThrottleError::ResourceGroupCpuExhausted);
            }
        }

        let handle = CpuTokenHandle {
            resource_group: resource_group.to_owned(),
            initial_estimated_us: estimated_cpu_us,
            allocated_us: AtomicU64::new(estimated_cpu_us),
            resource_group_allocated_us: AtomicU64::new(resource_group_allocated_us),
            global_allocated_us: AtomicU64::new(estimated_cpu_us),
            actual_used_us: AtomicU64::new(0),
            global_bucket: self.global_bucket.clone(),
            resource_group_bucket,
            resource_group_inflight_allocated_us: if resource_group_allocated_us > 0 {
                Some(self.resource_group_inflight_allocated_us.clone())
            } else {
                None
            },
            // Per-resource-group DAG CPU ratios intentionally only cover groups
            // that participate in CPU throttling. The default group is excluded
            // unless throttle_default_group is enabled.
            per_resource_group_dag_cpu_accum: if self.should_throttle_group(resource_group) {
                Some(self.resource_group_dag_cpu_accum.clone())
            } else {
                None
            },
            request_deadline,
            config,
            adaptive_estimator: self.adaptive_estimator.read().unwrap().clone(),
            is_burst: AtomicBool::new(is_burst),
        };
        if resource_group_allocated_us > 0 {
            handle.inflight_add(resource_group_allocated_us);
        }
        handle.update_available_metrics();
        Ok(handle)
    }

    fn get_capacity_cap(&self, resource_group: &str) -> u64 {
        let config = self.current_config();
        let global_capacity_us = self.global_capacity_us();
        let divisor = if config.enable_runtime_token_management {
            10
        } else {
            1
        };
        let group_cap = self
            .resource_group_buckets
            .get(resource_group)
            .map(|bucket| bucket.capacity() / divisor)
            .unwrap_or(global_capacity_us / divisor);
        group_cap.min(global_capacity_us / divisor).max(1)
    }

    fn should_throttle_group(&self, resource_group: &str) -> bool {
        resource_group != DEFAULT_RESOURCE_GROUP_NAME
            || self.current_config().throttle_default_group
    }

    fn is_unknown_group(&self, resource_group: &str) -> bool {
        resource_group != DEFAULT_RESOURCE_GROUP_NAME
            && !self.resource_group_weights.contains_key(resource_group)
            && !self.resource_group_buckets.contains_key(resource_group)
    }

    fn weight_for_group(&self, resource_group: &str, ru_quota: u64) -> u64 {
        let config = self.current_config();
        if resource_group == DEFAULT_RESOURCE_GROUP_NAME && config.throttle_default_group {
            config.default_group_weight()
        } else {
            ru_quota.max(1)
        }
    }

    fn remove_resource_group_bucket(&self, resource_group: &str) {
        self.resource_group_buckets.remove(resource_group);
        self.resource_group_inflight_allocated_us
            .remove(resource_group);
        self.resource_group_dag_cpu_accum.remove(resource_group);
        self.usage_state
            .write()
            .unwrap()
            .per_resource_group_dag_ratios
            .remove(resource_group);
        deregister_cpu_throttle_metrics(resource_group);
    }

    fn current_config(&self) -> Arc<CpuThrottleConfig> {
        self.config.read().unwrap().clone()
    }

    fn calculate_global_capacity_us(config: &CpuThrottleConfig) -> u64 {
        let refill_interval_ms = config.refill_interval_ms.max(1);
        let cpu_cores = SysQuota::cpu_cores_quota().max(1.0);
        ((cpu_cores * config.max_read_cpu_ratio * refill_interval_ms as f64 * 1000.0) as u64).max(1)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::*;
    use crate::metrics::CPU_THROTTLE_ALLOCATIONS;

    fn test_config() -> CpuThrottleConfig {
        CpuThrottleConfig {
            enabled: true,
            max_read_cpu_ratio: 0.1,
            estimated_cpu_per_request_us: 1_000,
            refill_interval_ms: 100,
            enable_runtime_token_management: false,
            ..CpuThrottleConfig::default()
        }
    }

    #[test]
    fn test_recalculate_all_quotas_skips_default_group_by_default() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed(DEFAULT_RESOURCE_GROUP_NAME, u64::MAX);
        manager.on_resource_group_changed("rg1", 100);
        manager.on_resource_group_changed("rg2", 300);

        assert!(
            manager
                .resource_group_buckets
                .get(DEFAULT_RESOURCE_GROUP_NAME)
                .is_none()
        );

        let rg1 = manager.resource_group_buckets.get("rg1").unwrap();
        let rg2 = manager.resource_group_buckets.get("rg2").unwrap();
        assert!(rg2.capacity() > rg1.capacity());
        assert_eq!(
            rg1.capacity() + rg2.capacity(),
            manager.global_capacity_us()
        );
    }

    #[test]
    fn test_recalculate_all_quotas_can_include_default_group() {
        let mut config = test_config();
        config.throttle_default_group = true;
        config.default_group_weight = Some(200);
        let manager = CpuThrottleManager::new(config);
        manager.on_resource_group_changed(DEFAULT_RESOURCE_GROUP_NAME, u64::MAX);
        manager.on_resource_group_changed("rg1", 100);

        let default_bucket = manager
            .resource_group_buckets
            .get(DEFAULT_RESOURCE_GROUP_NAME)
            .unwrap();
        let rg1_bucket = manager.resource_group_buckets.get("rg1").unwrap();
        assert!(default_bucket.capacity() > rg1_bucket.capacity());
    }

    #[test]
    fn test_non_throttle_group_change_does_not_recalculate_existing_quotas() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg1", 100);

        let rg1_bucket = manager.resource_group_buckets.get("rg1").unwrap().clone();
        let rg1_capacity = rg1_bucket.capacity();
        manager
            .resource_group_inflight_allocated_us
            .insert("rg1".to_owned(), AtomicU64::new(rg1_capacity + 1));

        manager.on_resource_group_changed(DEFAULT_RESOURCE_GROUP_NAME, u64::MAX);

        assert_eq!(rg1_bucket.available(), rg1_capacity);
        assert!(
            manager
                .resource_group_buckets
                .get(DEFAULT_RESOURCE_GROUP_NAME)
                .is_none()
        );
        assert!(
            manager
                .resource_group_weights
                .get(DEFAULT_RESOURCE_GROUP_NAME)
                .is_none()
        );
    }

    #[test]
    fn test_unknown_group_fallback_to_global_only() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("known", 100);

        let handle = block_on(manager.allocate_with_wait(
            "missing-group",
            1_000,
            Instant::now() + Duration::from_millis(200),
        ))
        .unwrap();

        assert!(handle.resource_group_bucket.is_none());
        assert_eq!(
            handle.resource_group_allocated_us.load(Ordering::Acquire),
            0
        );
        assert_eq!(handle.global_allocated_us.load(Ordering::Acquire), 1_000);
    }

    fn allocation_metric_value(resource_group: &str, result: &str) -> u64 {
        CPU_THROTTLE_ALLOCATIONS
            .get_metric_with_label_values(&[resource_group, result])
            .unwrap()
            .get()
    }

    #[test]
    fn test_allocate_with_wait_records_global_timeout_cause() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("global-timeout", 100);

        let before = allocation_metric_value("global-timeout", "global_exhausted");
        let _handle = block_on(manager.allocate_with_wait(
            "global-timeout",
            manager.global_capacity_us(),
            Instant::now() + Duration::from_millis(50),
        ))
        .unwrap();

        let err =
            block_on(manager.allocate_with_wait("global-timeout", 1, Instant::now())).unwrap_err();

        assert_eq!(err, ThrottleError::RequestTimeout);
        assert_eq!(
            allocation_metric_value("global-timeout", "global_exhausted"),
            before + 1
        );
    }

    #[test]
    fn test_allocate_with_wait_records_resource_group_timeout_cause() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg-timeout", 100);
        manager.on_resource_group_changed("rg-peer", 100);

        let rg_capacity = manager
            .resource_group_buckets
            .get("rg-timeout")
            .unwrap()
            .capacity();
        let before = allocation_metric_value("rg-timeout", "resource_group_exhausted");
        let _handle = block_on(manager.allocate_with_wait(
            "rg-timeout",
            rg_capacity,
            Instant::now() + Duration::from_millis(50),
        ))
        .unwrap();

        assert!(manager.global_bucket.available() > 0);

        let err =
            block_on(manager.allocate_with_wait("rg-timeout", 1, Instant::now())).unwrap_err();

        assert_eq!(err, ThrottleError::RequestTimeout);
        assert_eq!(
            allocation_metric_value("rg-timeout", "resource_group_exhausted"),
            before + 1
        );
    }

    #[test]
    fn test_cpu_token_bucket_can_override_and_restore_refill_rate() {
        let bucket = CpuTokenBucket::new(1_000, 500, Duration::from_millis(100));

        assert_eq!(bucket.base_refill_rate(), 500);
        assert_eq!(bucket.current_refill_rate(), 500);

        bucket.set_refill_rate(250);
        assert_eq!(bucket.base_refill_rate(), 500);
        assert_eq!(bucket.current_refill_rate(), 250);

        bucket.restore_base_rate();
        assert_eq!(bucket.current_refill_rate(), 500);
    }

    #[test]
    fn test_cpu_token_bucket_update_quota_preserves_dynamic_scaling() {
        let bucket = CpuTokenBucket::new(1_000, 1_000, Duration::from_millis(100));

        bucket.set_refill_rate(250);
        bucket.update_quota(2_000, 2_000);

        assert_eq!(bucket.base_refill_rate(), 2_000);
        assert_eq!(bucket.current_refill_rate(), 500);
    }

    #[test]
    fn test_take_per_resource_group_dag_cpu_deltas_resets_accumulator() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg1", 100);

        accumulate_dag_cpu_usage(&manager.resource_group_dag_cpu_accum, "rg1", 123);
        let deltas = manager.take_per_resource_group_dag_cpu_deltas();
        assert_eq!(deltas.get("rg1").copied(), Some(123));

        let second = manager.take_per_resource_group_dag_cpu_deltas();
        assert!(second.is_empty());
    }

    #[test]
    fn test_take_per_resource_group_dag_cpu_deltas_drops_removed_group() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg1", 100);
        manager.on_resource_group_removed("rg1");

        accumulate_dag_cpu_usage(&manager.resource_group_dag_cpu_accum, "rg1", 123);

        let deltas = manager.take_per_resource_group_dag_cpu_deltas();
        assert!(deltas.is_empty());
        assert!(manager.resource_group_dag_cpu_accum.get("rg1").is_none());
    }

    #[test]
    fn test_update_usage_filters_unmanaged_groups() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg1", 100);

        manager.update_usage(
            0.5,
            HashMap::from([
                (String::from("rg1"), 0.1),
                (String::from("removed-rg"), 0.2),
            ]),
        );

        let usage_state = manager.usage_state.read().unwrap();
        assert_eq!(usage_state.global_ratio, 0.5);
        assert_eq!(usage_state.per_resource_group_dag_ratios.len(), 1);
        assert_eq!(
            usage_state
                .per_resource_group_dag_ratios
                .get("rg1")
                .copied(),
            Some(0.1)
        );
    }

    #[test]
    fn test_cpu_token_handle_drop_accumulates_actual_usage_for_resource_group() {
        let manager = CpuThrottleManager::new(test_config());
        manager.on_resource_group_changed("rg1", 100);

        let handle = block_on(manager.allocate_with_wait(
            "rg1",
            1_000,
            Instant::now() + Duration::from_millis(50),
        ))
        .unwrap();
        handle.record_actual_usage(321);
        drop(handle);

        let deltas = manager.take_per_resource_group_dag_cpu_deltas();
        assert_eq!(deltas.get("rg1").copied(), Some(321));
    }

    #[test]
    fn test_cpu_token_handle_drop_skips_non_throttled_default_group() {
        let manager = CpuThrottleManager::new(test_config());

        let handle = block_on(manager.allocate_with_wait(
            DEFAULT_RESOURCE_GROUP_NAME,
            1_000,
            Instant::now() + Duration::from_millis(50),
        ))
        .unwrap();
        handle.record_actual_usage(321);
        drop(handle);

        let deltas = manager.take_per_resource_group_dag_cpu_deltas();
        assert!(deltas.is_empty());
    }

    #[test]
    fn test_adjust_refill_rates_recovers_global_bucket_gradually() {
        let mut config = test_config();
        config.enable_dynamic_adjustment = true;
        let manager = CpuThrottleManager::new(config);

        let base_rate = manager.global_bucket.base_refill_rate();
        manager.update_usage(0.95, HashMap::new());
        manager.adjust_refill_rates();
        let reduced_rate = manager.global_bucket.current_refill_rate();

        assert!(reduced_rate < base_rate);

        manager.update_usage(0.0, HashMap::new());
        manager.adjust_refill_rates();
        let recovered_once = manager.global_bucket.current_refill_rate();

        assert!(recovered_once > reduced_rate);
        assert!(recovered_once < base_rate);

        manager.update_usage(0.0, HashMap::new());
        manager.adjust_refill_rates();
        let recovered_twice = manager.global_bucket.current_refill_rate();

        assert!(recovered_twice > recovered_once);
        assert!(recovered_twice <= base_rate);
    }

    #[test]
    fn test_adjust_refill_rates_recovers_resource_group_bucket_gradually() {
        let mut config = test_config();
        config.enable_dynamic_adjustment = true;
        let manager = CpuThrottleManager::new(config);
        manager.on_resource_group_changed("rg1", 100);

        let bucket = manager.resource_group_buckets.get("rg1").unwrap().clone();
        let base_rate = bucket.base_refill_rate();

        manager.update_usage(0.0, HashMap::from([(String::from("rg1"), 0.95)]));
        manager.adjust_refill_rates();
        let reduced_rate = bucket.current_refill_rate();

        assert!(reduced_rate < base_rate);

        manager.update_usage(0.0, HashMap::from([(String::from("rg1"), 0.0)]));
        manager.adjust_refill_rates();
        let recovered_once = bucket.current_refill_rate();

        assert!(recovered_once > reduced_rate);
        assert!(recovered_once < base_rate);
    }

    #[test]
    fn test_refresh_config_updates_runtime_state() {
        let manager = CpuThrottleManager::new(test_config());
        manager.sync_resource_groups(vec![
            (DEFAULT_RESOURCE_GROUP_NAME.to_owned(), u64::MAX),
            ("rg1".to_owned(), 100),
        ]);

        let original_capacity = manager.global_capacity_us();
        assert!(!manager.has_resource_group_bucket(DEFAULT_RESOURCE_GROUP_NAME));

        let mut updated = test_config();
        updated.enabled = false;
        updated.max_read_cpu_ratio = 0.2;
        updated.refill_interval_ms = 250;
        updated.throttle_default_group = true;
        updated.default_group_weight = Some(200);

        manager.refresh_config(updated);
        manager.sync_resource_groups(vec![
            (DEFAULT_RESOURCE_GROUP_NAME.to_owned(), u64::MAX),
            ("rg1".to_owned(), 100),
        ]);

        assert!(!manager.is_enabled());
        assert_eq!(manager.refill_interval_ms(), 250);
        assert!(manager.global_capacity_us() > original_capacity);
        assert!(manager.has_resource_group_bucket(DEFAULT_RESOURCE_GROUP_NAME));
        assert_eq!(
            manager
                .resource_group_buckets
                .get(DEFAULT_RESOURCE_GROUP_NAME)
                .unwrap()
                .refill_interval_ms(),
            250
        );
    }

    #[test]
    fn test_refresh_config_preserves_global_dynamic_refill_scaling() {
        let mut config = test_config();
        config.enable_dynamic_adjustment = true;
        let manager = CpuThrottleManager::new(config);

        manager.update_usage(0.95, HashMap::new());
        manager.adjust_refill_rates();

        let old_base = manager.global_bucket.base_refill_rate();
        let old_current = manager.global_bucket.current_refill_rate();
        assert!(old_current < old_base);

        let mut updated = test_config();
        updated.enable_dynamic_adjustment = true;
        updated.max_read_cpu_ratio = 0.2;
        manager.refresh_config(updated);

        let new_base = manager.global_bucket.base_refill_rate();
        let new_current = manager.global_bucket.current_refill_rate();
        assert!(new_base > old_base);
        assert!(new_current < new_base);
        assert_eq!(
            new_current,
            ((old_current as u128 * new_base as u128 + old_base as u128 - 1) / old_base as u128)
                as u64
        );
    }

    #[test]
    fn test_sync_resource_groups_removes_default_bucket_when_disabled() {
        let mut config = test_config();
        config.throttle_default_group = true;
        config.default_group_weight = Some(100);
        let manager = CpuThrottleManager::new(config);

        manager.sync_resource_groups(vec![
            (DEFAULT_RESOURCE_GROUP_NAME.to_owned(), u64::MAX),
            ("rg1".to_owned(), 100),
        ]);
        assert!(manager.has_resource_group_bucket(DEFAULT_RESOURCE_GROUP_NAME));

        let mut updated = test_config();
        updated.throttle_default_group = false;
        manager.refresh_config(updated);
        manager.sync_resource_groups(vec![
            (DEFAULT_RESOURCE_GROUP_NAME.to_owned(), u64::MAX),
            ("rg1".to_owned(), 100),
        ]);

        assert!(!manager.has_resource_group_bucket(DEFAULT_RESOURCE_GROUP_NAME));
    }
}
