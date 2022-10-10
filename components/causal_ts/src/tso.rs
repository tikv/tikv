// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! ## The algorithm to make the TSO cache tolerate failure of TSO service
//!
//! 1. The scale of High-Available is specified by config item
//! `causal-ts.available-interval`.
//!
//! 2. Count usage of TSO on every renew interval.
//!
//! 3. Calculate `cache_multiplier` by `causal-ts.available-interval /
//! causal-ts.renew-interval`.
//!
//! 4. Then `tso_usage x cache_multiplier` is the expected number of TSO should
//! be cached.
//!
//! 5. And `tso_usage x cache_multiplier - tso_remain` is the expected number of
//! TSO to be requested from TSO service (if it's not a flush).
//!
//! Others:
//! * `cache_multiplier` is also used as capacity of TSO batch list, as we
//!   append an item to the list on every renew.

use std::{
    borrow::Borrow,
    collections::BTreeMap,
    error, result,
    sync::{
        atomic::{AtomicI32, AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
#[cfg(test)]
use futures::executor::block_on;
use parking_lot::RwLock;
use pd_client::PdClient;
use tikv_util::{
    time::{Duration, Instant},
    worker::{Builder as WorkerBuilder, Worker},
};
use tokio::sync::{
    mpsc,
    mpsc::{Receiver, Sender},
    oneshot,
};
use txn_types::TimeStamp;

use crate::{
    errors::{Error, Result},
    metrics::*,
    CausalTsProvider,
};

/// Renew on every 100ms, to adjust batch size rapidly enough.
pub(crate) const DEFAULT_TSO_BATCH_RENEW_INTERVAL_MS: u64 = 100;
/// Minimal batch size of TSO requests. This is an empirical value.
pub(crate) const DEFAULT_TSO_BATCH_MIN_SIZE: u32 = 100;
/// Maximum batch size of TSO requests.
/// As PD provides 262144 TSO per 50ms, conservatively set to 1/16 of 262144.
/// Exceed this space will cause PD to sleep for 50ms, waiting for physical
/// update interval. The 50ms limitation can not be broken through now (see
/// `tso-update-physical-interval`).
pub(crate) const DEFAULT_TSO_BATCH_MAX_SIZE: u32 = 8192;
/// Maximum available interval of TSO cache.
/// It means the duration that TSO we cache would be available despite failure
/// of PD. The longer of the value can provide better "High-Availability"
/// against PD failure, but more overhead of `TsoBatchList` & pressure to TSO
/// service.
pub(crate) const DEFAULT_TSO_BATCH_AVAILABLE_INTERVAL_MS: u64 = 3000;
/// Just a limitation for safety, in case user specify a too big
/// `available_interval`.
const MAX_TSO_BATCH_LIST_CAPACITY: u32 = 1024;

/// TSO range: [(physical, logical_start), (physical, logical_end))
#[derive(Debug)]
struct TsoBatch {
    size: u32,
    physical: u64,
    logical_end: u64, // exclusive
    logical_start: AtomicU64,
}

impl TsoBatch {
    pub fn pop(&self) -> Option<(TimeStamp, bool /* is_used_up */)> {
        let mut logical = self.logical_start.load(Ordering::Relaxed);
        while logical < self.logical_end {
            match self.logical_start.compare_exchange_weak(
                logical,
                logical + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some((
                        TimeStamp::compose(self.physical, logical),
                        logical + 1 == self.logical_end,
                    ));
                }
                Err(x) => logical = x,
            }
        }
        None
    }

    // `last_ts` is the last timestamp of the new batch.
    pub fn new(batch_size: u32, last_ts: TimeStamp) -> Self {
        let (physical, logical_end) = (last_ts.physical(), last_ts.logical() + 1);
        let logical_start = logical_end.checked_sub(batch_size as u64).unwrap();

        Self {
            size: batch_size,
            physical,
            logical_end,
            logical_start: AtomicU64::new(logical_start),
        }
    }

    /// Number of remaining (available) TSO in the batch.
    pub fn remain(&self) -> u32 {
        self.logical_end
            .saturating_sub(self.logical_start.load(Ordering::Relaxed)) as u32
    }

    /// The original start timestamp in the batch.
    pub fn original_start(&self) -> TimeStamp {
        TimeStamp::compose(self.physical, self.logical_end - self.size as u64)
    }

    /// The excluded end timestamp after the last in batch.
    pub fn excluded_end(&self) -> TimeStamp {
        TimeStamp::compose(self.physical, self.logical_end)
    }
}

/// `TsoBatchList` is a ordered list of `TsoBatch`. It aims to:
///
/// 1. Cache more number of TSO to improve high availability. See issue #12794.
/// `TsoBatch` can only cache at most 262144 TSO as logical clock is 18 bits.
///
/// 2. Fully utilize cached TSO when some regions require latest TSO (e.g. in
/// the scenario of leader transfer). Other regions without the requirement can
/// still use older TSO cache.
#[derive(Default, Debug)]
pub struct TsoBatchList {
    inner: RwLock<TsoBatchListInner>,

    /// Number of remaining (available) TSO.
    /// Using signed integer for avoiding a wrap around huge value as it's not
    /// precisely counted.
    tso_remain: AtomicI32,

    /// Statistics of TSO usage.
    tso_usage: AtomicU32,

    /// Length of batch list. It is used to limit size for efficiency, and keep
    /// batches fresh.
    capacity: u32,
}

/// Inner data structure of batch list.
/// The reasons why `crossbeam_skiplist::SkipMap` is not chosen:
///
/// 1. In `flush()` procedure, a reader of `SkipMap` can still acquire a batch
/// after the it is removed, which would violate the causality requirement.
/// The `RwLock<BTreeMap>` avoid this scenario by lock synchronization.
///
/// 2. It is a scenario with much more reads than writes. The `RwLock` would not
/// be less efficient than lock free implementation.
type TsoBatchListInner = BTreeMap<u64, TsoBatch>;

impl TsoBatchList {
    pub fn new(capacity: u32) -> Self {
        Self {
            capacity: std::cmp::min(capacity, MAX_TSO_BATCH_LIST_CAPACITY),
            ..Default::default()
        }
    }

    pub fn remain(&self) -> u32 {
        std::cmp::max(self.tso_remain.load(Ordering::Relaxed), 0) as u32
    }

    pub fn usage(&self) -> u32 {
        self.tso_usage.load(Ordering::Relaxed)
    }

    pub fn take_and_report_usage(&self) -> u32 {
        let usage = self.tso_usage.swap(0, Ordering::Relaxed);
        TS_PROVIDER_TSO_BATCH_LIST_COUNTING_STATIC
            .tso_usage
            .observe(usage as f64);
        usage
    }

    fn remove_batch(&self, key: u64) {
        if let Some(batch) = self.inner.write().remove(&key) {
            self.tso_remain
                .fetch_sub(batch.remain() as i32, Ordering::Relaxed);
        }
    }

    /// Pop timestamp.
    /// When `after_ts.is_some()`, it will pop timestamp larger that `after_ts`.
    /// It is used for the scenario that some regions have causality
    /// requirement (e.g. after transfer, the next timestamp of new leader
    /// should be larger than the store where it is transferred from).
    /// `after_ts` is included.
    pub fn pop(&self, after_ts: Option<TimeStamp>) -> Option<TimeStamp> {
        let inner = self.inner.read();
        let range = match after_ts {
            Some(after_ts) => inner.range(&after_ts.into_inner()..),
            None => inner.range(..),
        };
        for (key, batch) in range {
            if let Some((ts, is_used_up)) = batch.pop() {
                let key = *key;
                drop(inner);
                self.tso_usage.fetch_add(1, Ordering::Relaxed);
                self.tso_remain.fetch_sub(1, Ordering::Relaxed);
                if is_used_up {
                    // Note: do NOT try to make it async.
                    // According to benchmark, `remove_batch` can be done in ~50ns, while async
                    // implemented by `Worker` costs ~1us.
                    self.remove_batch(key);
                }
                return Some(ts);
            }
        }
        None
    }

    pub fn push(&self, batch_size: u32, last_ts: TimeStamp, need_flush: bool) -> Result<u64> {
        let new_batch = TsoBatch::new(batch_size, last_ts);

        if let Some((_, last_batch)) = self.inner.read().iter().next_back() {
            if new_batch.original_start() < last_batch.excluded_end() {
                error!("timestamp fall back"; "batch_size" => batch_size, "last_ts" => ?last_ts,
                    "last_batch" => ?last_batch, "new_batch" => ?new_batch);
                return Err(box_err!("timestamp fall back"));
            }
        }

        let key = new_batch.original_start().into_inner();
        {
            // Hold the write lock until new batch is inserted.
            // Otherwise a `pop()` would acquire the lock, meet no TSO available, and invoke
            // renew request.
            let mut inner = self.inner.write();
            if need_flush {
                self.flush_internal(&mut inner);
            }

            inner.insert(key, new_batch);
            self.tso_remain
                .fetch_add(batch_size as i32, Ordering::Relaxed);
        }

        // Remove items out of capacity limitation.
        // Note: do NOT try to make it async.
        // According to benchmark, `write().pop_first()` can be done in ~50ns, while
        // async implemented by `Worker` costs ~1us.
        if self.inner.read().len() > self.capacity as usize {
            if let Some((_, batch)) = self.inner.write().pop_first() {
                self.tso_remain
                    .fetch_sub(batch.remain() as i32, Ordering::Relaxed);
            }
        }

        Ok(key)
    }

    fn flush_internal(&self, inner: &mut TsoBatchListInner) {
        inner.clear();
        self.tso_remain.store(0, Ordering::Relaxed);
    }

    pub fn flush(&self) {
        let mut inner = self.inner.write();
        self.flush_internal(&mut inner);
    }
}

/// MAX_RENEW_BATCH_SIZE is the batch size of TSO renew. It is an empirical
/// value.
const MAX_RENEW_BATCH_SIZE: usize = 64;

type RenewError = Arc<dyn error::Error + Send + Sync>;
type RenewResult = result::Result<(), RenewError>;

struct RenewRequest {
    need_flush: bool,
    sender: oneshot::Sender<RenewResult>,
}

#[derive(Clone, Copy, Debug)]
struct RenewParameter {
    batch_min_size: u32,
    batch_max_size: u32,
    // `cache_multiplier` indicates that times on usage of TSO it should cache.
    // It is also used as capacity of `TsoBatchList`.
    cache_multiplier: u32,
}

pub struct BatchTsoProvider<C: PdClient> {
    pd_client: Arc<C>,
    batch_list: Arc<TsoBatchList>,
    causal_ts_worker: Worker,
    renew_interval: Duration,
    renew_parameter: RenewParameter,
    renew_request_tx: Sender<RenewRequest>,
}

impl<C: PdClient> std::fmt::Debug for BatchTsoProvider<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchTsoProvider")
            .field("batch_list", &self.batch_list)
            .field("renew_interval", &self.renew_interval)
            .field("renew_parameter", &self.renew_parameter)
            .finish()
    }
}

impl<C: PdClient + 'static> BatchTsoProvider<C> {
    pub async fn new(pd_client: Arc<C>) -> Result<Self> {
        Self::new_opt(
            pd_client,
            Duration::from_millis(DEFAULT_TSO_BATCH_RENEW_INTERVAL_MS),
            Duration::from_millis(DEFAULT_TSO_BATCH_AVAILABLE_INTERVAL_MS),
            DEFAULT_TSO_BATCH_MIN_SIZE,
            DEFAULT_TSO_BATCH_MAX_SIZE,
        )
        .await
    }

    #[allow(unused_mut)]
    fn calc_cache_multiplier(mut renew_interval: Duration, available_interval: Duration) -> u32 {
        #[cfg(any(test, feature = "testexport"))]
        if renew_interval.is_zero() {
            // Should happen in test only.
            renew_interval = Duration::from_millis(DEFAULT_TSO_BATCH_RENEW_INTERVAL_MS);
        }
        available_interval.div_duration_f64(renew_interval).ceil() as u32
    }

    pub async fn new_opt(
        pd_client: Arc<C>,
        renew_interval: Duration,
        available_interval: Duration,
        batch_min_size: u32,
        batch_max_size: u32,
    ) -> Result<Self> {
        let cache_multiplier = Self::calc_cache_multiplier(renew_interval, available_interval);
        let renew_parameter = RenewParameter {
            batch_min_size,
            batch_max_size,
            cache_multiplier,
        };
        let (renew_request_tx, renew_request_rx) = mpsc::channel(MAX_RENEW_BATCH_SIZE);
        let s = Self {
            pd_client: pd_client.clone(),
            batch_list: Arc::new(TsoBatchList::new(cache_multiplier)),
            causal_ts_worker: WorkerBuilder::new("causal_ts_batch_tso_worker").create(),
            renew_interval,
            renew_parameter,
            renew_request_tx,
        };
        s.init(renew_request_rx).await?;
        Ok(s)
    }

    async fn renew_tso_batch(&self, need_flush: bool, reason: TsoBatchRenewReason) -> Result<()> {
        Self::renew_tso_batch_internal(self.renew_request_tx.clone(), need_flush, reason).await
    }

    async fn renew_tso_batch_internal(
        renew_request_tx: Sender<RenewRequest>,
        need_flush: bool,
        reason: TsoBatchRenewReason,
    ) -> Result<()> {
        let start = Instant::now_coarse();
        let (request, response) = oneshot::channel();
        renew_request_tx
            .send(RenewRequest {
                need_flush,
                sender: request,
            })
            .await
            .map_err(|_| -> Error { box_err!("renew request channel is closed") })?;
        let res = response
            .await
            .map_err(|_| box_err!("renew response channel is dropped"))
            .and_then(|r| r.map_err(|err| Error::BatchRenew(err)));

        TS_PROVIDER_TSO_BATCH_RENEW_DURATION_STATIC
            .get(res.borrow().into())
            .get(reason)
            .observe(start.saturating_elapsed_secs());
        res
    }

    async fn renew_tso_batch_impl(
        pd_client: Arc<C>,
        tso_batch_list: Arc<TsoBatchList>,
        renew_parameter: RenewParameter,
        need_flush: bool,
    ) -> Result<()> {
        let tso_remain = tso_batch_list.remain();
        let new_batch_size =
            Self::calc_new_batch_size(tso_batch_list.clone(), renew_parameter, need_flush);

        TS_PROVIDER_TSO_BATCH_LIST_COUNTING_STATIC
            .tso_remain
            .observe(tso_remain as f64);
        TS_PROVIDER_TSO_BATCH_LIST_COUNTING_STATIC
            .new_batch_size
            .observe(new_batch_size as f64);

        let res = match pd_client.batch_get_tso(new_batch_size).await {
            Err(err) => {
                warn!("BatchTsoProvider::renew_tso_batch, pd_client.batch_get_tso error";
                    "new_batch_size" => new_batch_size, "error" => ?err, "need_flash" => need_flush);
                if need_flush {
                    tso_batch_list.flush();
                }
                Err(err.into())
            }
            Ok(ts) => {
                tso_batch_list
                    .push(new_batch_size, ts, need_flush)
                    .map_err(|e| {
                        if need_flush {
                            tso_batch_list.flush();
                        }
                        e
                    })?;
                debug!("BatchTsoProvider::renew_tso_batch";
                    "tso_batch_list.remain" => tso_batch_list.remain(), "ts" => ?ts);

                // Should only be invoked after successful renew. Otherwise the TSO usage will
                // be lost, and batch size requirement will be less than expected. Note that
                // invoked here is not precise. There would be `get_ts()` before here after
                // above `tso_batch_list.push()`, and make `tso_usage` a little bigger. This
                // error is acceptable.
                tso_batch_list.take_and_report_usage();

                Ok(())
            }
        };
        let total_batch_size = tso_batch_list.remain() + tso_batch_list.usage();
        TS_PROVIDER_TSO_BATCH_SIZE.set(total_batch_size as i64);
        res
    }

    async fn renew_thread(
        pd_client: Arc<C>,
        tso_batch_list: Arc<TsoBatchList>,
        renew_parameter: RenewParameter,
        mut rx: Receiver<RenewRequest>,
    ) {
        loop {
            let mut requests = Vec::with_capacity(MAX_RENEW_BATCH_SIZE);
            let mut need_flush = false;
            let mut push_request = |req: RenewRequest| -> usize {
                if req.need_flush {
                    need_flush = true;
                }
                requests.push(req.sender);
                requests.len()
            };

            let mut batch_size = if let Some(req) = rx.recv().await {
                push_request(req)
            } else {
                return;
            };
            while batch_size < MAX_RENEW_BATCH_SIZE {
                if let Ok(req) = rx.try_recv() {
                    batch_size = push_request(req);
                } else {
                    break;
                }
            }

            let res = Self::renew_tso_batch_impl(
                pd_client.clone(),
                tso_batch_list.clone(),
                renew_parameter,
                need_flush,
            )
            .await
            .map_err(|err| -> RenewError {
                let e: Box<dyn error::Error + Sync + Send> = format!("{}", err).into();
                e.into()
            });

            for sender in requests {
                let _ = sender.send(res.clone());
            }
        }
    }

    fn calc_new_batch_size(
        tso_batch_list: Arc<TsoBatchList>,
        renew_parameter: RenewParameter,
        need_flush: bool,
    ) -> u32 {
        // The expected number of TSO is `cache_multiplier` times on latest usage.
        // Note: There is a `batch_max_size` limitation, so the request batch size will
        // be less than expected, and will be fulfill in next renew.
        // TODO: consider schedule TSO requests exceed `batch_max_size` limitation to
        // fulfill requirement in time.
        let mut new_batch_size = tso_batch_list.usage() * renew_parameter.cache_multiplier;
        if !need_flush {
            new_batch_size = new_batch_size.saturating_sub(tso_batch_list.remain())
        }
        std::cmp::min(
            std::cmp::max(new_batch_size, renew_parameter.batch_min_size),
            renew_parameter.batch_max_size,
        )
    }

    async fn init(&self, renew_request_rx: Receiver<RenewRequest>) -> Result<()> {
        // Spawn renew thread.
        let pd_client = self.pd_client.clone();
        let tso_batch_list = self.batch_list.clone();
        let renew_parameter = self.renew_parameter;
        self.causal_ts_worker.remote().spawn(async move {
            Self::renew_thread(pd_client, tso_batch_list, renew_parameter, renew_request_rx).await;
        });

        self.renew_tso_batch(true, TsoBatchRenewReason::init)
            .await?;

        let request_tx = self.renew_request_tx.clone();
        let task = move || {
            let request_tx = request_tx.clone();
            async move {
                let _ = Self::renew_tso_batch_internal(
                    request_tx,
                    false,
                    TsoBatchRenewReason::background,
                )
                .await;
            }
        };

        // Duration::ZERO means never renew automatically. For test purpose ONLY.
        if self.renew_interval > Duration::ZERO {
            self.causal_ts_worker
                .spawn_interval_async_task(self.renew_interval, task);
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn tso_remain(&self) -> u32 {
        self.batch_list.remain()
    }

    #[cfg(test)]
    pub fn tso_usage(&self) -> u32 {
        self.batch_list.usage()
    }

    #[cfg(test)]
    pub fn get_ts(&self) -> Result<TimeStamp> {
        block_on(self.async_get_ts())
    }

    #[cfg(test)]
    pub fn flush(&self) -> Result<TimeStamp> {
        block_on(self.async_flush())
    }
}

const GET_TS_MAX_RETRY: u32 = 3;

#[async_trait]
impl<C: PdClient + 'static> CausalTsProvider for BatchTsoProvider<C> {
    // TODO: support `after_ts` argument.
    async fn async_get_ts(&self) -> Result<TimeStamp> {
        let start = Instant::now();
        let mut retries = 0;
        let mut last_batch_size: u32;
        loop {
            {
                last_batch_size = self.batch_list.remain() + self.batch_list.usage();
                match self.batch_list.pop(None) {
                    Some(ts) => {
                        trace!("BatchTsoProvider::get_ts: {:?}", ts);
                        TS_PROVIDER_GET_TS_DURATION_STATIC
                            .ok
                            .observe(start.saturating_elapsed_secs());
                        return Ok(ts);
                    }
                    None => {
                        warn!("BatchTsoProvider::get_ts, batch used up"; "last_batch_size" => last_batch_size, "retries" => retries);
                    }
                }
            }

            if retries >= GET_TS_MAX_RETRY {
                break;
            }
            if let Err(err) = self
                .renew_tso_batch(false, TsoBatchRenewReason::used_up)
                .await
            {
                // `renew_tso_batch` failure is likely to be caused by TSO timeout, which would
                // mean that PD is quite busy. So do not retry any more.
                error!("BatchTsoProvider::get_ts, renew_tso_batch fail on batch used-up"; "err" => ?err);
                break;
            }
            retries += 1;
        }
        error!("BatchTsoProvider::get_ts, batch used up"; "last_batch_size" => last_batch_size, "retries" => retries);
        TS_PROVIDER_GET_TS_DURATION_STATIC
            .err
            .observe(start.saturating_elapsed_secs());
        Err(Error::TsoBatchUsedUp(last_batch_size))
    }

    async fn async_flush(&self) -> Result<TimeStamp> {
        self.renew_tso_batch(true, TsoBatchRenewReason::flush)
            .await?;
        // TODO: Return the first tso by renew_tso_batch instead of async_get_ts
        self.async_get_ts().await
    }
}

/// A simple implementation acquiring TSO on every request.
/// For test purpose only. Do not use in production.
pub struct SimpleTsoProvider {
    pd_client: Arc<dyn PdClient>,
}

impl SimpleTsoProvider {
    pub fn new(pd_client: Arc<dyn PdClient>) -> SimpleTsoProvider {
        SimpleTsoProvider { pd_client }
    }
}

#[async_trait]
impl CausalTsProvider for SimpleTsoProvider {
    async fn async_get_ts(&self) -> Result<TimeStamp> {
        let ts = self.pd_client.get_tso().await?;
        debug!("SimpleTsoProvider::get_ts"; "ts" => ?ts);
        Ok(ts)
    }

    async fn async_flush(&self) -> Result<TimeStamp> {
        self.async_get_ts().await
    }
}

#[cfg(test)]
pub mod tests {
    use futures::executor::block_on;
    use test_pd_client::TestPdClient;

    use super::*;

    #[test]
    fn test_tso_batch() {
        let batch = TsoBatch::new(10, TimeStamp::compose(1, 100));

        assert_eq!(batch.original_start(), TimeStamp::compose(1, 91));
        assert_eq!(batch.excluded_end(), TimeStamp::compose(1, 101));
        assert_eq!(batch.remain(), 10);

        for logical in 91..=93 {
            assert_eq!(batch.pop(), Some((TimeStamp::compose(1, logical), false)));
        }
        assert_eq!(batch.remain(), 7);

        for logical in 94..=99 {
            assert_eq!(batch.pop(), Some((TimeStamp::compose(1, logical), false)));
        }
        assert_eq!(batch.remain(), 1);

        assert_eq!(batch.pop(), Some((TimeStamp::compose(1, 100), true)));
        assert_eq!(batch.pop(), None);
        assert_eq!(batch.remain(), 0);
    }

    #[test]
    fn test_cals_new_batch_size() {
        let cache_multiplier = 30;
        let cases = vec![
            (0, 0, true, 100),
            (50, 0, true, 100),
            (1000, 100, true, 3000),
            (
                1000,
                DEFAULT_TSO_BATCH_MAX_SIZE,
                true,
                DEFAULT_TSO_BATCH_MAX_SIZE,
            ),
            (0, 0, false, 100),
            (1000, 0, false, 100),
            (1000, 100, false, 2000),
            (5000, 100, false, 100),
            (
                1000,
                DEFAULT_TSO_BATCH_MAX_SIZE,
                false,
                DEFAULT_TSO_BATCH_MAX_SIZE,
            ),
        ];

        for (i, (remain, usage, need_flush, expected)) in cases.into_iter().enumerate() {
            let batch_list = Arc::new(TsoBatchList {
                inner: Default::default(),
                tso_remain: AtomicI32::new(remain as i32),
                tso_usage: AtomicU32::new(usage),
                capacity: cache_multiplier,
            });
            let renew_parameter = RenewParameter {
                batch_min_size: DEFAULT_TSO_BATCH_MIN_SIZE,
                batch_max_size: DEFAULT_TSO_BATCH_MAX_SIZE,
                cache_multiplier,
            };
            let new_size = BatchTsoProvider::<TestPdClient>::calc_new_batch_size(
                batch_list,
                renew_parameter,
                need_flush,
            );
            assert_eq!(new_size, expected, "case {}", i);
        }
    }

    #[test]
    fn test_tso_batch_list_basic() {
        let batch_list = TsoBatchList::new(10);

        assert_eq!(batch_list.remain(), 0);
        assert_eq!(batch_list.usage(), 0);
        assert_eq!(batch_list.pop(None), None);

        batch_list
            .push(10, TimeStamp::compose(1, 100), false)
            .unwrap();
        assert_eq!(batch_list.remain(), 10);
        assert_eq!(batch_list.usage(), 0);

        for logical in 91..=94 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch_list.remain(), 6);
        assert_eq!(batch_list.usage(), 4);

        for logical in 95..=100 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch_list.remain(), 0);
        assert_eq!(batch_list.usage(), 10);
        assert_eq!(batch_list.pop(None), None);
        assert_eq!(batch_list.remain(), 0);
        assert_eq!(batch_list.usage(), 10);

        batch_list
            .push(10, TimeStamp::compose(1, 110), false)
            .unwrap();
        assert_eq!(batch_list.remain(), 10);
        assert_eq!(batch_list.usage(), 10);
        // timestamp fall back
        batch_list
            .push(10, TimeStamp::compose(1, 119), false)
            .unwrap_err();
        batch_list
            .push(10, TimeStamp::compose(1, 200), false)
            .unwrap();
        assert_eq!(batch_list.remain(), 20);
        assert_eq!(batch_list.usage(), 10);

        for logical in 101..=110 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        for logical in 191..=195 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch_list.remain(), 5);
        assert_eq!(batch_list.usage(), 25);

        batch_list.flush();
        assert_eq!(batch_list.pop(None), None);
        assert_eq!(batch_list.remain(), 0);
        assert_eq!(batch_list.take_and_report_usage(), 25);
        assert_eq!(batch_list.usage(), 0);

        // need_flush
        batch_list
            .push(10, TimeStamp::compose(1, 300), false)
            .unwrap();
        let key391 = batch_list
            .push(10, TimeStamp::compose(1, 400), true)
            .unwrap();
        assert_eq!(key391, TimeStamp::compose(1, 391).into_inner());
        assert_eq!(batch_list.remain(), 10);
        assert_eq!(batch_list.usage(), 0);

        for logical in 391..=400 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch_list.remain(), 0);
        assert_eq!(batch_list.usage(), 10);
    }

    #[test]
    fn test_tso_batch_list_max_batch_count() {
        let batch_list = TsoBatchList::new(3);

        batch_list
            .push(10, TimeStamp::compose(1, 100), false)
            .unwrap(); // will be remove after the 4th push.
        batch_list
            .push(10, TimeStamp::compose(1, 200), false)
            .unwrap();
        batch_list
            .push(10, TimeStamp::compose(1, 300), false)
            .unwrap();
        batch_list
            .push(10, TimeStamp::compose(1, 400), false)
            .unwrap();

        for logical in 191..=195 {
            assert_eq!(batch_list.pop(None), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch_list.remain(), 25);
        assert_eq!(batch_list.usage(), 5);
    }

    #[test]
    fn test_tso_batch_list_pop_after_ts() {
        let batch_list = TsoBatchList::new(10);

        batch_list
            .push(10, TimeStamp::compose(1, 100), false)
            .unwrap();
        batch_list
            .push(10, TimeStamp::compose(1, 200), false)
            .unwrap();
        batch_list
            .push(10, TimeStamp::compose(1, 300), false)
            .unwrap();
        batch_list
            .push(10, TimeStamp::compose(1, 400), false)
            .unwrap();

        let after_ts = TimeStamp::compose(1, 291);
        for logical in 291..=300 {
            assert_eq!(
                batch_list.pop(Some(after_ts)),
                Some(TimeStamp::compose(1, logical))
            );
        }
        for logical in 391..=400 {
            assert_eq!(
                batch_list.pop(Some(after_ts)),
                Some(TimeStamp::compose(1, logical))
            );
        }
        assert_eq!(batch_list.pop(Some(after_ts)), None);
        assert_eq!(batch_list.remain(), 20);
        assert_eq!(batch_list.usage(), 20);
    }

    #[test]
    fn test_simple_tso_provider() {
        let pd_cli = Arc::new(TestPdClient::new(1, false));

        let provider = SimpleTsoProvider::new(pd_cli.clone());

        pd_cli.set_tso(100.into());
        let ts = block_on(provider.async_get_ts()).unwrap();
        assert_eq!(ts, 101.into(), "ts: {:?}", ts);
    }

    #[test]
    fn test_batch_tso_provider() {
        let pd_cli = Arc::new(TestPdClient::new(1, false));
        pd_cli.set_tso(1000.into());

        // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to
        // renew manually. allocated: [1001, 1100]
        let provider = block_on(BatchTsoProvider::new_opt(
            pd_cli.clone(),
            Duration::ZERO,
            Duration::from_secs(1), // cache_multiplier = 10
            100,
            80000,
        ))
        .unwrap();
        assert_eq!(provider.tso_remain(), 100);
        assert_eq!(provider.tso_usage(), 0);

        for ts in 1001..=1010u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        assert_eq!(provider.tso_remain(), 90);
        assert_eq!(provider.tso_usage(), 10);

        assert_eq!(provider.flush().unwrap(), TimeStamp::from(1101)); // allocated: [1101, 1200]
        assert_eq!(provider.tso_remain(), 99);
        assert_eq!(provider.tso_usage(), 1);
        // used up
        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1102..=1200u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        assert_eq!(provider.tso_remain(), 0);
        assert_eq!(provider.tso_usage(), 100);
        provider.get_ts().unwrap_err();
        assert_eq!(provider.tso_remain(), 0);
        assert_eq!(provider.tso_usage(), 100);

        assert_eq!(provider.flush().unwrap(), TimeStamp::from(1201)); // allocated: [1201, 2200]
        for ts in 1202..=1260u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        assert_eq!(provider.tso_remain(), 940);
        assert_eq!(provider.tso_usage(), 60);

        // allocated: [2201, 2300]
        block_on(provider.renew_tso_batch(false, TsoBatchRenewReason::background)).unwrap();
        assert_eq!(provider.tso_remain(), 1040); // 940 + 100
        assert_eq!(provider.tso_usage(), 0);

        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1261..=2300u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        provider.get_ts().unwrap_err();
        assert_eq!(provider.tso_remain(), 0);
        assert_eq!(provider.tso_usage(), 1040);

        // renew on used-up
        for ts in 2301..=100_000u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        // batch size: 10400, 80000, 80000
        // batch boundary: 2301, 12700, 92700, 100_000
        assert_eq!(provider.tso_remain(), 72700);
        assert_eq!(provider.tso_usage(), 7300);
    }

    #[test]
    fn test_batch_tso_provider_on_failure() {
        let pd_cli = Arc::new(TestPdClient::new(1, false));
        pd_cli.set_tso(1000.into());

        {
            pd_cli.trigger_tso_failure();
            block_on(BatchTsoProvider::new_opt(
                pd_cli.clone(),
                Duration::ZERO,
                Duration::from_secs(3),
                100,
                8192,
            ))
            .unwrap_err();
        }

        // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to
        // renew manually. allocated: [1001, 1100]
        let provider = block_on(BatchTsoProvider::new_opt(
            pd_cli.clone(),
            Duration::ZERO,
            Duration::from_secs(1), // cache_multiplier=10
            100,
            8192,
        ))
        .unwrap();
        assert_eq!(provider.tso_remain(), 100);
        for ts in 1001..=1010u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }

        pd_cli.trigger_tso_failure();
        for ts in 1011..=1020u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }

        provider.flush().unwrap_err();
        for ts in 1101..=1300u64 {
            // renew on used-up, allocated: [1101, 1300]
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }

        pd_cli.trigger_tso_failure();
        provider.get_ts().unwrap_err(); // renew fail on used-up

        pd_cli.trigger_tso_failure();
        provider.flush().unwrap_err();

        assert_eq!(provider.flush().unwrap(), TimeStamp::from(1301)); // allocated: [1301, 3300]
        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1302..=3300u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        provider.get_ts().unwrap_err();
    }
}
