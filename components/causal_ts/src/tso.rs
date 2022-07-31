// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error, result,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

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

// Renew on every 100ms, to adjust batch size rapidly enough.
pub(crate) const TSO_BATCH_RENEW_INTERVAL_DEFAULT: u64 = 100;
// Batch size on every renew interval.
// One TSO is required for every batch of Raft put messages, so by default 1K
// tso/s should be enough. Benchmark showed that with a 8.6w raw_put per second,
// the TSO requirement is 600 per second.
pub(crate) const TSO_BATCH_MIN_SIZE_DEFAULT: u32 = 100;
// Max batch size of TSO requests. Space of logical timestamp is 262144,
// exceed this space will cause PD to sleep, waiting for physical clock advance.
const TSO_BATCH_MAX_SIZE: u32 = 20_0000;

const TSO_BATCH_RENEW_ON_INITIALIZE: &str = "init";
const TSO_BATCH_RENEW_BY_BACKGROUND: &str = "background";
const TSO_BATCH_RENEW_FOR_USED_UP: &str = "used-up";
const TSO_BATCH_RENEW_FOR_FLUSH: &str = "flush";

/// TSO range: [(physical, logical_start), (physical, logical_end))
#[derive(Default, Debug)]
struct TsoBatch {
    size: u32,
    physical: u64,
    logical_end: u64, // exclusive
    logical_start: AtomicU64,
}

impl TsoBatch {
    pub fn pop(&self) -> Option<TimeStamp> {
        let mut logical = self.logical_start.load(Ordering::Relaxed);
        while logical < self.logical_end {
            match self.logical_start.compare_exchange_weak(
                logical,
                logical + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(TimeStamp::compose(self.physical, logical)),
                Err(x) => logical = x,
            }
        }
        None
    }

    // `last_ts` is the last timestamp of the new batch.
    pub fn renew(&mut self, batch_size: u32, last_ts: TimeStamp) -> Result<()> {
        let (physical, logical) = (last_ts.physical(), last_ts.logical() + 1);
        let logical_start = logical.checked_sub(batch_size as u64).unwrap();

        if physical < self.physical
            || (physical == self.physical && logical_start < self.logical_end)
        {
            error!("timestamp fall back"; "last_ts" => ?last_ts, "batch" => ?self,
                "physical" => physical, "logical" => logical, "logical_start" => logical_start);
            return Err(box_err!("timestamp fall back"));
        }

        self.size = batch_size;
        self.physical = physical;
        self.logical_end = logical;
        self.logical_start.store(logical_start, Ordering::Relaxed);
        Ok(())
    }

    // Note: batch is "used up" in flush, and batch size will be enlarged in next
    // renew.
    pub fn flush(&self) {
        self.logical_start
            .store(self.logical_end, Ordering::Relaxed);
    }

    // Return None if TsoBatch is empty.
    // Note that `logical_start` will be larger than `logical_end`. See `pop()`.
    pub fn used_size(&self) -> Option<u32> {
        if self.size > 0 {
            Some(
                self.size
                    .checked_sub(
                        self.logical_end
                            .saturating_sub(self.logical_start.load(Ordering::Relaxed))
                            as u32,
                    )
                    .unwrap(),
            )
        } else {
            None
        }
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

pub struct BatchTsoProvider<C: PdClient> {
    pd_client: Arc<C>,
    batch: Arc<RwLock<TsoBatch>>,
    batch_min_size: u32,
    causal_ts_worker: Worker,
    renew_interval: Duration,
    renew_request_tx: mpsc::Sender<RenewRequest>,
}

impl<C: PdClient + 'static> BatchTsoProvider<C> {
    pub async fn new(pd_client: Arc<C>) -> Result<Self> {
        Self::new_opt(
            pd_client,
            Duration::from_millis(TSO_BATCH_RENEW_INTERVAL_DEFAULT),
            TSO_BATCH_MIN_SIZE_DEFAULT,
        )
        .await
    }

    pub async fn new_opt(
        pd_client: Arc<C>,
        renew_interval: Duration,
        batch_min_size: u32,
    ) -> Result<Self> {
        let (renew_request_tx, renew_request_rx) = mpsc::channel(MAX_RENEW_BATCH_SIZE);
        let s = Self {
            pd_client: pd_client.clone(),
            batch: Arc::new(RwLock::new(TsoBatch::default())),
            batch_min_size,
            causal_ts_worker: WorkerBuilder::new("causal_ts_batch_tso_worker").create(),
            renew_interval,
            renew_request_tx,
        };
        s.init(renew_request_rx).await?;
        Ok(s)
    }

    async fn renew_tso_batch(&self, need_flush: bool, reason: &str) -> Result<()> {
        Self::renew_tso_batch_internal(self.renew_request_tx.clone(), need_flush, reason).await
    }

    async fn renew_tso_batch_internal(
        renew_request_tx: Sender<RenewRequest>,
        need_flush: bool,
        reason: &str,
    ) -> Result<()> {
        let start = Instant::now();
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

        let label = if res.is_ok() { "ok" } else { "err" };
        TS_PROVIDER_TSO_BATCH_RENEW_DURATION
            .with_label_values(&[label, reason])
            .observe(start.saturating_elapsed_secs());
        res
    }

    async fn renew_tso_batch_impl(
        pd_client: Arc<C>,
        tso_batch: Arc<RwLock<TsoBatch>>,
        batch_min_size: u32,
        need_flush: bool,
    ) -> Result<()> {
        let new_batch_size = {
            let batch = tso_batch.read();
            match batch.used_size() {
                None => batch_min_size,
                Some(used_size) => {
                    debug!("CachedTsoProvider::renew_tso_batch"; "batch before" => ?batch, "need_flush" => need_flush, "used size" => used_size);
                    Self::calc_new_batch_size(batch.size, used_size, batch_min_size)
                }
            }
        };

        match pd_client.batch_get_tso(new_batch_size).await {
            Err(err) => {
                warn!("BatchTsoProvider::renew_tso_batch, pd_client.batch_get_tso error"; "error" => ?err, "need_flash" => need_flush);
                if need_flush {
                    let batch = tso_batch.write();
                    batch.flush();
                }
                Err(err.into())
            }
            Ok(ts) => {
                {
                    let mut batch = tso_batch.write();
                    batch.renew(new_batch_size, ts).map_err(|e| {
                        if need_flush {
                            batch.flush();
                        }
                        e
                    })?;
                    debug!("BatchTsoProvider::renew_tso_batch"; "batch renew" => ?batch, "ts" => ?ts);
                }
                TS_PROVIDER_TSO_BATCH_SIZE.set(new_batch_size as i64);
                Ok(())
            }
        }
    }

    async fn renew_thread(
        pd_client: Arc<C>,
        tso_batch: Arc<RwLock<TsoBatch>>,
        batch_min_size: u32,
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
                tso_batch.clone(),
                batch_min_size,
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

    fn calc_new_batch_size(batch_size: u32, used_size: u32, batch_min_size: u32) -> u32 {
        if used_size > batch_size * 3 / 4 {
            // Enlarge to double if used more than 3/4.
            std::cmp::min(batch_size << 1, TSO_BATCH_MAX_SIZE)
        } else if used_size < batch_size / 4 {
            // Shrink to half if used less than 1/4.
            std::cmp::max(batch_size >> 1, batch_min_size)
        } else {
            batch_size
        }
    }

    async fn init(&self, renew_request_rx: Receiver<RenewRequest>) -> Result<()> {
        // Spawn renew thread.
        let pd_client = self.pd_client.clone();
        let tso_batch = self.batch.clone();
        let batch_min_size = self.batch_min_size;
        self.causal_ts_worker.remote().spawn(async move {
            Self::renew_thread(pd_client, tso_batch, batch_min_size, renew_request_rx).await;
        });

        self.renew_tso_batch(true, TSO_BATCH_RENEW_ON_INITIALIZE)
            .await?;

        let request_tx = self.renew_request_tx.clone();
        let task = move || {
            let request_tx = request_tx.clone();
            async move {
                let _ = Self::renew_tso_batch_internal(
                    request_tx,
                    false,
                    TSO_BATCH_RENEW_BY_BACKGROUND,
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

    // Get current batch_size, for test purpose.
    pub fn batch_size(&self) -> u32 {
        self.batch.read().size
    }
}

const GET_TS_MAX_RETRY: u32 = 3;

impl<C: PdClient + 'static> CausalTsProvider for BatchTsoProvider<C> {
    fn get_ts(&self) -> Result<TimeStamp> {
        let start = Instant::now();
        let mut retries = 0;
        let mut last_batch_size: u32;
        loop {
            {
                let batch = self.batch.read();
                last_batch_size = batch.size;
                match batch.pop() {
                    Some(ts) => {
                        trace!("BatchTsoProvider::get_ts: {:?}", ts);
                        TS_PROVIDER_GET_TS_DURATION
                            .with_label_values(&["ok"])
                            .observe(start.saturating_elapsed_secs());
                        return Ok(ts);
                    }
                    None => {
                        warn!("BatchTsoProvider::get_ts, batch used up"; "batch.size" => batch.size, "retries" => retries);
                    }
                }
            }

            if retries >= GET_TS_MAX_RETRY {
                break;
            }
            if let Err(err) = block_on(self.renew_tso_batch(false, TSO_BATCH_RENEW_FOR_USED_UP)) {
                // `renew_tso_batch` failure is likely to be caused by TSO timeout, which would
                // mean that PD is quite busy. So do not retry any more.
                error!("BatchTsoProvider::get_ts, renew_tso_batch fail on batch used-up"; "err" => ?err);
                break;
            }
            retries += 1;
        }
        error!("BatchTsoProvider::get_ts, batch used up"; "batch.size" => last_batch_size, "retries" => retries);
        TS_PROVIDER_GET_TS_DURATION
            .with_label_values(&["err"])
            .observe(start.saturating_elapsed_secs());
        Err(Error::TsoBatchUsedUp(last_batch_size))
    }

    fn flush(&self) -> Result<()> {
        block_on(self.renew_tso_batch(true, TSO_BATCH_RENEW_FOR_FLUSH))
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

impl CausalTsProvider for SimpleTsoProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        let ts = block_on(self.pd_client.get_tso())?;
        debug!("SimpleTsoProvider::get_ts"; "ts" => ?ts);
        Ok(ts)
    }
}

#[cfg(test)]
pub mod tests {
    use test_raftstore::TestPdClient;

    use super::*;

    #[test]
    fn test_tso_batch() {
        let mut batch = TsoBatch::default();

        assert_eq!(batch.used_size(), None);
        assert_eq!(batch.pop(), None);
        batch.flush();

        batch.renew(10, TimeStamp::compose(1, 100)).unwrap();
        for logical in 91..=95 {
            assert_eq!(batch.pop(), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch.used_size(), Some(5));

        for logical in 96..=100 {
            assert_eq!(batch.pop(), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch.used_size(), Some(10));
        assert_eq!(batch.pop(), None);

        batch.renew(10, TimeStamp::compose(1, 110)).unwrap();
        // timestamp fall back
        batch.renew(10, TimeStamp::compose(1, 119)).unwrap_err();

        batch.renew(10, TimeStamp::compose(1, 200)).unwrap();
        for logical in 191..=195 {
            assert_eq!(batch.pop(), Some(TimeStamp::compose(1, logical)));
        }
        batch.flush();
        assert_eq!(batch.used_size(), Some(10));
        assert_eq!(batch.pop(), None);
    }

    #[test]
    fn test_cals_new_batch_size() {
        let cases = vec![
            (100, 0, 100),
            (100, 76, 200),
            (200, 49, 100),
            (200, 50, 200),
            (200, 150, 200),
            (200, 151, 400),
            (200, 200, 400),
            (TSO_BATCH_MAX_SIZE, TSO_BATCH_MAX_SIZE, TSO_BATCH_MAX_SIZE),
        ];

        for (i, (batch_size, used_size, expected)) in cases.into_iter().enumerate() {
            let new_size =
                BatchTsoProvider::<TestPdClient>::calc_new_batch_size(batch_size, used_size, 100);
            assert_eq!(new_size, expected, "case {}", i);
        }
    }

    #[test]
    fn test_simple_tso_provider() {
        let pd_cli = Arc::new(TestPdClient::new(1, false));

        let provider = SimpleTsoProvider::new(pd_cli.clone());

        pd_cli.set_tso(100.into());
        let ts = provider.get_ts().unwrap();
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
            100,
        ))
        .unwrap();
        assert_eq!(provider.batch_size(), 100);
        for ts in 1001..=1010u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }

        provider.flush().unwrap(); // allocated: [1101, 1200]
        assert_eq!(provider.batch_size(), 100);
        // used up
        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1101..=1200u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        provider.get_ts().unwrap_err();

        provider.flush().unwrap(); // allocated: [1201, 1400]
        assert_eq!(provider.batch_size(), 200);

        // used < 20%
        for ts in 1201..=1249u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }

        provider.flush().unwrap(); // allocated: [1401, 1500]
        assert_eq!(provider.batch_size(), 100);

        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1401..=1500u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        provider.get_ts().unwrap_err();

        // renew on used-up
        for ts in 1501..=2500u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
    }

    #[test]
    fn test_batch_tso_provider_on_failure() {
        let pd_cli = Arc::new(TestPdClient::new(1, false));
        pd_cli.set_tso(1000.into());

        {
            pd_cli.trigger_tso_failure();
            assert!(
                block_on(BatchTsoProvider::new_opt(
                    pd_cli.clone(),
                    Duration::ZERO,
                    100
                ))
                .is_err()
            );
        }

        // Set `renew_interval` to 0 to disable background renew. Invoke `flush()` to
        // renew manually. allocated: [1001, 1100]
        let provider = block_on(BatchTsoProvider::new_opt(
            pd_cli.clone(),
            Duration::ZERO,
            100,
        ))
        .unwrap();
        assert_eq!(provider.batch_size(), 100);
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

        provider.flush().unwrap(); // allocated: [1301, 1700]
        pd_cli.trigger_tso_failure(); // make renew fail to verify used-up
        for ts in 1301..=1700u64 {
            assert_eq!(TimeStamp::from(ts), provider.get_ts().unwrap())
        }
        provider.get_ts().unwrap_err();
    }
}
