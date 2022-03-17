// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use parking_lot::RwLock;
use pd_client::PdClient;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tikv_util::time::Duration;
use tikv_util::worker::{Builder as WorkerBuilder, Worker};

use crate::errors::{Error, Result};
use crate::CausalTsProvider;
use txn_types::TimeStamp;

// Renew on every 100ms, to adjust batch size rapidly enough.
const TSO_BATCH_RENEW_INTERVAL_DEFAULT: Duration = Duration::from_millis(100);
// Batch size on every renew interval.
// One TSO is required for every batch of Raft put message, so by default 1K tso/s should be enough.
// Benchmark showed that with a 8.6w raw_put per second, the TSO requirement is 600 per second.
const TSO_BATCH_MIN_SIZE_DEFAULT: u32 = 100;
// Max batch size of TSO requests. Space of logical timestamp is 262144,
// exceed this space will cause PD to sleep, waiting for physical clock advance.
const TSO_BATCH_MAX_SIZE: u32 = 20_0000;

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
        let logical = self.logical_start.fetch_add(1, Ordering::Relaxed);
        if logical < self.logical_end {
            Some(TimeStamp::compose(self.physical, logical))
        } else {
            None
        }
    }

    // `last_ts` is the last timestamp of the new batch.
    pub fn renew(&mut self, batch_size: u32, last_ts: TimeStamp) {
        let (physical, logical) = (last_ts.physical(), last_ts.logical() + 1);
        self.size = batch_size;
        self.physical = physical;
        self.logical_end = logical;
        self.logical_start.store(
            logical.checked_sub(batch_size as u64).unwrap(),
            Ordering::Relaxed,
        );
    }

    // Note: batch is "used up" in flush, and batch size will be enlarged in next renew.
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

pub struct BatchTsoProvider {
    pd_client: Arc<dyn PdClient>,
    batch: Arc<RwLock<TsoBatch>>,
    batch_min_size: u32,
    renew_worker: Worker,
    renew_interval: Duration,
}

impl BatchTsoProvider {
    pub async fn new(pd_client: Arc<dyn PdClient>) -> Result<Self> {
        Self::new_opt(
            pd_client,
            TSO_BATCH_RENEW_INTERVAL_DEFAULT,
            TSO_BATCH_MIN_SIZE_DEFAULT,
        )
        .await
    }

    pub async fn new_opt(
        pd_client: Arc<dyn PdClient>,
        renew_interval: Duration,
        batch_min_size: u32,
    ) -> Result<Self> {
        let s = Self {
            pd_client: pd_client.clone(),
            batch: Arc::new(RwLock::new(TsoBatch::default())),
            batch_min_size,
            renew_worker: WorkerBuilder::new("causal_ts_batch_tso_worker").create(),
            renew_interval,
        };
        s.init().await?;
        Ok(s)
    }

    async fn renew_tso_batch(&self, need_flush: bool) -> Result<()> {
        Self::renew_tso_batch_internal(
            self.pd_client.clone(),
            self.batch.clone(),
            self.batch_min_size,
            need_flush,
        )
        .await
    }

    async fn renew_tso_batch_internal(
        pd_client: Arc<dyn PdClient>,
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
                let mut batch = tso_batch.write();
                batch.renew(new_batch_size, ts);
                debug!("BatchTsoProvider::renew_tso_batch"; "batch renew" => ?batch, "ts" => ?ts);
                Ok(())
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

    async fn init(&self) -> Result<()> {
        self.renew_tso_batch(true).await?;

        let pd_client = self.pd_client.clone();
        let tso_batch = self.batch.clone();
        let batch_min_size = self.batch_min_size;
        let task = move || {
            let pd_client = pd_client.clone();
            let tso_batch = tso_batch.clone();
            async move {
                let _ = Self::renew_tso_batch_internal(pd_client, tso_batch, batch_min_size, false)
                    .await;
            }
        };

        // Duration::ZERO means never renew automatically. For test purpose ONLY.
        if self.renew_interval > Duration::ZERO {
            self.renew_worker
                .spawn_interval_async_task(self.renew_interval, task);
        }
        Ok(())
    }

    // Get current batch_size, for test purpose.
    pub fn batch_size(&self) -> u32 {
        self.batch.read().size
    }
}

impl CausalTsProvider for BatchTsoProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        let batch = self.batch.read();
        match batch.pop() {
            Some(ts) => {
                trace!("BatchTsoProvider::get_ts: {:?}", ts);
                Ok(ts)
            }
            None => {
                error!("BatchTsoProvider::get_ts, batch used up"; "batch.size" => batch.size);
                Err(Error::TsoBatchUsedUp(batch.size))
            }
        }
    }

    fn flush(&self) -> Result<()> {
        block_on(self.renew_tso_batch(true))
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
    use super::*;

    #[test]
    fn test_tso_batch() {
        let mut batch = TsoBatch::default();

        assert_eq!(batch.used_size(), None);
        assert_eq!(batch.pop(), None);
        batch.flush();

        batch.renew(10, TimeStamp::compose(1, 100));
        for logical in 91..=95 {
            assert_eq!(batch.pop(), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch.used_size(), Some(5));

        for logical in 96..=100 {
            assert_eq!(batch.pop(), Some(TimeStamp::compose(1, logical)));
        }
        assert_eq!(batch.used_size(), Some(10));
        assert_eq!(batch.pop(), None);

        batch.renew(10, TimeStamp::compose(1, 200));
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
            let new_size = BatchTsoProvider::calc_new_batch_size(batch_size, used_size, 100);
            assert_eq!(new_size, expected, "case {}", i);
        }
    }
}
