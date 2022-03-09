// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tikv_util::worker::{Builder as WorkerBuilder, Worker};
use txn_types::TimeStamp;

use crate::{
    errors::{Error, Result},
    CausalTsProvider,
};

/// An implementation of Hybrid Logical Clock (HLC)
/// See proposal for detail:
/// https://github.com/tikv/rfcs/blob/master/text/0083-rawkv-cross-cluster-replication.md
#[derive(Debug)]
struct Hlc(AtomicU64);

impl Default for Hlc {
    fn default() -> Self {
        Self(AtomicU64::new(0))
    }
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.load(Ordering::Relaxed))
    }
}

impl Hlc {
    #[inline]
    fn next(&self) -> TimeStamp {
        let ts = self.0.fetch_add(1, Ordering::Relaxed).into();
        trace!("Hlc::next: {:?}", ts);
        ts
    }

    // self.next() == `to`
    #[inline]
    fn advance(&self, to: TimeStamp) {
        let before = self.0.fetch_max(to.into_inner(), Ordering::Relaxed).into();
        debug!("Hlc::advance"; "before" => ?before, "after" => ?std::cmp::max(before, to));
    }
}

const TSO_REFRESH_INTERVAL: Duration = Duration::from_millis(500);

/// A causal timestamp provider by HLC
pub struct HlcProvider {
    hlc: Arc<Hlc>,
    pd_client: Arc<dyn PdClient>,
    tso_worker: Worker,

    /// Interval of Physical Clock refresh from TSO.
    /// Smaller interval will get more accurate metrics, but more pressure on PD.
    /// Can be Duration::ZERO, which means never refresh from TSO. For test purpose ONLY.
    tso_refresh_interval: Duration,

    initialized: AtomicBool,
}

impl HlcProvider {
    pub async fn new(pd_client: Arc<dyn PdClient>) -> Result<Self> {
        Self::new_opt(pd_client, TSO_REFRESH_INTERVAL).await
    }

    pub async fn new_opt(
        pd_client: Arc<dyn PdClient>,
        tso_refresh_interval: Duration,
    ) -> Result<Self> {
        let s = Self {
            hlc: Arc::new(Hlc::default()),
            pd_client,
            tso_worker: WorkerBuilder::new("hlc_tso_worker").create(),
            tso_refresh_interval,
            initialized: AtomicBool::new(false),
        };
        s.init().await?;
        Ok(s)
    }

    async fn init(&self) -> Result<()> {
        let tso = self.pd_client.get_tso().await?;
        self.hlc.advance(tso);
        self.initialized.store(true, Ordering::Release);
        debug!("HlcProvider::init"; "tso" => ?tso);

        let hlc = self.hlc.clone();
        let pd_client = self.pd_client.clone();

        let task = move || {
            let hlc = hlc.clone();
            let pd_client = pd_client.clone();

            async move {
                match pd_client.get_tso().await {
                    Ok(tso) => {
                        debug!("HlcProvider::pd_client::get_tso"; "tso" => ?tso);
                        hlc.advance(tso);
                    }
                    Err(err) => {
                        warn!("HlcProvider::pd_client::get_tso error"; "error" => ?err);
                        // Fail to acquire TSO do not violate correctness. Only affect some duration metrics of CDC.
                    }
                };
            }
        };

        // Duration::ZERO means never refresh from TSO. For test purpose ONLY.
        if self.tso_refresh_interval > Duration::ZERO {
            self.tso_worker
                .spawn_interval_async_task(self.tso_refresh_interval, task);
        }
        Ok(())
    }
}

impl CausalTsProvider for HlcProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        if !self.initialized.load(Ordering::Acquire) {
            Err(Error::Hlc("Uninitialized".to_string()))
        } else {
            Ok(self.hlc.next())
        }
    }

    fn advance(&self, to: TimeStamp) -> Result<()> {
        self.hlc.advance(to);
        Ok(())
    }
}

/// A causal timestamp provider by HLC for TEST.
#[derive(Default)]
pub struct TestHlcProvider {
    hlc: Arc<Hlc>,
}

impl CausalTsProvider for TestHlcProvider {
    fn get_ts(&self) -> Result<TimeStamp> {
        Ok(self.hlc.next())
    }

    fn advance(&self, to: TimeStamp) -> Result<()> {
        self.hlc.advance(to);
        Ok(())
    }
}
