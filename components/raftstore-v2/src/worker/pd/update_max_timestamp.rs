// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use causal_ts::CausalTsProvider;
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use futures::{channel::oneshot, compat::Future01CompatExt, FutureExt, StreamExt};
use kvproto::pdpb;
use pd_client::PdClientV2;
use raftstore::{store::TxnExt, Result};
use slog::{error, info, warn};
use tikv_util::{box_err, mpsc::future as mpsc, timer::GLOBAL_TIMER_HANDLE};
use txn_types::TimeStamp;

use super::Runner;

type CallerChannel = oneshot::Sender<pd_client::Result<pdpb::TsoResponse>>;
pub type Transport = (mpsc::Sender<pdpb::TsoRequest>, mpsc::Sender<CallerChannel>);

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClientV2 + Clone + 'static,
{
    pub fn handle_update_max_timestamp(
        &mut self,
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    ) {
        if !self.maybe_create_transport() {
            return;
        }
        let transport = self.tso_transport.clone().unwrap();
        let concurrency_manager = self.concurrency_manager.clone();
        let causal_ts_provider = self.causal_ts_provider.clone();
        let logger = self.logger.clone();
        let shutdown = self.shutdown.clone();

        let f = async move {
            let mut success = false;
            while txn_ext.max_ts_sync_status.load(Ordering::SeqCst) == initial_status
                && !shutdown.load(Ordering::Relaxed)
            {
                // On leader transfer / region merge, RawKV API v2 need to
                // invoke causal_ts_provider.flush() to renew
                // cached TSO, to ensure that the next TSO
                // returned by causal_ts_provider.get_ts() on current
                // store must be larger than the store where the leader is on
                // before.
                //
                // And it won't break correctness of transaction commands, as
                // causal_ts_provider.flush() is implemented as
                // pd_client.get_tso() + renew TSO cached.
                let res: Result<TimeStamp> = if let Some(causal_ts_provider) = &causal_ts_provider {
                    causal_ts_provider
                        .async_flush()
                        .await
                        .map_err(|e| box_err!(e))
                } else {
                    let (tx, rx) = oneshot::channel();
                    let mut req = pdpb::TsoRequest::default();
                    // TODO(tabokie): header
                    req.set_count(1);
                    let _ = transport.0.send(req);
                    let _ = transport.1.send(tx);
                    rx.await
                        .unwrap()
                        .map(|resp| {
                            let ts = resp.timestamp.unwrap();
                            TimeStamp::compose(ts.physical as u64, ts.logical as u64)
                        })
                        .map_err(Into::into)
                };

                match res {
                    Ok(ts) => {
                        concurrency_manager.update_max_ts(ts);
                        success = txn_ext
                            .max_ts_sync_status
                            .compare_exchange(
                                initial_status,
                                initial_status | 1,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            )
                            .is_ok();
                        break;
                    }
                    Err(e) => {
                        warn!(
                            logger,
                            "failed to update max timestamp for region {}: {:?}", region_id, e
                        );
                    }
                }
            }

            if success {
                info!(logger, "succeed to update max timestamp"; "region_id" => region_id);
            } else {
                info!(
                    logger,
                    "updating max timestamp is stale";
                    "region_id" => region_id,
                    "initial_status" => initial_status,
                );
            }
        };

        #[cfg(feature = "failpoints")]
        let delay = (|| {
            fail_point!("delay_update_max_ts", |_| true);
            false
        })();
        #[cfg(not(feature = "failpoints"))]
        let delay = false;

        if delay {
            info!(self.logger, "[failpoint] delay update max ts for 1s"; "region_id" => region_id);
            let deadline = Instant::now() + Duration::from_secs(1);
            self.remote
                .spawn(GLOBAL_TIMER_HANDLE.delay(deadline).compat().then(|_| f));
        } else {
            self.remote.spawn(f);
        }
    }

    fn maybe_create_transport(&mut self) -> bool {
        if self.tso_transport.is_some() {
            return true;
        }
        match self
            .pd_client
            .create_tso_stream(mpsc::WakePolicy::Immediately)
        {
            Err(e) => error!(self.logger, "failed to create tso stream"; "err" => %e),
            Ok((tx, mut rx)) => {
                let store_id = self.store_id;
                let logger = self.logger.clone();
                let (caller_tx, mut caller_rx) =
                    mpsc::unbounded::<CallerChannel>(mpsc::WakePolicy::Immediately);
                self.remote.spawn(async move {
                    while let Some(resp) = rx.next().await {
                        if let Some(caller) = caller_rx.next().await {
                            let _ = caller.send(resp);
                        } else {
                            panic!("cannot pair a caller with tso response");
                        }
                    }
                    info!(
                        logger,
                        "tso response handler exit";
                        "store_id" => store_id,
                    );
                });
                self.tso_transport = Some((tx, caller_tx))
            }
        };
        self.tso_transport.is_some()
    }
}
