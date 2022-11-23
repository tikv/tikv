// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use causal_ts::CausalTsProvider;
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use futures::{compat::Future01CompatExt, FutureExt};
use pd_client::PdClient;
use raftstore::{store::TxnExt, Result};
use slog::{info, warn};
use tikv_util::{box_err, timer::GLOBAL_TIMER_HANDLE};
use txn_types::TimeStamp;

use super::Runner;

impl<EK, ER, T> Runner<EK, ER, T>
where
    EK: KvEngine,
    ER: RaftEngine,
    T: PdClient + 'static,
{
    pub fn handle_update_max_timestamp(
        &mut self,
        region_id: u64,
        initial_status: u64,
        txn_ext: Arc<TxnExt>,
    ) {
        let pd_client = self.pd_client.clone();
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
                    pd_client.get_tso().await.map_err(Into::into)
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
}
