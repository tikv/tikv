// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use engine_traits::{KvEngine, RaftEngine};
use futures::{compat::Future01CompatExt, stream::AbortHandle};
use raftstore::{
    errors::{Error, Result},
    router::RaftStoreRouter,
    store::{Callback, CasualMessage, RaftRouter, SignificantMsg, SignificantRouter},
};
use tikv_util::{
    future::paired_future_callback, sys::thread::StdThreadBuildWrapper, timer::GLOBAL_TIMER_HANDLE,
};

pub struct LeaderKeeper<EK: KvEngine, ER: RaftEngine> {
    router: RaftRouter<EK, ER>,
    to_keep: Vec<u64>,
}

#[derive(Default)]
pub struct StepResult {
    pub failed_leader: Vec<(u64, Error)>,
    pub campaign_failed: Vec<(u64, Error)>,
}

fn ellipse<'a, T: std::fmt::Debug>(ts: &'a [T], max_len: usize) -> String {
    if ts.len() < max_len {
        return format!("{:?}", &ts);
    }
    format!("{:?} (and {} more)", &ts[..max_len], ts.len() - max_len)
}

impl std::fmt::Debug for StepResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StepResult")
            .field(
                "failed_leader",
                &format_args!("{}", ellipse(&self.failed_leader, 8)),
            )
            .field(
                "campaign_failed",
                &format_args!("{}", ellipse(&self.campaign_failed, 8)),
            )
            .finish()
    }
}

impl<EK: KvEngine, ER: RaftEngine> LeaderKeeper<EK, ER> {
    pub fn new(router: RaftRouter<EK, ER>, to_keep: Vec<u64>) -> Self {
        Self { router, to_keep }
    }

    pub async fn spawn_loop(self) -> Option<AbortHandle> {
        let (tx, rx) = futures::channel::oneshot::channel();
        let res = std::thread::Builder::new()
                .name("leader_keeper".to_owned())
                .spawn_wrapper(move || {
                    let keep_loop = async move {
                        loop {
                            let now = Instant::now();
                            GLOBAL_TIMER_HANDLE
                                .delay(now + Duration::from_secs(30))
                                .compat()
                                .await
                                .expect("wrong with global timer, cannot stepping.");
                            let res = self.step().await;
                            info!("finished leader keeper stepping."; "result" => ?res, "take" => ?now.elapsed());
                        }
                    };
                    let (keep_loop, cancel) = futures::future::abortable(keep_loop);
                    if tx.send(cancel).is_err() {
                        // The outer function has gone.
                        info!("special path: the rpc finished so fast (faster than starting a background thread).");
                        return;
                    }
                    let _ = futures::executor::block_on(keep_loop);
                });
        res.map_err(|err| warn!("failed to start leader keeper, we might be stuck when there are too many regions."; "err" => %err))
            .ok()?;
        rx.await.ok()
    }

    pub async fn step(&self) -> StepResult {
        const CONCURRENCY: usize = 1024;
        let r = Mutex::new(StepResult::default());
        for batch in self.to_keep.as_slice().chunks(CONCURRENCY) {
            let tasks = batch.iter().map(|region_id| async {
                match self.check_leader(*region_id).await {
                    Err(err) => r.lock().unwrap().failed_leader.push((*region_id, err)),
                    Ok(_) => return,
                };

                match self.force_leader(*region_id) {
                    Err(err) => r.lock().unwrap().campaign_failed.push((*region_id, err)),
                    Ok(_) => return,
                }
            });
            futures::future::join_all(tasks).await;
        }
        r.into_inner().unwrap()
    }

    async fn check_leader(&self, region_id: u64) -> Result<()> {
        let (cb, fut) = paired_future_callback();
        let msg = SignificantMsg::LeaderCallback(Callback::<EK::Snapshot>::read(cb));
        self.router.significant_send(region_id, msg)?;
        let resp = fut
            .await
            .map_err(|_err| Error::Other("canceled by store".into()))?;
        let header = resp.response.get_header();
        if header.has_error() {
            return Err(Error::Other(box_err!(
                "got error: {:?}",
                header.get_error()
            )));
        }
        Ok(())
    }

    fn force_leader(&self, region_id: u64) -> Result<()> {
        let msg = CasualMessage::Campaign;
        self.router.send_casual_msg(region_id, msg)?;
        // We have nothing to do...
        Ok(())
    }
}
