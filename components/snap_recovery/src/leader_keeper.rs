// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashSet,
    marker::PhantomData,
    sync::Mutex,
    time::{Duration, Instant},
};

use engine_traits::KvEngine;
use futures::compat::Future01CompatExt;
use raftstore::{
    errors::{Error, Result},
    store::{Callback, CasualMessage, CasualRouter, SignificantMsg, SignificantRouter},
};
use tikv_util::{future::paired_future_callback, timer::GLOBAL_TIMER_HANDLE};

pub struct LeaderKeeper<'a, EK, Router: 'a> {
    router: Router,
    not_leader: HashSet<u64>,

    _ek: PhantomData<&'a EK>,
}

#[derive(Default)]
pub struct StepResult {
    pub failed_leader: Vec<(u64, Error)>,
    pub campaign_failed: Vec<(u64, Error)>,
}

fn ellipse<T: std::fmt::Debug>(ts: &[T], max_len: usize) -> String {
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

impl<'a, EK, Router> LeaderKeeper<'a, EK, Router>
where
    EK: KvEngine,
    Router: CasualRouter<EK> + SignificantRouter<EK> + 'a,
{
    pub fn new(router: Router, to_keep: impl IntoIterator<Item = u64>) -> Self {
        Self {
            router,

            not_leader: to_keep.into_iter().collect(),
            _ek: PhantomData,
        }
    }

    pub async fn elect_and_wait_all_ready(&mut self) {
        loop {
            let now = Instant::now();
            let res = self.step().await;
            info!("finished leader keeper stepping."; "result" => ?res, "take" => ?now.elapsed());
            GLOBAL_TIMER_HANDLE
                .delay(now + Duration::from_secs(10))
                .compat()
                .await
                .expect("wrong with global timer, cannot stepping.");
            if res.failed_leader.is_empty() {
                return;
            }
        }
    }

    pub async fn step(&mut self) -> StepResult {
        const CONCURRENCY: usize = 256;
        let r = Mutex::new(StepResult::default());
        let success = Mutex::new(HashSet::new());
        let regions = self.not_leader.iter().copied().collect::<Vec<_>>();
        for batch in regions.as_slice().chunks(CONCURRENCY) {
            let tasks = batch.iter().map(|region_id| async {
                match self.check_leader(*region_id).await {
                    Ok(_) => {
                        success.lock().unwrap().insert(*region_id);
                        return;
                    }
                    Err(err) => r.lock().unwrap().failed_leader.push((*region_id, err)),
                };

                if let Err(err) = self.force_leader(*region_id) {
                    r.lock().unwrap().campaign_failed.push((*region_id, err));
                }
            });
            futures::future::join_all(tasks).await;
        }
        success.lock().unwrap().iter().for_each(|i| {
            debug_assert!(self.not_leader.remove(i));
        });
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
        self.router.send(region_id, msg)?;
        // We have nothing to do...
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{cell::RefCell, collections::HashSet};

    use engine_rocks::RocksEngine;
    use engine_traits::KvEngine;
    use futures::executor::block_on;
    use kvproto::raft_cmdpb;
    use raftstore::store::{CasualRouter, SignificantRouter};

    use super::LeaderKeeper;

    #[derive(Default)]
    struct MockStore {
        regions: HashSet<u64>,
        leaders: RefCell<HashSet<u64>>,
    }

    impl<'a, EK, Router> LeaderKeeper<'a, EK, Router> {
        fn mut_router(&mut self) -> &mut Router {
            &mut self.router
        }
    }

    // impl SignificantRouter for MockStore, which only handles `LeaderCallback`,
    // return success when source region is leader, otherwise fill the error in
    // header.
    impl<EK: KvEngine> SignificantRouter<EK> for MockStore {
        fn significant_send(
            &self,
            region_id: u64,
            msg: raftstore::store::SignificantMsg<EK::Snapshot>,
        ) -> raftstore::errors::Result<()> {
            match msg {
                raftstore::store::SignificantMsg::LeaderCallback(cb) => {
                    let mut resp = raft_cmdpb::RaftCmdResponse::default();
                    let mut header = raft_cmdpb::RaftResponseHeader::default();
                    if !self.leaders.borrow().contains(&region_id) {
                        let mut err = kvproto::errorpb::Error::default();
                        err.set_not_leader(kvproto::errorpb::NotLeader::new());
                        header.set_error(err);
                    }
                    resp.set_header(header);
                    cb.invoke_with_response(resp);
                    Ok(())
                }
                _ => panic!("unexpected msg"),
            }
        }
    }

    // impl CasualRouter for MockStore, which only handles `Campaign`,
    // add the region to leaders list when handling it.
    impl<EK: KvEngine> CasualRouter<EK> for MockStore {
        fn send(
            &self,
            region_id: u64,
            msg: raftstore::store::CasualMessage<EK>,
        ) -> raftstore::errors::Result<()> {
            match msg {
                raftstore::store::CasualMessage::Campaign => {
                    if !self.regions.contains(&region_id) {
                        return Err(raftstore::Error::RegionNotFound(region_id));
                    }
                    self.leaders.borrow_mut().insert(region_id);
                    Ok(())
                }
                _ => panic!("unexpected msg"),
            }
        }
    }

    #[test]
    fn test_basic() {
        let leaders = vec![1, 2, 3];
        let mut store = MockStore::default();
        store.regions = leaders.iter().copied().collect();
        let mut lk = LeaderKeeper::<RocksEngine, _>::new(store, leaders);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 3);
        assert_eq!(res.campaign_failed.len(), 0);
    }

    #[test]
    fn test_failure() {
        let leaders = [1, 2, 3];
        let mut store = MockStore::default();
        store.regions = leaders.iter().copied().collect();
        let mut lk = LeaderKeeper::<RocksEngine, _>::new(store, vec![1, 2, 3, 4]);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 4);
        assert_eq!(res.campaign_failed.len(), 1);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 1);
        assert_eq!(res.campaign_failed.len(), 1);
        lk.mut_router().regions.insert(4);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 1);
        assert_eq!(res.campaign_failed.len(), 0);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 0);
        assert_eq!(res.campaign_failed.len(), 0);
    }

    #[test]
    fn test_many_regions() {
        let leaders = std::iter::repeat_with({
            let mut x = 0;
            move || {
                x += 1;
                x
            }
        })
        .take(2049)
        .collect::<Vec<_>>();
        let mut store = MockStore::default();
        store.regions = leaders.iter().copied().collect();
        let mut lk = LeaderKeeper::<RocksEngine, _>::new(store, leaders);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 2049);
        assert_eq!(res.campaign_failed.len(), 0);
        let res = block_on(lk.step());
        assert_eq!(res.failed_leader.len(), 0);
        assert_eq!(res.campaign_failed.len(), 0);
    }
}
