// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, io, sync::Arc};

use batch_system::BasicMailbox;
use collections::{HashMap, HashMapEntry, HashSet};
use crossbeam::channel::internal::SelectHandle;
use engine_traits::{
    CfOptions, KvEngine, MiscExt, OpenOptions, RaftEngine, RaftEngineReadOnly, TabletFactory,
    DATA_CFS,
};
use keys::enc_end_key;
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminRequest, AdminResponse},
};
use raftstore::{
    coprocessor::RegionChangeReason,
    store::{
        fsm::{
            apply::{
                self, amend_new_split_regions, init_split_regions, validate_and_get_split_keys,
                ApplyResult, NewSplitPeer,
            },
            ExecResult,
        },
        metrics::PEER_ADMIN_CMD_COUNTER,
        util::{self, KeysInfoFormatter},
        ReadDelegate, TrackVer, Transport, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use slog::{info, warn};
use tikv_util::box_err;
use time::Timespec;

use crate::{
    fsm::{PeerFsm, PeerFsmDelegate},
    raft::{write_initial_states, write_peer_state, Apply, Peer, Storage},
    router::PeerMsg,
};

fn hard_link_tablet(
    src_tablet_dir: &std::path::Path,
    target_tablet_dir: &std::path::Path,
) -> io::Result<()> {
    std::fs::create_dir_all(target_tablet_dir)?;
    for file in std::fs::read_dir(src_tablet_dir)? {
        let file = file?;
        std::fs::hard_link(file.path(), target_tablet_dir.join(file.file_name()))?;
    }

    Ok(())
}

impl<EK: KvEngine, ER: RaftEngine, R> Apply<EK, ER, R> {
    pub fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();

        let split_reqs = req.get_splits();
        let mut derived = self.region_state().get_region().clone();
        let region_id = derived.id;

        let mut split_keys =
            validate_and_get_split_keys(split_reqs, self.region_state().get_region())?;

        info!(
            self.logger,
            "split region";
            "region_id" => region_id,
            "peer_id" => 0, // FIXME
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(split_keys.iter()),
        );

        let (regions, mut new_split_regions) =
            init_split_regions(self.store_id, split_reqs, &mut derived, &mut split_keys);

        amend_new_split_regions(
            0, // FIXME
            self.logger.list(),
            &mut new_split_regions,
            self.pending_create_peers(),
            &|region_id| self.raft_engine().get_region_state(region_id),
        );

        let region_id = derived.get_id();
        let tablet = self.tablet().unwrap();
        for &cf in DATA_CFS {
            let mut cf_option = tablet.get_options_cf(cf).unwrap();
            cf_option.set_disable_auto_compactions(true);
        }
        tablet.flush_cfs(true).unwrap();
        let current_tablet_path = std::path::Path::new(tablet.path());

        let mut wb = self.raft_engine().log_batch(10);
        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }
            let new_split_peer = new_split_regions.get(&new_region_id).unwrap();
            if let Some(ref r) = new_split_peer.result {
                warn!(
                    self.logger,
                    "new region from splitting already exists";
                    "new_region_id" => new_region.get_id(),
                    "new_peer_id" => new_split_peer.peer_id,
                    "reason" => r,
                    "region_id" => region_id,
                    "peer_id" => 0, // FIXME
                );
                continue;
            }
            let new_tablet_path = self
                .tablet_factory
                .tablet_path(new_region_id, RAFT_INIT_LOG_INDEX);

            // Create the new tablet for the split region by hardlink the tablet being
            // split.
            hard_link_tablet(current_tablet_path, &new_tablet_path).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to hard link rocksdb with path {:?}: {:?}",
                    self.logger.list(),
                    new_tablet_path,
                    e
                )
            });

            write_initial_states(&mut wb, new_region.clone()).unwrap_or_else(|e| {
                panic!(
                    "{:?} fails to save split region {:?}: {:?}",
                    self.logger.list(),
                    new_region,
                    e
                )
            });
        }

        // Change the tablet path by using the new tablet suffix
        let tablet = self
            .tablet_factory
            .load_tablet(current_tablet_path, region_id, log_index)
            .unwrap();
        self.publish_tablet(tablet);

        write_peer_state(&mut wb, derived.clone(), log_index).unwrap_or_else(|e| {
            panic!(
                "{:?} fails to update region {:?}: {:?}",
                self.logger.list(),
                derived,
                e
            )
        });
        self.raft_engine.consume(&mut wb, true).unwrap_or_else(|e| {
            panic!(
                "{:?} fails to consume the write: {:?}",
                self.logger.list(),
                e,
            )
        });

        let mut resp = AdminResponse::default();
        resp.mut_splits().set_regions(regions.clone().into());
        PEER_ADMIN_CMD_COUNTER.batch_split.success.inc();

        Ok((
            resp,
            ApplyResult::Res(ExecResult::SplitRegion {
                regions,
                derived,
                new_split_regions,
            }),
        ))
    }
}

impl<'a, EK: KvEngine, ER: RaftEngine, T: Transport> PeerFsmDelegate<'a, EK, ER, T> {
    pub fn on_ready_split_region(
        &mut self,
        derived: Region,
        regions: Vec<Region>,
        new_split_regions: HashMap<u64, apply::NewSplitPeer>,
    ) {
        let region_id = derived.get_id();

        // Group in-memory pessimistic locks in the original region into new regions.
        // The locks of new regions will be put into the corresponding new regions
        // later. And the locks belonging to the old region will stay in the original
        // map.
        let region_locks = {
            let mut pessimistic_locks = self.fsm.peer().txn_ext.pessimistic_locks.write();
            info!(self.store_ctx.logger, "moving {} locks to new regions", pessimistic_locks.len(); "region_id"=> region_id);
            // Update the version so the concurrent reader will fail due to EpochNotMatch
            // instead of PessimisticLockNotFound.
            pessimistic_locks.version = derived.get_region_epoch().get_version();
            pessimistic_locks.group_by_regions(&regions, &derived)
        };

        // Roughly estimate the size and keys for new regions.
        let new_region_count = regions.len() as u64;
        let estimated_size = self
            .fsm
            .peer()
            .approximate_size()
            .map(|v| v / new_region_count);
        let estimated_keys = self
            .fsm
            .peer()
            .approximate_keys()
            .map(|v| v / new_region_count);
        let mut meta = self.store_ctx.store_meta.lock().unwrap();
        meta.set_region(
            self.store_ctx.coprocessor_host.as_ref().unwrap(),
            derived,
            self.fsm.peer_mut(),
            RegionChangeReason::Split,
        );
        self.fsm.peer_mut().post_split();

        // It's not correct anymore, so set it to false to schedule a split
        // check task.
        // self.fsm.peer.may_skip_split_check = false;

        let is_leader = self.fsm.peer().is_leader();
        if is_leader {
            self.fsm.peer_mut().set_approximate_size(estimated_size);
            self.fsm.peer_mut().set_approximate_keys(estimated_keys);

            // todo(SpadeA): report regions to PD
            // self.fsm.peer.heartbeat_pd(self.ctx);
            // info!(
            //     self.store_ctx.logger,
            //     "notify pd with split";
            //     "region_id" => self.fsm.region_id(),
            //     "peer_id" => self.fsm.peer_id(),
            //     "split_count" => regions.len(),
            // );
            // // Now pd only uses ReportBatchSplit for history operation show,
            // // so we send it independently here.
            // let task = PdTask::ReportBatchSplit {
            //     regions: regions.to_vec(),
            // };
            // if let Err(e) = self.ctx.pd_scheduler.schedule(task) {
            //     error!(
            //         "failed to notify pd";
            //         "region_id" => self.fsm.region_id(),
            //         "peer_id" => self.fsm.peer_id(),
            //         "err" => %e,
            //     );
            // }
        }

        let last_key = enc_end_key(regions.last().unwrap());
        if meta.region_ranges.remove(&last_key).is_none() {
            panic!(
                "{:?} original region should exist",
                self.store_ctx.logger.list()
            );
        }
        let last_region_id = regions.last().unwrap().get_id();
        for (new_region, locks) in regions.into_iter().zip(region_locks) {
            let new_region_id = new_region.get_id();

            if new_region_id == region_id {
                let not_exist = meta
                    .region_ranges
                    .insert(enc_end_key(&new_region), new_region_id)
                    .is_none();
                assert!(not_exist, "[region {}] should not exist", new_region_id);
                continue;
            }

            // Check if this new region should be splitted
            let new_split_peer = new_split_regions.get(&new_region.get_id()).unwrap();
            if new_split_peer.result.is_some() {
                unimplemented!()
            }

            // Now all checking passed.
            {
                let mut pending_create_peers = self.store_ctx.pending_create_peers.lock().unwrap();
                assert_eq!(
                    pending_create_peers.remove(&new_region_id),
                    Some((new_split_peer.peer_id, true))
                );
            }

            // Insert new regions and validation
            info!(
                self.store_ctx.logger,
                "insert new region";
                "region_id" => new_region_id,
                "region" => ?new_region,
            );

            if let Some(r) = meta.regions.get(&new_region_id) {
                // Suppose a new node is added by conf change and the snapshot comes slowly.
                // Then, the region splits and the first vote message comes to the new node
                // before the old snapshot, which will create an uninitialized peer on the
                // store. After that, the old snapshot comes, followed with the last split
                // proposal. After it's applied, the uninitialized peer will be met.
                // We can remove this uninitialized peer directly.
                if util::is_region_initialized(r) {
                    panic!(
                        "[region {}] duplicated region {:?} for split region {:?}",
                        new_region_id, r, new_region
                    );
                }
                self.store_ctx.router.close(new_region_id);
            }

            let storage = Storage::new(
                new_region_id,
                self.store_ctx.store.id,
                self.store_ctx.engine.clone(),
                self.store_ctx.log_fetch_scheduler.clone(),
                &self.store_ctx.logger,
            )
            .unwrap_or_else(|e| panic!("fail to create storage: {:?}", e))
            .unwrap();

            let (sender, mut new_peer) = match PeerFsm::new(
                &self.store_ctx.cfg,
                self.store_ctx.tablet_factory.as_ref(),
                storage,
            ) {
                Ok((sender, new_peer)) => (sender, new_peer),
                Err(e) => {
                    // peer information is already written into db, can't recover.
                    // there is probably a bug.
                    panic!("create new split region {:?} err {:?}", new_region, e);
                }
            };

            // let mut replication_state =
            // self.ctx.global_replication_state.lock().unwrap();
            // new_peer.peer.init_replication_mode(&mut replication_state);
            // drop(replication_state);

            let meta_peer = new_peer.peer().peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache
                new_peer.peer_mut().insert_peer_cache(p.clone());
            }

            // New peer derive write flow from parent region,
            // this will be used by balance write flow.
            new_peer.peer_mut().peer_stat = self.fsm.peer().peer_stat.clone();
            new_peer.peer_mut().last_compacted_idx = new_peer
                .peer()
                .storage()
                .apply_state()
                .get_truncated_state()
                .get_index()
                + 1;
            let campaigned = new_peer.peer_mut().maybe_campaign(is_leader);
            new_peer.set_has_ready(new_peer.has_ready() | campaigned);

            if is_leader {
                new_peer.peer_mut().approximate_size = estimated_size;
                new_peer.peer_mut().approximate_keys = estimated_keys;
                *new_peer.peer_mut().txn_ext.pessimistic_locks.write() = locks;
                // The new peer is likely to become leader, send a
                // heartbeatimmediately to reduce client query
                // miss. new_peer.peer.heartbeat_pd(self.ctx);
            }

            meta.tablet_caches
                .insert(new_region_id, new_peer.peer().tablet().clone());
            new_peer.peer().activate(self.store_ctx);
            meta.regions.insert(new_region_id, new_region.clone());
            let not_exist = meta
                .region_ranges
                .insert(enc_end_key(&new_region), new_region_id)
                .is_none();
            assert!(not_exist, "[region {}] should not exist", new_region_id);
            meta.readers
                .insert(new_region_id, new_peer.peer().generate_read_delegate());
            meta.region_read_progress
                .insert(new_region_id, new_peer.peer().read_progress().clone());

            if last_region_id == new_region_id {
                // To prevent from big region, the right region needs run split
                // check again after split.
                // new_peer.peer().size_diff_hint =
                // self.store_ctx.cfg.region_split_check_diff().0;
            }
            let mailbox =
                BasicMailbox::new(sender, new_peer, self.store_ctx.router.state_cnt().clone());
            self.store_ctx.router.register(new_region_id, mailbox);
            self.store_ctx
                .router
                .force_send(new_region_id, PeerMsg::Start)
                .unwrap();

            if !campaigned {
                if let Some(msg) = meta
                    .pending_msgs
                    .swap_remove_front(|m| m.get_to_peer() == &meta_peer)
                {
                    let peer_msg = PeerMsg::RaftMessage(Box::new(msg));
                    if let Err(e) = self.store_ctx.router.force_send(new_region_id, peer_msg) {
                        // warn!("handle first requset failed"; "region_id" =>
                        // region_id, "error" => ?e);
                    }
                }
            }
        }
        drop(meta);
        if is_leader {
            self.on_split_region_check_tick();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::{channel, Sender};

    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactoryV2,
        raft,
    };
    use engine_traits::ALL_CFS;
    use futures::channel::mpsc::unbounded;
    use kvproto::{
        raft_cmdpb::{BatchSplitRequest, SplitRequest},
        raft_serverpb::{PeerState, RaftApplyState, RegionLocalState},
    };
    use raftstore::store::Config;
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        config::VersionTrack,
        store::{new_learner_peer, new_peer},
        worker::{FutureScheduler, Scheduler},
    };

    use super::*;
    use crate::{fsm::ApplyFsm, raft::Apply, tablet::CachedTablet};

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    fn assert_split(
        apply: &mut Apply<engine_test::kv::KvTestEngine, raft::RaftTestEngine, Sender<u64>>,
        factory: &Arc<TestTabletFactoryV2>,
        region_to_split: &Region,
        get_region_local_state: &dyn Fn(u64) -> RegionLocalState,
        get_raft_apply_state: &dyn Fn(u64) -> RaftApplyState,
        right_derived: bool,
        new_region_ids: Vec<u64>,
        split_keys: Vec<Vec<u8>>,
        children_peers: Vec<Vec<u64>>,
        log_index: u64,
    ) -> (HashMap<u64, Region>, RegionLocalState) {
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(right_derived);
        let mut region_boundries = split_keys.clone();
        region_boundries.insert(0, region_to_split.get_start_key().to_vec());
        region_boundries.push(region_to_split.get_end_key().to_vec());

        for ((new_region_id, children), split_key) in new_region_ids
            .into_iter()
            .zip(children_peers.clone())
            .zip(split_keys)
        {
            splits
                .mut_requests()
                .push(new_split_req(&split_key, new_region_id, children));
        }

        let mut req = AdminRequest::default();
        req.set_splits(splits);

        // Exec batch split
        let (resp, apply_res) = apply.exec_batch_split(&req, log_index).unwrap();

        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == region_boundries.len() - 1);

        let mut epoch = region_to_split.get_region_epoch().clone();
        epoch.version += children_peers.len() as u64;

        let mut child_idx = 0;
        let mut region_state = RegionLocalState::default();
        for (i, region) in regions.iter().enumerate() {
            let state = get_region_local_state(region.id);
            assert_eq!(state.get_state(), PeerState::Normal);
            assert_eq!(state.get_region().get_id(), region.id);
            assert_eq!(
                state.get_region().get_start_key().to_vec(),
                region_boundries[i]
            );
            assert_eq!(
                state.get_region().get_end_key().to_vec(),
                region_boundries[i + 1]
            );
            assert_eq!(*state.get_region().get_region_epoch(), epoch);

            if region.id == region_to_split.id {
                assert_eq! {
                    state.get_region().get_peers(),
                    region_to_split.get_peers()
                }

                assert_eq!(state.tablet_index, log_index);
                // assert we can read the tablet by the new tablet_index
                let tablet_path = factory.tablet_path(region_to_split.id, log_index);
                assert!(factory.exists_raw(&tablet_path));

                region_state = state.clone();
            } else {
                assert_eq! {
                    state.get_region().get_peers().iter().map(|peer| peer.id).collect::<Vec<_>>(),
                    children_peers[child_idx]
                }
                child_idx += 1;

                assert_eq!(state.tablet_index, RAFT_INIT_LOG_INDEX);
                let tablet_path = factory.tablet_path(region.id, RAFT_INIT_LOG_INDEX);
                assert!(factory.exists_raw(&tablet_path));

                let initial_state = get_raft_apply_state(region.id);
                assert_eq!(initial_state.get_applied_index(), RAFT_INIT_LOG_INDEX);
                assert_eq!(
                    initial_state.get_truncated_state().get_index(),
                    RAFT_INIT_LOG_INDEX
                );
                assert_eq!(
                    initial_state.get_truncated_state().get_term(),
                    RAFT_INIT_LOG_INDEX
                );
            }
        }

        (
            regions
                .iter()
                .map(|region| (region.id, region.clone()))
                .collect(),
            region_state,
        )
    }

    #[test]
    fn test_split() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k10".to_vec());
        region.mut_region_epoch().set_version(3);
        let peers = vec![new_peer(2, 3), new_peer(4, 5), new_learner_peer(6, 7)];
        region.set_peers(peers.into());

        let logger = slog_global::borrow_global().new(o!());
        let path = TempDir::new().unwrap();
        let cf_opts = ALL_CFS
            .iter()
            .copied()
            .map(|cf| (cf, CfOptions::default()))
            .collect();
        let factory = Arc::new(TestTabletFactoryV2::new(
            path.path(),
            DbOptions::default(),
            cf_opts,
        ));
        let raft_engine =
            raft::new_engine(&format!("{}", path.path().join("raft").display()), None).unwrap();

        let tablet = factory
            .open_tablet(
                region.id,
                Some(5),
                OpenOptions::default().set_create_new(true),
            )
            .unwrap();

        let mut region_state = RegionLocalState::default();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region.clone());
        region_state.set_tablet_index(5);
        let (tx, rx) = channel();
        let mut apply = Apply::new(
            store_id,
            region_state,
            tx,
            CachedTablet::new(Some(tablet)),
            raft_engine.clone(),
            factory.clone(),
            logger.clone(),
        );

        let get_region_local_state = |region_id| -> RegionLocalState {
            raft_engine.get_region_state(region_id).unwrap().unwrap()
        };

        let get_raft_apply_state = |region_id| -> RaftApplyState {
            raft_engine.get_apply_state(region_id).unwrap().unwrap()
        };

        let mut log_index = 10;

        // After split: region 1 ["", "k09"], region 2 ["k09", "k10"]
        let (regions, region_state) = assert_split(
            &mut apply,
            &factory,
            &region,
            &get_region_local_state,
            &get_raft_apply_state,
            false,
            vec![10],
            vec![b"k09".to_vec()],
            vec![vec![11, 12, 13]],
            log_index,
        );
        // update region state in apply manually
        apply.set_region_state(region_state);

        log_index = 20;
        // After split: region 3 ["", "k01"], region  1 ["k01", "k09"]
        let (regions, region_state) = assert_split(
            &mut apply,
            &factory,
            regions.get(&1).unwrap(),
            &get_region_local_state,
            &get_raft_apply_state,
            true,
            vec![3],
            vec![b"k01".to_vec()],
            vec![vec![21, 22, 23]],
            log_index,
        );
        // update region state in apply manually
        apply.set_region_state(region_state);
    }
}
