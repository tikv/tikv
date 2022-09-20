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
    batch::ApplyContext,
    fsm::{ApplyFsmDelegate, PeerFsm, PeerFsmDelegate},
    raft::{write_initial_states, write_peer_state, Peer, Storage},
    router::PeerMsg,
};

fn create_read_delegate<EK: KvEngine, ER: RaftEngine>(peer: &Peer<EK, ER>) -> ReadDelegate {
    let region = peer.region().clone();
    let region_id = region.get_id();
    let peer_id = peer.peer().get_id();
    ReadDelegate {
        region: Arc::new(region),
        peer_id,
        term: peer.term(),
        applied_term: peer.storage().applied_term(),
        leader_lease: None,
        last_valid_ts: Timespec::new(0, 0),
        tag: format!("[region {}] {}", region_id, peer_id),
        txn_extra_op: peer.txn_extra_op.clone(),
        txn_ext: peer.txn_ext.clone(),
        read_progress: peer.read_progress.clone(),
        pending_remove: false,
        bucket_meta: peer.region_buckets.as_ref().map(|b| b.meta.clone()),
        track_ver: TrackVer::new(),
    }
}

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

impl<'a, EK: KvEngine, ER: RaftEngine> ApplyFsmDelegate<'a, EK, ER> {
    pub fn exec_batch_split(
        &mut self,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();
        let ctx = &mut self.apply_ctx;

        let split_reqs = req.get_splits();
        let mut derived = self.region.clone();
        let region_id = derived.id;

        let mut split_keys = validate_and_get_split_keys(split_reqs, &self.region)?;

        info!(
            self.fsm.apply.logger(),
            "split region";
            "region_id" => region_id,
            "peer_id" => 0, // FIXME
            "region" => ?derived,
            "keys" => %KeysInfoFormatter(split_keys.iter()),
        );

        let (regions, mut new_split_regions) =
            init_split_regions(ctx.store_id, split_reqs, &mut derived, &mut split_keys);

        amend_new_split_regions(
            0, // FIXME
            self.fsm.apply.logger().list(),
            &mut new_split_regions,
            &ctx.pending_create_peers,
            &|region_id| ctx.raft_engine.get_region_state(region_id),
        );

        let region_id = derived.get_id();
        let tablet = self.fsm.apply.tablet().unwrap();
        for &cf in DATA_CFS {
            let mut cf_option = tablet.get_options_cf(cf).unwrap();
            cf_option.set_disable_auto_compactions(true);
        }
        tablet.flush_cfs(true).unwrap();
        let current_tablet_path = std::path::Path::new(tablet.path());

        let mut wb = ctx.raft_engine.log_batch(10);
        for new_region in &regions {
            let new_region_id = new_region.id;
            if new_region_id == region_id {
                continue;
            }
            let new_split_peer = new_split_regions.get(&new_region_id).unwrap();
            if let Some(ref r) = new_split_peer.result {
                warn!(
                    self.fsm.apply.logger(),
                    "new region from splitting already exists";
                    "new_region_id" => new_region.get_id(),
                    "new_peer_id" => new_split_peer.peer_id,
                    "reason" => r,
                    "region_id" => region_id,
                    "peer_id" => 0, // FIXME
                );
                continue;
            }
            let new_tablet_path = ctx
                .tablet_factory
                .tablet_path(new_region_id, RAFT_INIT_LOG_INDEX);

            hard_link_tablet(current_tablet_path, &new_tablet_path).unwrap_or_else(|e| {
                panic!(
                    "{} fails to hard link rocksdb with path {:?}: {:?}",
                    self.tag, new_tablet_path, e
                )
            });

            write_initial_states(&mut wb, new_region.clone()).unwrap_or_else(|e| {
                panic!(
                    "{} fails to save split region {:?}: {:?}",
                    self.tag, new_region, e
                )
            });
        }
        write_peer_state(&mut wb, derived.clone(), log_index).unwrap_or_else(|e| {
            panic!("{} fails to update region {:?}: {:?}", self.tag, derived, e)
        });
        ctx.raft_engine
            .consume(&mut wb, true)
            .unwrap_or_else(|e| panic!("{} fails to consume the write: {:?}", self.tag, e,));

        // Change the tablet path by the new tablet index
        let _ = ctx
            .tablet_factory
            .load_tablet(current_tablet_path, region_id, log_index)
            .unwrap();

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
            .approximate_size
            .map(|v| v / new_region_count);
        let estimated_keys = self
            .fsm
            .peer()
            .approximate_keys
            .map(|v| v / new_region_count);
        let mut meta = self
            .store_ctx
            .store_meta
            .as_ref()
            .unwrap()
            .as_ref()
            .lock()
            .unwrap();
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
            self.fsm.peer_mut().approximate_size = estimated_size;
            self.fsm.peer_mut().approximate_keys = estimated_keys;
            // self.fsm.peer.heartbeat_pd(self.ctx);
            info!(
                self.store_ctx.logger,
                "notify pd with split";
                "region_id" => self.fsm.region_id(),
                "peer_id" => self.fsm.peer_id(),
                "split_count" => regions.len(),
            );
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
                .insert(new_region_id, create_read_delegate(new_peer.peer()));
            meta.region_read_progress
                .insert(new_region_id, new_peer.peer().read_progress.clone());

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
    use engine_test::{
        ctor::{CfOptions, DbOptions},
        kv::TestTabletFactoryV2,
        raft,
    };
    use engine_traits::ALL_CFS;
    use futures::channel::mpsc::unbounded;
    use kvproto::raft_cmdpb::{BatchSplitRequest, SplitRequest};
    use raftstore::store::{util::new_learner_peer, Config};
    use slog::o;
    use tempfile::TempDir;
    use tikv_util::{
        config::VersionTrack,
        worker::{FutureScheduler, Scheduler},
    };
    use util::new_peer;

    use super::*;
    use crate::{batch::ApplyPoller, fsm::ApplyFsm, raft::Apply, tablet::CachedTablet};

    fn new_split_req(key: &[u8], id: u64, children: Vec<u64>) -> SplitRequest {
        let mut req = SplitRequest::default();
        req.set_split_key(key.to_vec());
        req.set_new_region_id(id);
        req.set_new_peer_ids(children);
        req
    }

    #[test]
    fn test_split() {
        let store_id = 2;

        let mut region = Region::default();
        region.set_id(1);
        region.set_end_key(b"k5".to_vec());
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

        let mut apply_ctx = ApplyContext::new(
            Config::default(),
            store_id,
            raft_engine.clone(),
            factory.clone(),
        );

        let tablet = factory
            .open_tablet(
                region.id,
                Some(5),
                OpenOptions::default().set_create_new(true),
            )
            .unwrap();

        let apply = Apply::mock(CachedTablet::new(Some(tablet)), logger.clone());
        let (sender, mut apply_fsm) = ApplyFsm::new(apply);
        let mut apply_fsm_delegate = ApplyFsmDelegate::new(&mut apply_fsm, &mut apply_ctx);
        apply_fsm_delegate.region = region;

        let split_region_id = 8;
        let mut splits = BatchSplitRequest::default();
        splits.set_right_derive(true);
        splits
            .mut_requests()
            .push(new_split_req(b"k1", split_region_id, vec![9, 10, 11]));

        let log_index = 10;
        let mut req = AdminRequest::default();
        req.set_splits(splits);
        let (resp, apply_res) = apply_fsm_delegate
            .exec_batch_split(&req, log_index)
            .unwrap();

        let regions = resp.get_splits().get_regions();
        assert!(regions.len() == 2);

        // check derived region
        let state = raft_engine.get_region_state(1).unwrap().unwrap();
        assert_eq!(state.get_region().get_start_key().to_vec(), b"k1".to_vec());
        assert_eq!(state.get_region().get_end_key().to_vec(), b"k5".to_vec());
        assert_eq!(state.tablet_index, log_index);
        // assert we can read the tablet by the new tablet_index
        let tablet_path = factory.tablet_path(1, 10);
        assert!(factory.exists_raw(&tablet_path));

        // check split region
        let state = raft_engine
            .get_region_state(split_region_id)
            .unwrap()
            .unwrap();
        assert_eq!(state.get_region().get_start_key().to_vec(), b"".to_vec());
        assert_eq!(state.get_region().get_end_key().to_vec(), b"k1".to_vec());
        assert_eq!(state.tablet_index, RAFT_INIT_LOG_INDEX);
        let tablet_path = factory.tablet_path(split_region_id, RAFT_INIT_LOG_INDEX);
        assert!(factory.exists_raw(&tablet_path));
    }
}
