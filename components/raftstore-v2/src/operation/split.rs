// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, sync::Arc};

use collections::{HashMap, HashMapEntry, HashSet};
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
            apply::{self, ApplyResult, NewSplitPeer},
            ExecResult,
        },
        metrics::PEER_ADMIN_CMD_COUNTER,
        util, ReadDelegate, TrackVer, Transport, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use tikv_util::box_err;
use time::Timespec;

use crate::{
    batch::ApplyContext,
    fsm::{ApplyFsmDelegate, PeerFsm, PeerFsmDelegate},
    raft::{write_initial_states, write_peer_state, Peer, Storage},
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

impl<'a, EK: KvEngine, ER: RaftEngine> ApplyFsmDelegate<'a, EK, ER> {
    pub fn exec_batch_split(
        &mut self,
        ctx: &mut ApplyContext<EK, ER>,
        req: &AdminRequest,
        log_index: u64,
    ) -> Result<(AdminResponse, ApplyResult<EK::Snapshot>)> {
        PEER_ADMIN_CMD_COUNTER.batch_split.all.inc();
        let split_reqs = req.get_splits();
        let right_derive = split_reqs.get_right_derive();
        if split_reqs.get_requests().is_empty() {
            return Err(box_err!("missing split requests"));
        }

        let mut derived = self.region.clone();
        let new_region_cnt = split_reqs.get_requests().len();
        let mut regions = Vec::with_capacity(new_region_cnt + 1);
        let mut keys: VecDeque<Vec<u8>> = VecDeque::with_capacity(new_region_cnt + 1);
        for req in split_reqs.get_requests() {
            let split_key = req.get_split_key();
            if split_key.is_empty() {
                return Err(box_err!("missing split key"));
            }
            if split_key
                <= keys
                    .back()
                    .map_or_else(|| derived.get_start_key(), Vec::as_slice)
            {
                return Err(box_err!("invalid split request: {:?}", split_reqs));
            }
            if req.get_new_peer_ids().len() != derived.get_peers().len() {
                return Err(box_err!(
                    "invalid new peer id count, need {:?}, but got {:?}",
                    derived.get_peers(),
                    req.get_new_peer_ids()
                ));
            }
            keys.push_back(split_key.to_vec());
        }

        util::check_key_in_region(keys.back().unwrap(), &self.region)?;
        // info!(
        //     "split region";
        //     "region_id" => self.region_id(),
        //     "peer_id" => self.id(),
        //     "region" => ?derived,
        //     "keys" => %KeysInfoFormatter(keys.iter()),
        // );
        let new_version = derived.get_region_epoch().get_version() + new_region_cnt as u64;
        derived.mut_region_epoch().set_version(new_version);
        // Note that the split requests only contain ids for new regions, so we need
        // to handle new regions and old region separately.
        if right_derive {
            // So the range of new regions is [old_start_key, split_key1, ...,
            // last_split_key].
            keys.push_front(derived.get_start_key().to_vec());
        } else {
            // So the range of new regions is [split_key1, ..., last_split_key,
            // old_end_key].
            keys.push_back(derived.get_end_key().to_vec());
            derived.set_end_key(keys.front().unwrap().to_vec());
            regions.push(derived.clone());
        }

        let mut new_split_regions: HashMap<u64, NewSplitPeer> = HashMap::default();
        for req in split_reqs.get_requests() {
            let mut new_region = Region::default();
            new_region.set_id(req.get_new_region_id());
            new_region.set_region_epoch(derived.get_region_epoch().to_owned());
            new_region.set_start_key(keys.pop_front().unwrap());
            new_region.set_end_key(keys.front().unwrap().to_vec());
            new_region.set_peers(derived.get_peers().to_vec().into());
            for (peer, peer_id) in new_region
                .mut_peers()
                .iter_mut()
                .zip(req.get_new_peer_ids())
            {
                peer.set_id(*peer_id);
            }
            new_split_regions.insert(
                new_region.get_id(),
                NewSplitPeer {
                    peer_id: util::find_peer(&new_region, ctx.store_id).unwrap().get_id(),
                    result: None,
                },
            );
            regions.push(new_region);
        }

        if right_derive {
            derived.set_start_key(keys.pop_front().unwrap());
            regions.push(derived.clone());
        }

        let mut replace_regions = HashSet::default();
        {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, new_split_peer) in new_split_regions.iter_mut() {
                match pending_create_peers.entry(*region_id) {
                    HashMapEntry::Occupied(mut v) => {
                        if *v.get() != (new_split_peer.peer_id, false) {
                            new_split_peer.result =
                                Some(format!("status {:?} is not expected", v.get()));
                        } else {
                            replace_regions.insert(*region_id);
                            v.insert((new_split_peer.peer_id, true));
                        }
                    }
                    HashMapEntry::Vacant(v) => {
                        v.insert((new_split_peer.peer_id, true));
                    }
                }
            }
        }

        // region_id -> peer_id
        let mut already_exist_regions = Vec::new();
        for (region_id, new_split_peer) in new_split_regions.iter_mut() {
            let region_state_key = keys::region_state_key(*region_id);
            match ctx.raft_engine.get_region_state(*region_id) {
                Ok(None) => (),
                Ok(Some(state)) => {
                    if replace_regions.get(region_id).is_some() {
                        // It's marked replaced, then further destroy will skip cleanup, so there
                        // should be no region local state.
                        panic!(
                            "{} failed to replace region {} peer {} because state {:?} alread exist in raft engine",
                            self.tag, region_id, new_split_peer.peer_id, state
                        )
                    }
                    already_exist_regions.push((*region_id, new_split_peer.peer_id));
                    new_split_peer.result = Some(format!("state {:?} exist in raft engine", state));
                }
                e => panic!(
                    "{} failed to get regions state of {}: {:?}",
                    self.tag, region_id, e
                ),
            }
        }

        if !already_exist_regions.is_empty() {
            let mut pending_create_peers = ctx.pending_create_peers.lock().unwrap();
            for (region_id, peer_id) in &already_exist_regions {
                assert_eq!(
                    pending_create_peers.remove(region_id),
                    Some((*peer_id, true))
                );
            }
        }

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
                // warn!(
                //     "new region from splitting already exists";
                //     "new_region_id" => new_region.get_id(),
                //     "new_peer_id" => new_split_peer.peer_id,
                //     "reason" => r,
                //     "region_id" => self.region_id(),
                //     "peer_id" => self.id(),
                // );
                continue;
            }
            let new_tablet_path = ctx
                .factory
                .as_ref()
                .unwrap()
                .tablet_path(new_region_id, RAFT_INIT_LOG_INDEX);

            std::fs::hard_link(current_tablet_path, &new_tablet_path).unwrap_or_else(|e| {
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
            .factory
            .as_ref()
            .unwrap()
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
            // info!("moving {} locks to new regions", pessimistic_locks.len(); "region_id"
            // => region_id); Update the version so the concurrent reader will
            // fail due to EpochNotMatch instead of PessimisticLockNotFound.
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
            self.fsm.peer_mut().set_approximate_size(estimated_size);
            self.fsm.peer_mut().set_approximate_keys(estimated_keys);
            // self.fsm.peer.heartbeat_pd(self.ctx);
            // info!(
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
            unimplemented!()
            // panic!("{} original region should exist", self.fsm.peer.tag);
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

            // // Insert new regions and validation
            // info!(
            //     "insert new region";
            //     "region_id" => new_region_id,
            //     "region" => ?new_region,
            // );
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
                // self.store_ctx.router.close(new_region_id);
            }

            let storage = Storage::new(
                new_region_id,
                self.store_ctx.store.id,
                self.store_ctx.engine,
                self.store_ctx.log_fetch_scheduler,
                &self.store_ctx.logger,
            )?
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

            // let meta_peer = new_peer.peer.peer.clone();

            for p in new_region.get_peers() {
                // Add this peer to cache
                new_peer.peer_mut().insert_peer_cache(p.clone());
            }

            // // New peer derive write flow from parent region,
            // // this will be used by balance write flow.
            // new_peer.peer.peer_stat = self.fsm.peer.peer_stat.clone();
            // new_peer.peer.last_compacted_idx = new_peer
            //     .peer
            //     .get_store()
            //     .apply_state()
            //     .get_truncated_state()
            //     .get_index()
            //     + 1;
            // let campaigned = new_peer.peer.maybe_campaign(is_leader);
            // new_peer.has_ready |= campaigned;

            // if is_leader {
            //     new_peer.peer.approximate_size = estimated_size;
            //     new_peer.peer.approximate_keys = estimated_keys;
            //     *new_peer.peer.txn_ext.pessimistic_locks.write() = locks;
            //     // The new peer is likely to become leader, send a heartbeat
            // immediately to     // reduce client query miss.
            //     new_peer.peer.heartbeat_pd(self.ctx);
            // }

            new_peer.peer().activate(self.ctx);
            meta.regions.insert(new_region_id, new_region.clone());
            let _ = self
                .store_ctx
                .tablet_factory
                .open_tablet(
                    new_region_id,
                    Some(RAFT_INIT_LOG_INDEX),
                    OpenOptions::default().set_create(true),
                )
                .unwrap_or_else(|e| panic!("Fails to open tablet created by split: {:?}", e));
        }
    }
}

mod test {
    use super::*;

    #[test]
    fn it_work() {}
}
