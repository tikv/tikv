// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

use collections::{HashMap, HashMapEntry, HashSet};
use engine_traits::{KvEngine, RaftEngine, RaftEngineReadOnly, TabletFactory};
use kvproto::{
    metapb::Region,
    raft_cmdpb::{AdminRequest, AdminResponse},
};
use raftstore::{
    store::{
        fsm::{
            apply::{ApplyResult, NewSplitPeer},
            ExecResult,
        },
        metrics::PEER_ADMIN_CMD_COUNTER,
        util, RAFT_INIT_LOG_INDEX,
    },
    Result,
};
use tikv_util::box_err;

use crate::{
    batch::ApplyContext,
    fsm::ApplyFsmDelegate,
    raft::{write_initial_states, write_peer_state},
};

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
        let state = ctx.raft_engine.get_region_state(region_id)?.unwrap();
        let current_tablet_path = ctx
            .factory
            .as_ref()
            .unwrap()
            .tablet_path(region_id, state.tablet_index);

        assert!(std::path::Path::try_exists(&current_tablet_path).unwrap());

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

            match std::fs::hard_link(&current_tablet_path, &new_tablet_path) {
                Ok(_) => (),
                Err(_) => unimplemented!(),
            }

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

        let _ = ctx
            .factory
            .as_ref()
            .unwrap()
            .load_tablet(&current_tablet_path, region_id, log_index)
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

mod test {
    use super::*;

    #[test]
    fn it_work() {}
}
