// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;

use collections::{HashMap, HashMapEntry, HashSet};
use engine_traits::{KvEngine, RaftEngine, RaftEngineReadOnly};
use kvproto::{metapb::Region, raft_cmdpb::AdminRequest};
use raftstore::{
    store::{fsm::apply::NewSplitPeer, metrics::PEER_ADMIN_CMD_COUNTER, util},
    Result,
};
use tikv_util::box_err;

use crate::{batch::ApplyContext, fsm::ApplyFsmDelegate};

impl<'a, EK: KvEngine, ER: RaftEngine> ApplyFsmDelegate<'a, EK, ER> {
    pub fn exec_batch_split(&mut self, ctx: &mut ApplyContext, req: &AdminRequest) -> Result<()> {
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
            for (region_id, new_split_peer) in new_split_regions {
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
        let mut alread_exist_regions = Vec::new();
        for (region_id, new_split_peer) in new_split_regions.iter_mut() {
            let region_state_key = keys::region_state_key(*region_id);
            match ctx.raft_engine.get_region_state(*region_id) {
                Ok(None) => (),
                Ok(Some(state)) => {}
                e => panic!(
                    "{} failed to get regions state of {}: {:?}",
                    self.tag, region_id, e
                ),
            }
        }

        Ok(())
    }
}

mod test {
    use super::*;

    #[test]
    fn it_work() {}
}
