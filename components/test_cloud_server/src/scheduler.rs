// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use futures::executor::block_on;
use kvproto::metapb::{Peer, PeerRole};
use pd_client::PdClient;
use rand::Rng;
use test_raftstore::TestPdClient;

use crate::{must_wait, try_wait};

pub struct RegionScheduler {
    pub(crate) pd: Arc<TestPdClient>,
    pub(crate) store_ids: Vec<u64>,
}

impl RegionScheduler {
    pub fn move_random_region(&self) {
        let regions = self.pd.get_all_regions();
        loop {
            let region_idx = rand::thread_rng().gen_range(0..regions.len());
            let region = &regions[region_idx];
            if region.get_peers().len() != 3 {
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            let &target_store_id = self
                .store_ids
                .iter()
                .find(|&&store_id| {
                    let contains = region
                        .get_peers()
                        .iter()
                        .any(|peer| peer.store_id == store_id);
                    !contains
                })
                .unwrap();
            self.move_peer(region.id, target_store_id);
            return;
        }
    }

    fn move_peer(&self, region_id: u64, store_id: u64) {
        self.pd.disable_default_operator();
        let peer_id = self.pd.alloc_id().unwrap();
        let mut peer = Peer::new();
        peer.store_id = store_id;
        peer.id = peer_id;
        peer.role = PeerRole::Learner;
        self.pd.add_peer(region_id, peer);
        must_wait(
            || {
                let region = block_on(self.pd.get_region_by_id(region_id))
                    .unwrap()
                    .unwrap();
                region.get_peers().iter().any(|peer| peer.id == peer_id)
            },
            5,
            "failed to add leaner",
        );
        let mut peer = Peer::new();
        peer.store_id = store_id;
        peer.id = peer_id;
        peer.role = PeerRole::Voter;
        self.pd.add_peer(region_id, peer);
        must_wait(
            || {
                let region = block_on(self.pd.get_region_by_id(region_id))
                    .unwrap()
                    .unwrap();
                region
                    .get_peers()
                    .iter()
                    .any(|peer| peer.id == peer_id && peer.role == PeerRole::Voter)
            },
            8,
            "failed to promote leaner",
        );
        must_wait(
            || {
                block_on(self.pd.get_region_leader_by_id(region_id))
                    .unwrap()
                    .is_some()
            },
            5,
            "failed to get leader",
        );
        let (region, leader) = block_on(self.pd.get_region_leader_by_id(region_id))
            .unwrap()
            .unwrap();
        let to_remove = region
            .peers
            .iter()
            .find(|peer| peer.id != leader.id)
            .unwrap();
        self.pd.remove_peer(region_id, to_remove.clone());
        try_wait(
            || {
                let region = block_on(self.pd.get_region_by_id(region_id))
                    .unwrap()
                    .unwrap();
                region
                    .get_peers()
                    .iter()
                    .all(|peer| peer.id != to_remove.id)
            },
            5,
        );
        self.pd.enable_default_operator();
    }
}
