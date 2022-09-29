// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::{Peer, PeerRole, Region};

pub fn find_peer(region: &Region, store_id: u64) -> Option<&Peer> {
    region
        .get_peers()
        .iter()
        .find(|&p| p.get_store_id() == store_id)
}

pub fn find_peer_mut(region: &mut Region, store_id: u64) -> Option<&mut Peer> {
    region
        .mut_peers()
        .iter_mut()
        .find(|p| p.get_store_id() == store_id)
}

pub fn remove_peer(region: &mut Region, store_id: u64) -> Option<Peer> {
    region
        .get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| region.mut_peers().remove(i))
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> Peer {
    let mut peer = Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(PeerRole::Voter);
    peer
}

pub fn new_learner_peer(store_id: u64, peer_id: u64) -> Peer {
    let mut peer = Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(PeerRole::Learner);
    peer
}

pub fn is_learner(peer: &Peer) -> bool {
    peer.get_role() == PeerRole::Learner
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer() {
        let mut region = Region::default();
        region.set_id(1);
        region.mut_peers().push(new_peer(1, 1));
        region.mut_peers().push(new_learner_peer(2, 2));

        assert!(!is_learner(find_peer(&region, 1).unwrap()));
        assert!(is_learner(find_peer(&region, 2).unwrap()));

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(find_peer(&region, 1).is_none());
    }

    #[test]
    fn test_on_same_store() {
        let cases = vec![
            (vec![2, 3, 4], vec![], vec![1, 2, 3], vec![], false),
            (vec![2, 3, 1], vec![], vec![1, 2, 3], vec![], true),
            (vec![2, 3, 4], vec![], vec![1, 2], vec![], false),
            (vec![1, 2, 3], vec![], vec![1, 2, 3], vec![], true),
            (vec![1, 3], vec![2, 4], vec![1, 2], vec![3, 4], false),
            (vec![1, 3], vec![2, 4], vec![1, 3], vec![], false),
            (vec![1, 3], vec![2, 4], vec![], vec![2, 4], false),
            (vec![1, 3], vec![2, 4], vec![3, 1], vec![4, 2], true),
        ];

        for (s1, s2, s3, s4, exp) in cases {
            let mut r1 = Region::default();
            for (store_id, peer_id) in s1.into_iter().zip(0..) {
                r1.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s2.into_iter().zip(0..) {
                r1.mut_peers().push(new_learner_peer(store_id, peer_id));
            }

            let mut r2 = Region::default();
            for (store_id, peer_id) in s3.into_iter().zip(10..) {
                r2.mut_peers().push(new_peer(store_id, peer_id));
            }
            for (store_id, peer_id) in s4.into_iter().zip(10..) {
                r2.mut_peers().push(new_learner_peer(store_id, peer_id));
            }
            let res = super::super::region_on_same_stores(&r1, &r2);
            assert_eq!(res, exp, "{:?} vs {:?}", r1, r2);
        }
    }
}
