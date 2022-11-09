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

pub fn find_peer_by_id(region: &Region, peer_id: u64) -> Option<&Peer> {
    region.get_peers().iter().find(|&p| p.get_id() == peer_id)
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

pub fn new_witness_peer(store_id: u64, peer_id: u64) -> Peer {
    let mut peer = Peer::default();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer.set_role(PeerRole::Voter);
    peer.set_is_witness(true);
    peer
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
}
