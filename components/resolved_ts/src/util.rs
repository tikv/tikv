// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::metapb::Peer;

pub fn find_store_id(peer_list: &[Peer], peer_id: u64) -> Option<u64> {
    for peer in peer_list {
        if peer.id == peer_id {
            return Some(peer.store_id);
        }
    }
    None
}
