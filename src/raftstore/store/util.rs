// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::option::Option;

use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_serverpb::RaftMessage;
use raftstore::{Result, Error};

use super::peer_storage;

pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    for peer in region.get_peers() {
        if peer.get_store_id() == store_id {
            return Some(peer);
        }
    }

    None
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<metapb::Peer> {
    region.get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| region.mut_peers().remove(i))
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer
}

/// Check if key in region range [`start_key`, `end_key`].
pub fn check_key_in_region_inclusive(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key <= end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if key in region range [`start_key`, `end_key`).
pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

/// Check if range [`start_key`, `end_key`) in region range [`region_start_key`, `region_end_key`).
pub fn check_range_in_region(star_key: &[u8],
                             end_key: &[u8],
                             region: &metapb::Region)
                             -> Result<()> {
    let region_end_key = region.get_end_key();
    let region_start_key = region.get_start_key();
    if star_key >= region_start_key && (end_key.is_empty() || end_key <= region_end_key) {
        Ok(())
    } else {
        Err(Error::RangeNotInRegion(star_key.to_vec(), end_key.to_vec(), region.clone()))
    }
}

#[inline]
pub fn is_first_vote_msg(msg: &RaftMessage) -> bool {
    msg.get_message().get_msg_type() == MessageType::MsgRequestVote &&
    msg.get_message().get_term() == peer_storage::RAFT_INIT_LOG_TERM + 1
}

const STR_CONF_CHANGE_ADD_NODE: &'static str = "AddNode";
const STR_CONF_CHANGE_REMOVE_NODE: &'static str = "RemoveNode";

pub fn conf_change_type_str(conf_type: &eraftpb::ConfChangeType) -> &'static str {
    match *conf_type {
        ConfChangeType::AddNode => STR_CONF_CHANGE_ADD_NODE,
        ConfChangeType::RemoveNode => STR_CONF_CHANGE_REMOVE_NODE,
    }
}

// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &metapb::RegionEpoch, check_epoch: &metapb::RegionEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version() ||
    epoch.get_conf_ver() < check_epoch.get_conf_ver()
}

#[cfg(test)]
mod tests {
    use kvproto::metapb;
    use kvproto::raft_serverpb::RaftMessage;
    use kvproto::eraftpb::{Message, ConfChangeType, MessageType};

    use super::*;
    use raftstore::store::peer_storage;

    // Tests the util function `check_key_in_region`.
    #[test]
    fn test_check_key_in_region() {
        let test_cases = vec![("", "", "", true, true),
                              ("", "", "6", true, true),
                              ("", "3", "6", false, false),
                              ("4", "3", "6", true, true),
                              ("4", "3", "", true, true),
                              ("2", "3", "6", false, false),
                              ("", "3", "6", false, false),
                              ("", "3", "", false, false),
                              ("6", "3", "6", false, true)];
        for (key, start_key, end_key, is_in_region, is_in_region_inclusive) in test_cases {
            let mut region = metapb::Region::new();
            region.set_start_key(start_key.as_bytes().to_vec());
            region.set_end_key(end_key.as_bytes().to_vec());
            let mut result = check_key_in_region(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), is_in_region);
            result = check_key_in_region_inclusive(key.as_bytes(), &region);
            assert_eq!(result.is_ok(), is_in_region_inclusive)
        }
    }

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::new();
        region.set_id(1);
        region.mut_peers().push(new_peer(1, 1));

        assert!(find_peer(&region, 1).is_some());
        assert!(find_peer(&region, 10).is_none());

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(find_peer(&region, 1).is_none());

    }

    #[test]
    fn test_first_vote_msg() {
        let tbl = vec![(MessageType::MsgRequestVote, peer_storage::RAFT_INIT_LOG_TERM + 1, true),
                       (MessageType::MsgRequestVote, peer_storage::RAFT_INIT_LOG_TERM, false),
                       (MessageType::MsgHup, peer_storage::RAFT_INIT_LOG_TERM + 1, false)];

        for (msg_type, term, is_vote) in tbl {
            let mut msg = Message::new();
            msg.set_msg_type(msg_type);
            msg.set_term(term);

            let mut m = RaftMessage::new();
            m.set_message(msg);
            assert_eq!(is_first_vote_msg(&m), is_vote);
        }
    }

    #[test]
    fn test_conf_change_type_str() {
        assert_eq!(conf_change_type_str(&ConfChangeType::AddNode),
                   STR_CONF_CHANGE_ADD_NODE);
        assert_eq!(conf_change_type_str(&ConfChangeType::RemoveNode),
                   STR_CONF_CHANGE_REMOVE_NODE);
    }

    #[test]
    fn test_epoch_stale() {
        let mut epoch = metapb::RegionEpoch::new();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![(11, 10, true), (10, 11, true), (10, 10, false), (10, 9, false)];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = metapb::RegionEpoch::new();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }
}
