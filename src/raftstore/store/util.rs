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

use time::{self, Timespec};
use uuid::Uuid;

use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raftstore::{Result, Error};

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

pub fn get_uuid_from_req(cmd: &RaftCmdRequest) -> Option<Uuid> {
    Uuid::from_bytes(cmd.get_header().get_uuid()).ok()
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

pub fn format_clocktime(ts: Timespec) -> String {
    let tm = time::at(ts);
    let res = time::strftime("%z %Y-%m-%d %H:%M:%S", &tm).unwrap();
    res + &format!(" {}", tm.tm_nsec)
}

#[cfg(test)]
mod tests {
    use time;
    use kvproto::metapb;

    use super::*;

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
    fn test_format_clocktime() {
        let s = String::from("+0800 2016-11-10 15:01:37");
        let tm = time::strptime(&s, "%z %Y-%m-%d %H:%M:%S").unwrap();
        let ts = tm.to_timespec();
        let expect = time::strftime("%z %Y-%m-%d %H:%M:%S", &tm).unwrap() + " 0";
        assert_eq!(expect, format_clocktime(ts));
    }
}
