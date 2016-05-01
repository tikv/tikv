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

use uuid::Uuid;

use kvproto::metapb;
use kvproto::raftpb::{self, ConfChangeType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raftstore::{Result, Error};

pub fn find_peer(region: &metapb::Region, store_id: u64) -> bool {
    region.get_store_ids().iter().any(|&id| id == store_id)
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<u64> {
    match region.get_store_ids()
                .iter()
                .position(|&id| id == store_id) {
        None => None,
        Some(index) => Some(region.mut_store_ids().remove(index)),
    }
}

pub fn get_uuid_from_req(cmd: &RaftCmdRequest) -> Option<Uuid> {
    Uuid::from_bytes(cmd.get_header().get_uuid())
}

pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
    let end_key = region.get_end_key();
    let start_key = region.get_start_key();
    if key >= start_key && (end_key.is_empty() || key < end_key) {
        Ok(())
    } else {
        Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
    }
}

pub fn conf_change_type_str(conf_type: &raftpb::ConfChangeType) -> String {
    match *conf_type {
        ConfChangeType::AddNode => "AddNode".to_owned(),
        ConfChangeType::RemoveNode => "RemoveNode".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use kvproto::metapb;

    use super::*;

    #[test]
    fn test_peer() {
        let mut region = metapb::Region::new();
        region.set_id(1);
        region.mut_store_ids().push(1);

        assert!(find_peer(&region, 1));
        assert!(!find_peer(&region, 10));

        assert!(remove_peer(&mut region, 1).is_some());
        assert!(remove_peer(&mut region, 1).is_none());
        assert!(!find_peer(&region, 1));

    }
}
