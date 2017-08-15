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
use raftstore::{Error, Result};
use raftstore::store::keys;
use rocksdb::{Range, TablePropertiesCollection, DB};
use storage::LARGE_CFS;
use util::properties::SizeProperties;
use util::rocksdb as rocksdb_util;

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
    region
        .get_peers()
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

pub fn get_region_properties_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<TablePropertiesCollection> {
    let cf = try!(rocksdb_util::get_cf_handle(db, cfname));
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    db.get_properties_of_tables_in_range(cf, &[range])
        .map_err(|e| e.into())
}

pub fn get_region_approximate_size_cf(
    db: &DB,
    cfname: &str,
    region: &metapb::Region,
) -> Result<u64> {
    let cf = try!(rocksdb_util::get_cf_handle(db, cfname));
    let start = keys::enc_start_key(region);
    let end = keys::enc_end_key(region);
    let range = Range::new(&start, &end);
    let (_, mut size) = db.get_approximate_memtable_stats_cf(cf, &range);
    let collection = try!(db.get_properties_of_tables_in_range(cf, &[range]));
    for (_, v) in &*collection {
        let props = try!(SizeProperties::decode(v.user_collected_properties()));
        size += props.get_approximate_size_in_range(&start, &end);
    }
    Ok(size)
}

pub fn get_region_approximate_size(db: &DB, region: &metapb::Region) -> Result<u64> {
    let mut size = 0;
    for cfname in LARGE_CFS {
        size += try!(get_region_approximate_size_cf(db, cfname, region))
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use kvproto::metapb;
    use kvproto::raft_serverpb::RaftMessage;
    use kvproto::eraftpb::{ConfChangeType, Message, MessageType};

    use super::*;
    use tempdir::TempDir;
    use raftstore::store::peer_storage;
    use rocksdb::{ColumnFamilyOptions, DBOptions, Writable};
    use util::rocksdb::CFOptions;
    use util::properties::SizePropertiesCollectorFactory;

    // Tests the util function `check_key_in_region`.
    #[test]
    fn test_check_key_in_region() {
        let test_cases = vec![
            ("", "", "", true, true),
            ("", "", "6", true, true),
            ("", "3", "6", false, false),
            ("4", "3", "6", true, true),
            ("4", "3", "", true, true),
            ("2", "3", "6", false, false),
            ("", "3", "6", false, false),
            ("", "3", "", false, false),
            ("6", "3", "6", false, true),
        ];
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
        let tbl = vec![
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                true,
            ),
            (
                MessageType::MsgRequestVote,
                peer_storage::RAFT_INIT_LOG_TERM,
                false,
            ),
            (
                MessageType::MsgHup,
                peer_storage::RAFT_INIT_LOG_TERM + 1,
                false,
            ),
        ];

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
        assert_eq!(
            conf_change_type_str(&ConfChangeType::AddNode),
            STR_CONF_CHANGE_ADD_NODE
        );
        assert_eq!(
            conf_change_type_str(&ConfChangeType::RemoveNode),
            STR_CONF_CHANGE_REMOVE_NODE
        );
    }

    #[test]
    fn test_epoch_stale() {
        let mut epoch = metapb::RegionEpoch::new();
        epoch.set_version(10);
        epoch.set_conf_ver(10);

        let tbl = vec![
            (11, 10, true),
            (10, 11, true),
            (10, 10, false),
            (10, 9, false),
        ];

        for (version, conf_version, is_stale) in tbl {
            let mut check_epoch = metapb::RegionEpoch::new();
            check_epoch.set_version(version);
            check_epoch.set_conf_ver(conf_version);
            assert_eq!(is_epoch_stale(&epoch, &check_epoch), is_stale);
        }
    }

    fn make_region(id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> metapb::Region {
        let mut peer = metapb::Peer::new();
        peer.set_id(id);
        peer.set_store_id(id);
        let mut region = metapb::Region::new();
        region.set_id(id);
        region.set_start_key(start_key);
        region.set_end_key(end_key);
        region.mut_peers().push(peer);
        region
    }

    #[test]
    fn test_region_approximate_size() {
        let path = TempDir::new("_test_raftstore_region_approximate_size").expect("");
        let path_str = path.path().to_str().unwrap();
        let db_opts = DBOptions::new();
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.set_level_zero_file_num_compaction_trigger(10);
        let f = Box::new(SizePropertiesCollectorFactory::default());
        cf_opts.add_table_properties_collector_factory("tikv.size-collector", f);
        let cfs_opts = LARGE_CFS
            .iter()
            .map(|cf| CFOptions::new(cf, cf_opts.clone()))
            .collect();
        let db = rocksdb_util::new_engine_opt(path_str, db_opts, cfs_opts).unwrap();

        let cases = [("a", 1024), ("b", 2048), ("c", 4096)];
        let cf_size = 2 + 1024 + 2 + 2048 + 2 + 4096;
        for &(key, vlen) in &cases {
            for cfname in LARGE_CFS {
                let k1 = keys::data_key(b" ");
                let v1 = vec![];
                let k2 = keys::data_key(key.as_bytes());
                let v2 = vec![0; vlen as usize];
                assert_eq!(k2.len(), 2);
                let cf = db.cf_handle(cfname).unwrap();
                db.put_cf(cf, &k1, &v1).unwrap();
                db.put_cf(cf, &k2, &v2).unwrap();
                db.flush_cf(cf, true).unwrap();
            }
        }

        let region = make_region(1, vec![], vec![]);
        let size = get_region_approximate_size(&db, &region).unwrap();
        assert_eq!(size, cf_size * LARGE_CFS.len() as u64);
        for cfname in LARGE_CFS {
            let size = get_region_approximate_size_cf(&db, cfname, &region).unwrap();
            assert_eq!(size, cf_size);
        }
    }
}
