// Copyright 2019 PingCAP, Inc.
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

use std::path::Path;
use std::sync::{Arc, RwLock};

use kvproto::metapb::Region;
use kvproto::raft_serverpb::*;
use raft::eraftpb::Entry;
use rocksdb::{DBOptions, Writable, WriteBatch, WriteOptions, DB};
use tempdir::TempDir;

use test_raftstore::*;
use tikv::config::DbConfig;
use tikv::pd::PdClient;
use tikv::raftstore::store::{
    keys, Engines, Mutable, Peekable, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER, RAFT_INIT_LOG_INDEX,
    RAFT_INIT_LOG_TERM,
};
use tikv::storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use tikv::util::rocksdb_util;

fn test_upgrade_from_v2_to_v3(fp: &str) {
    let tmp_path = TempDir::new("test_upgrade").unwrap();

    let all_cfs_v2 = &[CF_LOCK, CF_RAFT, CF_WRITE, CF_DEFAULT];
    // Create a v2 kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path.path().to_str().unwrap(), None, all_cfs_v2, None)
            .unwrap();

    // Create a raft engine.
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine =
        rocksdb_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap();

    let cluster_id = 1000_000_000;
    let store_id = 1;
    let region_id = 3;
    let peer_id = 4;

    let term = 6;
    let applied_index = 9;
    let snapshot_index = applied_index;
    let committed_index = 10;
    let last_index = 11;
    let stale_last_index = applied_index - 1;

    let kv_wb = WriteBatch::new();
    // For meta data in the default CF.
    //
    //  1. store_ident_key: 0x01 0x01
    //  2. prepare_boostrap_key: 0x01 0x02
    let mut store_ident = StoreIdent::new();
    store_ident.set_cluster_id(cluster_id);
    store_ident.set_store_id(store_id);
    kv_wb.put_msg(keys::STORE_IDENT_KEY, &store_ident).unwrap();

    let mut region = Region::new();
    region.set_id(region_id);
    region.set_peers(vec![new_peer(store_id, peer_id)].into());
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    kv_wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &region).unwrap();

    // For meta data in the raft CF.
    //
    //  1. apply_state_key:         0x01 0x02 region_id 0x03
    //  2. snapshot_raft_state_key: 0x01 0x02 region_id 0x04
    //  3. region_state_key:        0x01 0x03 region_id 0x01
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(applied_index);
    let mut truncated_state = RaftTruncatedState::new();
    truncated_state.set_index(snapshot_index);
    truncated_state.set_term(term);
    apply_state.set_truncated_state(truncated_state);
    let raft_cf_handle = kv_engine.cf_handle(CF_RAFT).unwrap();
    kv_wb
        .put_msg_cf(
            raft_cf_handle,
            &keys::apply_state_key(region_id),
            &apply_state,
        )
        .unwrap();

    let mut snap_raft_state = RaftLocalState::new();
    snap_raft_state.set_last_index(snapshot_index);
    snap_raft_state.mut_hard_state().set_term(term);
    snap_raft_state.mut_hard_state().set_vote(peer_id);
    snap_raft_state.mut_hard_state().set_commit(committed_index);
    kv_wb
        .put_msg_cf(
            raft_cf_handle,
            &keys::snapshot_raft_state_key(region_id),
            &snap_raft_state,
        )
        .unwrap();

    let mut region_state = RegionLocalState::new();
    region_state.set_region(region.clone());
    region_state.set_state(PeerState::Normal);
    kv_wb
        .put_msg_cf(
            raft_cf_handle,
            &keys::region_state_key(region_id),
            &region_state,
        )
        .unwrap();

    let mut write_opt = WriteOptions::new();
    write_opt.set_sync(true);
    kv_engine.write_opt(kv_wb, &write_opt).unwrap();

    // Data in the raft engine.
    //
    //  1. raft logs
    //  2. raft state
    let raft_wb = WriteBatch::new();
    // A stale raft local state in the raft engine.
    let mut raft_state: RaftLocalState = snap_raft_state.clone();
    raft_state.set_last_index(stale_last_index);
    raft_state.mut_hard_state().set_commit(stale_last_index);
    raft_wb
        .put_msg(&keys::raft_state_key(region_id), &raft_state)
        .unwrap();

    for idx in applied_index..=last_index {
        let mut entry = Entry::new();
        entry.set_term(term);
        entry.set_index(idx);
        raft_wb
            .put_msg(&keys::raft_log_key(region_id, idx), &entry)
            .unwrap();
    }
    raft_engine.write_opt(raft_wb, &write_opt).unwrap();

    drop(kv_engine);

    // Return early.
    fail::cfg(fp, "return").unwrap();
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path.path().to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    );
    fail::remove(fp);
    // Retry upgrade.
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path.path().to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    );

    // Check kv engine, no RAFT cf.
    let cfs =
        DB::list_column_families(&DBOptions::new(), tmp_path.path().to_str().unwrap()).unwrap();
    assert! {
        cfs
        .iter()
        .find(|cf| *cf == CF_RAFT)
        .is_none(), "{:?}", cfs
    }

    // Check raft engine.
    assert_eq!(
        store_ident,
        raft_engine.get_msg(keys::STORE_IDENT_KEY).unwrap().unwrap(),
    );
    assert_eq!(
        region,
        raft_engine
            .get_msg(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        snap_raft_state,
        raft_engine
            .get_msg(&keys::raft_state_key(region_id))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        apply_state,
        raft_engine
            .get_msg(&keys::apply_state_key(region_id))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        region_state,
        raft_engine
            .get_msg(&keys::region_state_key(region_id))
            .unwrap()
            .unwrap(),
    );

    // To start a cluster, we need to clean the PREPARE_BOOTSTRAP_KEY and
    // update the last index in RaftLocalState.
    raft_engine.delete(keys::PREPARE_BOOTSTRAP_KEY).unwrap();
    snap_raft_state.set_last_index(last_index);
    raft_engine
        .put_msg(&keys::raft_state_key(region_id), &snap_raft_state)
        .unwrap();

    let pd_client = Arc::new(TestPdClient::new(cluster_id, false));
    pd_client
        .bootstrap_cluster(new_store(store_id, "".to_owned()), region)
        .unwrap();
    for _ in 0..peer_id {
        // Advance the id allocator.
        pd_client.alloc_id().unwrap();
        break;
    }

    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    let mut cluster = Cluster::new(cluster_id, 5, sim, pd_client);
    cluster.create_engines();

    // Update upgraded engines.
    // Create a kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
    let engines = Engines::new(Arc::new(kv_engine), Arc::new(raft_engine));
    cluster.dbs[0] = engines.clone();
    cluster.paths[0] = tmp_path;
    cluster.engines.insert(store_id, engines);
    cluster.start().unwrap();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(store_id), k, v);
    for id in cluster.engines.keys() {
        if *id == store_id {
            continue;
        }
        must_get_equal(&cluster.get_engine(*id), k, v);
    }
}

#[test]
fn test_upgrade_2_3_return_before_update_raft() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_update_raft");
}

#[test]
fn test_upgrade_2_3_return_before_update_kv() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_update_kv");
}

#[test]
fn test_upgrade_2_3_return_before_drop_raft_cf() {
    let _guard = crate::setup();
    test_upgrade_from_v2_to_v3("upgrade_2_3_before_drop_raft_cf");
}

#[test]
fn test_double_upgrade_2_3() {
    let _guard = crate::setup();
    // An empty fail point injects nothing.
    test_upgrade_from_v2_to_v3("");
}

fn test_upgrade_2_3_boostrap(fp: &str) {
    let tmp_path = TempDir::new("test_upgrade_bootstrap").unwrap();

    let all_cfs_v2 = &[CF_LOCK, CF_RAFT, CF_WRITE, CF_DEFAULT];
    // Create a v2 kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path.path().to_str().unwrap(), None, all_cfs_v2, None)
            .unwrap();

    // Create a raft engine.
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine = Arc::new(
        rocksdb_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap(),
    );

    let cluster_id = 1000_000_000;
    let store_id = 1;
    let region_id = 3;
    let peer_id = 4;

    let kv_wb = WriteBatch::new();
    // Mock boostrap store in v2.x.
    let mut store_ident = StoreIdent::new();
    store_ident.set_cluster_id(cluster_id);
    store_ident.set_store_id(store_id);
    kv_wb.put_msg(keys::STORE_IDENT_KEY, &store_ident).unwrap();

    // Mock boostrap cluster in v2.x.
    let mut region = Region::new();
    region.set_id(region_id);
    region.set_peers(vec![new_peer(store_id, peer_id)].into());
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);
    kv_wb.put_msg(keys::PREPARE_BOOTSTRAP_KEY, &region).unwrap();

    let raft_cf_handle = kv_engine.cf_handle(CF_RAFT).unwrap();
    // RegionLocalState
    let mut region_state = RegionLocalState::new();
    region_state.set_region(region.clone());
    kv_wb
        .put_msg_cf(
            raft_cf_handle,
            &keys::region_state_key(region_id),
            &region_state,
        )
        .unwrap();
    // RaftApplyState
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_index(RAFT_INIT_LOG_INDEX);
    apply_state
        .mut_truncated_state()
        .set_term(RAFT_INIT_LOG_TERM);
    kv_wb
        .put_msg_cf(
            raft_cf_handle,
            &keys::apply_state_key(region_id),
            &apply_state,
        )
        .unwrap();
    // RaftLocalState
    let raft_wb = WriteBatch::new();
    let mut raft_state = RaftLocalState::new();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);
    raft_wb
        .put_msg(&keys::raft_state_key(region_id), &raft_state)
        .unwrap();

    let mut write_opt = WriteOptions::new();
    write_opt.set_sync(true);
    kv_engine.write_opt(kv_wb, &write_opt).unwrap();
    raft_engine.write_opt(raft_wb, &write_opt).unwrap();

    drop(kv_engine);

    // Return early.
    fail::cfg(fp, "return").unwrap();
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path.path().to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    );
    fail::remove(fp);
    // Retry upgrade.
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path.path().to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    );

    // create a v3.x. node

    let pd_client = Arc::new(TestPdClient::new(0, false));
    for _ in 0..peer_id {
        // Advance the id allocator.
        pd_client.alloc_id().unwrap();
        break;
    }

    // Update upgraded engines.
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(cluster_id, 5, sim, pd_client.clone());
    cluster.create_engines();

    // Create a kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap();
    let engines = Engines::new(Arc::new(kv_engine), raft_engine.clone());
    cluster.dbs[0] = engines.clone();
    cluster.paths[0] = tmp_path;
    cluster.engines.insert(store_id, engines);
    cluster.start().unwrap();

    // Wait for bootstraping cluster.
    let mut count = 1;
    while !pd_client.is_cluster_bootstrapped().unwrap() {
        sleep_ms(10);
        count += 1;
        if count > 1000 {
            panic!("failed to bootstrap cluster");
        }
    }
    assert!(raft_engine
        .get_msg::<Region>(keys::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());
    assert_eq!(pd_client.get_region(b"").unwrap().get_id(), region_id);
}

#[test]
fn test_upgrade_2_3_bootstrap_return_before_update_raft() {
    let _guard = crate::setup();
    test_upgrade_2_3_boostrap("upgrade_2_3_before_update_raft");
}

#[test]
fn test_upgrade_2_3_bootstrap_return_before_update_kv() {
    let _guard = crate::setup();
    test_upgrade_2_3_boostrap("upgrade_2_3_before_update_kv");
}

#[test]
fn test_upgrade_2_3_bootstrap_return_before_drop_raft_cf() {
    let _guard = crate::setup();
    test_upgrade_2_3_boostrap("upgrade_2_3_before_drop_raft_cf");
}
