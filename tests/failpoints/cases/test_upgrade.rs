// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, RwLock};

use kvproto::metapb::Region;
use kvproto::raft_serverpb::*;
use raft::eraftpb::Entry;
use tempfile::Builder;

use test_raftstore::*;
use tikv::config::DbConfig;
use tikv::raftstore::store::{
    keys, Engines, Mutable, Peekable, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
};
use tikv::storage::kv::{DBOptions, Writable, DB};
use tikv::storage::{ALL_CFS, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use tikv_util::rocksdb_util;
use tikv_util::config::MB;
use pd_client::PdClient;

const CLUSTER_ID: u64 = 1_000_000_000;
const STOER_ID: u64 = 1;
const REGION_ID: u64 = 3;
const PEER_ID: u64 = 4;

const TERM: u64 = 6;
const APPLIED_INDEX: u64 = 9;
const SNAPSHOT_INDEX: u64 = APPLIED_INDEX;
const COMMITTED_INDEX: u64 = 10;
const LAST_INDEX: u64 = 11;
const STALE_LAST_INDEX: u64 = APPLIED_INDEX - 1;

fn write_store_ident(db: &DB, cf: &str, key: &[u8]) -> StoreIdent {
    let mut store_ident = StoreIdent::default();
    store_ident.set_cluster_id(CLUSTER_ID);
    store_ident.set_store_id(STOER_ID);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &store_ident).unwrap();

    store_ident
}

fn write_region(db: &DB, cf: &str, key: &[u8]) -> Region {
    let mut region = Region::default();
    region.set_id(REGION_ID);
    region.set_peers(vec![new_peer(STOER_ID, PEER_ID)].into());
    region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);
    region.mut_region_epoch().set_version(INIT_EPOCH_VER);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &region).unwrap();

    region
}

fn write_apply_state(db: &DB, cf: &str, key: &[u8]) -> RaftApplyState {
    let mut apply_state = RaftApplyState::default();
    apply_state.set_applied_index(APPLIED_INDEX);
    let mut truncated_state = RaftTruncatedState::default();
    truncated_state.set_index(SNAPSHOT_INDEX);
    truncated_state.set_term(TERM);
    apply_state.set_truncated_state(truncated_state);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &apply_state).unwrap();

    apply_state
}

fn write_snap_raft_state(db: &DB, cf: &str, key: &[u8]) -> RaftLocalState {
    let mut snap_raft_state = RaftLocalState::default();
    snap_raft_state.set_last_index(SNAPSHOT_INDEX);
    snap_raft_state.mut_hard_state().set_term(TERM);
    snap_raft_state.mut_hard_state().set_vote(PEER_ID);
    snap_raft_state.mut_hard_state().set_commit(COMMITTED_INDEX);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &snap_raft_state).unwrap();

    snap_raft_state
}

fn write_region_state(db: &DB, cf: &str, key: &[u8], region: Region) -> RegionLocalState {
    let mut region_state = RegionLocalState::default();
    region_state.set_region(region);
    region_state.set_state(PeerState::Normal);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &region_state).unwrap();

    region_state
}

fn write_stale_raft_state(
    db: &DB,
    cf: &str,
    key: &[u8],
    mut raft_state: RaftLocalState,
) -> RaftLocalState {
    raft_state.set_last_index(STALE_LAST_INDEX);
    raft_state.mut_hard_state().set_commit(STALE_LAST_INDEX);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &raft_state).unwrap();

    raft_state
}

fn write_log_entry(db: &DB, cf: &str, key: &[u8], idx: u64) -> Entry {
    let mut entry = Entry::default();
    entry.set_term(TERM);
    entry.set_index(idx);

    let cf_handle = db.cf_handle(cf).unwrap();
    db.put_msg_cf(cf_handle, key, &entry).unwrap();

    entry
}

fn test_upgrade_from_v2_to_v3(fp: &str) {
    let tmp_dir = Builder::new().prefix("test_upgrade").tempdir().unwrap();
    let tmp_path_kv = tmp_dir.path().join("kv");

    // Create a raft engine.
    let tmp_path_raft = tmp_dir.path().join(Path::new("raft"));
    let raft_engine =
        rocksdb_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap();
    let mut cache_opts = LRUCacheOptions::new();
    cache_opts.set_capacity(8 * MB);

    // No need to upgrade an empty node.
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
        Some(Cache::new_lru_cache(cache_opts)),
    )
    .unwrap();
    // Check whether there is a kv engine.
    assert!(
        DB::list_column_families(&DBOptions::new(), tmp_path_kv.to_str().unwrap())
            .unwrap_err()
            .contains("No such file or directory")
    );

    let all_cfs_v2 = &[CF_LOCK, CF_RAFT, CF_WRITE, CF_DEFAULT];
    // Create a v2 kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path_kv.to_str().unwrap(), None, all_cfs_v2, None).unwrap();

    // For meta data in the default CF.
    //
    //  1. store_ident_key: 0x01 0x01
    //  2. prepare_bootstrap_key: 0x01 0x02
    let store_ident = write_store_ident(&kv_engine, CF_DEFAULT, keys::STORE_IDENT_KEY);
    let region = write_region(&kv_engine, CF_DEFAULT, keys::PREPARE_BOOTSTRAP_KEY);

    // For meta data in the raft CF.
    //
    //  1. apply_state_key:         0x01 0x02 region_id 0x03
    //  2. snapshot_raft_state_key: 0x01 0x02 region_id 0x04
    //  3. region_state_key:        0x01 0x03 region_id 0x01
    let apply_state = write_apply_state(&kv_engine, CF_RAFT, &keys::apply_state_key(REGION_ID));
    let mut snap_raft_state = write_snap_raft_state(
        &kv_engine,
        CF_RAFT,
        &keys::snapshot_raft_state_key(REGION_ID),
    );
    let region_state = write_region_state(
        &kv_engine,
        CF_RAFT,
        &keys::region_state_key(REGION_ID),
        region.clone(),
    );
    kv_engine.sync_wal().unwrap();

    // Data in the raft engine.
    //
    //  1. raft logs
    //  2. raft state
    // A stale raft local state in the raft engine to whether the upgrade procedure can correctly
    // merge RaftLocalState.
    write_stale_raft_state(
        &raft_engine,
        CF_DEFAULT,
        &keys::raft_state_key(REGION_ID),
        snap_raft_state.clone(),
    );
    for idx in APPLIED_INDEX..=LAST_INDEX {
        write_log_entry(
            &raft_engine,
            CF_DEFAULT,
            &keys::raft_log_key(REGION_ID, idx),
            idx,
        );
    }
    raft_engine.sync_wal().unwrap();

    drop(kv_engine);

    // Return early.
    fail::cfg(fp, "return").unwrap();
    let res = tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    );
    // `unwrap` or `unwrap_err` depends on whether we enable a fail point.
    if fp.is_empty() {
        res.unwrap();
    } else {
        res.unwrap_err();
    }
    fail::remove(fp);
    // Retry upgrade.
    tikv::raftstore::store::maybe_upgrade_from_2_to_3(
        &raft_engine,
        tmp_path_kv.to_str().unwrap(),
        DBOptions::new(),
        &DbConfig::default(),
    )
    .unwrap();

    // Check kv engine, no RAFT cf.
    let cfs = DB::list_column_families(&DBOptions::new(), tmp_path_kv.to_str().unwrap()).unwrap();
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
            .get_msg(&keys::raft_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        apply_state,
        raft_engine
            .get_msg(&keys::apply_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        region_state,
        raft_engine
            .get_msg(&keys::region_state_key(REGION_ID))
            .unwrap()
            .unwrap(),
    );

    // To start a cluster, we need to delete the PREPARE_BOOTSTRAP_KEY and
    // update the last index in RaftLocalState.
    raft_engine.delete(keys::PREPARE_BOOTSTRAP_KEY).unwrap();
    snap_raft_state.set_last_index(LAST_INDEX);
    raft_engine
        .put_msg(&keys::raft_state_key(REGION_ID), &snap_raft_state)
        .unwrap();

    let pd_client = Arc::new(TestPdClient::new(CLUSTER_ID, false));
    pd_client
        .bootstrap_cluster(new_store(STOER_ID, "".to_owned()), region)
        .unwrap();
    for _ in 0..PEER_ID {
        // Advance the id allocator.
        pd_client.alloc_id().unwrap();
    }

    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    let mut cluster = Cluster::new(CLUSTER_ID, 5, sim, pd_client);
    cluster.create_engines();

    // Update upgraded engines.
    // Create a kv engine.
    let kv_engine =
        rocksdb_util::new_engine(tmp_path_kv.to_str().unwrap(), None, ALL_CFS, None).unwrap();
    let shared_block_cache = false;
    let engines = Engines::new(Arc::new(kv_engine), Arc::new(raft_engine), shared_block_cache);
    cluster.dbs[0] = engines.clone();
    cluster.paths[0] = tmp_dir;
    cluster.engines.insert(STOER_ID, engines);
    cluster.start().unwrap();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(STOER_ID), k, v);
    for id in cluster.engines.keys() {
        if *id == STOER_ID {
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
