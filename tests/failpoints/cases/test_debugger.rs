// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine_traits::{RaftEngine, RaftLogBatch, TabletRegistry};
use kvproto::{
    kvrpcpb::MvccInfo,
    metapb,
    raft_serverpb::{PeerState, RegionLocalState},
};
use raft_log_engine::RaftLogEngine;
use test_raftstore::new_peer;
use tikv::{
    config::TikvConfig,
    server::{debug::Debugger, debug2::new_debugger, KvEngineFactoryBuilder},
    storage::{txn::tests::must_prewrite_put, TestEngineBuilder},
};

const INITIAL_TABLET_INDEX: u64 = 5;
const INITIAL_APPLY_INDEX: u64 = 5;

// Prepare some data
// Region meta range and rocksdb range of each region:
// Region 1: k01 .. k04 rocksdb: zk00 .. zk04
// Region 2: k05 .. k09 rocksdb: zk05 .. zk09
// Region 3: k10 .. k14 rocksdb: zk10 .. zk14
// Region 4: k15 .. k19 rocksdb: zk15 .. zk19 <tombstone>
// Region 5: k20 .. k24 rocksdb: zk20 .. zk24
// Region 6: k26 .. k27 rocksdb: zk25 .. zk29
fn prepare_data_on_disk(path: &Path) {
    let mut cfg = TikvConfig::default();
    cfg.storage.data_dir = path.to_str().unwrap().to_string();
    cfg.raft_store.raftdb_path = cfg.infer_raft_db_path(None).unwrap();
    cfg.raft_engine.mut_config().dir = cfg.infer_raft_engine_path(None).unwrap();
    cfg.gc.enable_compaction_filter = false;
    let cache = cfg.storage.block_cache.build_shared_cache();
    let env = cfg.build_shared_rocks_env(None, None).unwrap();

    let factory = KvEngineFactoryBuilder::new(env, &cfg, cache, None).build();
    let reg = TabletRegistry::new(Box::new(factory), path).unwrap();

    let raft_engine = RaftLogEngine::new(cfg.raft_engine.config(), None, None).unwrap();
    let mut wb = raft_engine.log_batch(5);
    for i in 0..6 {
        let mut region = metapb::Region::default();
        let start_key = if i != 0 {
            format!("k{:02}", i * 5)
        } else {
            String::from("k01")
        };
        let end_key = format!("k{:02}", (i + 1) * 5);
        region.set_id(i + 1);
        region.set_start_key(start_key.into_bytes());
        region.set_end_key(end_key.into_bytes());
        let mut region_state = RegionLocalState::default();
        region_state.set_tablet_index(INITIAL_TABLET_INDEX);
        if region.get_id() == 4 {
            region_state.set_state(PeerState::Tombstone);
        } else if region.get_id() == 6 {
            region.set_start_key(b"k26".to_vec());
            region.set_end_key(b"k28".to_vec());
        }
        // add dummy peer to pass verification
        region.mut_peers().push(new_peer(0, 0));
        region_state.set_region(region);

        let tablet_path = reg.tablet_path(i + 1, INITIAL_TABLET_INDEX);
        // Use tikv_kv::RocksEngine instead of loading tablet from registry in order to
        // use prewrite method to prepare mvcc data
        let mut engine = TestEngineBuilder::new().path(tablet_path).build().unwrap();
        for i in i * 5..(i + 1) * 5 {
            let key = format!("zk{:02}", i);
            let val = format!("val{:02}", i);
            // Use prewrite only is enough for preparing mvcc data
            must_prewrite_put(
                &mut engine,
                key.as_bytes(),
                val.as_bytes(),
                key.as_bytes(),
                10,
            );
        }

        wb.put_region_state(i + 1, INITIAL_APPLY_INDEX, &region_state)
            .unwrap();
    }
    raft_engine.consume(&mut wb, true).unwrap();
}

// For simplicity, the format of the key is inline with data in
// prepare_data_on_disk
fn extract_key(key: &[u8]) -> &[u8] {
    &key[1..4]
}

#[test]
fn test_scan_mvcc() {
    // We deliberately make region meta not match with rocksdb, set unlimited range
    // compaction filter to avoid trim operation.
    fail::cfg("unlimited_range_compaction_filter", "return").unwrap();

    let dir = test_util::temp_dir("test-debugger", false);
    prepare_data_on_disk(dir.path());
    let debugger = new_debugger(dir.path());
    // Test scan with bad start, end or limit.
    assert!(debugger.scan_mvcc(b"z", b"", 0).is_err());
    assert!(debugger.scan_mvcc(b"z", b"x", 3).is_err());

    let verify_scanner =
        |range, scanner: &mut dyn Iterator<Item = raftstore::Result<(Vec<u8>, MvccInfo)>>| {
            for i in range {
                let key = format!("k{:02}", i).into_bytes();
                assert_eq!(key, extract_key(&scanner.next().unwrap().unwrap().0));
            }
        };

    // full scan
    let mut scanner = debugger.scan_mvcc(b"", b"", 100).unwrap();
    verify_scanner(1..15, &mut scanner);
    verify_scanner(20..25, &mut scanner);
    verify_scanner(26..28, &mut scanner);
    assert!(scanner.next().is_none());

    // Range has more elements than limit
    let mut scanner = debugger.scan_mvcc(b"zk01", b"zk09", 5).unwrap();
    verify_scanner(1..6, &mut scanner);
    assert!(scanner.next().is_none());

    // Range has less elements than limit
    let mut scanner = debugger.scan_mvcc(b"zk07", b"zk10", 10).unwrap();
    verify_scanner(7..10, &mut scanner);
    assert!(scanner.next().is_none());

    // Start from the key where no region contains it
    let mut scanner = debugger.scan_mvcc(b"zk16", b"", 100).unwrap();
    verify_scanner(20..25, &mut scanner);
    verify_scanner(26..28, &mut scanner);
    assert!(scanner.next().is_none());

    // Scan a range not existed in the cluster
    let mut scanner = debugger.scan_mvcc(b"zk16", b"zk19", 100).unwrap();
    assert!(scanner.next().is_none());

    // The end key is less than the start_key of the first region
    let mut scanner = debugger.scan_mvcc(b"", b"zj", 100).unwrap();
    assert!(scanner.next().is_none());
}
