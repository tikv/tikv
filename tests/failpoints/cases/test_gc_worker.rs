// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::AtomicU64, Arc},
    thread,
    time::Duration,
};

use engine_traits::Peekable;
use grpcio::{ChannelBuilder, Environment};
use keys::data_key;
use kvproto::{kvrpcpb::*, metapb::Region, tikvpb::TikvClient};
use raftstore::coprocessor::{
    RegionInfo, RegionInfoCallback, RegionInfoProvider, Result as CopResult, SeekRegionCallback,
};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::{
    server::gc_worker::{
        sync_gc, AutoGcConfig, GcSafePointProvider, GcTask, Result as GcWorkerResult, TestGcRunner,
    },
    storage::{
        kv::TestEngineBuilder,
        mvcc::tests::must_get_none,
        txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put},
    },
};
use tikv_util::HandyRwLock;
use txn_types::{Key, TimeStamp};

// Test write CF's compaction filter can call `orphan_versions_handler`
// correctly.
#[test]
fn test_error_in_compaction_filter() {
    let mut engine = TestEngineBuilder::new().build().unwrap();
    let raw_engine = engine.get_rocksdb();

    let large_value = vec![b'x'; 300];
    must_prewrite_put(&mut engine, b"zkey", &large_value, b"zkey", 101);
    must_commit(&mut engine, b"zkey", 101, 102);
    must_prewrite_put(&mut engine, b"zkey", &large_value, b"zkey", 103);
    must_commit(&mut engine, b"zkey", 103, 104);
    must_prewrite_delete(&mut engine, b"zkey", b"zkey", 105);
    must_commit(&mut engine, b"zkey", 105, 106);

    let fp = "write_compaction_filter_flush_write_batch";
    fail::cfg(fp, "return").unwrap();

    let mut gc_runner = TestGcRunner::new(200);
    gc_runner.gc(&raw_engine);

    match gc_runner.gc_receiver.recv().unwrap() {
        GcTask::OrphanVersions { wb, .. } => assert_eq!(wb.count(), 2),
        GcTask::GcKeys { .. } => {}
        _ => unreachable!(),
    }

    // Although versions on default CF is not cleaned, write CF is GCed correctly.
    must_get_none(&mut engine, b"zkey", 102);
    must_get_none(&mut engine, b"zkey", 104);

    fail::remove(fp);
}

#[derive(Clone)]
struct MockSafePointProvider;
impl GcSafePointProvider for MockSafePointProvider {
    fn get_safe_point(&self) -> GcWorkerResult<TimeStamp> {
        Ok(TimeStamp::from(0))
    }
}

#[derive(Clone)]
struct MockRegionInfoProvider;
impl RegionInfoProvider for MockRegionInfoProvider {
    fn seek_region(&self, _: &[u8], _: SeekRegionCallback) -> CopResult<()> {
        Ok(())
    }
    fn find_region_by_id(
        &self,
        _: u64,
        _: RegionInfoCallback<Option<RegionInfo>>,
    ) -> CopResult<()> {
        Ok(())
    }
    fn get_regions_in_range(&self, _start_key: &[u8], _end_key: &[u8]) -> CopResult<Vec<Region>> {
        Ok(vec![])
    }
}

// Test GC worker can receive and handle orphan versions emit from write CF's
// compaction filter correctly.
#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_orphan_versions_from_compaction_filter() {
    let (cluster, leader, ctx) = new_cluster(|cluster| {
        cluster.cfg.gc.enable_compaction_filter = true;
        cluster.cfg.gc.compaction_filter_skip_version_check = true;
        cluster.pd_client.disable_default_operator();
    });

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    // Call `start_auto_gc` like `cmd/src/server.rs` does. It will combine
    // compaction filter and GC worker so that GC worker can help to process orphan
    // versions on default CF.
    {
        let sim = cluster.sim.rl();
        let gc_worker = sim.get_gc_worker(leader_store);
        gc_worker
            .start_auto_gc(
                AutoGcConfig::new(MockSafePointProvider, MockRegionInfoProvider, 1),
                Arc::new(AtomicU64::new(0)),
                None,
            )
            .unwrap();
    }
    let engine = cluster.get_engine(leader_store);

    let pk = b"k1".to_vec();
    let large_value = vec![b'x'; 300];
    for &start_ts in &[10, 20, 30, 40] {
        let commit_ts = start_ts + 5;
        let op = if start_ts < 40 { Op::Put } else { Op::Del };
        let muts = vec![new_mutation(op, b"k1", &large_value)];
        must_kv_prewrite(&client, ctx.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx.clone(), keys, start_ts, commit_ts, commit_ts);
        if start_ts < 40 {
            let key = Key::from_raw(b"k1").append_ts(start_ts.into());
            let key = data_key(key.as_encoded());
            assert!(engine.get_value(&key).unwrap().is_some());
        }
    }

    let fp = "write_compaction_filter_flush_write_batch";
    fail::cfg(fp, "return").unwrap();

    let gc_safe_ponit = TimeStamp::from(100);
    let gc_scheduler = cluster.sim.rl().get_gc_worker(1).scheduler();
    let region = cluster.get_region(&pk);
    sync_gc(&gc_scheduler, region, gc_safe_ponit).unwrap();

    'IterKeys: for &start_ts in &[10, 20, 30] {
        let key = Key::from_raw(b"k1").append_ts(start_ts.into());
        let key = data_key(key.as_encoded());
        for _ in 0..100 {
            if engine.get_value(&key).unwrap().is_some() {
                thread::sleep(Duration::from_millis(20));
                continue;
            }
            continue 'IterKeys;
        }
        panic!("orphan versions should already been cleaned by GC worker");
    }

    fail::remove(fp);
}
