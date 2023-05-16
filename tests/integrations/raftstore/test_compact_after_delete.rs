// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Mutex},
    time::Duration,
};

use collections::HashMap;
use engine_rocks::{raw::Range, util::get_cf_handle};
use engine_traits::{CachedTablet, MiscExt, CF_WRITE};
use keys::{data_key, DATA_MAX_KEY};
use test_raftstore::*;
use tikv::storage::mvcc::{TimeStamp, Write, WriteType};
use tikv_util::config::*;
use txn_types::Key;

fn gen_mvcc_put_kv(
    k: &[u8],
    v: &[u8],
    start_ts: TimeStamp,
    commit_ts: TimeStamp,
) -> (Vec<u8>, Vec<u8>) {
    let k = Key::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    let w = Write::new(WriteType::Put, start_ts, Some(v.to_vec()));
    (k.as_encoded().clone(), w.as_ref().to_bytes())
}

fn gen_delete_k(k: &[u8], commit_ts: TimeStamp) -> Vec<u8> {
    let k = Key::from_encoded(data_key(k));
    let k = k.append_ts(commit_ts);
    k.as_encoded().clone()
}

fn test_compact_after_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_compact_min_tombstones = 500;
    cluster.cfg.raft_store.region_compact_tombstones_percent = 50;
    cluster.cfg.raft_store.region_compact_check_step = Some(1);
    cluster.cfg.rocksdb.titan.enabled = true;
    cluster.run();

    for i in 0..1000 {
        let (k, v) = (format!("k{}", i), format!("value{}", i));
        let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        cluster.must_put_cf(CF_WRITE, &k, &v);
    }
    for engines in cluster.engines.values() {
        engines.kv.flush_cf(CF_WRITE, true).unwrap();
    }
    let (sender, receiver) = mpsc::channel();
    let sync_sender = Mutex::new(sender);
    fail::cfg_callback(
        "raftstore::compact::CheckAndCompact:AfterCompact",
        move || {
            let sender = sync_sender.lock().unwrap();
            sender.send(true).unwrap();
        },
    )
    .unwrap();
    for i in 0..1000 {
        let k = format!("k{}", i);
        let k = gen_delete_k(k.as_bytes(), 2.into());
        cluster.must_delete_cf(CF_WRITE, &k);
    }
    for engines in cluster.engines.values() {
        engines.kv.flush_cf(CF_WRITE, true).unwrap();
    }

    // wait for compaction.
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();

    for engines in cluster.engines.values() {
        let cf_handle = get_cf_handle(engines.kv.as_inner(), CF_WRITE).unwrap();
        let approximate_size = engines
            .kv
            .as_inner()
            .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
        assert_eq!(approximate_size, 0);
    }
}

#[test]
fn test_node_compact_after_delete() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_compact_after_delete(&mut cluster);
}

#[test]
fn test_node_compact_after_delete_v2() {
    let count = 1;
    let mut cluster = test_raftstore_v2::new_node_cluster(0, count);

    cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.region_compact_min_tombstones = 50;
    cluster.cfg.raft_store.region_compact_tombstones_percent = 50;
    // disable it
    cluster.cfg.raft_store.region_compact_min_redundant_rows = 10000000;
    cluster.cfg.raft_store.region_compact_check_step = Some(2);
    cluster.cfg.rocksdb.titan.enabled = true;
    cluster.run();

    let region = cluster.get_region(b"");
    let (split_key, _) = gen_mvcc_put_kv(b"k100", b"", 1.into(), 2.into());
    cluster.must_split(&region, &split_key);

    for i in 0..200 {
        let (k, v) = (format!("k{:03}", i), format!("value{}", i));
        let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        cluster.must_put_cf(CF_WRITE, &k, &v);
    }
    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                db.flush_cf(CF_WRITE, true).unwrap();
            }
            true
        })
    }

    let (sender, receiver) = mpsc::channel();
    let sync_sender = Mutex::new(sender);
    fail::cfg_callback("raftstore-v2::CheckAndCompact::AfterCompact", move || {
        let sender = sync_sender.lock().unwrap();
        sender.send(true).unwrap();
    })
    .unwrap();
    for i in 0..200 {
        let k = format!("k{:03}", i);
        let k = gen_delete_k(k.as_bytes(), 2.into());
        cluster.must_delete_cf(CF_WRITE, &k);
    }
    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                db.flush_cf(CF_WRITE, true).unwrap();
            }
            true
        })
    }

    // wait for 2 regions' compaction.
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();

    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                let cf_handle = get_cf_handle(db.as_inner(), CF_WRITE).unwrap();
                let approximate_size = db
                    .as_inner()
                    .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
                assert_eq!(approximate_size, 0);
            }
            true
        })
    }
}

#[test]
fn test_node_compact_after_update_v2() {
    let count = 1;
    let mut cluster = test_raftstore_v2::new_node_cluster(0, count);

    cluster.cfg.raft_store.region_compact_check_interval = ReadableDuration::millis(100);
    // disable it
    cluster.cfg.raft_store.region_compact_min_tombstones = 1000000;
    cluster.cfg.raft_store.region_compact_redundant_rows_percent = 40;
    cluster.cfg.raft_store.region_compact_min_redundant_rows = 50;
    cluster.cfg.raft_store.region_compact_check_step = Some(2);
    cluster.cfg.rocksdb.titan.enabled = true;
    cluster.run();

    let region = cluster.get_region(b"");
    let (split_key, _) = gen_mvcc_put_kv(b"k100", b"", 1.into(), 2.into());
    cluster.must_split(&region, &split_key);

    for i in 0..200 {
        let (k, v) = (format!("k{:03}", i), format!("value{}", i));
        let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 1.into(), 2.into());
        cluster.must_put_cf(CF_WRITE, &k, &v);

        let (k, v) = (format!("k{:03}", i), format!("value{}", i));
        let (k, v) = gen_mvcc_put_kv(k.as_bytes(), v.as_bytes(), 3.into(), 4.into());
        cluster.must_put_cf(CF_WRITE, &k, &v);
    }
    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|_, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                db.flush_cf(CF_WRITE, true).unwrap();
            }
            true
        })
    }

    fail::cfg("on_collect_regions_to_compact", "pause").unwrap();
    let mut db_size_before_compact = HashMap::default();
    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|id, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                let cf_handle = get_cf_handle(db.as_inner(), CF_WRITE).unwrap();
                let approximate_size = db
                    .as_inner()
                    .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
                db_size_before_compact.insert(id, approximate_size);
            }
            true
        })
    }
    fail::remove("on_collect_regions_to_compact");

    let (sender, receiver) = mpsc::channel();
    let sync_sender = Mutex::new(sender);
    fail::cfg_callback("raftstore-v2::CheckAndCompact::AfterCompact", move || {
        let sender = sync_sender.lock().unwrap();
        sender.send(true).unwrap();
    })
    .unwrap();

    // wait for 2 regions' compaction.
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();
    receiver.recv_timeout(Duration::from_millis(5000)).unwrap();

    for (registry, _) in &cluster.engines {
        registry.for_each_opened_tablet(|id, db: &mut CachedTablet<_>| {
            if let Some(db) = db.latest() {
                let cf_handle = get_cf_handle(db.as_inner(), CF_WRITE).unwrap();
                let approximate_size = db
                    .as_inner()
                    .get_approximate_sizes_cf(cf_handle, &[Range::new(b"", DATA_MAX_KEY)])[0];
                let size_before = db_size_before_compact.get(&id).unwrap();
                assert!(approximate_size < *size_before);
            }
            true
        })
    }
}
