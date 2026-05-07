// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::Path, time::Duration};

use engine_traits::{DbOptionsExt, MiscExt, Peekable, CF_DEFAULT, CF_LOCK, CF_WRITE, DATA_CFS};
use futures::executor::block_on;
use raftstore::store::RAFT_INIT_LOG_INDEX;
use raftstore_v2::{router::PeerMsg, SimpleWriteEncoder};

use crate::cluster::Cluster;

fn count_file(path: &Path, pat: impl Fn(&Path) -> bool) -> usize {
    let mut count = 0;
    for path in std::fs::read_dir(path).unwrap() {
        if pat(&path.unwrap().path()) {
            count += 1;
        }
    }
    count
}

fn count_sst(path: &Path) -> usize {
    count_file(path, |path| {
        path.extension().map_or(false, |ext| ext == "sst")
    })
}

fn count_info_log(path: &Path) -> usize {
    count_file(path, |path| {
        path.file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("LOG")
    })
}

/// Test if data will be recovered correctly after being restarted.
#[test]
fn test_data_recovery() {
    let mut cluster = Cluster::default();
    let registry = cluster.node(0).tablet_registry();
    let tablet_2_path = registry.tablet_path(2, RAFT_INIT_LOG_INDEX);
    // The rocksdb is a bootstrapped tablet, so it will be opened and closed in
    // bootstrap, and then open again in fsm initialization.
    assert_eq!(count_info_log(&tablet_2_path), 2);
    let router = &mut cluster.routers[0];
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    // Write 100 keys to default CF and not flush.
    let header = Box::new(router.new_request_for(2).take_header());
    for i in 0..100 {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(
            CF_DEFAULT,
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        router
            .send(2, PeerMsg::simple_write(header.clone(), put.encode()).0)
            .unwrap();
    }

    // Write 100 keys to write CF and flush half.
    let mut sub = None;
    for i in 0..50 {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(
            CF_WRITE,
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        let (msg, s) = PeerMsg::simple_write(header.clone(), put.encode());
        router.send(2, msg).unwrap();
        sub = Some(s);
    }
    let resp = block_on(sub.take().unwrap().result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    let mut cached = cluster.node(0).tablet_registry().get(2).unwrap();
    cached.latest().unwrap().flush_cf(CF_WRITE, true).unwrap();
    let router = &mut cluster.routers[0];
    for i in 50..100 {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(
            CF_WRITE,
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        router
            .send(2, PeerMsg::simple_write(header.clone(), put.encode()).0)
            .unwrap();
    }

    // Write 100 keys to lock CF and flush all.
    for i in 0..100 {
        let mut put = SimpleWriteEncoder::with_capacity(64);
        put.put(
            CF_LOCK,
            format!("key{}", i).as_bytes(),
            format!("value{}", i).as_bytes(),
        );
        let (msg, s) = PeerMsg::simple_write(header.clone(), put.encode());
        router.send(2, msg).unwrap();
        sub = Some(s);
    }
    let resp = block_on(sub.take().unwrap().result()).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    cached = cluster.node(0).tablet_registry().get(2).unwrap();
    cached.latest().unwrap().flush_cf(CF_LOCK, true).unwrap();

    // Make sure all keys must be written.
    let router = &mut cluster.routers[0];
    let snap = router.stale_snapshot(2);
    for cf in DATA_CFS {
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = snap.get_value_cf(cf, key.as_bytes()).unwrap();
            assert_eq!(
                value.as_deref(),
                Some(format!("value{}", i).as_bytes()),
                "{} {}",
                cf,
                key
            );
        }
    }
    let registry = cluster.node(0).tablet_registry();
    cached = registry.get(2).unwrap();
    cached
        .latest()
        .unwrap()
        .set_db_options(&[("avoid_flush_during_shutdown", "true")])
        .unwrap();
    drop((snap, cached));

    cluster.restart(0);

    let registry = cluster.node(0).tablet_registry();
    cached = registry.get(2).unwrap();
    cached
        .latest()
        .unwrap()
        .set_db_options(&[("avoid_flush_during_shutdown", "true")])
        .unwrap();
    let router = &mut cluster.routers[0];

    // Write another key to ensure all data are recovered.
    let mut put = SimpleWriteEncoder::with_capacity(64);
    put.put(CF_DEFAULT, b"key101", b"value101");
    let resp = router.simple_write(2, header, put).unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);

    // After being restarted, all unflushed logs should be applied again. So there
    // should be no missing data.
    let snap = router.stale_snapshot(2);
    for cf in DATA_CFS {
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = snap.get_value_cf(cf, key.as_bytes()).unwrap();
            assert_eq!(
                value.as_deref(),
                Some(format!("value{}", i).as_bytes()),
                "{} {}",
                cf,
                key
            );
        }
    }

    // There is a restart, so LOG file should be rotate.
    assert_eq!(count_info_log(&tablet_2_path), 3);
    // We only trigger Flush twice, so there should be only 2 files. And because WAL
    // is disabled, so when rocksdb is restarted, there should be no WAL to recover,
    // so no additional flush will be triggered.
    assert_eq!(count_sst(&tablet_2_path), 2);

    cached = cluster.node(0).tablet_registry().get(2).unwrap();
    cached.latest().unwrap().flush_cfs(DATA_CFS, true).unwrap();

    // Although all CFs are triggered again, but recovery should only write:
    // 1. [0, 101) to CF_DEFAULT
    // 2. [50, 100) to CF_WRITE
    //
    // So there will be only 2 memtables to be flushed.
    assert_eq!(count_sst(&tablet_2_path), 4);

    drop((snap, cached));

    cluster.restart(0);

    let router = &mut cluster.routers[0];

    assert_eq!(count_info_log(&tablet_2_path), 4);
    // Because data is flushed before restarted, so all data can be read
    // immediately.
    let snap = router.stale_snapshot(2);
    for cf in DATA_CFS {
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = snap.get_value_cf(cf, key.as_bytes()).unwrap();
            assert_eq!(
                value.as_deref(),
                Some(format!("value{}", i).as_bytes()),
                "{} {}",
                cf,
                key
            );
        }
    }
    // Trigger flush again.
    cached = cluster.node(0).tablet_registry().get(2).unwrap();
    cached.latest().unwrap().flush_cfs(DATA_CFS, true).unwrap();

    // There is no recovery, so there should be nothing to flush.
    assert_eq!(count_sst(&tablet_2_path), 4);
}
