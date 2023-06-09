// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{
    MiscExt, RaftEngineReadOnly, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, DATA_CFS,
};

#[test]
fn test_flush_before_stop() {
    use test_raftstore_v2::*;

    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    for i in 0..100 {
        let key = format!("k{:03}", i);
        cluster.must_put_cf(CF_WRITE, key.as_bytes(), b"val");
        cluster.must_put_cf(CF_LOCK, key.as_bytes(), b"val");
    }

    cluster.must_delete_range_cf(CF_DEFAULT, b"k000", b"k020");
    cluster.must_delete_range_cf(CF_DEFAULT, b"k020", b"k040");

    let raft_engine = cluster.get_raft_engine(1);
    let mut cache = cluster.engines[0].0.get(1).unwrap();
    let tablet = cache.latest().unwrap();
    tablet.flush_cfs(DATA_CFS, true).unwrap();

    // wait for persist admin flush index
    std::thread::sleep(Duration::from_secs(5));

    let admin_flush = raft_engine.get_flushed_index(1, CF_RAFT).unwrap().unwrap();
    println!("region_id {}, index {:?}", 1, admin_flush);
    assert!(admin_flush > 200);
}
