// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    thread::sleep,
    time::{Duration, Instant},
};

use engine_traits::{
    MiscExt, RaftEngineReadOnly, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE, DATA_CFS,
};

// It tests that delete range for an empty cf does not block the progress of
// persisted_applied. See the description of the PR #14905.
#[test]
fn test_delete_range_does_not_block_flushed_index() {
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

    let start = Instant::now();
    loop {
        let admin_flush = raft_engine.get_flushed_index(1, CF_RAFT).unwrap().unwrap();
        if admin_flush > 200 {
            return;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!(
                "persisted_apply is not progressed, current persisted_apply {}",
                admin_flush
            );
        }
        // wait for persist admin flush index
        sleep(Duration::from_millis(200));
    }
}
