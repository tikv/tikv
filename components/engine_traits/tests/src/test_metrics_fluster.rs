// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use rocksdb::DBOptions;
use tempfile::Builder;

use engine_rocks::util::{self as rocks_util, RocksCFOptions};
use engine_rocks::{RocksColumnFamilyOptions, RocksDBOptions};
use engine_traits::{
    ColumnFamilyOptions, KvEngines, MetricsFlusher, CF_DEFAULT, CF_LOCK, CF_WRITE,
};

#[test]
fn test_metrics_flusher() {
    let path = Builder::new()
        .prefix("_test_metrics_flusher")
        .tempdir()
        .unwrap();
    let raft_path = path.path().join(Path::new("raft"));
    let db_opt = RocksDBOptions::from_raw(DBOptions::new());
    let cf_opts = RocksColumnFamilyOptions::new();
    let cfs_opts = vec![
        RocksCFOptions::new(CF_DEFAULT, ColumnFamilyOptions::new()),
        RocksCFOptions::new(CF_LOCK, ColumnFamilyOptions::new()),
        RocksCFOptions::new(CF_WRITE, cf_opts),
    ];
    let engine =
        rocks_util::new_engine_opt(path.path().to_str().unwrap(), db_opt, cfs_opts).unwrap();

    let cfs_opts = vec![RocksCFOptions::new(
        CF_DEFAULT,
        RocksColumnFamilyOptions::new(),
    )];
    let raft_engine = rocks_util::new_engine_opt(
        raft_path.to_str().unwrap(),
        RocksDBOptions::from_raw(DBOptions::new()),
        cfs_opts,
    )
    .unwrap();
    let shared_block_cache = false;
    let engines = KvEngines::new(engine, raft_engine, shared_block_cache);
    let mut metrics_flusher = MetricsFlusher::new(engines);
    metrics_flusher.set_flush_interval(Duration::from_millis(100));

    if let Err(e) = metrics_flusher.start() {
        error!("failed to start metrics flusher, error = {:?}", e);
    }

    let rtime = Duration::from_millis(300);
    sleep(rtime);

    metrics_flusher.stop();
}
