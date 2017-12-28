extern crate tempdir;

use std::time::{Duration, SystemTime};
use std::sync::mpsc::channel;
use std::sync::Arc;

use tikv::storage::{CfName, Key, Modify, Mutation, Options, Snapshot, Value, ALL_CFS, CF_DEFAULT,
                    CF_LOCK, CF_RAFT, CF_WRITE, TEMP_DIR};
use tikv::storage::mvcc::MvccTxn;
use tikv::raftstore::store::engine::SyncSnapshot;
use tikv::util::rocksdb;
use tikv::util::rocksdb::CFOptions;
use rocksdb::{ColumnFamilyOptions, DB};
use tikv::config;
use tikv::util::threadpool::{ThreadPool, ThreadPoolBuilder, DefaultContext};

use kvproto::kvrpcpb::IsolationLevel;

use tempdir::TempDir;

use utils::*;


fn create_default_db() -> DB {
    let cfs = ALL_CFS;
    let mut cfs_opts = Vec::with_capacity(cfs.len());
    let cfg_rocksdb = config::DbConfig::default();
    for cf in cfs {
        let cf_opt = match *cf {
            CF_DEFAULT => CFOptions::new(CF_DEFAULT, cfg_rocksdb.defaultcf.build_opt()),
            CF_LOCK => CFOptions::new(CF_LOCK, cfg_rocksdb.lockcf.build_opt()),
            CF_WRITE => CFOptions::new(CF_WRITE, cfg_rocksdb.writecf.build_opt()),
            CF_RAFT => CFOptions::new(CF_RAFT, cfg_rocksdb.raftcf.build_opt()),
            _ => CFOptions::new(*cf, ColumnFamilyOptions::new()),
        };
        cfs_opts.push(cf_opt);
    }

    let dir = TempDir::new("temp-rocksdb-concurrent-test").unwrap();
    let path = dir.path().to_str().unwrap().to_owned();
    rocksdb::new_engine(&path, cfs, Some(cfs_opts)).unwrap()
}

fn record_time<F>(mut job: F, iterations: u32) -> u64
where
    F: FnMut() -> Duration,
{
    let mut total_time = Duration::new(0, 0);
    for _ in 0..iterations {
        total_time += job();
    }
    total_time /= iterations;
    total_time.as_secs() * 1_000_000_000 + (total_time.subsec_nanos() as u64)
}

fn do_bench(batch_size: usize, data_count: usize, threads: usize) {
    let time_ns = record_time(
        || {
            let db = create_default_db();
            // Is it ok to use only one snapshot?
            let snapshot = box SyncSnapshot::new(Arc::new(db)) as Box<Snapshot>;
            let mut mutations = Vec::with_capacity(data_count * 2);
            let keys = generate_row_keys(1, 0,data_count);
            for key in keys {
                let mut txn =
                    MvccTxn::new(snapshot.clone(), next_ts(), None, IsolationLevel::SI, false);
                txn.prewrite(
                    Mutation::Put((Key::from_raw(&key), vec![0u8; 128])),
                    &key,
                    &Options::default(),
                );
                mutations.extend(txn.into_modifies());
            }

            let pool = ThreadPoolBuilder::<DefaultContext, _>::with_default_factory(
                "bench-concurrent-rocksdb".to_string(),
            ).thread_count(threads)
                .build();

            let (tx, rx) = channel::<()>();
            Duration::new(0,0)
        },
        10,
    );
}

pub fn bench_concurrent_rocksdb() {}
