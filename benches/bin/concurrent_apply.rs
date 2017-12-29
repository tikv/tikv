extern crate tempdir;

use std::time::{Duration, SystemTime};
use std::sync::mpsc::channel;
use std::sync::Arc;

use tikv::storage::{CfName, Key, Modify, Mutation, Options, Snapshot, Value, ALL_CFS, CF_DEFAULT,
                    CF_LOCK, CF_RAFT, CF_WRITE};
use tikv::storage::mvcc::MvccTxn;
//use tikv::raftstore::store::engine::SyncSnapshot;
use tikv::util::rocksdb;
use tikv::util::rocksdb::CFOptions;
use rocksdb::{ColumnFamilyOptions, DB};
use tikv::config;
use tikv::util::threadpool::{DefaultContext, ThreadPool, ThreadPoolBuilder};

use tempdir::TempDir;

use utils::*;

use tikv::storage::engine::write_modifies;

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

fn fake_lock_value(primary_len: usize) -> Vec<u8> {
    vec![0; primary_len + 21]
}

fn fake_write_value() -> Vec<u8> {
    vec![0; 11]
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

fn do_bench(batch_size: usize, data_count: usize, threads: usize, value_len: usize) {
    let data_count = if data_count % batch_size == 0 {
        data_count
    } else {
        data_count / batch_size * batch_size + data_count
    };

    println!(
        "benching concurrent applying prewritet\t\
         batch_size:{} threads:{} value_len:{} count:{} ...",
        batch_size,
        threads,
        value_len,
        data_count
    );

    let time_ns = record_time(
        || {
            let db = Arc::new(create_default_db());

            // ceiling of (ata_count / batch_size)
            let task_count = (data_count - 1) / batch_size + 1;
            let mut tasks = Vec::with_capacity(task_count * 2);

            let mut keys = generate_row_keys(1, 0, data_count);
            shuffle(&mut keys);

            let mut current_batch_size = 0;

            for key in keys.iter() {
                if current_batch_size % batch_size == 0 {
                    tasks.push(Vec::with_capacity(batch_size * 2));
                }
                current_batch_size += 1;

                let task = &mut tasks.last_mut().unwrap();

                let encoded_key = Key::from_raw(&key);

                let start_ts = next_ts();

                task.push(Modify::Put(
                    CF_DEFAULT,
                    encoded_key.append_ts(start_ts),
                    vec![0u8; value_len],
                ));
                task.push(Modify::Put(
                    CF_LOCK,
                    encoded_key,
                    fake_lock_value(key.len()),
                ));
            }

            current_batch_size = 0;
            for key in keys {
                if current_batch_size % batch_size == 0 {
                    tasks.push(Vec::with_capacity(batch_size * 2));
                }
                current_batch_size += 1;

                let task = &mut tasks.last_mut().unwrap();

                let encoded_key = Key::from_raw(&key);

                let commit_ts = next_ts();
                task.push(Modify::Put(
                    CF_WRITE,
                    encoded_key.append_ts(commit_ts),
                    fake_write_value(),
                ));
                task.push(Modify::Delete(CF_LOCK, encoded_key));
            }
            let pool = ThreadPoolBuilder::<DefaultContext, _>::with_default_factory(
                "bench-concurrent-apply".to_string(),
            ).thread_count(threads)
                .build();

            let (tx, rx) = channel::<()>();
            let total_task_count = tasks.len();

            let start_time = SystemTime::now();

            // println!(">> Start: {:?}", start_time.elapsed());

            for task in tasks.drain(..) {
                let db = db.clone();
                let tx = tx.clone();
                pool.execute(move |_| {
                    write_modifies(&*db, task);
                    tx.send(()).unwrap();
                });
            }

            // println!(">> Dispatched: {:?}", start_time.elapsed());

            for _ in 0..total_task_count {
                rx.recv().unwrap();
            }

            // println!(">> Finished: {:?}", start_time.elapsed());
            start_time.elapsed().unwrap()
        },
        1,
    );
    println!(
        "    total time:{:>11} ns  average time per key:{:>11} ns",
        time_ns,
        time_ns / (data_count as u64)
    );
}

pub fn bench_concurrent_rocksdb() {
    for batch_size in &[32, 64, 128, 256, 512] {
        for threads in &[1, 2, 4, 8, 16, 32] {
            do_bench(*batch_size, 100000, *threads, 128);
        }
    }
}
