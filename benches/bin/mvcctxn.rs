extern crate rand;

use std::time::SystemTime;
use std::sync::mpsc::channel;

use tikv::storage::{new_local_engine, Engine, Key, Modify, Mutation, Options, SnapshotStore,
                    Statistics, ALL_CFS, TEMP_DIR};
use tikv::storage::mvcc::MvccTxn;
use tikv::util::threadpool::{DefaultContext, ThreadPoolBuilder};
use kvproto::kvrpcpb::{Context, IsolationLevel};

use super::print_result;
use test::BenchSamples;

use rand::Rng;

use utils::*;



#[inline]
fn do_write(engine: &Engine, modifies: Vec<Modify>) {
    engine.write(&Context::new(), modifies).unwrap();
}

#[inline]
fn prewrite(engine: &Engine, mutations: &[Mutation], primary: &[u8], start_ts: u64) {
    let snapshot = engine.snapshot(&Context::new()).unwrap();
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for m in mutations {
        txn.prewrite(m.clone(), primary, &Options::default())
            .unwrap();
    }
    do_write(engine, txn.into_modifies());
}

#[inline]
fn commit(engine: &Engine, keys: &[Key], start_ts: u64, commit_ts: u64) {
    let snapshot = engine.snapshot(&Context::new()).unwrap();
    let mut txn = MvccTxn::new(snapshot, start_ts, None, IsolationLevel::SI, false);
    for key in keys {
        txn.commit(key, commit_ts).unwrap();
    }
    do_write(engine, txn.into_modifies());
}

fn prepare_test_engine(versions: usize, value_len: usize, keys: &[Vec<u8>]) -> Box<Engine> {
    let engine = new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

    for _ in 0..versions {
        for key in keys {
            let value = vec![0u8; value_len];
            let start_ts = next_ts();
            let commit_ts = next_ts();

            prewrite(
                &*engine,
                &[Mutation::Put((Key::from_raw(key), value))],
                key,
                start_ts,
            );
            commit(&*engine, &[Key::from_raw(key)], start_ts, commit_ts);
        }
    }
    engine
}

#[inline]
fn get(engine: &Engine, key: &Key, statistics: &mut Statistics) -> Option<Vec<u8>> {
    let snapshot = engine.snapshot(&Context::new()).unwrap();
    let start_ts = next_ts();
    let snapstore = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, false);
    snapstore.get(key, statistics).unwrap()
}

fn bench_get(engine: &Engine, keys: &[Vec<u8>]) -> BenchSamples {
    let mut fake_statistics = Statistics::default();
    let mut rng = rand::thread_rng();
    bench!{
        let index = rng.gen_range(0, keys.len());
        let key = Key::from_raw(&keys[index]);

        get(engine, &key, &mut fake_statistics).unwrap()
    }
}

fn bench_batch_set_impl(
    engine: &Engine,
    keys: &[Vec<u8>],
    value_len: usize,
    batch_size: usize,
) -> BenchSamples {
    // Avoid writing duplicated keys in a single transaction
    let mut indices: Vec<_> = (0..keys.len()).collect();
    let mut rng = rand::thread_rng();

    let mut keys_to_write: Vec<Key> = Vec::with_capacity(batch_size);
    let mut mutations: Vec<Mutation> = Vec::with_capacity(batch_size);

    bench!{
        let start_ts = next_ts();
        let commit_ts = next_ts();

        keys_to_write.clear();
        mutations.clear();
        for i in 0..batch_size {
            let selected = rng.gen_range(i, keys.len());
            let tmp = indices[selected];
            indices[selected] = indices[i];
            indices[i] = tmp;

            let key = Key::from_raw(&keys[tmp]);
            let value = vec![0u8; value_len];

            mutations.push(Mutation::Put((key.clone(), value)));
            keys_to_write.push(key);
        };

        let primary = &keys[indices[0]];
        prewrite(engine, &mutations, primary, start_ts);
        commit(engine, &keys_to_write, start_ts, commit_ts)
    }
}

fn bench_set(engine: &Engine, keys: &[Vec<u8>], value_len: usize) -> BenchSamples {
    let mut rng = rand::thread_rng();
    bench!{
        let start_ts = next_ts();
        let commit_ts = next_ts();
        let value = vec![0u8; value_len];

        let key = &keys[rng.gen_range(0, keys.len())];

        prewrite(engine, &[Mutation::Put((Key::from_raw(key), value))], key, start_ts);
        commit(engine, &[Key::from_raw(key)], start_ts, commit_ts)
    }
}

fn bench_delete(engine: &Engine, keys: &[Vec<u8>]) -> BenchSamples {
    let mut rng = rand::thread_rng();
    bench!{
        let start_ts = next_ts();
        let commit_ts = next_ts();

        let key = &keys[rng.gen_range(0, keys.len())];
        prewrite(engine, &[Mutation::Delete(Key::from_raw(key))], key, start_ts);
        commit(engine, &[Key::from_raw(key)], start_ts, commit_ts)
    }
}

enum BenchType {
    Row,
    UniqueIndex,
}

// Run all bench with specified parameters
fn bench_single_row(
    table_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {

    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    shuffle(&mut keys);

    let engine = prepare_test_engine(version_count, value_len, &keys);

    printf!(
        "benching mvcctxn {} get\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    print_result(bench_get(&*engine, &keys));

    printf!(
        "benching mvcctxn {} set\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    print_result(bench_set(&*engine, &keys, value_len));

    // Generate new engine to bench delete, for the size of content was increased when benching set
    let engine = prepare_test_engine(version_count, value_len, &keys);

    printf!(
        "benching mvcctxn {} delete\trows:{} versions:{} data len:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len
    );
    print_result(bench_delete(&*engine, &keys));
}

fn bench_batch_set(
    table_size: usize,
    batch_size: usize,
    version_count: usize,
    data_len: usize,
    bench_type: &BenchType,
) {
    let (mut keys, value_len, log_name) = match *bench_type {
        BenchType::Row => (generate_row_keys(1, 0, table_size), data_len, "row"),
        BenchType::UniqueIndex => (
            generate_unique_index_keys(1, 1, data_len, table_size),
            8,
            "unique index",
        ),
    };

    shuffle(&mut keys);

    let engine = prepare_test_engine(version_count, value_len, &keys);

    printf!(
        "benching mvcctxn {} batch write\trows:{} versions:{} data len:{} batch:{}\t...",
        log_name,
        table_size,
        version_count,
        data_len,
        batch_size,
    );
    print_result(bench_batch_set_impl(&*engine, &keys, value_len, batch_size));
}


fn bench_concurrent_batch_impl(
    txn_count: usize,
    data_len: usize,
    batch_size: usize,
    threads: usize,
    bench_type: &BenchType,
) {
    let (value_len, log_name) = match *bench_type {
        BenchType::Row => (data_len, "row"),
        BenchType::UniqueIndex => (8, "unique index"),
    };

    println!(
        "benching mvcctxn {} concurrent write\tbatch size:{} batch count:{} threads:{}\t...",
        log_name,
        batch_size,
        txn_count,
        threads
    );
    let time_record = record_time(
        || {
            let mut keys = match *bench_type {
                BenchType::Row => generate_row_keys(1, 0, txn_count * batch_size),
                BenchType::UniqueIndex => {
                    generate_unique_index_keys(1, 1, data_len, txn_count * batch_size)
                }
            };

            shuffle(&mut keys);

            let mut keys = keys.drain(..);
            let mut txns: Vec<Vec<_>> = (0..txn_count)
                .map(|_| (&mut keys).take(batch_size).collect())
                .collect();

            let engine = new_local_engine(TEMP_DIR, ALL_CFS).unwrap();

            let pool = ThreadPoolBuilder::<DefaultContext, _>::with_default_factory(
                String::from("bench-concurrent-mvcctxn"),
            ).thread_count(threads)
                .build();

            let (tx, rx) = channel::<()>();

            let start_time = SystemTime::now();

            let actual_count = txns.len();

            for mut txn in txns.drain(..) {
                let engine = engine.clone();
                let tx = tx.clone();
                pool.execute(move |_| {
                    let mutations: Vec<_> = txn.iter()
                        .map(|ref item| {
                            Mutation::Put((Key::from_raw(&item), vec![0u8; value_len]))
                        })
                        .collect();
                    let primary = txn[0].clone();
                    let keys: Vec<_> = txn.drain(..).map(|ref item| Key::from_raw(&item)).collect();
                    let start_ts = next_ts();
                    prewrite(&*engine, &mutations, &primary, start_ts);
                    commit(&*engine, &keys, start_ts, next_ts());
                    tx.send(()).unwrap();
                })
            }

            for _ in 0..actual_count {
                rx.recv().unwrap();
            }

            start_time.elapsed().unwrap()
        },
        10,
    );
    let total_time = average(&time_record);
    println!(
        "    total:{:>10}ns, avg {:>10}ns per batch {:>10}ns per key",
        total_time,
        total_time / (txn_count as u64),
        total_time / (txn_count as u64) / (batch_size as u64)
    );
}



pub fn bench_mvcctxn() {
    for bench_type in &[BenchType::Row, BenchType::UniqueIndex] {
        for table_size in &[1_000, 10_000, 100_000] {
            bench_single_row(*table_size, 5, 128, bench_type);
        }

        for version_count in &[1, 16, 32, 64] {
            bench_single_row(10_000, *version_count, 128, bench_type);
        }

        for value_len in &[32, 128, 1024] {
            bench_single_row(10_000, 5, *value_len, bench_type);
        }
    }

    for batch_size in &[1, 8, 32, 64, 128, 256, 512] {
        bench_batch_set(10_000, *batch_size, 5, 128, &BenchType::Row);
    }
}


pub fn bench_concurrent_batch() {
    let txn_count = 100_000;
    for batch_size in &[1, 8, 32, 64, 128, 256] {
        for threads in &[1, 2, 4, 8, 16] {
            bench_concurrent_batch_impl(txn_count, 128, *batch_size, *threads, &BenchType::Row);
        }
    }
}
