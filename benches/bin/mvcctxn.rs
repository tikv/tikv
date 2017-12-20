extern crate rand;

use std::sync::atomic::{ATOMIC_U64_INIT, AtomicU64, Ordering};

use tikv::storage::{new_local_engine, Engine, Key, Modify, Mutation, Options, SnapshotStore,
                    Statistics, ALL_CFS, TEMP_DIR};
use tikv::storage::mvcc::MvccTxn;
use kvproto::kvrpcpb::{Context, IsolationLevel};

use super::print_result;
use test::BenchSamples;

use rand::Rng;


#[inline]
fn next_ts() -> u64 {
    static CURRENT: AtomicU64 = ATOMIC_U64_INIT;
    CURRENT.fetch_add(1, Ordering::SeqCst)
}

#[inline]
fn to_bytes(value: u64) -> Vec<u8> {
    let mut result = Vec::new();
    // big endian
    for shift in (0..8).rev() {
        result.push(((value >> shift) & 0xff) as u8);
    }
    result
}

fn create_row_key(tableid: u64, rowid: u64) -> Vec<u8> {
    let mut result = to_bytes(tableid);
    result.extend(b"_r".iter());
    result.extend(to_bytes(rowid).iter());
    result
}

fn generate_keys(count: usize) -> Vec<Vec<u8>> {
    let mut result = Vec::new();
    for i in 0..count {
        result.push(create_row_key(1u64, i as u64));
    }
    result
}

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
    let mut rng = rand::thread_rng();

    for _ in 0..versions {
        for key in keys {
            let mut value = vec![0u8; value_len];
            rng.fill_bytes(&mut value);
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

fn bench_set(engine: &Engine, keys: &[Vec<u8>], value_len: usize) -> BenchSamples {
    let mut rng = rand::thread_rng();
    bench!{
        let start_ts = next_ts();
        let commit_ts = next_ts();
        let mut value = vec![0u8; value_len];
        rng.fill_bytes(&mut value);

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


// Run all bench with specified parameters
fn bench_all(table_size: usize, version_count: usize, value_len: usize) {
    let keys = generate_keys(table_size);
    let engine = prepare_test_engine(version_count, value_len, &keys);

    printf!(
        "benching mvcctxn get\trows:{} versions:{} value len:{}\t...",
        table_size,
        version_count,
        value_len
    );
    print_result(bench_get(&*engine, &keys));

    printf!(
        "benching mvcctxn set\trows:{} versions:{} value len:{}\t...",
        table_size,
        version_count,
        value_len
    );
    print_result(bench_set(&*engine, &keys, value_len));

    // Generate new engine to bench delete, for the size of content was increased when benching set
    let engine = prepare_test_engine(version_count, value_len, &keys);

    printf!(
        "benching mvcctxn delete\trows:{} versions:{} value len:{}\t...",
        table_size,
        version_count,
        value_len
    );
    print_result(bench_delete(&*engine, &keys));
}

pub fn bench_mvcctxn() {
    for table_size in &[1_000, 10_000, 100_000] {
        bench_all(*table_size, 5, 128);
    }

    for version_count in &[1, 5, 20] {
        bench_all(10_000, *version_count, 128);
    }

    for value_len in &[32, 128, 1024] {
        bench_all(10_000, 5, *value_len);
    }
}
