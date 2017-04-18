
use std::u64;
use tempdir::TempDir;
use test::Bencher;

use tikv::storage::engine::{self, Engine, Statistics, ScanMode};
use tikv::storage::mvcc::{MvccReader, MvccTxn};
use tikv::storage::txn::{SnapshotStore, StoreScanner};
use tikv::storage::{make_key, Options, ALL_CFS, Mutation};
use kvproto::kvrpcpb::Context;

const MB: usize = 1024 * 1024;

fn must_prewrite_put(engine: &Engine, key: &[u8], value: &[u8], pk: &[u8], ts: u64) {
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut statistics = Statistics::default();
    let mut txn = MvccTxn::new(snapshot.as_ref(), &mut statistics, ts, None);
    txn.prewrite(Mutation::Put((make_key(key), value.to_vec())),
                 pk,
                 &Options::default())
    .unwrap();
    engine.write(&ctx, txn.modifies()).unwrap();
}

fn must_commit(engine: &Engine, key: &[u8], start_ts: u64, commit_ts: u64) {
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut statistics = Statistics::default();
    let mut txn = MvccTxn::new(snapshot.as_ref(), &mut statistics, start_ts, None);
    txn.commit(&make_key(key), commit_ts).unwrap();
    engine.write(&ctx, txn.modifies()).unwrap();
}

fn init_data(engine: &Engine, target_size: usize, commit: bool) {
    let v = b"this is some content of value";
    let mut current_size = 0;
    for i in 0.. {
        let k = format!("key_a_{}", i);
        must_prewrite_put(engine, k.as_bytes(), k.as_bytes(), v, i as u64);
        if commit {
            must_commit(engine, k.as_bytes(), i as u64, i as u64);
        }
        current_size += k.len() + v.len();
        if current_size > target_size {
            break;
        }
    }
}

#[bench]
fn bench_mvcc_reader_get(b: &mut Bencher) {
    let path = TempDir::new("bench_mvcc_reader_get").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
    init_data(engine.as_ref(), 10 * MB, true);

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut reader = MvccReader::new(snapshot.as_ref(), &mut statistics, None, true, None);

    b.iter(|| {
        reader.get(&make_key(b"key_a_1000"), 1001);
    });
}

#[bench]
fn bench_mvcc_reader_scan_1000_lock(b: &mut Bencher) {
    let path = TempDir::new("bench_mvcc_reader_scan_1000_lock").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
    init_data(engine.as_ref(), 1 * MB, false);

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();

    b.iter(|| {
        let mut reader = MvccReader::new(snapshot.as_ref(),
                                         &mut statistics,
                                         Some(ScanMode::Forward),
                                         true,
                                         None);
        let max_ts = u64::MAX;
        reader.scan_lock(None, |lock| lock.ts <= max_ts, Some(1000)).unwrap();
    });
}

#[bench]
fn bench_mvcc_reader_scan_1000_keys(b: &mut Bencher) {
    let path = TempDir::new("bench_mvcc_reader_scan_1000_keys").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
    init_data(engine.as_ref(), 10 * MB, true);

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut reader = MvccReader::new(snapshot.as_ref(),
                                     &mut statistics,
                                     Some(ScanMode::Forward),
                                     true,
                                     None);
    b.iter(|| {
        reader.scan_keys(None, 1000).unwrap();
    });
}

fn bench_mvcc_reverse_scan_1000_keys(b: &mut Bencher, key_only: bool) {
    let path = TempDir::new("bench_mvcc_reverse_scan_1000_keys").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
    init_data(engine.as_ref(), 10 * MB, true);

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut snap = SnapshotStore::new(snapshot.as_ref(), u64::MAX);

    b.iter(|| {
        let mut scanner = snap.scanner(ScanMode::Backward, key_only, None, &mut statistics);
        scanner.reverse_scan(make_key(b"key_b"), 1000);
    });
}

#[bench]
fn bench_mvcc_reverse_scan_1000_keys_key_only(b: &mut Bencher) {
    bench_mvcc_reverse_scan_1000_keys(b, true);
}

#[bench]
fn bench_mvcc_reverse_scan_1000_keys_key_value(b: &mut Bencher) {
    bench_mvcc_reverse_scan_1000_keys(b, false);
}

fn bench_mvcc_scan_1000_keys(b: &mut Bencher, key_only: bool) {
    let path = TempDir::new("bench_mvcc_scan_1000_keys").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();
    init_data(engine.as_ref(), 10 * MB, true);

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut snap = SnapshotStore::new(snapshot.as_ref(), u64::MAX);

    b.iter(|| {
        let mut scanner = snap.scanner(ScanMode::Forward, key_only, None, &mut statistics);
        scanner.scan(make_key(b"key_a"), 1000);
    });
}

#[bench]
fn bench_mvcc_scan_1000_keys_key_only(b: &mut Bencher) {
    bench_mvcc_scan_1000_keys(b, true);
}

#[bench]
fn bench_mvcc_scan_1000_keys_key_value(b: &mut Bencher) {
    bench_mvcc_scan_1000_keys(b, false);
}

#[bench]
fn bench_txn_prewrite(b: &mut Bencher) {
    let path = TempDir::new("bench_mvcc_txn_prewrite").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut ts: u64 = 1;
    let v = b"this is some content of value";

    b.iter(|| {
        let mut txn = MvccTxn::new(snapshot.as_ref(), &mut statistics, ts, None);
        let key = make_key(format!("key_a_{}", ts).as_bytes());
        txn.prewrite(Mutation::Put((key.clone(), v)),
                     key,
                     &Options::default())
        .unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();
        ts = ts + 1;
    });
}

#[bench]
fn bench_txn_prewrite_commit(b: &mut Bencher) {
    let path = TempDir::new("bench_mvcc_txn_prewrite_commit").unwrap();
    let engine = engine::new_local_engine(path.path().to_str().unwrap(), ALL_CFS).unwrap();

    let mut statistics = Statistics::default();
    let ctx = Context::new();
    let snapshot = engine.snapshot(&ctx).unwrap();
    let mut ts: u64 = 1;
    let v = b"this is some content of value";

    b.iter(|| {
        // prewrite
        let mut txn = MvccTxn::new(snapshot.as_ref(), &mut statistics, ts, None);
        let key = make_key(format!("key_a_{}", ts).as_bytes());
        txn.prewrite(Mutation::Put((key.clone(), v.to_vec())),
                     key.clone().as_bytes(),
                     &Options::default())
        .unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();

        // commit
        let mut txn = MvccTxn::new(snapshot.as_ref(), &mut statistics, ts , None);
        ts = ts + 1;
        txn.commit(&key, ts).unwrap();
        engine.write(&ctx, txn.modifies()).unwrap();

        ts = ts + 1;
    });
}