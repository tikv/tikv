use tikv::storage::{self, Dsn, Mutation};
use tikv::storage::txn::TxnStore;
use util::KVGenerator;

use tempdir::TempDir;
use test::Bencher;

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
fn bench_tombstone_scan(b: &mut Bencher, dsn: Dsn) {
    let engine = storage::new_engine(dsn).unwrap();

    let store = TxnStore::new(engine);
    let mut ts_generator = 1..;

    let mut kvs = KVGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store.prewrite(vec![Mutation::Put((k.clone(), v))], k.clone(), ts).expect("");
        store.commit(vec![k.clone()], ts, ts_generator.next().unwrap()).expect("");

        ts = ts_generator.next().unwrap();
        store.prewrite(vec![Mutation::Delete(k.clone())], k.clone(), ts).expect("");
        store.commit(vec![k.clone()], ts, ts_generator.next().unwrap()).expect("");
    }

    kvs = KVGenerator::new(100, 1000);
    b.iter(|| {
        let (k, _) = kvs.next().unwrap();
        assert!(store.scan(&k, 1, ts_generator.next().unwrap()).unwrap().is_empty());
    })
}

#[bench]
fn bench_tombstone_scan_in_rocksdb(b: &mut Bencher) {
    let path = TempDir::new("bench-mvcc").unwrap();
    bench_tombstone_scan(b, Dsn::RocksDBPath(path.path().to_str().unwrap()));
}

#[bench]
fn bench_tombstone_scan_in_memory(b: &mut Bencher) {
    bench_tombstone_scan(b, Dsn::Memory);
}
