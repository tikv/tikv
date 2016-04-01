use test::BenchSamples;
use tempdir::TempDir;

use test_util::*;
use tikv::storage::{self, Dsn, Mutation, Key};
use tikv::storage::txn::TxnStore;
use kvproto::kvrpcpb::Context;

use super::print_result;

/// In mvcc kv is not actually deleted, which may cause performance issue
/// when doing scan.
fn bench_tombstone_scan(dsn: Dsn) -> BenchSamples {
    let engine = storage::new_engine(dsn).unwrap();

    let store = TxnStore::new(engine);
    let mut ts_generator = 1..;

    let mut kvs = KvGenerator::new(100, 1000);

    for (k, v) in kvs.take(100000) {
        let mut ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Put((Key::from_raw(k.clone()), v))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(k.clone())],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");

        ts = ts_generator.next().unwrap();
        store.prewrite(Context::new(),
                       vec![Mutation::Delete(Key::from_raw(k.clone()))],
                       k.clone(),
                       ts)
             .expect("");
        store.commit(Context::new(),
                     vec![Key::from_raw(k.clone())],
                     ts,
                     ts_generator.next().unwrap())
             .expect("");
    }

    kvs = KvGenerator::new(100, 1000);
    bench!{
        let (k, _) = kvs.next().unwrap();
        assert!(store.scan(Context::new(),
                           Key::from_raw(k.clone()),
                           1,
                           ts_generator.next().unwrap())
                     .unwrap()
                     .is_empty())
    }
}

pub fn bench_engine() {
    let path = TempDir::new("bench-mvcc").unwrap();
    let dsn = Dsn::RocksDBPath(path.path().to_str().unwrap());
    printf!("benching tombstone scan with rocksdb\t...\t");
    print_result(bench_tombstone_scan(dsn));
    printf!("benching tombstone scan with memory\t...\t");
    print_result(bench_tombstone_scan(Dsn::Memory));
}
