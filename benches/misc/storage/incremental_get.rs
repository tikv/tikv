use test::{black_box, Bencher};

use engine_rocks::RocksSyncSnapshot;
use keys::Key;
use kvproto::kvrpcpb::{Context, IsolationLevel};
use test_storage::SyncTestStorageBuilder;
use tidb_query::codec::table;
use tikv::storage::{Engine, Mutation, SnapshotStore, Statistics, Store};

fn table_lookup_gen_data() -> (SnapshotStore<RocksSyncSnapshot>, Vec<Key>) {
    let store = SyncTestStorageBuilder::new().build().unwrap();
    let mut mutations = Vec::new();
    let mut keys = Vec::new();
    for i in 0..30000 {
        let user_key = table::encode_row_key(5, i);
        let user_value = vec![b'x'; 60];
        let key = Key::from_raw(&user_key);
        let mutation = Mutation::Put((key.clone(), user_value));
        mutations.push(mutation);
        keys.push(key);
    }

    let pk = table::encode_row_key(5, 0);

    store
        .prewrite(Context::default(), mutations, pk, 1)
        .unwrap();
    store.commit(Context::default(), keys, 1, 2).unwrap();

    let engine = store.get_engine();
    let db = engine.get_rocksdb();
    db.compact_range_cf(db.cf_handle("write").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("default").unwrap(), None, None);
    db.compact_range_cf(db.cf_handle("lock").unwrap(), None, None);

    let snapshot = engine.snapshot(&Context::default()).unwrap();
    let store = SnapshotStore::new(
        snapshot,
        10.into(),
        IsolationLevel::Si,
        true,
        Default::default(),
    );

    // Keys are given in order, and are far away from each other to simulate a normal table lookup
    // scenario.
    let mut get_keys = Vec::new();
    for i in (0..30000).step_by(30) {
        get_keys.push(Key::from_raw(&table::encode_row_key(5, i)));
    }
    (store, get_keys)
}

#[bench]
fn bench_table_lookup_mvcc_get(b: &mut Bencher) {
    let (store, keys) = table_lookup_gen_data();
    b.iter(|| {
        let mut stats = Statistics::default();
        for key in &keys {
            black_box(store.get(key, &mut stats).unwrap());
        }
    });
}

#[bench]
fn bench_table_lookup_mvcc_incremental_get(b: &mut Bencher) {
    let (mut store, keys) = table_lookup_gen_data();
    b.iter(|| {
        for key in &keys {
            black_box(store.incremental_get(key).unwrap());
        }
    })
}
