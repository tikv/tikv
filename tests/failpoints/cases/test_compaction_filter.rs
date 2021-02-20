use std::sync::Arc;

use engine_rocks::RocksWriteBatch;
use tikv::server::gc_worker::TestGCRunner;
use tikv::storage::kv::TestEngineBuilder;
use tikv::storage::mvcc::tests::must_get_none;
use tikv::storage::txn::tests::{must_commit, must_prewrite_delete, must_prewrite_put};

// Test write CF's compaction filter can call `orphan_versions_handler` correctly.
#[test]
fn test_error_in_compaction_filter() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let raw_engine = engine.get_rocksdb();

    let large_value = vec![b'x'; 300];
    must_prewrite_put(&engine, b"zkey", &large_value, b"zkey", 101);
    must_commit(&engine, b"zkey", 101, 102);
    must_prewrite_put(&engine, b"zkey", &large_value, b"zkey", 103);
    must_commit(&engine, b"zkey", 103, 104);
    must_prewrite_delete(&engine, b"zkey", b"zkey", 105);
    must_commit(&engine, b"zkey", 105, 106);

    let fp = "write_compaction_filter_flush_write_batch";
    fail::cfg(fp, "return").unwrap();

    let mut gc_runner = TestGCRunner::default();
    gc_runner.safe_point = 200;
    gc_runner.orphan_versions_handler = Some(Arc::new(move |wb: RocksWriteBatch| {
        assert_eq!(wb.as_inner().count(), 2);
    }));
    gc_runner.gc(&raw_engine);

    // Although versions on default CF is not cleaned, write CF is GCed correctly.
    must_get_none(&engine, b"zkey", 102);
    must_get_none(&engine, b"zkey", 104);

    fail::remove(fp);
}
