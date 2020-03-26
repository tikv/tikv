// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use fail;
use tikv::storage::mvcc::tests::*;
use tikv::storage::tests_util::*;
use tikv::storage::TestEngineBuilder;

#[test]
fn test_txn_failpoints() {
    let engine = TestEngineBuilder::new().build().unwrap();
    let (k, v) = (b"k", b"v");
    fail::cfg("prewrite", "return(WriteConflict)").unwrap();
    must_prewrite_put_err(&engine, k, v, k, 10);
    fail::remove("prewrite");
    must_prewrite_put(&engine, k, v, k, 10);
    fail::cfg("commit", "delay(100)").unwrap();
    must_commit(&engine, k, 10, 20);
    fail::remove("commit");
}

#[test]
fn test_pipelined_txn_failpoints() {
    fail::cfg("pipelined_txn_write_finished", "delay(200)").unwrap();
    test_pessimistic_lock_impl(true);
    fail::remove("pipelined_txn_write_finished");
    fail::cfg("pipelined_txn_return_to_caller", "delay(00)").unwrap();
    test_pessimistic_lock_impl(true);
    fail::remove("pipelined_txn_return_to_caller");
}
