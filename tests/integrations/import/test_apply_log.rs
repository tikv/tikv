// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{CF_DEFAULT, CF_WRITE, Peekable};
use external_storage::LocalStorage;
use kvproto::import_sstpb::ApplyRequest;
use tempfile::TempDir;
use test_sst_importer::*;
use tikv_util::sys::disk::{self, DiskUsage};
use txn_types::{Key, TimeStamp, Write, WriteRef, WriteType};

use super::util::*;

#[test]
fn test_basic_apply() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();
    let tmp = TempDir::new().unwrap();
    let storage = LocalStorage::new(tmp.path()).unwrap();
    let default = [
        (b"k1", b"v1", 1),
        (b"k2", b"v2", 2),
        (b"k3", b"v3", 3),
        (b"k4", b"v4", 4),
    ];
    let default_rewritten = [(b"r1", b"v1", 1), (b"r2", b"v2", 2), (b"r3", b"v3", 3)];
    let mut sst_meta = make_plain_file(&storage, "file1.log", default.into_iter());
    register_range_for(&mut sst_meta, b"k1", b"k3a");
    let mut req = ApplyRequest::new();
    req.set_context(ctx.clone());
    req.set_rewrite_rules(vec![rewrite_for(&mut sst_meta, b"k", b"r")].into());
    req.set_metas(vec![sst_meta].into());
    req.set_storage_backend(local_storage(&tmp));
    import.apply(&req).unwrap();
    check_applied_kvs_cf(&tikv, &ctx, CF_DEFAULT, default_rewritten.into_iter());
}

#[test]
fn test_apply_write_cf_sets_txn_source() {
    let (cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();
    let tmp = TempDir::new().unwrap();
    let storage = LocalStorage::new(tmp.path()).unwrap();
    let old_txn_source = 0b101;
    let write = Write::new(WriteType::Put, TimeStamp::new(9), None)
        .set_txn_source(old_txn_source)
        .as_ref()
        .to_bytes();
    let mut sst_meta = make_plain_file(
        &storage,
        "file_write.log",
        std::iter::once((&b"k1"[..], write.as_slice(), 10)),
    );
    sst_meta.set_cf(CF_WRITE.to_owned());
    register_range_for(&mut sst_meta, b"k1", b"k2");
    let mut req = ApplyRequest::new();
    req.set_context(ctx.clone());
    req.set_rewrite_rules(vec![rewrite_for(&mut sst_meta, b"k", b"r")].into());
    req.set_metas(vec![sst_meta].into());
    req.set_storage_backend(local_storage(&tmp));
    import.apply(&req).unwrap();

    let engine = cluster.get_engine(ctx.get_peer().get_store_id());
    let key = Key::from_raw(b"r1").append_ts(TimeStamp::new(10));
    let write_value = engine
        .get_value_cf(CF_WRITE, &keys::data_key(key.as_encoded()))
        .unwrap()
        .unwrap();
    let write = WriteRef::parse(&write_value).unwrap();
    // See components/cdc/src/txn_source.rs, LIGHTNING_PHYSICAL_IMPORT_SHIFT
    let expected_txn_source = old_txn_source | (1 << 16);
    assert_eq!(write.txn_source, expected_txn_source);
}

#[test]
fn test_apply_full_resource() {
    let (_cluster, ctx, _tikv, import) = new_cluster_and_tikv_import_client();
    let tmp = TempDir::new().unwrap();
    let storage = LocalStorage::new(tmp.path()).unwrap();
    let default = [
        (b"k1", b"v1", 1),
        (b"k2", b"v2", 2),
        (b"k3", b"v3", 3),
        (b"k4", b"v4", 4),
    ];
    let mut sst_meta = make_plain_file(&storage, "file1.log", default.into_iter());
    register_range_for(&mut sst_meta, b"k1", b"k3a");
    let mut req = ApplyRequest::new();
    req.set_context(ctx.clone());
    req.set_rewrite_rules(vec![rewrite_for(&mut sst_meta, b"k", b"r")].into());
    req.set_metas(vec![sst_meta].into());
    req.set_storage_backend(local_storage(&tmp));
    disk::set_disk_status(DiskUsage::AlmostFull);
    let result = import.apply(&req).unwrap();
    assert!(result.has_error());
    assert_eq!(
        result.get_error().get_message(),
        "TiKV disk space is not enough."
    );
    disk::set_disk_status(DiskUsage::Normal);

    fail::cfg("mock_memory_usage", "return(10307921510)").unwrap(); // 9.5G
    fail::cfg("mock_memory_limit", "return(10737418240)").unwrap(); // 10G
    let result = import.apply(&req).unwrap();
    assert!(result.has_error());
    assert!(
        result
            .get_error()
            .get_message()
            .contains("Memory usage too high")
    );
    fail::remove("mock_memory_usage");
    fail::remove("mock_memory_limit");
}

#[test]
fn test_apply_twice() {
    let (_cluster, ctx, tikv, import) = new_cluster_and_tikv_import_client();
    let tmp = TempDir::new().unwrap();
    let storage = LocalStorage::new(tmp.path()).unwrap();
    let default = [(
        b"k1",
        b"In this case, we are going to test write twice, but with different rewrite rule.",
        1,
    )];
    let default_fst = [(
        b"r1",
        b"In this case, we are going to test write twice, but with different rewrite rule.",
        1,
    )];
    let default_snd = [(
        b"z1",
        b"In this case, we are going to test write twice, but with different rewrite rule.",
        1,
    )];

    let mut sst_meta = make_plain_file(&storage, "file2.log", default.into_iter());
    register_range_for(&mut sst_meta, b"k1", b"k1a");
    let mut req = ApplyRequest::new();
    req.set_context(ctx.clone());
    req.set_rewrite_rules(vec![rewrite_for(&mut sst_meta, b"k", b"r")].into());
    req.set_metas(vec![sst_meta.clone()].into());
    req.set_storage_backend(local_storage(&tmp));
    import.apply(&req).unwrap();
    check_applied_kvs_cf(&tikv, &ctx, CF_DEFAULT, default_fst.into_iter());

    register_range_for(&mut sst_meta, b"k1", b"k1a");
    req.set_rewrite_rules(vec![rewrite_for(&mut sst_meta, b"k", b"z")].into());
    req.set_metas(vec![sst_meta].into());
    import.apply(&req).unwrap();
    check_applied_kvs_cf(
        &tikv,
        &ctx,
        CF_DEFAULT,
        default_fst.into_iter().chain(default_snd),
    );
}
