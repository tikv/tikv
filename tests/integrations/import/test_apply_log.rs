// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CF_DEFAULT;
use external_storage::LocalStorage;
use kvproto::import_sstpb::ApplyRequest;
use tempfile::TempDir;
use test_sst_importer::*;
use tikv_util::sys::disk::{self, DiskUsage};

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
