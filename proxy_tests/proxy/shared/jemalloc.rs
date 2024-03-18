// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap;
use mock_engine_store::ThreadInfoJealloc;
use more_asserts::assert_gt;
use proxy_ffi::jemalloc_utils::{get_allocatep_on_thread_start, get_deallocatep_on_thread_start};

use crate::utils::v1::*;

#[test]
fn test_alloc_dealloc() {
    let dummy: Vec<u64> = Vec::with_capacity(100000);
    let ptr_alloc = get_allocatep_on_thread_start();
    let actual_alloc: &u64 = unsafe { &*(ptr_alloc as *const u64) };
    let ptr_dealloc = get_deallocatep_on_thread_start();
    let actual_dealloc: &u64 = unsafe { &*(ptr_dealloc as *const u64) };
    assert_gt!(*actual_alloc, 100000 * std::mem::size_of::<u64>() as u64);
    let dummy2: Vec<u64> = Vec::with_capacity(100000);
    assert_gt!(
        *actual_alloc,
        2 * 100000 * std::mem::size_of::<u64>() as u64
    );
    drop(dummy);
    assert_gt!(*actual_dealloc, 100000 * std::mem::size_of::<u64>() as u64);
    drop(dummy2);
    assert_gt!(
        *actual_dealloc,
        2 * 100000 * std::mem::size_of::<u64>() as u64
    );
}

fn collect_thread_state(
    cluster_ext: &ClusterExt,
    store_id: u64,
) -> HashMap<String, ThreadInfoJealloc> {
    let mut res: HashMap<String, ThreadInfoJealloc> = Default::default();
    cluster_ext.iter_ffi_helpers(Some(vec![store_id]), &mut |_, ffi: &mut FFIHelperSet| {
        res = (*ffi.engine_store_server.thread_info_map.lock().expect("")).clone();
    });
    res
}

fn gather(m: &HashMap<String, ThreadInfoJealloc>, pattern: &str) -> i64 {
    m.iter()
        .filter(|(k, _)| k.contains(pattern))
        .fold(0, |acc, e| acc + e.1.remaining())
}

#[test]
fn test_ffi() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 1);

    let _ = cluster.run();

    let prev = collect_thread_state(&cluster.cluster_ext, 1);
    let before_raftstore = gather(&prev, "raftstore");
    let before_apply = gather(&prev, "apply");

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    for i in 0..10 {
        let k = format!("k{}", i);
        let v = format!("v{}", i);
        check_key(&cluster, k.as_bytes(), v.as_bytes(), Some(true), None, None);
    }

    let after = collect_thread_state(&cluster.cluster_ext, 1);
    let after_raftstore = gather(&after, "raftstore");
    let after_apply = gather(&after, "apply");
    assert_gt!(after_raftstore, before_raftstore);
    assert_gt!(after_apply, before_apply);

    cluster.shutdown();
}
