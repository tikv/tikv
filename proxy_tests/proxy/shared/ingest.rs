// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::utils::v1::*;

pub fn new_ingest_sst_cmd(meta: SstMeta) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::IngestSst);
    cmd.mut_ingest_sst().set_sst(meta);
    cmd
}

#[test]
fn test_handle_ingest_sst() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
    let _ = cluster.run();

    let key = "k";
    cluster.must_put(key.as_bytes(), b"v");
    let region = cluster.get_region(key.as_bytes());

    let (file, meta, sst_path) = make_sst(
        &cluster,
        region.get_id(),
        region.get_region_epoch().clone(),
        (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>(),
    );

    let req = new_ingest_sst_cmd(meta);
    let _ = cluster.request(
        key.as_bytes(),
        vec![req],
        false,
        Duration::from_secs(5),
        true,
    );

    check_key(&cluster, b"k66", b"2", Some(true), Some(true), None);

    assert!(sst_path.as_path().is_file());
    assert!(!file.as_path().is_file());
    std::fs::remove_file(sst_path.as_path()).unwrap();
    cluster.shutdown();
}

#[test]
fn test_handle_multiple_ingest_sst() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 1);
    let _ = cluster.run();

    let key = "k";
    cluster.must_put(key.as_bytes(), b"v");
    let region = cluster.get_region(key.as_bytes());

    let v = make_ssts(
        &cluster,
        region.get_id(),
        region.get_region_epoch().clone(),
        (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>(),
        5,
    );

    let mut ssts = vec![];
    for (save, m, _) in v.iter() {
        ssts.push((save.clone(), proxy_ffi::name_to_cf(m.get_cf_name())));
    }
    let sorted_ssts = engine_store_ffi::core::forward_raft::sort_sst_by_start_key(ssts, None);
    for sl in sorted_ssts.windows(2) {
        let a = &sl[0];
        let b = &sl[1];
        let fk1 =
            engine_store_ffi::core::forward_raft::get_first_key(a.0.to_str().unwrap(), a.1, None);
        let fk2 =
            engine_store_ffi::core::forward_raft::get_first_key(b.0.to_str().unwrap(), b.1, None);
        assert!(fk1 < fk2);
    }

    let mut reqs = vec![];
    for (_, m, _) in v.iter() {
        reqs.push(new_ingest_sst_cmd(m.clone()));
    }

    let _ = cluster.request(key.as_bytes(), reqs, false, Duration::from_secs(5), true);

    check_key(&cluster, b"k66", b"2", Some(true), Some(true), None);

    for (file, _, sst_path) in v.into_iter() {
        assert!(sst_path.as_path().is_file());
        assert!(!file.as_path().is_file());
        std::fs::remove_file(sst_path.as_path()).unwrap();
    }
    cluster.shutdown();
}

#[test]
fn test_invalid_ingest_sst() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 1);

    let _ = cluster.run();

    let key = "k";
    cluster.must_put(key.as_bytes(), b"v");
    let region = cluster.get_region(key.as_bytes());

    let mut bad_epoch = RegionEpoch::default();
    bad_epoch.set_conf_ver(999);
    bad_epoch.set_version(999);
    let (file, meta, sst_path) = make_sst(
        &cluster,
        region.get_id(),
        bad_epoch,
        (0..100).map(|i| format!("k{}", i)).collect::<Vec<_>>(),
    );

    let req = new_ingest_sst_cmd(meta);
    let _ = cluster.request(
        key.as_bytes(),
        vec![req],
        false,
        Duration::from_secs(5),
        false,
    );
    check_key(&cluster, b"k66", b"2", Some(false), Some(false), None);

    assert!(sst_path.as_path().is_file());
    assert!(!file.as_path().is_file());
    std::fs::remove_file(sst_path.as_path()).unwrap();
    cluster.shutdown();
}

#[test]
fn test_ingest_return_none() {
    let (mut cluster, _pd_client) = new_mock_cluster(0, 1);

    disable_auto_gen_compact_log(&mut cluster);

    let _ = cluster.run();

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k5", b"v5");
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"k5");
    let region1 = cluster.get_region(b"k1");
    let region5 = cluster.get_region(b"k5");
    assert_ne!(region1.get_id(), region5.get_id());

    fail::cfg("on_handle_ingest_sst_return", "return").unwrap();

    let prev_states1 = collect_all_states(&cluster.cluster_ext, region1.get_id());
    let prev_states5 = collect_all_states(&cluster.cluster_ext, region5.get_id());
    let (file1, meta1, sst_path1) = make_sst(
        &cluster,
        region1.get_id(),
        region1.get_region_epoch().clone(),
        (0..100).map(|i| format!("k1_{}", i)).collect::<Vec<_>>(),
    );
    assert!(sst_path1.as_path().is_file());

    let req = new_ingest_sst_cmd(meta1);
    let _ = cluster.request(b"k1", vec![req], false, Duration::from_secs(5), true);

    let (file5, meta5, _sst_path5) = make_sst(
        &cluster,
        region5.get_id(),
        region5.get_region_epoch().clone(),
        (0..100).map(|i| format!("k5_{}", i)).collect::<Vec<_>>(),
    );
    let req = new_ingest_sst_cmd(meta5);
    let _ = cluster.request(b"k5", vec![req], false, Duration::from_secs(5), true);

    check_key(&cluster, b"k1_66", b"2", Some(true), Some(false), None);
    check_key(&cluster, b"k5_66", b"2", Some(true), Some(false), None);

    let new_states1 = collect_all_states(&cluster.cluster_ext, region1.get_id());
    let new_states5 = collect_all_states(&cluster.cluster_ext, region5.get_id());
    must_altered_memory_apply_state(&prev_states1, &new_states1);
    must_unaltered_memory_apply_term(&prev_states1, &new_states1);
    must_unaltered_disk_apply_state(&prev_states1, &new_states1);

    must_altered_memory_apply_state(&prev_states5, &new_states5);
    must_unaltered_memory_apply_term(&prev_states5, &new_states5);
    must_unaltered_disk_apply_state(&prev_states5, &new_states5);
    let prev_states1 = new_states1;
    let prev_states5 = new_states5;
    // Not deleted
    assert!(file1.as_path().is_file());
    assert!(file5.as_path().is_file());
    fail::remove("on_handle_ingest_sst_return");

    let (file11, meta11, sst_path11) = make_sst(
        &cluster,
        region1.get_id(),
        region1.get_region_epoch().clone(),
        (200..300).map(|i| format!("k1_{}", i)).collect::<Vec<_>>(),
    );
    assert!(sst_path11.as_path().is_file());

    let req = new_ingest_sst_cmd(meta11);
    let _ = cluster.request(b"k1", vec![req], false, Duration::from_secs(5), true);

    check_key(&cluster, b"k1_222", b"2", Some(true), None, None);
    check_key(&cluster, b"k5_66", b"2", Some(false), None, None);

    let new_states1 = collect_all_states(&cluster.cluster_ext, region1.get_id());
    let new_states5 = collect_all_states(&cluster.cluster_ext, region5.get_id());
    // Region 1 is persisted.
    must_altered_memory_apply_state(&prev_states1, &new_states1);
    must_unaltered_memory_apply_term(&prev_states1, &new_states1);
    must_altered_disk_apply_state(&prev_states1, &new_states1);
    // Region 5 not persisted yet.
    must_unaltered_disk_apply_state(&prev_states5, &new_states5);
    // file1 and file11 for region 1 is deleted.
    assert!(!file1.as_path().is_file());
    assert!(!file11.as_path().is_file());
    assert!(file5.as_path().is_file());

    // ssp_path1/11/5 share one path.
    std::fs::remove_file(sst_path1.as_path()).unwrap();
    cluster.shutdown();
}
