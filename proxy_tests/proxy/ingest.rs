// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use crate::proxy::*;
use sst_importer::SstImporter;
use test_sst_importer::gen_sst_file_with_kvs;

use super::*;

pub fn new_ingest_sst_cmd(meta: SstMeta) -> Request {
    let mut cmd = Request::default();
    cmd.set_cmd_type(CmdType::IngestSst);
    cmd.mut_ingest_sst().set_sst(meta);
    cmd
}

pub fn create_tmp_importer(cfg: &Config, kv_path: &str) -> (PathBuf, Arc<SstImporter>) {
    let dir = Path::new(kv_path).join("import-sst");
    let importer = {
        Arc::new(
            SstImporter::new(&cfg.import, dir.clone(), None, cfg.storage.api_version())
                .unwrap(),
        )
    };
    (dir, importer)
}

fn make_sst(
    cluster: &Cluster<NodeCluster>,
    region_id: u64,
    region_epoch: RegionEpoch,
    keys: Vec<String>,
) -> (PathBuf, SstMeta, PathBuf) {
    let path = cluster.engines.iter().last().unwrap().1.kv.path();
    let (import_dir, importer) = create_tmp_importer(&cluster.cfg, path);

    // Prepare data
    let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
    let mut keys = keys;
    keys.sort();
    for i in 0..keys.len() {
        kvs.push((keys[i].as_bytes(), b"2"));
    }

    // Make file
    let sst_path = import_dir.join("test.sst");
    let (mut meta, data) = gen_sst_file_with_kvs(&sst_path, &kvs);
    meta.set_region_id(region_id);
    meta.set_region_epoch(region_epoch);
    meta.set_cf_name("default".to_owned());
    let mut file = importer.create(&meta).unwrap();
    file.append(&data).unwrap();
    file.finish().unwrap();

    // copy file to save dir.
    let src = sst_path.clone();
    let dst = file.get_import_path().save.to_str().unwrap();
    let _ = std::fs::copy(src.clone(), dst);

    (file.get_import_path().save.clone(), meta, sst_path)
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

    let prev_states1 = collect_all_states(&cluster, region1.get_id());
    let prev_states5 = collect_all_states(&cluster, region5.get_id());
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

    let new_states1 = collect_all_states(&cluster, region1.get_id());
    let new_states5 = collect_all_states(&cluster, region5.get_id());
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

    let new_states1 = collect_all_states(&cluster, region1.get_id());
    let new_states5 = collect_all_states(&cluster, region5.get_id());
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