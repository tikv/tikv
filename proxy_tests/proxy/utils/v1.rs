// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(dead_code)]
#![allow(unused_variables)]

pub use mock_engine_store::mock_cluster::v1::{
    node::NodeCluster,
    transport_simulate::{
        CloneFilterFactory, CollectSnapshotFilter, Direction, RegionPacketFilter,
    },
    Cluster, Simulator,
};
use mock_engine_store::mock_cluster::TiFlashEngine;
use rand::seq::SliceRandom;
use sst_importer::SstImporter;
use test_sst_importer::gen_sst_file_with_kvs;

pub use super::common::*;

pub fn new_mock_cluster(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    tikv_util::set_panic_hook(true, "./");
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), ProxyConfig::default());
    // Compat new proxy
    cluster.cfg.mock_cfg.proxy_compat = true;

    #[cfg(feature = "enable-pagestorage")]
    {
        debug!("new_mock_cluster created with enable_unips = true");
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
    }
    #[cfg(not(feature = "enable-pagestorage"))]
    {
        debug!("new_mock_cluster created with enable_unips = false");
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
    }
    (cluster, pd_client)
}

pub fn new_mock_cluster_snap(id: u64, count: usize) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut proxy_config = ProxyConfig::default();
    proxy_config.raft_store.snap_handle_pool_size = 2;
    let mut cluster = Cluster::new(id, count, sim, pd_client.clone(), proxy_config);
    // Compat new proxy
    cluster.cfg.mock_cfg.proxy_compat = true;

    #[cfg(feature = "enable-pagestorage")]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
    }
    #[cfg(not(feature = "enable-pagestorage"))]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
    }
    (cluster, pd_client)
}

pub fn new_split_region_cluster(count: u64) -> (Cluster<NodeCluster>, Arc<TestPdClient>) {
    let (mut cluster, pd_client) = new_mock_cluster(0, 3);
    // Disable raft log gc in this test case.
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::secs(60);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    #[cfg(feature = "enable-pagestorage")]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = true;
    }
    #[cfg(not(feature = "enable-pagestorage"))]
    {
        cluster.cfg.proxy_cfg.engine_store.enable_unips = false;
    }

    let _ = cluster.run_conf_change();
    for i in 0..count {
        let k = format!("k{:0>4}", 2 * i + 1);
        let v = format!("v{}", 2 * i + 1);
        cluster.must_put(k.as_bytes(), v.as_bytes());
    }

    // k1 in [ , ]  splited by k2 -> (, k2] [k2, )
    // k3 in [k2, ) splited by k4 -> [k2, k4) [k4, )
    for i in 0..count {
        let k = format!("k{:0>4}", 2 * i + 1);
        let region = cluster.get_region(k.as_bytes());
        let sp = format!("k{:0>4}", 2 * i + 2);
        cluster.must_split(&region, sp.as_bytes());
    }

    (cluster, pd_client)
}

pub fn create_tmp_importer(
    cfg: &MixedClusterConfig,
    kv_path: &str,
) -> (PathBuf, Arc<SstImporter<TiFlashEngine>>) {
    let dir = Path::new(kv_path).join("import-sst");
    let importer = {
        Arc::new(
            SstImporter::new(
                &cfg.import,
                dir.clone(),
                None,
                cfg.storage.api_version(),
                false,
            )
            .unwrap(),
        )
    };
    (dir, importer)
}

pub fn make_ssts(
    cluster: &Cluster<NodeCluster>,
    region_id: u64,
    region_epoch: RegionEpoch,
    keys: Vec<String>,
    split_num: usize,
) -> Vec<(PathBuf, SstMeta, PathBuf)> {
    let path = cluster.engines.iter().last().unwrap().1.kv.path();
    let (import_dir, importer) = create_tmp_importer(&cluster.cfg, path);

    // Prepare data
    let mut kvs: Vec<(&[u8], &[u8])> = Vec::new();
    let mut keys = keys;
    keys.sort();
    for i in 0..keys.len() {
        kvs.push((keys[i].as_bytes(), b"2"));
    }

    assert!(keys.len() > split_num);
    let per_file_num = keys.len() / split_num;

    // Make files
    let mut res = vec![];
    for i in 0..split_num {
        let (import_dir, importer) = create_tmp_importer(cluster.get_config(), path);
        let sst_path = import_dir.join(format!("test{}.sst", i));
        let (mut meta, data) = if i == split_num - 1 {
            gen_sst_file_with_kvs(&sst_path, &kvs[i * per_file_num..])
        } else {
            gen_sst_file_with_kvs(&sst_path, &kvs[i * per_file_num..(i + 1) * per_file_num])
        };
        assert!(Path::new(sst_path.to_str().unwrap()).exists());
        meta.set_region_id(region_id);
        meta.set_region_epoch(region_epoch.clone());
        meta.set_cf_name("default".to_owned());
        let mut file = importer.create(&meta).unwrap();
        file.append(&data).unwrap();
        file.finish().unwrap();

        // copy file to save dir.
        let src = sst_path.clone();
        let dst = file.get_import_path().save.to_str().unwrap();
        let _ = std::fs::copy(src.clone(), dst);
        res.push((file.get_import_path().save.clone(), meta, sst_path));
    }

    let mut rnd = rand::thread_rng();
    res.shuffle(&mut rnd);
    res
}

pub fn make_sst(
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
