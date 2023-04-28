// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use proxy_server::config::{
    address_proxy_config, ensure_no_common_unrecognized_keys, get_last_config,
    setup_default_tikv_config, validate_and_persist_config, TIFLASH_DEFAULT_LISTENING_ADDR,
};
use tikv::config::{TikvConfig, LAST_CONFIG_FILE};

use crate::utils::v1::*;

mod store {
    use super::*;
    #[test]
    fn test_store_stats() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 1);

        let _ = cluster.run();

        for id in cluster.engines.keys() {
            let engine = cluster.get_tiflash_engine(*id);
            assert_eq!(engine.get_store_stats().fs_stats.capacity_size, 444444);
        }

        for id in cluster.engines.keys() {
            cluster.must_send_store_heartbeat(*id);
        }
        std::thread::sleep(std::time::Duration::from_millis(1000));
        // let resp = block_on(pd_client.store_heartbeat(Default::default(), None,
        // None)).unwrap();
        for id in cluster.engines.keys() {
            let store_stat = pd_client.get_store_stats(*id).unwrap();
            assert_eq!(store_stat.get_capacity(), 444444);
            assert_eq!(store_stat.get_available(), 333333);
        }
        // The same to mock-engine-store
        cluster.shutdown();
    }
}

mod config {
    use super::*;

    /// Test for double read into both ProxyConfig and TikvConfig.
    #[test]
    fn test_config() {
        // Test double read.
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = "memory-usage-high-water=0.65\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
        write!(file, "{}", text).unwrap();
        let path = file.path();

        let mut unrecognized_keys = Vec::new();
        let mut config = TikvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
        // Otherwise we have no default addr for TiKv.
        setup_default_tikv_config(&mut config);
        assert_eq!(config.memory_usage_high_water, 0.65);
        assert_eq!(config.rocksdb.max_open_files, 111);
        assert_eq!(config.server.addr, TIFLASH_DEFAULT_LISTENING_ADDR);
        assert_eq!(unrecognized_keys.len(), 3);

        let mut proxy_unrecognized_keys = Vec::new();
        let proxy_config =
            ProxyConfig::from_file(path, Some(&mut proxy_unrecognized_keys)).unwrap();
        assert_eq!(proxy_config.raft_store.snap_handle_pool_size, 4);
        assert_eq!(proxy_config.server.engine_addr, "1.2.3.4:5");
        assert_eq!(proxy_config.memory_usage_high_water, 0.65);
        assert!(proxy_unrecognized_keys.contains(&"nosense".to_string()));
        let v1 = vec!["a.b", "b"]
            .iter()
            .map(|e| String::from(*e))
            .collect::<Vec<String>>();
        let v2 = vec!["a.b", "b.b", "c"]
            .iter()
            .map(|e| String::from(*e))
            .collect::<Vec<String>>();
        let unknown = ensure_no_common_unrecognized_keys(&v1, &v2);
        assert_eq!(unknown.is_err(), true);
        assert_eq!(unknown.unwrap_err(), "a.b, b.b");
        let unknown =
            ensure_no_common_unrecognized_keys(&proxy_unrecognized_keys, &unrecognized_keys);
        assert_eq!(unknown.is_err(), true);
        assert_eq!(unknown.unwrap_err(), "nosense, rocksdb.z");

        // Common config can be persisted.
        // Need run this test with ENGINE_LABEL_VALUE=tiflash, otherwise will fatal
        // exit.
        let _ = std::fs::remove_file(
            PathBuf::from_str(&config.storage.data_dir)
                .unwrap()
                .join(LAST_CONFIG_FILE),
        );
        validate_and_persist_config(&mut config, true);

        // Will not override ProxyConfig
        let proxy_config_new = ProxyConfig::from_file(path, None).unwrap();
        assert_eq!(proxy_config_new.raft_store.snap_handle_pool_size, 4);
    }

    /// Test for basic address_proxy_config.
    #[test]
    fn test_validate_config() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        let text = "[raftstore.aaa]\nbbb=2\n[server]\nengine-addr=\"1.2.3.4:5\"\n[raftstore]\nsnap-handle-pool-size=4\nclean-stale-ranges-tick=9999\n[nosense]\nfoo=2\n[rocksdb]\nmax-open-files = 111\nz=1";
        write!(file, "{}", text).unwrap();
        let path = file.path();
        let tmp_store_folder = tempfile::TempDir::new().unwrap();
        let tmp_last_config_path = tmp_store_folder.path().join(LAST_CONFIG_FILE);
        std::fs::copy(path, tmp_last_config_path.as_path()).unwrap();
        get_last_config(tmp_store_folder.path().to_str().unwrap());

        let mut unrecognized_keys: Vec<String> = vec![];
        let mut config = TikvConfig::from_file(path, Some(&mut unrecognized_keys)).unwrap();
        assert_eq!(config.raft_store.clean_stale_ranges_tick, 9999);
        address_proxy_config(&mut config, &ProxyConfig::default());
        let clean_stale_ranges_tick =
            (10_000 / config.raft_store.region_worker_tick_interval.as_millis()) as usize;
        assert_eq!(
            config.raft_store.clean_stale_ranges_tick,
            clean_stale_ranges_tick
        );
    }

    #[test]
    fn test_store_setup() {
        let (mut cluster, pd_client) = new_mock_cluster(0, 3);

        // Add label to cluster
        address_proxy_config(&mut cluster.cfg.tikv, &ProxyConfig::default());

        // Try to start this node, return after persisted some keys.
        let _ = cluster.run();
        // Not use start to avoid "start new node" branch.
        // let _ = cluster.start();
        let store_id = cluster.engines.keys().last().unwrap();
        let store = pd_client.get_store(*store_id).unwrap();
        println!("store {:?}", store);
        assert!(
            store
                .get_labels()
                .iter()
                .find(|&x| x.key == "engine" && x.value == "tiflash")
                .is_some()
        );
        cluster.shutdown();
    }
}
