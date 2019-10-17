// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use engine::rocks::util::get_cf_handle;
use engine::*;
use test_raftstore::*;
use tikv_util::config::ReadableSize;

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    let titan_paths: Vec<String> = cluster
        .get_kv_paths()
        .iter()
        .map(|kv| Path::new(kv).join("titandb").to_str().unwrap().to_owned())
        .collect();
    cluster.run();

    assert_eq!(cluster.must_get(b"k1"), None);

    let size = 100;
    for i in 0..size {
        assert!(cluster
            .put(
                format!("key-{}", i).as_bytes(),
                format!("value-{}", i).as_bytes(),
            )
            .is_ok());
    }

    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    for path in &titan_paths {
        assert!(tikv_util::config::check_data_dir_non_empty(path.as_str(), "blob").is_ok());
    }
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        let handle = get_cf_handle(&db, CF_DEFAULT).unwrap();
        let mut opt = Vec::new();
        opt.push(("blob_run_mode", "kFallback"));
        assert!(db.set_options_cf(handle, &opt).is_ok());
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    sleep_ms(500);
    cluster.shutdown();
    configure_for_disable_titan(&mut cluster);
    cluster.start_new_engines().unwrap();
    for path in &titan_paths {
        assert!(tikv_util::config::check_data_dir_empty(path.as_str(), "blob").is_ok());
    }
}
