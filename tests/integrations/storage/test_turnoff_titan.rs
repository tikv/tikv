// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::util::get_cf_handle;
use engine::*;
use test_raftstore::*;
use tikv_util::config::ReadableSize;

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
    cluster.cfg.rocksdb.defaultcf.num_levels = 1;
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    cluster.run();
    assert_eq!(cluster.must_get(b"k1"), None);
    let size = 50;
    for i in 0..size {
        assert!(cluster
            .put(
                format!("k{:02}0", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .is_ok());
    }
    // make sure data is flushed to disk.
    cluster.must_flush_cf(CF_DEFAULT, true);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        info!("CLUSTER LOG"; "level0" => db.get_property_int(&"rocksdb.num-files-at-level0").unwrap());
        info!("CLUSTER LOG"; "level1" => db.get_property_int(&"rocksdb.num-files-at-level1").unwrap());
        // info!("CLUSTER LOG"; "level2" => db.get_property_int(&"rocksdb.num-files-at-level2").unwrap());
        // info!("CLUSTER LOG"; "level3" => db.get_property_int(&"rocksdb.num-files-at-level3").unwrap());
        // info!("CLUSTER LOG"; "level4" => db.get_property_int(&"rocksdb.num-files-at-level4").unwrap());
        // info!("CLUSTER LOG"; "level5" => db.get_property_int(&"rocksdb.num-files-at-level5").unwrap());
        // info!("CLUSTER LOG"; "level6" => db.get_property_int(&"rocksdb.num-files-at-level6").unwrap());
    }
    cluster.shutdown();

    // try reopen db when titandb dir is non-empty
    configure_for_disable_titan(&mut cluster);
    assert!(cluster.pre_start_check().is_err());

    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    assert!(cluster.pre_start_check().is_ok());
    cluster.start().unwrap();
    assert_eq!(cluster.must_get(b"k1"), None);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        let handle = get_cf_handle(&db, CF_DEFAULT).unwrap();
        let mut opt = Vec::new();
        opt.push(("blob_run_mode", "kFallback"));
        assert!(db.set_options_cf(handle, &opt).is_ok());
    }
    for i in 0..size {
        assert!(cluster
            .put(
                format!("k{:02}1", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .is_ok());
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        info!("CLUSTER LOG"; "level0" => db.get_property_int(&"rocksdb.num-files-at-level0").unwrap());
        info!("CLUSTER LOG"; "level1" => db.get_property_int(&"rocksdb.num-files-at-level1").unwrap());
        // info!("CLUSTER LOG"; "level2" => db.get_property_int(&"rocksdb.num-files-at-level2").unwrap());
        // info!("CLUSTER LOG"; "level3" => db.get_property_int(&"rocksdb.num-files-at-level3").unwrap());
        // info!("CLUSTER LOG"; "level4" => db.get_property_int(&"rocksdb.num-files-at-level4").unwrap());
        // info!("CLUSTER LOG"; "level5" => db.get_property_int(&"rocksdb.num-files-at-level5").unwrap());
        // info!("CLUSTER LOG"; "level6" => db.get_property_int(&"rocksdb.num-files-at-level6").unwrap());
    }
    cluster.compact_data();
    cluster.must_flush_cf(CF_DEFAULT, true);
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        info!("CLUSTER LOG"; "level0" => db.get_property_int(&"rocksdb.num-files-at-level0").unwrap());
        info!("CLUSTER LOG"; "level1" => db.get_property_int(&"rocksdb.num-files-at-level1").unwrap());
        // info!("CLUSTER LOG"; "level2" => db.get_property_int(&"rocksdb.num-files-at-level2").unwrap());
        // info!("CLUSTER LOG"; "level3" => db.get_property_int(&"rocksdb.num-files-at-level3").unwrap());
        // info!("CLUSTER LOG"; "level4" => db.get_property_int(&"rocksdb.num-files-at-level4").unwrap());
        // info!("CLUSTER LOG"; "level5" => db.get_property_int(&"rocksdb.num-files-at-level5").unwrap());
        // info!("CLUSTER LOG"; "level6" => db.get_property_int(&"rocksdb.num-files-at-level6").unwrap());
    }
    sleep_ms(2000);
    cluster.shutdown();

    configure_for_disable_titan(&mut cluster);
    assert!(cluster
        .pre_start_check()
        .map_err(|e| error!("{}", e))
        .is_ok());
    cluster.start().unwrap();
}
