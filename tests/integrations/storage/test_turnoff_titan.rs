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

    let size = 5;
    for i in 0..size {
        assert!(cluster
            .put(
                format!("k{:02}0", i).as_bytes(),
                format!("v{}", i).as_bytes(),
            )
            .is_ok());
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
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
        assert_eq!(
            db.get_property_int(&"rocksdb.num-files-at-level0").unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int(&"rocksdb.num-files-at-level1").unwrap(),
            0
        );
        assert_eq!(
            db.get_property_int(&"rocksdb.titandb.num-live-blob-file")
                .unwrap(),
            2
        );
        assert_eq!(
            db.get_property_int(&"rocksdb.titandb.num-obsolete-blob-file")
                .unwrap(),
            0
        );
    }
    cluster.shutdown();

    // try reopen db when titan isn't properly turned off.
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
    cluster.compact_data();
    let mut all_check_pass = true;
    for _ in 0..10 {
        // wait for gc completes.
        sleep_ms(10);
        all_check_pass = true;
        for i in cluster.get_node_ids().into_iter() {
            let db = cluster.get_engine(i);
            if db.get_property_int(&"rocksdb.num-files-at-level0").unwrap() != 0 {
                all_check_pass = false;
                break;
            }
            if db.get_property_int(&"rocksdb.num-files-at-level1").unwrap() != 1 {
                all_check_pass = false;
                break;
            }
            if db
                .get_property_int(&"rocksdb.titandb.num-live-blob-file")
                .unwrap()
                != 0
            {
                all_check_pass = false;
                break;
            }
        }
        if all_check_pass {
            break;
        }
    }
    if !all_check_pass {
        panic!("unexpected titan gc results");
    }
    cluster.shutdown();

    configure_for_disable_titan(&mut cluster);
    // wait till files are purged, timeout set to purge_obsolete_files_period.
    for _ in 1..100 {
        sleep_ms(10);
        if cluster.pre_start_check().is_ok() {
            return;
        }
    }
    assert!(cluster.pre_start_check().is_ok());
}
