// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::util::get_cf_handle;
use engine::*;
use test_raftstore::*;
use tikv_util::config::ReadableSize;

#[test]
fn test_turnoff_titan() {
    let mut cluster = new_server_cluster(0, 3);
    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
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
    // make sure data is flushed to disk.
    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    cluster.shutdown();

    // try reopen db when titandb dir is non-empty
    configure_for_disable_titan(&mut cluster);
    assert!(cluster.pre_start().is_err());

    configure_for_enable_titan(&mut cluster, ReadableSize::kb(0));
    assert!(cluster.pre_start().is_ok());
    cluster.start().unwrap();
    for i in cluster.get_node_ids().into_iter() {
        let db = cluster.get_engine(i);
        let handle = get_cf_handle(&db, CF_DEFAULT).unwrap();
        let mut opt = Vec::new();
        opt.push(("blob_run_mode", "kFallback"));
        assert!(db.set_options_cf(handle, &opt).is_ok());
    }
    cluster.must_flush_cf(CF_DEFAULT, true);
    cluster.compact_data();
    cluster.shutdown();

    configure_for_disable_titan(&mut cluster);
    assert!(cluster.pre_start().is_ok());
    cluster.start().unwrap();
}
