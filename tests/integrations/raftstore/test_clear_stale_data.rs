// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine::rocks::{CompactOptions, Writable, DB};
use engine_traits::{CF_DEFAULT, CF_LOCK};
use test_raftstore::*;

fn init_db_with_sst_files(db: &DB, level: i32, n: u8) {
    let mut opts = CompactOptions::new();
    opts.set_change_level(true);
    opts.set_target_level(level);
    for cf_name in &[CF_DEFAULT, CF_LOCK] {
        let handle = db.cf_handle(cf_name).unwrap();
        // Each SST file has only one kv.
        for i in 0..n {
            let k = keys::data_key(&[i]);
            db.put_cf(handle, &k, &k).unwrap();
            db.flush_cf(handle, true).unwrap();
            db.compact_range_cf_opt(handle, &opts, None, None);
        }
    }
}

fn check_db_files_at_level(db: &DB, level: i32, num_files: u64) {
    for cf_name in &[CF_DEFAULT, CF_LOCK] {
        let handle = db.cf_handle(cf_name).unwrap();
        let name = format!("rocksdb.num-files-at-level{}", level);
        let value = db.get_property_int_cf(handle, &name).unwrap();
        if value != num_files {
            panic!(
                "cf {} level {} should have {} files, got {}",
                cf_name, level, num_files, value
            );
        }
    }
}

fn check_kv_in_all_cfs(db: &DB, i: u8, found: bool) {
    for cf_name in &[CF_DEFAULT, CF_LOCK] {
        let handle = db.cf_handle(cf_name).unwrap();
        let k = keys::data_key(&[i]);
        let v = db.get_cf(handle, &k).unwrap();
        if found {
            assert_eq!(v.unwrap(), &k);
        } else {
            assert!(v.is_none());
        }
    }
}

fn test_clear_stale_data<T: Simulator>(cluster: &mut Cluster<T>) {
    // Disable compaction at level 0.
    cluster
        .cfg
        .rocksdb
        .defaultcf
        .level0_file_num_compaction_trigger = 100;
    cluster
        .cfg
        .rocksdb
        .writecf
        .level0_file_num_compaction_trigger = 100;
    cluster
        .cfg
        .rocksdb
        .lockcf
        .level0_file_num_compaction_trigger = 100;
    cluster
        .cfg
        .rocksdb
        .raftcf
        .level0_file_num_compaction_trigger = 100;

    cluster.run();

    let n = 6;
    // Choose one node.
    let node_id = *cluster.get_node_ids().iter().next().unwrap();
    let db = cluster.get_engine(node_id);

    // Split into `n` regions.
    for i in 0..n {
        let region = cluster.get_region(&[i]);
        cluster.must_split(&region, &[i + 1]);
    }

    // Generate `n` files in db at level 6.
    let level = 6;
    init_db_with_sst_files(&db, level, n);
    check_db_files_at_level(&db, level, u64::from(n));
    for i in 0..n {
        check_kv_in_all_cfs(&db, i, true);
    }

    // Remove some peers from the node.
    cluster.pd_client.disable_default_operator();
    for i in 0..n {
        if i % 2 == 0 {
            continue;
        }
        let region = cluster.get_region(&[i]);
        let peer = find_peer(&region, node_id).unwrap().clone();
        cluster.pd_client.must_remove_peer(region.get_id(), peer);
    }

    // Restart the node.
    cluster.stop_node(node_id);
    cluster.run_node(node_id).unwrap();

    // Keys in removed peers should not exist.
    for i in 0..n {
        check_kv_in_all_cfs(&db, i, i % 2 == 0);
    }
    check_db_files_at_level(&db, level, u64::from(n) / 2);
}

#[test]
fn test_server_clear_stale_data() {
    let mut cluster = new_server_cluster(0, 3);
    test_clear_stale_data(&mut cluster);
}
