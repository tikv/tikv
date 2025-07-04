use std::default;

use engine_rocks::BlobRunMode;
use engine_traits::{CompactExt, MiscExt, CF_DEFAULT};
use futures::executor::block_on;
use raft_log_engine::print_all_traces;
use test_raftstore::*;
use tikv_util::config::ReadableSize;

#[test]
fn test_titan() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.gc.enable_compaction_filter = false;
    cluster.cfg.rocksdb.titan.enabled = true;
    cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
    cluster.cfg.rocksdb.defaultcf.titan.min_blob_size = ReadableSize::kb(1);
    cluster.cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize::kb(0);
    cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
    cluster.run();

    // Disable PD auto load balancing
    cluster.pd_client.disable_default_operator();

    // k1's value will be stored in blob file
    // k2's value will be stored in sst file
    cluster.must_put(b"k1", &b"v".repeat(2000));
    cluster.must_put(b"k3", &b"v".repeat(500));
    let region = cluster.get_region(b"k1");
    // Find the store id among first 3 stores that does not have a peer
    // if let Some(store_id) = missing_store_id {
    //     let mut peer = region
    //         .get_peers()
    //         .iter()
    //         .find(|p| p.store_id == 4)
    //         .unwrap()
    //         .clone();
    //     cluster
    //         .pd_client
    //         .must_remove_peer(region.get_id(), peer.clone());
    //     peer.set_store_id(store_id);
    //     cluster
    //         .pd_client
    //         .must_add_peer(region.get_id(), peer.clone());
    // }

    // Create 2 new regions
    let split_key = b"k2";
    cluster.must_split(&region, split_key);
    let region1 = cluster.get_region(b"k1");
    let region2 = cluster.get_region(b"k3");

    // Compact node 3
    cluster.engines[&3].kv.flush_cf(CF_DEFAULT, true).unwrap();
    cluster.engines[&3]
        .kv
        .compact_range_cf(CF_DEFAULT, None, None, true, 1)
        .unwrap();
    let db = cluster.engines[&3].kv.as_inner();
    let defaultcf = db.cf_handle(CF_DEFAULT).unwrap();
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level6")
            .unwrap()
    );
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.titandb.num-live-blob-file")
            .unwrap()
    );
    // Restart node 3 with titan in fallback mode
    cluster.cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Fallback;
    cluster.cfg.rocksdb.defaultcf.titan.discardable_ratio = 1.0;
    cluster.stop_node(3);
    // cluster.stop_node(1);
    // cluster.stop_node(2);
    sleep_ms(3000);
    // print_all_traces();
    // cluster.restart_engines(&vec![1, 2, 3]);
    // cluster.restart_engine(2);
    cluster.restart_engine(3);
    println!("restart node 3=======================");
    // cluster.restart_engine(2);
    // println!("restart node 2=======================");
    // cluster.restart_engine(1);
    // println!("restart node 1=======================");
    cluster.run_node(3).unwrap();
    // Check blob file is deleted
    let mut blob_file_reclaimed = false;
    let db = cluster.engines[&3].kv.as_inner();
    let defaultcf = db.cf_handle(CF_DEFAULT).unwrap();
    for _i in 0..10 {
        if db
            .get_property_int_cf(defaultcf, "rocksdb.titandb.num-live-blob-file")
            .unwrap()
            == 0
        {
            blob_file_reclaimed = true;
            break;
        }
        sleep_ms(100);
    }
    assert!(blob_file_reclaimed);
    cluster.engines[&3].kv.flush_cf(CF_DEFAULT, true).unwrap();
    cluster.engines[&3]
        .kv
        .compact_files_in_range_cf(CF_DEFAULT, None, None, Some(5))
        .unwrap();
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level6")
            .unwrap()
    );
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level5")
            .unwrap()
    );

    cluster.cfg.rocksdb.titan.enabled = false;
    cluster.stop_node(3);
    cluster.restart_engine(3);
    cluster.run_node(3).unwrap();
    // Deletion record written by blob file GC is deleted by DeleteFiles, while the
    // original record that is suppose to be deleted by DeleteByWriter is not
    // deleted.
    fail::cfg("after_delete_files_in_range", "return()").unwrap();
    let peer = region1
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 3)
        .unwrap()
        .clone();
    assert!(!is_error_response(&block_on(
        cluster
            .async_remove_peer(region1.get_id(), peer.clone())
            .unwrap(),
    )));
    cluster.must_remove_region(3, region1.get_id());
    let db = cluster.engines[&3].kv.as_inner();
    let defaultcf = db.cf_handle(CF_DEFAULT).unwrap();
    assert_eq!(
        0,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level5")
            .unwrap()
    );
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level6")
            .unwrap()
    );
    assert!(!is_error_response(&block_on(
        cluster
            .async_add_peer(region1.get_id(), peer.clone())
            .unwrap(),
    )));
    sleep_ms(3000);
}
