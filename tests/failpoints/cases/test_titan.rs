use engine_rocks::BlobRunMode;
use engine_traits::{CF_DEFAULT, CompactExt, ManualCompactionOptions, MiscExt};
use test_raftstore::*;
use tikv_util::config::{ReadableDuration, ReadableSize};

// Test Titan can be turned off even if there are blob indices left in the
// sst files due to unfinished data cleanup during peer removal.
// Peer removal first delete SST files that are fully covered by the region's
// range, and then delete residual key-value pairs in rest of the SST files.
// They are not atomic. It is possible that this strategy expose some older
// RocksDB MVCC versions, causing deleted key-blob_ref pairs to be visible to
// TiKV.
#[test]
fn test_titan() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.raft_engine_purge_interval = ReadableDuration::secs(0);
    cluster.cfg.gc.enable_compaction_filter = false;
    cluster.cfg.rocksdb.titan.enabled = Some(true);
    cluster.cfg.rocksdb.defaultcf.disable_auto_compactions = true;
    cluster.cfg.rocksdb.defaultcf.titan.min_blob_size = Some(ReadableSize::kb(10));
    cluster.cfg.rocksdb.defaultcf.titan.min_gc_batch_size = ReadableSize::kb(0);
    cluster.cfg.rocksdb.writecf.disable_auto_compactions = true;
    cluster.run();

    cluster.pd_client.disable_default_operator();

    let region = cluster.get_region(b"k1");

    let split_key = b"k2";
    cluster.must_split(&region, split_key);
    let region1 = cluster.get_region(b"k1");
    // k1's value will be stored in blob file
    // k3's value will be stored in sst file
    cluster.must_put(b"k1", &b"v".repeat(20000));
    cluster.must_put(b"k3", &b"v".repeat(5000));

    let peer_on_store1 = region1
        .get_peers()
        .iter()
        .find(|p| p.get_store_id() == 1)
        .unwrap()
        .clone();

    cluster.must_transfer_leader(region1.get_id(), peer_on_store1);

    cluster.engines[&3].kv.flush_cf(CF_DEFAULT, true).unwrap();
    cluster.engines[&3]
        .kv
        .compact_range_cf(
            CF_DEFAULT,
            None,
            None,
            ManualCompactionOptions::new(true, 1, true),
        )
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
    // lv6: file0 [k1: ref_to_blob_file, k3: v]
    // blob db: file0 [k1: v]

    // Restart node 3 with titan in fallback mode
    cluster.cfg.rocksdb.defaultcf.titan.blob_run_mode = BlobRunMode::Fallback;
    cluster.cfg.rocksdb.defaultcf.titan.discardable_ratio = 1.0;
    cluster.stop_node(3);
    cluster.restart_engine(3);
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
    assert_eq!(
        1,
        db.get_property_int_cf(defaultcf, "rocksdb.num-files-at-level0")
            .unwrap()
    );
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
    // lv5: file1 [k1: v]
    // lv6: file0 [k1: ref_to_blob_file, k3: v]

    cluster.cfg.rocksdb.titan.enabled = Some(false);
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
    cluster
        .pd_client
        .must_remove_peer(region1.get_id(), peer.clone());
    cluster.must_remove_region(3, region1.get_id());
    cluster.engines[&3]
        .kv
        .compact_files_in_range_cf(CF_DEFAULT, None, None, None)
        .unwrap();
    // lv5: empty, file 1 got deleted, since it is fully covered by region1
    // lv6: file0 [k1: ref_to_blob_file, k3: v]
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
    // lv5: file1 got deleted, since it is fully covered by region1
    // lv6: file0 [k1: ref_to_blob_file, k3: v]
    // blob db: file0 [k1: v]
    cluster
        .pd_client
        .must_add_peer(region1.get_id(), peer.clone());
    fail::remove("after_delete_files_in_range");
    cluster.must_transfer_leader(region1.get_id(), peer.clone());
    assert_eq!(cluster.must_get(b"k1").unwrap(), b"v".repeat(20000));
    cluster.must_put(b"k11", &b"v".repeat(30000));
    assert_eq!(cluster.must_get(b"k11").unwrap(), b"v".repeat(30000));
    // TiKV does not crash, even the add peer will clean up the data again,
    // thus able to see the obesolete blob reference, the blob reference will
    // not be evaluated.
}
