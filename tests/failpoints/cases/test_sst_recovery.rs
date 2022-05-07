// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io::Write, path::Path, sync::Arc, time::Duration};

use engine_rocks::{
    raw::{CompactionOptions, DB},
    util::get_cf_handle,
};
use engine_rocks_helper::sst_recovery::*;
use engine_traits::CF_DEFAULT;
use test_raftstore::*;

const CHECK_DURATION: Duration = Duration::from_millis(50);

#[test]
fn test_sst_recovery_basic() {
    let (mut cluster, pd_client, engine1) = create_tikv_cluster_with_one_node_damaged();

    // Test that only sst recovery can delete the sst file, remove peer don't delete it.
    fail::cfg("sst_recovery_before_delete_files", "pause").unwrap();

    let store_meta = cluster.store_metas.get(&1).unwrap().clone();
    std::thread::sleep(CHECK_DURATION);
    assert_eq!(
        store_meta
            .lock()
            .unwrap()
            .get_all_damaged_region_ids()
            .len(),
        2
    );

    // Remove peers for safe deletion of files in sst recovery.
    let region = cluster.get_region(b"2");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());
    let region = cluster.get_region(b"4");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());

    // Read from other store must success.
    assert_eq!(cluster.must_get(b"4").unwrap(), b"val");

    std::thread::sleep(CHECK_DURATION);

    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    assert!(engine1.get(b"z4").unwrap_err().contains("Corruption"));

    fail::remove("sst_recovery_before_delete_files");
    std::thread::sleep(CHECK_DURATION);

    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    assert!(engine1.get(b"z4").unwrap().is_none());

    // Damaged file has been deleted.
    let files = engine1.get_live_files();
    assert_eq!(files.get_files_count(), 2);
    assert_eq!(store_meta.lock().unwrap().damaged_ranges.len(), 0);

    // only store 1 remove peer so key "4" should be accessed by cluster.
    assert_eq!(cluster.must_get(b"4").unwrap(), b"val");
}

#[test]
fn test_sst_recovery_overlap_range_sst_exist() {
    let (mut cluster, pd_client, engine1) = create_tikv_cluster_with_one_node_damaged();

    // create a new sst [1,7] flushed to L0.
    cluster.must_put_cf(CF_DEFAULT, b"1", b"val_1");
    cluster.must_put_cf(CF_DEFAULT, b"3", b"val_1");
    cluster.must_put_cf(CF_DEFAULT, b"4", b"val_1");
    cluster.must_put_cf(CF_DEFAULT, b"5", b"val_1");
    cluster.must_put_cf(CF_DEFAULT, b"7", b"val_1");
    cluster.flush_data();

    let files = engine1.get_live_files();
    assert_eq!(files.get_files_count(), 4);

    // Remove peers for safe deletion of files in sst recovery.
    let region = cluster.get_region(b"2");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());
    let region = cluster.get_region(b"4");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());

    // Peer has been removed from store 1 so it won't get this replica.
    cluster.must_put_cf(CF_DEFAULT, b"4", b"val_2");

    std::thread::sleep(CHECK_DURATION);
    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val_1");
    assert_eq!(&engine1.get(b"z4").unwrap().unwrap().to_owned(), b"val_1");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val_1");

    // Validate the damaged sst has been deleted.
    compact_files_to_target_level(&engine1, true, 3).unwrap();
    let files = engine1.get_live_files();
    assert_eq!(files.get_files_count(), 1);

    must_get_equal(&engine1, b"4", b"val_1");
    assert_eq!(cluster.must_get(b"4").unwrap(), b"val_2");
}

#[test]
fn test_sst_recovery_atomic_when_adding_peer() {
    let (mut cluster, pd_client, engine1) = create_tikv_cluster_with_one_node_damaged();

    // To validate atomic of sst recovery.
    fail::cfg("sst_recovery_before_delete_files", "pause").unwrap();

    // Remove peers for safe deletion of files in sst recovery.
    let region = cluster.get_region(b"2");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());
    let region = cluster.get_region(b"4");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());

    std::thread::sleep(CHECK_DURATION);
    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    // delete file action is paused before.
    assert!(engine1.get(b"z4").unwrap_err().contains("Corruption"));

    let region = cluster.get_region(b"3");
    // add peer back on store 1 to validate atomic of sst recovery.
    pd_client.must_add_peer(region.id, new_peer(1, 1099));

    // store meta should be locked in sst recovery so conf change can't be finished.
    cluster.must_region_not_exist(region.id, 1);
    fail::remove("sst_recovery_before_delete_files");
    std::thread::sleep(CHECK_DURATION);
    cluster.must_region_exist(region.id, 1);

    must_get_equal(&engine1, b"3", b"val");
}

// Make the sst file corrupted.
fn disturb_sst_file(path: &Path) {
    assert!(path.exists());
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(b"surprise").unwrap();
    file.sync_all().unwrap();
}

// To trigger compaction and test background error.
// set `compact_all` to `false` only compact the latest flushed file.
fn compact_files_to_target_level(
    engine: &Arc<DB>,
    compact_all: bool,
    level: i32,
) -> Result<(), String> {
    let files = engine.get_live_files();
    let mut file_names = vec![];
    if compact_all {
        for i in 0..files.get_files_count() {
            let mut name = files.get_name(i as _);
            name.remove(0);
            file_names.push(name);
        }
    } else {
        let mut name = files.get_name(0);
        name.remove(0);
        file_names.push(name);
    }

    let handle = get_cf_handle(engine, CF_DEFAULT).unwrap();
    engine.compact_files_cf(handle, &CompactionOptions::new(), &file_names, level)
}

fn create_tikv_cluster_with_one_node_damaged()
-> (Cluster<ServerCluster>, Arc<TestPdClient>, Arc<DB>) {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();

    assert_eq!(cluster.count, 3);
    assert_eq!(cluster.sst_workers.len(), 3);
    assert_eq!(cluster.sst_workers_map.len(), 3);

    // start sst workers on each tikv instance.
    for (&id, &offset) in &cluster.sst_workers_map {
        let engine = cluster.get_engine(id);
        let runner = RecoveryRunner::new(
            engine.clone(),
            cluster.store_metas.get(&id).unwrap().clone(),
            Duration::from_secs(10),
            CHECK_DURATION,
        );
        assert!(cluster.sst_workers[offset].start_with_timer(runner));
    }

    // create raft replicas in 3 stores.
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    // select a node engine as the corruption test node.
    let engine1 = cluster.get_engine(1);

    // create a sst [lowest,2]
    cluster.must_put_cf(CF_DEFAULT, b"1", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"2", b"val");
    cluster.flush_data();
    compact_files_to_target_level(&engine1, false, 2).unwrap();

    // create a sst [3,5]
    cluster.must_put_cf(CF_DEFAULT, b"3", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"4", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"5", b"val");
    cluster.flush_data();
    compact_files_to_target_level(&engine1, false, 2).unwrap();

    // create a sst [6,7]
    cluster.must_put_cf(CF_DEFAULT, b"6", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"7", b"val");
    cluster.flush_data();
    compact_files_to_target_level(&engine1, false, 2).unwrap();

    let region = cluster.get_region(b"2");
    cluster.must_split(&region, b"2");
    let region = cluster.get_region(b"4");
    cluster.must_split(&region, b"4");
    let region = cluster.get_region(b"7");
    cluster.must_split(&region, b"7");

    // after 3 flushing and compacts, now 3 sst files exist.
    let files = engine1.get_live_files();
    assert_eq!(files.get_files_count(), 3);

    // disturb sst file range [3,5]
    let mut file_name = files.get_name(1);
    assert_eq!(files.get_smallestkey(1), b"z3");
    assert_eq!(files.get_largestkey(1), b"z5");
    file_name.remove(0);
    let sst_path = cluster.paths[cluster.sst_workers_map[&1]]
        .path()
        .to_path_buf()
        .join("db")
        .join(file_name.clone());
    disturb_sst_file(&sst_path);

    // The sst file is damaged, so this action will fail.
    assert!(
        compact_files_to_target_level(&engine1, true, 3)
            .unwrap_err()
            .contains("Corruption")
    );

    (cluster, pd_client, engine1)
}
