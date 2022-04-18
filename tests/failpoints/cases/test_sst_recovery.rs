// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::raw::{CompactionOptions, DB};
use engine_rocks::util::get_cf_handle;
use engine_rocks_helper::sst_recovery::*;
use engine_traits::CF_DEFAULT;
use test_raftstore::*;

#[test]
fn test_sst_recovery_basic() {
    let check_duration = Duration::from_millis(50);

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
            check_duration,
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
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

    // create a sst [3,5]
    cluster.must_put_cf(CF_DEFAULT, b"3", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"4", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"5", b"val");
    cluster.flush_data();
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

    // create a sst [6,7]
    cluster.must_put_cf(CF_DEFAULT, b"6", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"7", b"val");
    cluster.flush_data();
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

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
    file_name.remove(0);
    let sst_path = cluster.paths[cluster.sst_workers_map[&1]]
        .path()
        .to_path_buf()
        .join("db")
        .join(file_name.clone());
    disturb_sst_file(&sst_path);

    let files = engine1.get_live_files();
    let mut file_names = vec![];
    for i in 0..files.get_files_count() {
        let mut name = files.get_name(i as _);
        name.remove(0);
        file_names.push(name);
    }
    // The sst file is damaged, so this action will fail.
    assert!(
        compact_files_to_bottom(&engine1, file_names)
            .unwrap_err()
            .contains("Corruption")
    );

    // Test that only sst recovery can delete the sst file, remove peer cannot delete it.
    fail::cfg("sst_recovery_should_overlap", "return(true)").unwrap();

    // Remove peers for safe deletion of files in sst recovery.
    let region = cluster.get_region(b"3");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());
    let region = cluster.get_region(b"5");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());

    std::thread::sleep(check_duration);
    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    assert!(engine1.get(b"z4").unwrap_err().contains("Corruption"));

    fail::remove("sst_recovery_should_overlap");
    std::thread::sleep(check_duration);

    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    assert!(engine1.get(b"z4").unwrap().is_none());

    // only store 1 remove peer so key "4" should be accessed by cluster.
    assert_eq!(cluster.must_get(b"4").unwrap(), b"val");
}

#[test]
fn test_sst_recovery_atomic_when_adding_peer() {
    let check_duration = Duration::from_millis(50);

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
            check_duration,
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
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

    // create a sst [3,5]
    cluster.must_put_cf(CF_DEFAULT, b"3", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"4", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"5", b"val");
    cluster.flush_data();
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

    // create a sst [6,7]
    cluster.must_put_cf(CF_DEFAULT, b"6", b"val");
    cluster.must_put_cf(CF_DEFAULT, b"7", b"val");
    cluster.flush_data();
    let files = engine1.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine1, vec![file_name]).unwrap();

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
    file_name.remove(0);
    let sst_path = cluster.paths[cluster.sst_workers_map[&1]]
        .path()
        .to_path_buf()
        .join("db")
        .join(file_name.clone());
    disturb_sst_file(&sst_path);

    let files = engine1.get_live_files();
    let mut file_names = vec![];
    for i in 0..files.get_files_count() {
        let mut name = files.get_name(i as _);
        name.remove(0);
        file_names.push(name);
    }
    // The sst file is damaged, so this action will fail.
    assert!(
        compact_files_to_bottom(&engine1, file_names)
            .unwrap_err()
            .contains("Corruption")
    );

    fail::cfg("sst_recovery_before_delete_files", "pause").unwrap();

    // Remove peers for safe deletion of files in sst recovery.
    let region = cluster.get_region(b"3");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());
    let region = cluster.get_region(b"5");
    let peer = find_peer(&region, 1).unwrap();
    pd_client.must_remove_peer(region.id, peer.clone());

    std::thread::sleep(check_duration);
    assert_eq!(&engine1.get(b"z1").unwrap().unwrap().to_owned(), b"val");
    assert_eq!(&engine1.get(b"z7").unwrap().unwrap().to_owned(), b"val");
    assert!(engine1.get(b"z4").unwrap_err().contains("Corruption"));

    let region = cluster.get_region(b"3");
    // add peer back on store 1.
    pd_client.must_add_peer(region.id, new_peer(1, 1099));

    // store meta should be locked in sst recovery so conf change can't be finished.
    assert!(!region_exist_with_timeout(
        &cluster,
        region.id,
        1,
        Duration::from_millis(1000)
    ));

    fail::remove("sst_recovery_before_delete_files");
    std::thread::sleep(check_duration);
    assert!(region_exist_with_timeout(
        &cluster,
        region.id,
        1,
        Duration::from_millis(1000)
    ));

    must_get_equal(&engine1, b"3", b"val");
}

fn disturb_sst_file(path: &Path) {
    assert!(path.exists());
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(b"surprise").unwrap();
    file.sync_all().unwrap();
}

fn compact_files_to_bottom(engine: &Arc<DB>, files: Vec<String>) -> Result<(), String> {
    let handle = get_cf_handle(engine, CF_DEFAULT).unwrap();
    // output_level should be from 0 to 6.
    engine.compact_files_cf(handle, &CompactionOptions::new(), &files, 2)
}

/// Test region exists on that store.
fn region_exist_with_timeout(
    cluster: &Cluster<ServerCluster>,
    region_id: u64,
    store_id: u64,
    timeout: Duration,
) -> bool {
    let find_leader = new_status_request(region_id, new_peer(store_id, 0), new_region_leader_cmd());
    let resp = cluster.call_command(find_leader, timeout).unwrap();
    !is_error_response(&resp)
}
