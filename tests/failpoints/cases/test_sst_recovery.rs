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
use tikv_util::config::ReadableDuration;

#[test]
fn test_sst_recovery_for_rocksdb() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.cfg.tikv.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::secs(1);
    cluster.start().unwrap();

    let node_ids = cluster.get_node_ids();
    let corruption_id = *node_ids.iter().next().unwrap();
    assert_eq!(cluster.count, 1);
    assert_eq!(cluster.sst_workers.len(), 1);
    assert_eq!(cluster.sst_workers_map.len(), 1);

    // start sst workers on each tikv instance.
    for (&id, &offset) in &cluster.sst_workers_map {
        let engine = cluster.get_engine(id);
        let runner = RecoveryRunner::new(
            engine.clone(),
            cluster.store_metas.get(&id).unwrap().clone(),
            Duration::from_secs(2),
            Duration::from_millis(500),
        );
        assert!(cluster.sst_workers[offset].start_with_timer(runner));
    }

    // select a node engine as the corruption node.
    let engine = cluster.get_engine(corruption_id);

    cluster.must_put_cf(CF_DEFAULT, b"1", b" ");
    cluster.flush_data();

    let files = engine.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine, vec![file_name]).unwrap();

    cluster.must_put_cf(CF_DEFAULT, b"3", b" ");
    cluster.flush_data();

    let files = engine.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    compact_files_to_bottom(&engine, vec![file_name]).unwrap();

    // after 2 flushing and compact, now 2 sst files exist.
    let files = engine.get_live_files();
    assert_eq!(files.get_files_count(), 2);
    let mut file_name = files.get_name(1);
    file_name.remove(0);
    let sst_path = cluster.paths[cluster.sst_workers_map[&corruption_id]]
        .path()
        .to_path_buf()
        .join("db")
        .join(file_name.clone());
    disturb_sst_file(&sst_path);

    let files = engine.get_live_files();
    let mut file_names = vec![];
    for i in 0..files.get_files_count() {
        let mut name = files.get_name(i as _);
        name.remove(0);
        file_names.push(name);
    }
    // The sst file is damaged, so this action will fail.
    assert!(
        compact_files_to_bottom(&engine, file_names)
            .unwrap_err()
            .contains("Corruption")
    );
    // It will not automatically recover for the time being,
    // just test that it will not panic within `max_hang_duration`.
    cluster.must_put_cf(CF_DEFAULT, b"5", b" ");
    fail::cfg("sst_recovery_inject", "return(false)").unwrap();
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(cluster.must_get(b"1").unwrap(), b" ".to_vec());
    // If the corrupted file is not deleted, panic here.
    assert!(cluster.must_get(b"3").is_none());
    assert_eq!(cluster.must_get(b"5").unwrap(), b" ".to_vec());
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
    engine.compact_files_cf(handle, &CompactionOptions::new(), &files, 1)
}
