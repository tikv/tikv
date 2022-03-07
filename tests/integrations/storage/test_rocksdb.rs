// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use engine_rocks::raw::{CompactionOptions, DB};
use engine_rocks::util::get_cf_handle;
use engine_rocks_helper::sst_recovery::*;
use engine_traits::CF_DEFAULT;
use futures::executor::block_on;
use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

#[test]
fn test_sst_recovery_for_rocksdb() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.tikv.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::secs(1);
    cluster.start().unwrap();

    let node_ids = cluster.get_node_ids();
    let corruption_id = node_ids.iter().next().unwrap().clone();
    assert_eq!(cluster.count, 3);
    assert_eq!(cluster.sst_workers.len(), 3);
    assert_eq!(cluster.sst_workers_map.len(), 3);

    // start sst workers on each tikv instance.
    for (&id, &offset) in &cluster.sst_workers_map {
        let engine = cluster.get_engine(id);
        let runner = RecoveryRunner::new(
            engine.clone(),
            cluster.store_metas.get(&id).unwrap().clone(),
            Duration::from_secs(1),
        );
        assert!(cluster.sst_workers[offset].start_with_timer(runner));
    }

    // select a node engine as the corruption node.
    let engine = cluster.get_engine(corruption_id);

    cluster.must_put_cf(CF_DEFAULT, b"k1", b" ");
    cluster.flush_data();

    let files = engine.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    // only the bottommost sst can be safely deleted.
    compact_files_to_bottom(&engine, vec![file_name]).unwrap();

    cluster.must_put_cf(CF_DEFAULT, b"k2", b" ");
    cluster.flush_data();

    let files = engine.get_live_files();
    let mut file_name = files.get_name(0);
    file_name.remove(0);
    // only the bottommost sst can be safely deleted.
    compact_files_to_bottom(&engine, vec![file_name]).unwrap();

    let files = engine.get_live_files();
    // after 2 flushing and compact, now 2 sst files exist.
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
    // wait secs and corrupted file should be removed.
    // recover_work_mock(&cluster, corruption_id);
    std::thread::sleep(std::time::Duration::from_secs(5));

    let files = engine.get_live_files();
    let mut file_names = vec![];
    for i in 0..files.get_files_count() {
        let mut name = files.get_name(i as _);
        name.remove(0);
        file_names.push(name);
    }
    compact_files_to_bottom(&engine, file_names).unwrap();
}

fn disturb_sst_file(path: &Path) {
    assert!(path.exists());
    let mut file = std::fs::File::create(path).unwrap();
    file.write(b"surprise").unwrap();
    file.sync_all().unwrap();
}

fn compact_files_to_bottom(engine: &Arc<DB>, files: Vec<String>) -> Result<(), String> {
    let handle = get_cf_handle(&engine, CF_DEFAULT).unwrap();
    let mut opt = CompactionOptions::new();
    opt.set_max_subcompactions(1);
    // output_level should be from 0 to 6.
    engine.compact_files_cf(handle, &opt, &files, 6)
}

// this function is designed because some functions can be reused through
// PdClient instead of re-implementing in `store_heartbeat`.
fn recover_work_mock(cluster: &Cluster<ServerCluster>, store_id: u64) {
    let meta = cluster.store_metas.get(&store_id).unwrap();
    let region_ids;
    {
        let meta = meta.lock().unwrap();
        region_ids = meta.damaged_regions_id.clone();
    }
    for region_id in region_ids {
        let pd_client = cluster.pd_client.clone();
        let region = block_on(pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap();
        let peers = region.get_peers();
        for p in peers {
            if p.get_store_id() == store_id {
                pd_client.must_remove_peer(region_id, new_peer(store_id, p.get_id()));
            }
        }
    }
}
