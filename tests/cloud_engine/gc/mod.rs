// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, time::Duration};

use collections::HashSet;
use kvengine::{new_tmp_filename, table::sstable::new_filename};
use test_cloud_server::ServerCluster;
use tikv_util::config::ReadableDuration;

#[test]
fn test_local_file_gc() {
    test_util::init_log_for_test();
    let node_id = 3;
    let cluster = ServerCluster::new(vec![node_id], |_, cfg| {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        cfg.raft_store.local_file_gc_timeout = ReadableDuration::secs(1);
        cfg.raft_store.local_file_gc_tick_interval = ReadableDuration::millis(300);
    });
    let kv = cluster.get_kvengine(node_id);
    let new_file_id = cluster.get_ts().into_inner();
    let new_file_path = new_filename(new_file_id, kv.opts.local_dir.as_path());
    fs::write(&new_file_path, "abc").unwrap();
    let new_tmp_file_path = kv.opts.local_dir.join(new_tmp_filename(new_file_id, 1));
    fs::write(&new_tmp_file_path, "def").unwrap();
    cluster.put_kv(0..1000, gen_key, gen_val);
    cluster.split(&gen_key(500));
    std::thread::sleep(Duration::from_secs(2));
    let shard_ids = kv.get_all_shard_id_vers();
    assert_eq!(shard_ids.len(), 2);
    let mut all_files = HashSet::default();
    for shard_id in shard_ids {
        let snap = kv.get_snap_access(shard_id.id).unwrap();
        all_files.extend(snap.get_all_files());
    }
    assert!(!all_files.is_empty());
    for &file_id in &all_files {
        let file_path = new_filename(file_id, kv.opts.local_dir.as_path());
        assert!(file_path.exists());
    }
    assert!(!new_file_path.exists());
    assert!(!new_tmp_file_path.exists());
}

fn gen_key(i: usize) -> Vec<u8> {
    format!("key_{:08}", i).into_bytes()
}

fn gen_val(i: usize) -> Vec<u8> {
    format!("val_{:08}", i).into_bytes()
}
