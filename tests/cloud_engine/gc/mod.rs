// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs, time::Duration};

use collections::HashSet;
use kvengine::{new_tmp_filename, table::sstable::new_filename, ShardStats};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftRequestHeader};
use rfstore::store::rlog::*;
use test_cloud_server::{must_wait, ServerCluster};
use tikv_util::config::{ReadableDuration, ReadableSize};

use crate::alloc_node_id;

#[test]
fn test_local_file_gc() {
    test_util::init_log_for_test();
    let node_id = alloc_node_id();
    let mut cluster = ServerCluster::new(vec![node_id], |_, cfg| {
        cfg.raft_store.local_file_gc_timeout = ReadableDuration::secs(1);
        cfg.raft_store.local_file_gc_tick_interval = ReadableDuration::millis(300);
    });
    let kv = cluster.get_kvengine(node_id);
    let mut client = cluster.new_client();
    let new_file_id = client.get_ts().into_inner();
    let new_file_path = new_filename(new_file_id, kv.opts.local_dir.as_path());
    fs::write(&new_file_path, "abc").unwrap();
    let new_tmp_file_path = kv.opts.local_dir.join(new_tmp_filename(new_file_id, 1));
    fs::write(&new_tmp_file_path, "def").unwrap();
    cluster.wait_pd_region_count(1);
    client.put_kv(0..1000, gen_key, gen_val);
    client.split(&gen_key(500));
    let shard_ids = kv.get_all_shard_id_vers();
    assert_eq!(shard_ids.len(), 2);
    let mut all_files = HashSet::default();
    for _ in 0..10 {
        for shard_id in &shard_ids {
            let snap = kv.get_snap_access(shard_id.id).unwrap();
            all_files.extend(snap.get_all_files());
        }
        if !all_files.is_empty() {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(!all_files.is_empty());
    for &file_id in &all_files {
        let file_path = new_filename(file_id, kv.opts.local_dir.as_path());
        assert!(file_path.exists());
    }
    for _ in 0..10 {
        if !new_file_path.exists() {
            break;
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(!new_file_path.exists());
    assert!(!new_tmp_file_path.exists());
    cluster.stop();
}

fn gen_key(i: usize) -> Vec<u8> {
    format!("key_{:08}", i).into_bytes()
}

fn gen_val(i: usize) -> Vec<u8> {
    format!("val_{:08}", i).into_bytes()
}

#[test]
fn test_raft_log_gc() {
    test_util::init_log_for_test();
    let mut node_ids = (0..3).map(|_| alloc_node_id()).collect::<Vec<_>>();
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, cfg| {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.hibernate_regions = false;
        cfg.raft_store.max_peer_down_duration = ReadableDuration::secs(2);
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
    });
    cluster.wait_region_replicated(&[], 3);
    let mut client = cluster.new_client();

    let region_id = client.get_region_id(&[]);
    let before_stats = node_ids
        .iter()
        .map(|id| {
            cluster
                .get_rfengine(*id)
                .get_truncated_state(region_id)
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();

    // Trigger switching and flushing memtable.
    for i in 0..50 {
        client.put_kv(i * 20..(i + 1) * 20, gen_key, gen_val);
    }
    let shard_stats = get_shard_stats(&cluster, region_id);
    assert!(
        shard_stats
            .iter()
            .all(|stat| stat.mem_table_count + stat.l0_table_count > 1 && stat.mem_table_size > 0)
    );

    // Wait for raftlog truncation.
    let wait_truncated = |before_stats: Vec<(_, _)>| -> Vec<_> {
        let mut curr_stats = vec![];
        for i in 0..10 {
            curr_stats = node_ids
                .iter()
                .map(|id| {
                    cluster
                        .get_rfengine(*id)
                        .get_truncated_state(region_id)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            if curr_stats
                .iter()
                .zip(before_stats.iter())
                .all(|(curr, before)| curr.0 > before.0)
            {
                break;
            }
            if i == 9 {
                panic!("waiting for raftlog truncation timeouts");
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        curr_stats
    };
    let curr_stats = wait_truncated(before_stats);

    // Trigger switching and flushing memtable.
    let flush_memtable = |cluster: &ServerCluster, node_ids: &[u16]| {
        let mut client = cluster.new_client();
        let region_id = client.get_region_id(&[]);
        let ctx = client.new_rpc_ctx(region_id);
        ctx.get_peer().get_store_id();
        let mut req = RaftCmdRequest::default();
        let mut header = RaftRequestHeader::default();
        header.set_region_id(ctx.get_region_id());
        header.set_peer(ctx.get_peer().clone());
        header.set_region_epoch(ctx.get_region_epoch().clone());
        let rfengine = cluster.get_rfengine(node_ids[0]);
        header.set_term(rfengine.get_truncated_state(region_id).unwrap().1);
        req.set_header(header);
        let mut custom_builder = CustomBuilder::new();
        custom_builder.set_switch_mem_table(1);
        req.set_custom_request(custom_builder.build());
        cluster.send_raft_command(req);
        // Wait for memtable flushing.
        let mut curr_shard_stats = vec![];
        for i in 0..30 {
            curr_shard_stats = node_ids
                .iter()
                .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
                .collect::<Vec<_>>();
            if curr_shard_stats
                .iter()
                .all(|curr| curr.mem_table_size == 0 && curr.mem_table_count == 1)
            {
                break;
            }
            if i == 29 {
                panic!("wait for memtable flush timeouts");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        curr_shard_stats
    };

    let curr_shard_stats = flush_memtable(&cluster, &node_ids);
    let curr_stats = wait_truncated(curr_stats);
    std::thread::sleep(Duration::from_secs(1));
    // Flushing memtable will propose a ChangeSet request which won't write data to memtable,
    // so it can't trigger memtable flush. We shoud be able to truncate these logs.
    assert!(
        curr_stats
            .iter()
            .zip(curr_shard_stats.iter())
            .any(|(truncated_stat, shard_stat)| truncated_stat.0 >= shard_stat.write_sequence),
        "truncated_stat: {:?} shard_stats: {:?}",
        curr_stats,
        curr_shard_stats
    );

    // Stop one node, and leader shouldn't truncate logs immediately.
    cluster.stop_node(node_ids[2]);
    node_ids.pop();
    let mut client = cluster.new_client();
    for i in 1000..1010 {
        client.put_kv(i..i + 1, gen_key, gen_val);
    }
    flush_memtable(&cluster, &node_ids);
    std::thread::sleep(Duration::from_millis(200));
    // Leader's truncated index doesn't change immediately.
    assert_eq!(
        cluster
            .get_rfengine(node_ids[0])
            .get_truncated_state(region_id)
            .unwrap()
            .0,
        curr_stats[0].0
    );
    // Wait for marking down peer.
    std::thread::sleep(Duration::from_secs(3));
    let truncated_stat = cluster
        .get_rfengine(node_ids[0])
        .get_truncated_state(region_id)
        .unwrap();
    assert!(
        truncated_stat.0 > curr_stats[0].0,
        "{:?} {:?}",
        truncated_stat,
        curr_stats[0]
    );
    cluster.stop();
}

#[test]
fn test_raft_log_gc_size_limit() {
    test_util::init_log_for_test();
    let node_ids = (0..3).map(|_| alloc_node_id()).collect::<Vec<_>>();
    let mut cluster = ServerCluster::new(node_ids.clone(), |_, cfg| {
        cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(50);
        cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(100);
        cfg.raft_store.pd_heartbeat_tick_interval = ReadableDuration::millis(100);
        cfg.rocksdb.writecf.write_buffer_size = ReadableSize::kb(16);
        cfg.raft_store.raft_log_gc_size_limit = Some(ReadableSize::kb(1));
    });
    cluster.wait_region_replicated(&[], 3);
    cluster.stop_node(node_ids[2]);

    let mut client = cluster.new_client();
    let region_id = client.get_region_id(&[]);
    std::thread::sleep(Duration::from_millis(300));
    let prev_state = cluster
        .get_rfengine(node_ids[0])
        .get_truncated_state(region_id)
        .unwrap();
    for i in 0..50 {
        client.put_kv(i * 20..(i + 1) * 20, gen_key, gen_val);
    }
    must_wait(
        || {
            let curr_state = cluster
                .get_rfengine(node_ids[0])
                .get_truncated_state(region_id)
                .unwrap();
            curr_state.0 > prev_state.0 && curr_state.0 > 40
        },
        3,
        "raft log size limit doesn't take effect",
    );
    cluster.stop();
}

fn get_shard_stats(cluster: &ServerCluster, region_id: u64) -> Vec<ShardStats> {
    let nodes = cluster.get_nodes();
    nodes
        .iter()
        .map(|id| cluster.get_kvengine(*id).get_shard_stat(region_id))
        .collect::<Vec<_>>()
}
