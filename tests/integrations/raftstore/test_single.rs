// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use raftstore::store::*;
use rand::prelude::*;
use test_raftstore::*;
use tikv_util::{config::*, time::Instant};

// TODO add epoch not match test cases.

fn test_put<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let mut data_set: Vec<_> = (1..1000)
        .map(|i| {
            (
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
        })
        .collect();

    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cmd(k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }
    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let (key, value) = data_set.choose(&mut rng).unwrap();
        let v = cluster.get(key);
        assert_eq!(v.as_ref(), Some(value));
    }

    data_set = data_set
        .into_iter()
        .enumerate()
        .map(|(i, (k, _))| (k, format!("value{}", i + 2).into_bytes()))
        .collect();

    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cmd(k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }
    // value should be overwrited.
    for _ in 0..50 {
        let (key, value) = data_set.choose(&mut rng).unwrap();
        let v = cluster.get(key);
        assert_eq!(v.as_ref(), Some(value));
    }
}

fn test_delete<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let data_set: Vec<_> = (1..1000)
        .map(|i| {
            (
                format!("key{}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
        })
        .collect();

    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cmd(k, v)).collect();
        // key999 is always the last region.
        cluster.batch_put(b"key999", requests).unwrap();
    }

    let mut rng = rand::thread_rng();
    for (key, value) in data_set.choose_multiple(&mut rng, 50) {
        let v = cluster.get(key);
        assert_eq!(v.as_ref(), Some(value));
        cluster.must_delete(key);
        assert!(cluster.get(key).is_none());
    }
}

fn test_delete_range<T: Simulator>(cluster: &mut Cluster<T>, cf: CfName) {
    let data_set: Vec<_> = (1..500)
        .map(|i| {
            (
                format!("key{:08}", i).into_bytes(),
                format!("value{}", i).into_bytes(),
            )
        })
        .collect();
    for kvs in data_set.chunks(50) {
        let requests = kvs.iter().map(|(k, v)| new_put_cf_cmd(cf, k, v)).collect();
        // key9 is always the last region.
        cluster.batch_put(b"key9", requests).unwrap();
    }

    // delete_range request with notify_only set should not actually delete data.
    cluster.must_notify_delete_range_cf(cf, b"", b"");

    let mut rng = rand::thread_rng();
    for _ in 0..50 {
        let (k, v) = data_set.choose(&mut rng).unwrap();
        assert_eq!(cluster.get_cf(cf, k).unwrap(), *v);
    }

    // Empty keys means the whole range.
    cluster.must_delete_range_cf(cf, b"", b"");

    for _ in 0..50 {
        let k = &data_set.choose(&mut rng).unwrap().0;
        assert!(cluster.get_cf(cf, k).is_none());
    }
}

fn test_wrong_store_id<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();

    let (k, v) = (b"k", b"v");
    let mut region = cluster.get_region(k);
    let region_id = region.get_id();
    let cmd = new_put_cmd(k, v);
    let mut req = new_request(region_id, region.take_region_epoch(), vec![cmd], true);
    let mut leader = cluster.leader_of_region(region_id).unwrap();
    // setup wrong store id.
    let store_id = leader.get_store_id();
    leader.set_store_id(store_id + 1);
    req.mut_header().set_peer(leader);
    let result = cluster.call_command_on_node(store_id, req, Duration::from_secs(5));
    assert!(
        !result
            .unwrap()
            .get_header()
            .get_error()
            .get_message()
            .is_empty()
    );
}

fn test_put_large_entry<T: Simulator>(cluster: &mut Cluster<T>) {
    let max_size: usize = 1024;
    cluster.cfg.raft_store.raft_entry_max_size = ReadableSize(max_size as u64);

    cluster.run();

    let large_value = vec![b'v'; max_size + 1];
    let res = cluster.put(b"key", large_value.as_slice());
    assert!(res.as_ref().err().unwrap().has_raft_entry_too_large());
}

#[test]
fn test_node_put() {
    let mut cluster = new_node_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_node_delete() {
    let mut cluster = new_node_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_node_use_delete_range() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.use_delete_range = true;
    cluster.run();
    test_delete_range(&mut cluster, CF_DEFAULT);
    // Prefix bloom filter is always enabled in the Write CF.
    test_delete_range(&mut cluster, CF_WRITE);
}

#[test]
fn test_node_not_use_delete_range() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.use_delete_range = false;
    cluster.run();
    test_delete_range(&mut cluster, CF_DEFAULT);
    // Prefix bloom filter is always enabled in the Write CF.
    test_delete_range(&mut cluster, CF_WRITE);
}

#[test]
fn test_node_wrong_store_id() {
    let mut cluster = new_node_cluster(0, 1);
    test_wrong_store_id(&mut cluster);
}

#[test]
fn test_server_put() {
    let mut cluster = new_server_cluster(0, 1);
    test_put(&mut cluster);
}

#[test]
fn test_server_delete() {
    let mut cluster = new_server_cluster(0, 1);
    test_delete(&mut cluster);
}

#[test]
fn test_server_wrong_store_id() {
    let mut cluster = new_server_cluster(0, 1);
    test_wrong_store_id(&mut cluster);
}

#[test]
fn test_node_put_large_entry() {
    let mut cluster = new_node_cluster(0, 1);
    test_put_large_entry(&mut cluster);
}

#[test]
fn test_server_put_large_entry() {
    let mut cluster = new_server_cluster(0, 1);
    test_put_large_entry(&mut cluster);
}

#[test]
fn test_node_apply_no_op() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.run();

    let timer = Instant::now();
    loop {
        let state = cluster.apply_state(1, 1);
        if state.get_applied_index() > RAFT_INIT_LOG_INDEX {
            break;
        }
        if timer.saturating_elapsed() > Duration::from_secs(3) {
            panic!("apply no-op log not finish after 3 seconds");
        }
        sleep_ms(10);
    }
}
