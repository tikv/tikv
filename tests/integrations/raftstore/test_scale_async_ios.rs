// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use test_raftstore::*;
use tikv_util::{
    sys::thread::{self, Pid},
    HandyRwLock,
};

fn get_async_writers_tids() -> Vec<Pid> {
    let prefix = "store-writer-";
    let mut writers_tids = vec![];
    let pid = thread::process_id();
    let all_tids: Vec<_> = thread::thread_ids(pid).unwrap();
    for tid in all_tids {
        if let Ok(stat) = thread::full_thread_stat(pid, tid) {
            if stat.command.starts_with(prefix) {
                writers_tids.push(tid);
            }
        }
    }
    writers_tids
}

#[test]
fn test_increase_async_ios() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_io_pool_size = 1;
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // Save current async-io tids before shrinking
    let org_writers_tids = get_async_writers_tids();
    assert_eq!(1, org_writers_tids.len());
    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    // Update config, expand from 1 to 2
    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();

        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-io-pool-size".to_owned(), "2".to_owned());
            change
        };

        cfg_controller.update(change).unwrap();
        assert_eq!(
            cfg_controller.get_current().raft_store.store_io_pool_size,
            2
        );
    }
    // Save current async-io tids after scaling up, and compared with the
    // orginial one before scaling up, the thread num should be added up to TWO.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len() - 1, org_writers_tids.len());

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test]
fn test_decrease_async_ios() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_io_pool_size = 4;
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // Save current async-io tids before shrinking
    let org_writers_tids = get_async_writers_tids();
    assert_eq!(4, org_writers_tids.len());
    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    // Update config, shrink from 4 to 1
    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-io-pool-size".to_owned(), "1".to_owned());
            change
        };

        cfg_controller.update(change).unwrap();
        assert_eq!(
            cfg_controller.get_current().raft_store.store_io_pool_size,
            1
        );
    }

    // Save current async-io tids after scaling down, and compared with the
    // orginial one before shrinking, the thread num should be reduced by THREE.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len() + 3, org_writers_tids.len());
    // After shrinking, all the left tids must be there before
    for tid in cur_writers_tids {
        assert!(org_writers_tids.contains(&tid));
    }
    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test]
fn test_resize_async_ios_failed_1() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_io_pool_size = 2;
    cluster.pd_client.disable_default_operator();
    cluster.run();

    // Save current async-io tids before shrinking
    let org_writers_tids = get_async_writers_tids();
    assert_eq!(2, org_writers_tids.len());
    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    // Update config, expand from async-mode(async-ios == 2) to
    // sync-mode(async-ios == 0).
    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();

        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-io-pool-size".to_owned(), "0".to_owned());
            change
        };

        assert!(cfg_controller.update(change).is_err());
        assert_eq!(
            cfg_controller.get_current().raft_store.store_io_pool_size,
            2
        );
    }
    // Save current async-io tids after scaling up, and compared with the
    // orginial one before scaling up, the thread num should be added up to TWO.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len(), org_writers_tids.len());

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test]
fn test_resize_async_ios_failed_2() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_io_pool_size = 0;
    cluster.pd_client.disable_default_operator();
    let _ = cluster.run_conf_change();

    // Save current async-io tids before shrinking
    let org_writers_tids = get_async_writers_tids();
    assert_eq!(0, org_writers_tids.len());
    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    // Update config, expand from sync-mode(async-ios == 0) to
    // async-mode(async-ios == 2).
    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();

        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-io-pool-size".to_owned(), "2".to_owned());
            change
        };

        assert!(cfg_controller.update(change).is_err());
        assert_eq!(
            cfg_controller.get_current().raft_store.store_io_pool_size,
            0
        );
    }
    // Save current async-io tids after scaling up, and compared with the
    // orginial one before scaling up, the thread num should be added up to TWO.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len(), org_writers_tids.len());

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}
