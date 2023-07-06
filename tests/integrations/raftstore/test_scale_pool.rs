// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, time::Duration};

use engine_traits::CF_DEFAULT;
use kvproto::raft_cmdpb::RaftCmdResponse;
use raftstore::Result;
use test_raftstore::*;
<<<<<<< HEAD
=======
use test_raftstore_macro::test_case;
use tikv::config::ConfigurableDb;
>>>>>>> f69f721e71 (raftstore-v2:  support dynamic change apply pool size and store io size (#15060))
use tikv_util::{
    sys::thread::{self, Pid},
    HandyRwLock,
};

fn put_with_timeout<T: Simulator>(
    cluster: &mut Cluster<T>,
    key: &[u8],
    value: &[u8],
    timeout: Duration,
) -> Result<RaftCmdResponse> {
    let mut region = cluster.get_region(key);
    let region_id = region.get_id();
    let req = new_request(
        region_id,
        region.take_region_epoch(),
        vec![new_put_cf_cmd(CF_DEFAULT, key, value)],
        false,
    );
    cluster.call_command_on_node(0, req, timeout)
}

#[test]
fn test_increase_pool() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    cluster.pd_client.disable_default_operator();
    let fp1 = "poll";

    // Pause at the entrance of the apply-0, apply-low-0, rafstore-1-0 threads
    fail::cfg(fp1, "3*pause").unwrap();
    let _ = cluster.run_conf_change();

    // Request cann't be handled as all pollers have been paused
    put_with_timeout(&mut cluster, b"k1", b"k1", Duration::from_secs(1)).unwrap();
    must_get_none(&cluster.get_engine(1), b"k1");

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();

        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-pool-size".to_owned(), "2".to_owned());
            change.insert("raftstore.apply_pool_size".to_owned(), "2".to_owned());
            change
        };

        // Update config, expand from 1 to 2
        cfg_controller.update(change).unwrap();
        assert_eq!(
            cfg_controller
                .get_current()
                .raft_store
                .apply_batch_system
                .pool_size,
            2
        );
        assert_eq!(
            cfg_controller
                .get_current()
                .raft_store
                .store_batch_system
                .pool_size,
            2
        );
    }

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");

    fail::remove(fp1);
}

fn get_poller_thread_ids() -> Vec<Pid> {
    let prefixs = ("raftstore", "apply-");
    let mut poller_tids = vec![];
    let pid = thread::process_id();
    let all_tids: Vec<_> = thread::thread_ids(pid).unwrap();
    for tid in all_tids {
        if let Ok(stat) = thread::full_thread_stat(pid, tid) {
            if stat.command.starts_with(prefixs.0) || stat.command.starts_with(prefixs.1) {
                poller_tids.push(tid);
            }
        }
    }
    poller_tids
}

#[test]
fn test_decrease_pool() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let _ = cluster.run_conf_change();

    // Save current poller tids before shrinking
    let original_poller_tids = get_poller_thread_ids();

    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store_pool_size".to_owned(), "1".to_owned());
            change.insert("raftstore.apply-pool-size".to_owned(), "1".to_owned());
            change
        };

        // Update config, shrink from 2 to 1
        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));

        assert_eq!(
            cfg_controller
                .get_current()
                .raft_store
                .apply_batch_system
                .pool_size,
            1
        );
        assert_eq!(
            cfg_controller
                .get_current()
                .raft_store
                .store_batch_system
                .pool_size,
            1
        );
    }

    // Save current poller tids after scaling down
    let current_poller_tids = get_poller_thread_ids();
    // Compared with before shrinking, the thread num should be reduced by two
    assert_eq!(current_poller_tids.len(), original_poller_tids.len() - 2);
    // After shrinking, all the left tids must be there before
    for tid in current_poller_tids {
        assert!(original_poller_tids.contains(&tid));
    }

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

<<<<<<< HEAD
=======
#[test]
fn test_increase_apply_pool_v2() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.apply_batch_system.pool_size = 1;
    let _ = cluster.run_conf_change();
    std::thread::sleep(std::time::Duration::from_millis(200));

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k10");
    let region = cluster.get_region(b"k11");
    cluster.must_split(&region, b"k20");
    let region = cluster.get_region(b"k21");
    cluster.must_split(&region, b"k30");

    fail::cfg("before_handle_tasks", "1*pause").unwrap();
    put_with_timeout(&mut cluster, 1, b"k35", b"val", Duration::from_secs(2)).unwrap_err();

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.apply-pool-size".to_owned(), "2".to_owned());
            change
        };

        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    cluster.must_put(b"k05", b"val");
    cluster.must_put(b"k15", b"val");
    cluster.must_put(b"k25", b"val");

    fail::remove("before_handle_tasks");
}

#[test]
fn test_decrease_apply_pool_v2() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.apply_batch_system.pool_size = 3;
    let _ = cluster.run_conf_change();

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.apply-pool-size".to_owned(), "1".to_owned());
            change
        };

        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    fail::cfg("before_handle_tasks", "1*pause").unwrap();
    put_with_timeout(&mut cluster, 1, b"k10", b"val", Duration::from_secs(2)).unwrap_err();

    fail::remove("before_handle_tasks");
}

#[test]
fn test_decrease_pool_v2() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 1);
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.store_batch_system.pool_size = 2;
    let _ = cluster.run_conf_change();

    // Save current poller tids before shrinking
    let original_poller_tids = get_raft_poller_thread_ids();

    // Request can be handled as usual
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller().unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store_pool_size".to_owned(), "1".to_owned());
            change
        };

        // Update config, shrink from 2 to 1
        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));

        assert_eq!(
            cfg_controller
                .get_current()
                .raft_store
                .store_batch_system
                .pool_size,
            1
        );
    }

    // Save current poller tids after scaling down
    let current_poller_tids = get_raft_poller_thread_ids();
    // Compared with before shrinking, the thread num should be reduced by one
    assert_eq!(current_poller_tids.len(), original_poller_tids.len() - 1);
    // After shrinking, all the left tids must be there before
    for tid in current_poller_tids {
        assert!(original_poller_tids.contains(&tid));
    }

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

>>>>>>> f69f721e71 (raftstore-v2:  support dynamic change apply pool size and store io size (#15060))
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

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_increase_async_ios() {
    let mut cluster = new_cluster(0, 1);
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
        // Wait for the completion of increasing async-ios
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    // Save current async-io tids after scaling up, and compared with the
    // orginial one before scaling up, the thread num should be added up to TWO.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len() - 1, org_writers_tids.len());

    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test_case(test_raftstore::new_node_cluster)]
#[test_case(test_raftstore_v2::new_node_cluster)]
fn test_decrease_async_ios() {
    let mut cluster = new_cluster(0, 1);
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
        // Wait for the completion of decreasing async-ios
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    // Save current async-io tids after scaling down, and compared with the
    // orginial one before shrinking. As the decreasing of async-ios won't
    // release asynchronous writers, the thread num should not be updated.
    let cur_writers_tids = get_async_writers_tids();
    assert_eq!(cur_writers_tids.len(), org_writers_tids.len());
    // After shrinking, all the left tids must be there before
    for tid in cur_writers_tids {
        assert!(org_writers_tids.contains(&tid));
    }
    // Request can be handled as usual
    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
}

#[test]
// v2 sets store_io_pool_size to 1 in `validate` if store_io_pool_size = 0.
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
// v2 sets store_io_pool_size to 1 in `validate` if store_io_pool_size = 0.
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
