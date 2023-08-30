// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    sync::{mpsc::sync_channel, Mutex},
    time::Duration,
};

use engine_traits::{MiscExt, Peekable};
use raft::prelude::MessageType;
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::config::ConfigurableDb;
use tikv_util::{
    config::ReadableDuration,
    sys::thread::{self, Pid},
    time::Instant,
    HandyRwLock,
};

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
    put_with_timeout(&mut cluster, 1, b"k1", b"k1", Duration::from_secs(1)).unwrap_err();
    must_get_none(&cluster.get_engine(1), b"k1");

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller(1).unwrap();

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

#[test]
fn test_increase_pool_v2() {
    use test_raftstore_v2::*;

    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.store_batch_system.pool_size = 1;
    cluster.pd_client.disable_default_operator();
    let fp1 = "poll";

    // Pause at the entrance of the rafstore-1-0 thread
    fail::cfg(fp1, "1*pause").unwrap();
    let _ = cluster.run_conf_change();

    // Request cann't be handled as all pollers have been paused
    put_with_timeout(&mut cluster, 1, b"k1", b"k1", Duration::from_secs(1)).unwrap_err();
    must_get_none(&cluster.get_engine(1), b"k1");

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller(1).unwrap();

        let change = {
            let mut change = HashMap::new();
            change.insert("raftstore.store-pool-size".to_owned(), "2".to_owned());
            change
        };
        // Update config, expand from 1 to 2
        cfg_controller.update(change).unwrap();
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
    get_poller_thread_ids_by_prefix(vec!["raftstore", "apply-"])
}

fn get_raft_poller_thread_ids() -> Vec<Pid> {
    get_poller_thread_ids_by_prefix(vec!["rs-"])
}

fn get_poller_thread_ids_by_prefix(prefixs: Vec<&str>) -> Vec<Pid> {
    let mut poller_tids = vec![];
    let pid = thread::process_id();
    let all_tids: Vec<_> = thread::thread_ids(pid).unwrap();
    for tid in all_tids {
        if let Ok(stat) = thread::full_thread_stat(pid, tid) {
            for &prefix in &prefixs {
                if stat.command.starts_with(prefix) {
                    poller_tids.push(tid);
                }
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();

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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();

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
        let cfg_controller = sim.get_cfg_controller(1).unwrap();

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

#[test]
fn test_adjust_hight_priority_background_threads() {
    use test_raftstore_v2::*;
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.rocksdb.max_background_flushes = 2;
    // pause one flush thread
    fail::cfg("on_flush_completed", "1*pause").unwrap();
    cluster.run();

    cluster.must_put(b"k1", b"val");
    let registry = &cluster.engines[0].0;
    // set high priority background thread (flush thread) to 1 so that puase one
    // thread will make flush unable to proceed
    registry
        .set_high_priority_background_threads(1, true)
        .unwrap();

    let mut cache = registry.get(1).unwrap();
    let tablet = cache.latest().unwrap().clone();
    assert_eq!(tablet.get_value(b"zk1").unwrap().unwrap(), b"val");

    let tablet2 = tablet.clone();
    let h = std::thread::spawn(move || {
        // it will block at on_memtable_flush
        tablet2.flush_cf("default", true).unwrap();
    });

    cluster.must_put(b"k2", b"val");
    let (tx, rx) = sync_channel(1);
    let tx = Mutex::new(tx);
    let h2 = std::thread::spawn(move || {
        tablet.flush_cf("default", true).unwrap();
        tx.lock().unwrap().send(()).unwrap();
    });

    rx.recv_timeout(Duration::from_secs(2)).unwrap_err();

    let registry = &cluster.engines[0].0;
    registry
        .set_high_priority_background_threads(2, false)
        .unwrap();

    fail::remove("on_flush_completed");
    h.join().unwrap();
    h2.join().unwrap();
}

#[test]
fn test_increase_snap_generator_pool_size() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.right_derive_when_split = false;
    cluster.cfg.raft_store.snap_generator_pool_size = 1;
    cluster.cfg.raft_store.raft_log_gc_threshold = 20;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(20);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration(Duration::from_millis(50));
    cluster.run();
    // wait for yatp threads to sleep
    std::thread::sleep(Duration::from_millis(200));

    cluster.must_transfer_leader(1, new_peer(1, 1));
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"key0020");
    let id1 = cluster.get_region(b"").get_id();
    let id2 = cluster.get_region(b"key0020").get_id();
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(id1, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(id2, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));

    fail::cfg("before_region_gen_snap", "1*pause").unwrap();

    for i in 0..20 {
        let key = format!("key{:04}", i);
        cluster.must_put(key.as_bytes(), b"val");
    }

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert(
                "raftstore.snap-generator-pool-size".to_owned(),
                "2".to_owned(),
            );
            change
        };

        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    let engine = cluster.get_engine(2);
    assert!(engine.get_value(b"zkey0001").unwrap().is_none());

    for i in 20..40 {
        let key = format!("key{:04}", i);
        cluster.must_put(key.as_bytes(), b"val");
    }

    std::thread::sleep(std::time::Duration::from_millis(500));
    let t = Instant::now();
    while t.saturating_elapsed() < Duration::from_secs(1) {
        let val = engine.get_value(b"zkey0030").unwrap();
        if val.is_some() {
            assert_eq!(val.unwrap(), b"val");
            break;
        }
    }
    assert!(engine.get_value(b"zkey0001").unwrap().is_none());

    fail::remove("before_region_gen_snap");
}

#[test]
fn test_decrease_snap_generator_pool_size() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.right_derive_when_split = false;
    cluster.cfg.raft_store.snap_generator_pool_size = 2;
    cluster.cfg.raft_store.raft_log_gc_threshold = 20;
    cluster.cfg.raft_store.raft_log_gc_count_limit = Some(20);
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration(Duration::from_millis(50));
    cluster.run();
    // wait for yatp threads to sleep
    std::thread::sleep(Duration::from_millis(200));

    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgAppend)
            .direction(Direction::Recv),
    ));
    fail::cfg("before_region_gen_snap", "1*pause").unwrap();

    {
        let sim = cluster.sim.rl();
        let cfg_controller = sim.get_cfg_controller(1).unwrap();
        let change = {
            let mut change = HashMap::new();
            change.insert(
                "raftstore.snap-generator-pool-size".to_owned(),
                "0".to_owned(),
            );
            change
        };
        cfg_controller.update(change).unwrap_err();

        let change = {
            let mut change = HashMap::new();
            change.insert(
                "raftstore.snap-generator-pool-size".to_owned(),
                "1".to_owned(),
            );
            change
        };

        cfg_controller.update(change).unwrap();
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    for i in 0..20 {
        let key = format!("key{:04}", i);
        cluster.must_put(key.as_bytes(), b"val");
    }

    std::thread::sleep(std::time::Duration::from_secs(2));
    let engine = cluster.get_engine(2);
    assert!(engine.get_value(b"zkey0001").unwrap().is_none());

    fail::remove("before_region_gen_snap");
}
