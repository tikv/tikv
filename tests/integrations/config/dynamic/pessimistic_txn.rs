// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{atomic::Ordering, mpsc, Arc},
    time::Duration,
};

use security::SecurityManager;
use test_pd_client::TestPdClient;
use tikv::{
    config::*,
    server::{lock_manager::*, resolve},
};
use tikv_util::config::ReadableDuration;

#[test]
fn test_config_validate() {
    let cfg = Config::default();
    cfg.validate().unwrap();

    let mut invalid_cfg = Config::default();
    invalid_cfg.wait_for_lock_timeout = ReadableDuration::millis(0);
    invalid_cfg.validate().unwrap_err();
}

fn setup(
    cfg: TikvConfig,
) -> (
    ConfigController,
    WaiterMgrScheduler,
    DetectorScheduler,
    LockManager,
) {
    let mut lock_mgr = LockManager::new(&cfg.pessimistic_txn);
    let pd_client = Arc::new(TestPdClient::new(0, true));
    let security_mgr = Arc::new(SecurityManager::new(&cfg.security).unwrap());
    lock_mgr
        .start(
            1,
            pd_client,
            resolve::MockStoreAddrResolver::default(),
            security_mgr,
            &cfg.pessimistic_txn,
        )
        .unwrap();

    let mgr = lock_mgr.config_manager();
    let (w, d) = (
        mgr.waiter_mgr_scheduler.clone(),
        mgr.detector_scheduler.clone(),
    );
    let cfg_controller = ConfigController::new(cfg);
    cfg_controller.register(Module::PessimisticTxn, Box::new(mgr));

    (cfg_controller, w, d, lock_mgr)
}

fn validate_waiter<F>(router: &WaiterMgrScheduler, f: F)
where
    F: FnOnce(ReadableDuration) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v1| {
        f(v1);
        tx.send(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

fn validate_dead_lock<F>(router: &DetectorScheduler, f: F)
where
    F: FnOnce(u64) + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    router.validate(Box::new(move |v| {
        f(v);
        tx.send(()).unwrap();
    }));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();
}

#[test]
fn test_lock_manager_cfg_update() {
    const DEFAULT_TIMEOUT: u64 = 3000;
    const DEFAULT_DELAY: u64 = 100;
    const DEFAULT_IN_MEMORY_PEER_SIZE_LIMIT: u64 = 512 << 10;
    const DEFAULT_IN_MEMORY_INSTANCE_SIZE_LIMIT: u64 = 100 << 20;
    let (mut cfg, _dir) = TikvConfig::with_tmp().unwrap();
    cfg.pessimistic_txn.wait_for_lock_timeout = ReadableDuration::millis(DEFAULT_TIMEOUT);
    cfg.pessimistic_txn.wake_up_delay_duration = ReadableDuration::millis(DEFAULT_DELAY);
    cfg.pessimistic_txn.pipelined = false;
    cfg.pessimistic_txn.in_memory = false;
    cfg.validate().unwrap();
    let (cfg_controller, waiter, deadlock, mut lock_mgr) = setup(cfg);

    // update of other module's config should not effect lock manager config
    cfg_controller
        .update_config("raftstore.raft-log-gc-threshold", "2000")
        .unwrap();
    validate_waiter(&waiter, move |timeout: ReadableDuration| {
        assert_eq!(timeout.as_millis(), DEFAULT_TIMEOUT);
    });
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, DEFAULT_TIMEOUT);
    });

    // only update wait_for_lock_timeout
    cfg_controller
        .update_config("pessimistic-txn.wait-for-lock-timeout", "4000ms")
        .unwrap();
    validate_waiter(&waiter, move |timeout: ReadableDuration| {
        assert_eq!(timeout.as_millis(), 4000);
    });
    validate_dead_lock(&deadlock, move |ttl: u64| {
        assert_eq!(ttl, 4000);
    });

    // update pipelined
    assert!(
        !lock_mgr
            .get_storage_dynamic_configs()
            .pipelined_pessimistic_lock
            .load(Ordering::SeqCst)
    );
    cfg_controller
        .update_config("pessimistic-txn.pipelined", "true")
        .unwrap();
    assert!(
        lock_mgr
            .get_storage_dynamic_configs()
            .pipelined_pessimistic_lock
            .load(Ordering::SeqCst)
    );

    // update in-memory
    assert!(
        !lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_pessimistic_lock
            .load(Ordering::SeqCst)
    );
    cfg_controller
        .update_config("pessimistic-txn.in-memory", "true")
        .unwrap();
    assert!(
        lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_pessimistic_lock
            .load(Ordering::SeqCst)
    );

    // update wake-up-delay-duration
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .wake_up_delay_duration_ms
            .load(Ordering::SeqCst),
        DEFAULT_DELAY
    );
    cfg_controller
        .update_config("pessimistic-txn.wake-up-delay-duration", "500ms")
        .unwrap();
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .wake_up_delay_duration_ms
            .load(Ordering::SeqCst),
        500
    );

    // update in-memory-peer-size-limit.
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_peer_size_limit
            .load(Ordering::SeqCst),
        DEFAULT_IN_MEMORY_PEER_SIZE_LIMIT
    );
    cfg_controller
        .update_config("pessimistic-txn.in-memory-peer-size-limit", "2MiB")
        .unwrap();
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_peer_size_limit
            .load(Ordering::SeqCst),
        2 << 20
    );

    // update in-memory-peer-size-limit.
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_instance_size_limit
            .load(Ordering::SeqCst),
        DEFAULT_IN_MEMORY_INSTANCE_SIZE_LIMIT
    );
    cfg_controller
        .update_config("pessimistic-txn.in-memory-instance-size-limit", "1GiB")
        .unwrap();
    assert_eq!(
        lock_mgr
            .get_storage_dynamic_configs()
            .in_memory_instance_size_limit
            .load(Ordering::SeqCst),
        1 << 30
    );

    lock_mgr.stop();
}
