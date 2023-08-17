// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, RecvTimeoutError},
        Arc, Mutex,
    },
    thread,
    time::Duration,
};

use api_version::{ApiV1, ApiV2, KvFormat};
use collections::HashMap;
use errors::{extract_key_error, extract_region_error};
use futures::executor::block_on;
use grpcio::*;
use kvproto::{
    kvrpcpb::{
        self, AssertionLevel, BatchRollbackRequest, CommandPri, CommitRequest, Context, GetRequest,
        Op, PrewriteRequest, PrewriteRequestPessimisticAction::*, RawPutRequest,
    },
    tikvpb::TikvClient,
};
use resource_control::ResourceGroupManager;
use test_raftstore::*;
use tikv::{
    config::{ConfigController, Module},
    storage::{
        self,
        config_manager::StorageConfigManger,
        kv::{Error as KvError, ErrorInner as KvErrorInner, SnapContext, SnapshotExt},
        lock_manager::MockLockManager,
        mvcc::{Error as MvccError, ErrorInner as MvccErrorInner},
        test_util::*,
        txn::{
            commands,
            flow_controller::{EngineFlowController, FlowController},
            Error as TxnError, ErrorInner as TxnErrorInner,
        },
        Error as StorageError, ErrorInner as StorageErrorInner, *,
    },
};
use tikv_util::{future::paired_future_callback, worker::dummy_scheduler, HandyRwLock};
use txn_types::{Key, Mutation, TimeStamp};

#[test]
fn test_scheduler_leader_change_twice() {
    let snapshot_fp = "scheduler_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let storage0 =
        TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine0, MockLockManager::new())
            .build()
            .unwrap();

    let mut ctx0 = Context::default();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::cfg(snapshot_fp, "pause").unwrap();
    storage0
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::make_put(Key::from_raw(b"k"), b"v".to_vec())],
                b"k".to_vec(),
                10.into(),
                0,
                false,
                0,
                TimeStamp::default(),
                TimeStamp::default(),
                None,
                false,
                AssertionLevel::Off,
                ctx0,
            ),
            Box::new(move |res: storage::Result<_>| {
                prewrite_tx.send(res).unwrap();
            }),
        )
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    fail::remove(snapshot_fp);

    match prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap() {
        Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Engine(KvError(
            box KvErrorInner::Request(ref e),
        ))))))
        | Err(Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(MvccError(
            box MvccErrorInner::Kv(KvError(box KvErrorInner::Request(ref e))),
        ))))))
        | Err(Error(box ErrorInner::Kv(KvError(box KvErrorInner::Request(ref e))))) => {
            assert!(e.has_stale_command(), "{:?}", e);
        }
        res => {
            panic!("expect stale command, but got {:?}", res);
        }
    }
}

#[test]
fn test_server_catching_api_error() {
    let raftkv_fp = "raftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();

    fail::cfg(raftkv_fp, "return").unwrap();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx.clone());
    let mutation = kvrpcpb::Mutation {
        op: Op::Put,
        key: b"k3".to_vec(),
        value: b"v3".to_vec(),
        ..Default::default()
    };
    prewrite_req.set_mutations(vec![mutation].into_iter().collect());
    prewrite_req.primary_lock = b"k3".to_vec();
    prewrite_req.start_version = 1;
    prewrite_req.lock_ttl = prewrite_req.start_version + 1;
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(prewrite_resp.has_region_error(), "{:?}", prewrite_resp);
    assert!(
        prewrite_resp.get_region_error().has_region_not_found(),
        "{:?}",
        prewrite_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = b"k3".to_vec();
    put_req.value = b"v3".to_vec();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(put_resp.has_region_error(), "{:?}", put_resp);
    assert!(
        put_resp.get_region_error().has_region_not_found(),
        "{:?}",
        put_resp
    );
    must_get_none(&cluster.get_engine(1), b"k3");

    fail::remove(raftkv_fp);
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error(), "{:?}", put_resp);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}

#[test]
fn test_raftkv_early_error_report() {
    let raftkv_fp = "raftkv_early_error_report";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    cluster.must_split(&cluster.get_region(b"k0"), b"k1");

    let env = Arc::new(Environment::new(1));
    let mut clients: HashMap<&[u8], (Context, TikvClient)> = HashMap::default();
    for &k in &[b"k0", b"k1"] {
        let region = cluster.get_region(k);
        let leader = region.get_peers()[0].clone();
        let mut ctx = Context::default();
        let channel = ChannelBuilder::new(env.clone())
            .connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
        let client = TikvClient::new(channel);
        ctx.set_region_id(region.get_id());
        ctx.set_region_epoch(region.get_region_epoch().clone());
        ctx.set_peer(leader);
        clients.insert(k, (ctx, client));
    }

    // Inject error to all regions.
    fail::cfg(raftkv_fp, "return").unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        assert!(put_resp.has_region_error(), "{:?}", put_resp);
        assert!(
            put_resp.get_region_error().has_region_not_found(),
            "{:?}",
            put_resp
        );
        must_get_none(&cluster.get_engine(1), k);
    }
    fail::remove(raftkv_fp);

    // Inject only one region
    let injected_region_id = clients[b"k0".as_ref()].0.get_region_id();
    fail::cfg(raftkv_fp, &format!("return({})", injected_region_id)).unwrap();
    for (k, (ctx, client)) in &clients {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = k.to_vec();
        put_req.value = b"v".to_vec();
        let put_resp = client.raw_put(&put_req).unwrap();
        if ctx.get_region_id() == injected_region_id {
            assert!(put_resp.has_region_error(), "{:?}", put_resp);
            assert!(
                put_resp.get_region_error().has_region_not_found(),
                "{:?}",
                put_resp
            );
            must_get_none(&cluster.get_engine(1), k);
        } else {
            assert!(!put_resp.has_region_error(), "{:?}", put_resp);
            must_get_equal(&cluster.get_engine(1), k, b"v");
        }
    }
    fail::remove(raftkv_fp);
}

#[test]
fn test_scale_scheduler_pool() {
    let snapshot_fp = "scheduler_start_execute";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let origin_pool_size = cluster.cfg.storage.scheduler_worker_pool_size;

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .config(cluster.cfg.tikv.storage.clone())
        .build()
        .unwrap();

    let cfg = new_tikv_config(1);
    let kv_engine = storage.get_engine().kv_engine().unwrap();
    let (_tx, rx) = std::sync::mpsc::channel();
    let flow_controller = Arc::new(FlowController::Singleton(EngineFlowController::new(
        &cfg.storage.flow_control,
        kv_engine.clone(),
        rx,
    )));

    let cfg_controller = ConfigController::new(cfg);
    let (scheduler, _receiver) = dummy_scheduler();
    cfg_controller.register(
        Module::Storage,
        Box::new(StorageConfigManger::new(
            kv_engine,
            scheduler,
            flow_controller,
            storage.get_scheduler(),
        )),
    );
    let scheduler = storage.get_scheduler();

    let region = cluster.get_region(b"k1");
    let mut ctx = Context::default();
    ctx.set_region_id(region.id);
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(cluster.leader_of_region(region.id).unwrap());
    let do_prewrite = |key: &[u8], val: &[u8]| {
        // prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(key), val.to_vec())],
                    key.to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    ctx.clone(),
                ),
                Box::new(move |res: storage::Result<_>| {
                    let _ = prewrite_tx.send(res);
                }),
            )
            .unwrap();
        prewrite_rx.recv_timeout(Duration::from_secs(2))
    };

    let scale_pool = |size: usize| {
        cfg_controller
            .update_config("storage.scheduler-worker-pool-size", &format!("{}", size))
            .unwrap();
        assert_eq!(
            scheduler.get_sched_pool().get_pool_size(CommandPri::Normal),
            size
        );
    };

    scale_pool(1);
    fail::cfg(snapshot_fp, "1*pause").unwrap();
    // propose one prewrite to block the only worker
    do_prewrite(b"k1", b"v1").unwrap_err();

    scale_pool(2);

    // do prewrite again, as we scale another worker, this request should success
    do_prewrite(b"k2", b"v2").unwrap().unwrap();

    // restore to original config.
    scale_pool(origin_pool_size);
    fail::remove(snapshot_fp);
}

#[test]
fn test_scheduler_pool_auto_switch_for_resource_ctl() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let resource_manager = ResourceGroupManager::default();
    let resource_ctl = resource_manager.derive_controller("test".to_string(), true);

    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .config(cluster.cfg.tikv.storage.clone())
        .build_for_resource_controller(resource_ctl)
        .unwrap();

    let region = cluster.get_region(b"k1");
    let mut ctx = Context::default();
    ctx.set_region_id(region.id);
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(cluster.leader_of_region(region.id).unwrap());

    let do_prewrite = |key: &[u8], val: &[u8]| {
        // prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(key), val.to_vec())],
                    key.to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    None,
                    false,
                    AssertionLevel::Off,
                    ctx.clone(),
                ),
                Box::new(move |res: storage::Result<_>| {
                    let _ = prewrite_tx.send(res);
                }),
            )
            .unwrap();
        prewrite_rx.recv_timeout(Duration::from_secs(2))
    };

    let (sender, receiver) = channel();
    let priority_queue_sender = Mutex::new(sender.clone());
    let single_queue_sender = Mutex::new(sender);
    fail::cfg_callback("priority_pool_task", move || {
        let sender = priority_queue_sender.lock().unwrap();
        sender.send("priority_queue").unwrap();
    })
    .unwrap();
    fail::cfg_callback("single_queue_pool_task", move || {
        let sender = single_queue_sender.lock().unwrap();
        sender.send("single_queue").unwrap();
    })
    .unwrap();

    // Default is use single queue
    assert_eq!(do_prewrite(b"k1", b"v1").is_ok(), true);
    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
        "single_queue"
    );

    // Add group use priority queue
    use kvproto::resource_manager::{GroupMode, GroupRequestUnitSettings, ResourceGroup};
    let mut group = ResourceGroup::new();
    group.set_name("rg1".to_string());
    group.set_mode(GroupMode::RuMode);
    let mut ru_setting = GroupRequestUnitSettings::new();
    ru_setting.mut_r_u().mut_settings().set_fill_rate(100000);
    group.set_r_u_settings(ru_setting);
    resource_manager.add_resource_group(group);
    thread::sleep(Duration::from_millis(200));
    assert_eq!(do_prewrite(b"k2", b"v2").is_ok(), true);
    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
        "priority_queue"
    );

    // Delete group use single queue
    resource_manager.remove_resource_group("rg1");
    thread::sleep(Duration::from_millis(200));
    assert_eq!(do_prewrite(b"k3", b"v3").is_ok(), true);
    assert_eq!(
        receiver.recv_timeout(Duration::from_millis(500)).unwrap(),
        "single_queue"
    );

    // Scale pool size
    let scheduler = storage.get_scheduler();
    let pool = scheduler.get_sched_pool();
    assert_eq!(pool.get_pool_size(CommandPri::Normal), 1);
    pool.scale_pool_size(2);
    assert_eq!(pool.get_pool_size(CommandPri::Normal), 2);
}

#[test]
fn test_pipelined_pessimistic_lock() {
    let rockskv_async_write_fp = "rockskv_async_write";
    let rockskv_write_modifies_fp = "rockskv_write_modifies";
    let scheduler_async_write_finish_fp = "scheduler_async_write_finish";
    let before_pipelined_write_finish_fp = "before_pipelined_write_finish";

    {
        let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
            .pipelined_pessimistic_lock(false)
            .build()
            .unwrap();
        let (tx, rx) = channel();
        // If storage fails to write the lock to engine, client should
        // receive the error when pipelined locking is disabled.
        fail::cfg(rockskv_write_modifies_fp, "return()").unwrap();
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(Key::from_raw(b"key"), false)],
                    10,
                    10,
                    true,
                    false,
                ),
                Box::new(move |res| {
                    res.unwrap_err();
                    tx.send(()).unwrap();
                }),
            )
            .unwrap();
        rx.recv().unwrap();
        fail::remove(rockskv_write_modifies_fp);
    }

    let storage = TestStorageBuilderApiV1::new(MockLockManager::new())
        .pipelined_pessimistic_lock(true)
        .build()
        .unwrap();

    let (tx, rx) = channel();
    let (key, val) = (Key::from_raw(b"key"), b"val".to_vec());

    // Even if storage fails to write the lock to engine, client should
    // receive the successful response.
    fail::cfg(rockskv_write_modifies_fp, "return()").unwrap();
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 10, 10, true, false),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockResults(vec![PessimisticLockKeyResult::Value(None)]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_write_modifies_fp);
    fail::remove(scheduler_async_write_finish_fp);
    storage
        .sched_txn_command(
            commands::PrewritePessimistic::new(
                vec![(
                    Mutation::make_put(key.clone(), val.clone()),
                    DoPessimisticCheck,
                )],
                key.to_raw().unwrap(),
                10.into(),
                3000,
                10.into(),
                1,
                11.into(),
                TimeStamp::default(),
                None,
                false,
                AssertionLevel::Off,
                vec![],
                Context::default(),
            ),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();
    storage
        .sched_txn_command(
            commands::Commit::new(vec![key.clone()], 10.into(), 20.into(), Context::default()),
            expect_ok_callback(tx.clone(), 0),
        )
        .unwrap();
    rx.recv().unwrap();

    // Should report failure if storage fails to schedule write request to engine.
    fail::cfg(rockskv_async_write_fp, "return()").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 30, 30, true, false),
            expect_fail_callback(tx.clone(), 0, |_| ()),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(rockskv_async_write_fp);

    // Shouldn't release latches until async write finished.
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    for blocked in &[false, true] {
        storage
            .sched_txn_command(
                new_acquire_pessimistic_lock_command(
                    vec![(key.clone(), false)],
                    40,
                    40,
                    true,
                    false,
                ),
                expect_pessimistic_lock_res_callback(
                    tx.clone(),
                    PessimisticLockResults(vec![PessimisticLockKeyResult::Value(Some(
                        val.clone(),
                    ))]),
                ),
            )
            .unwrap();

        if !*blocked {
            rx.recv().unwrap();
        } else {
            // Blocked by latches.
            rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
        }
    }
    fail::remove(scheduler_async_write_finish_fp);
    rx.recv().unwrap();
    delete_pessimistic_lock(&storage, key.clone(), 40, 40);

    // Pipelined write is finished before async write.
    fail::cfg(scheduler_async_write_finish_fp, "pause").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 50, 50, true, false),
            expect_pessimistic_lock_res_callback(
                tx.clone(),
                PessimisticLockResults(vec![PessimisticLockKeyResult::Value(Some(val.clone()))]),
            ),
        )
        .unwrap();
    rx.recv().unwrap();
    fail::remove(scheduler_async_write_finish_fp);
    delete_pessimistic_lock(&storage, key.clone(), 50, 50);

    // The proposed callback, which is responsible for returning response, is not
    // guaranteed to be invoked. In this case it should still be continued
    // properly.
    fail::cfg(before_pipelined_write_finish_fp, "return()").unwrap();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(
                vec![(key.clone(), false), (Key::from_raw(b"nonexist"), false)],
                60,
                60,
                true,
                false,
            ),
            expect_pessimistic_lock_res_callback(
                tx,
                PessimisticLockResults(vec![
                    PessimisticLockKeyResult::Value(Some(val)),
                    PessimisticLockKeyResult::Value(None),
                ]),
            ),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
    fail::remove(before_pipelined_write_finish_fp);
    delete_pessimistic_lock(&storage, key, 60, 60);
}

fn test_pessimistic_lock_resumable_blocked_twice_impl(canceled_when_resumed: bool) {
    let lock_mgr = MockLockManager::new();
    let storage = TestStorageBuilderApiV1::new(lock_mgr.clone())
        .wake_up_delay_duration(100)
        .build()
        .unwrap();
    let (tx, rx) = channel();

    let empty = PessimisticLockResults(vec![PessimisticLockKeyResult::Empty]);

    fail::cfg("lock_waiting_queue_before_delayed_notify_all", "pause").unwrap();
    let (first_resume_tx, first_resume_rx) = channel();
    let (first_resume_continue_tx, first_resume_continue_rx) = channel();
    let first_resume_tx = Mutex::new(first_resume_tx);
    let first_resume_continue_rx = Mutex::new(first_resume_continue_rx);
    fail::cfg_callback(
        "acquire_pessimistic_lock_resumed_before_process_write",
        move || {
            // Notify that the failpoint is reached, and block until it receives a continue
            // signal.
            first_resume_tx.lock().unwrap().send(()).unwrap();
            first_resume_continue_rx.lock().unwrap().recv().unwrap();
        },
    )
    .unwrap();

    let key = Key::from_raw(b"key");

    // Lock the key.
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 10, 10, false, false),
            expect_pessimistic_lock_res_callback(tx, empty.clone()),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    // Another non-resumable request blocked.
    let (tx_blocked_1, rx_blocked_1) = channel();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 11, 11, false, false),
            expect_fail_callback(tx_blocked_1, 0, |e| match e {
                Error(box ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                    box mvcc::ErrorInner::WriteConflict { .. },
                ))))) => (),
                e => panic!("unexpected error chain: {:?}", e),
            }),
        )
        .unwrap();
    rx_blocked_1
        .recv_timeout(Duration::from_millis(50))
        .unwrap_err();

    let tokens_before = lock_mgr.get_all_tokens();
    // Another resumable request blocked, and is queued behind the above one.
    let (tx_blocked_2, rx_blocked_2) = channel();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 12, 12, false, false)
                .allow_lock_with_conflict(true),
            if !canceled_when_resumed {
                expect_pessimistic_lock_res_callback(tx_blocked_2, empty.clone())
            } else {
                expect_value_with_checker_callback(
                    tx_blocked_2,
                    0,
                    |res: storage::Result<PessimisticLockResults>| {
                        let res = res.unwrap().0;
                        assert_eq!(res.len(), 1);
                        let e = res[0].unwrap_err();
                        match e.inner() {
                            ErrorInner::Txn(TxnError(box TxnErrorInner::Mvcc(mvcc::Error(
                                box mvcc::ErrorInner::KeyIsLocked(_),
                            )))) => (),
                            e => panic!("unexpected error chain: {:?}", e),
                        }
                    },
                )
            },
        )
        .unwrap();
    rx_blocked_2
        .recv_timeout(Duration::from_millis(50))
        .unwrap_err();
    // Find the lock wait token of the above request.
    let tokens_after = lock_mgr.get_all_tokens();
    let token_of_12 = {
        use std::ops::Sub;
        let diff = tokens_after.sub(&tokens_before);
        assert_eq!(diff.len(), 1);
        diff.into_iter().next().unwrap()
    };

    // Release the lock, so that the former (non-resumable) request will be woken
    // up, and the other one (resumable) will be woken up after delaying for
    // `wake_up_delay_duration`.
    delete_pessimistic_lock(&storage, key.clone(), 10, 10);
    rx_blocked_1.recv_timeout(Duration::from_secs(1)).unwrap();

    // The key should be unlocked at this time.
    must_have_locks(&storage, 100, b"", b"\xff\xff\xff", &[]);

    // Simulate the transaction at ts=11 retries the pessimistic lock request, and
    // succeeds.
    let (tx, rx) = channel();
    storage
        .sched_txn_command(
            new_acquire_pessimistic_lock_command(vec![(key.clone(), false)], 11, 11, false, false),
            expect_pessimistic_lock_res_callback(tx, empty),
        )
        .unwrap();
    rx.recv_timeout(Duration::from_secs(1)).unwrap();

    // Remove `pause` in delayed wake up, so that the request of txn 12 can be woken
    // up.
    fail::remove("lock_waiting_queue_before_delayed_notify_all");
    first_resume_rx.recv().unwrap();

    if canceled_when_resumed {
        lock_mgr.simulate_timeout(token_of_12);
    }

    fail::remove("acquire_pessimistic_lock_resumed_before_process_write");
    first_resume_continue_tx.send(()).unwrap();

    if canceled_when_resumed {
        rx_blocked_2.recv_timeout(Duration::from_secs(1)).unwrap();
        must_have_locks(
            &storage,
            100,
            b"",
            b"\xff\xff\xff",
            &[(&key.to_raw().unwrap(), Op::PessimisticLock, 11, 11)],
        );
    } else {
        rx_blocked_2
            .recv_timeout(Duration::from_millis(100))
            .unwrap_err();
        must_have_locks(
            &storage,
            100,
            b"",
            b"\xff\xff\xff",
            &[(&key.to_raw().unwrap(), Op::PessimisticLock, 11, 11)],
        );
        delete_pessimistic_lock(&storage, key.clone(), 11, 11);
        rx_blocked_2.recv_timeout(Duration::from_secs(1)).unwrap();
        must_have_locks(
            &storage,
            100,
            b"",
            b"\xff\xff\xff",
            &[(&key.to_raw().unwrap(), Op::PessimisticLock, 12, 12)],
        );
    }
}

#[test]
fn test_pessimistic_lock_resumable_blocked_twice() {
    test_pessimistic_lock_resumable_blocked_twice_impl(false);
    test_pessimistic_lock_resumable_blocked_twice_impl(true);
}

#[test]
fn test_async_commit_prewrite_with_stale_max_ts() {
    test_async_commit_prewrite_with_stale_max_ts_impl::<ApiV1>();
    test_async_commit_prewrite_with_stale_max_ts_impl::<ApiV2>();
}

fn test_async_commit_prewrite_with_stale_max_ts_impl<F: KvFormat>() {
    let mut cluster = new_server_cluster_with_api_ver(0, 2, F::TAG);
    cluster.run();

    let mut engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilder::<_, _, F>::from_engine_and_lock_mgr(
        engine.clone(),
        MockLockManager::new(),
    )
    .build()
    .unwrap();

    // Fail to get timestamp from PD at first
    fail::cfg("test_raftstore_get_tso", "pause").unwrap();
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_api_version(F::TAG);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    let check_max_timestamp_not_synced = |expected: bool| {
        // prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(b"xk1"), b"v".to_vec())],
                    b"xk1".to_vec(),
                    10.into(),
                    100,
                    false,
                    2,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"xk2".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    ctx.clone(),
                ),
                Box::new(move |res: storage::Result<_>| {
                    prewrite_tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let region_error = extract_region_error(&res);
        assert_eq!(
            region_error
                .map(|e| e.has_max_timestamp_not_synced())
                .unwrap_or(false),
            expected
        );

        // pessimistic prewrite
        let (prewrite_tx, prewrite_rx) = channel();
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(Key::from_raw(b"xk1"), b"v".to_vec()),
                        DoPessimisticCheck,
                    )],
                    b"xk1".to_vec(),
                    10.into(),
                    100,
                    20.into(),
                    2,
                    TimeStamp::default(),
                    TimeStamp::default(),
                    Some(vec![b"xk2".to_vec()]),
                    false,
                    AssertionLevel::Off,
                    vec![],
                    ctx.clone(),
                ),
                Box::new(move |res: storage::Result<_>| {
                    prewrite_tx.send(res).unwrap();
                }),
            )
            .unwrap();
        let res = prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let region_error = extract_region_error(&res);
        assert_eq!(
            region_error
                .map(|e| e.has_max_timestamp_not_synced())
                .unwrap_or(false),
            expected
        );
    };

    // should get max timestamp not synced error
    check_max_timestamp_not_synced(true);

    // can get timestamp from PD
    fail::remove("test_raftstore_get_tso");

    // wait for timestamp synced
    let snap_ctx = SnapContext {
        pb_ctx: &ctx,
        ..Default::default()
    };
    let snapshot = engine.snapshot(snap_ctx).unwrap();
    let txn_ext = snapshot.txn_ext.clone().unwrap();
    for retry in 0..10 {
        if txn_ext.is_max_ts_synced() {
            break;
        }
        thread::sleep(Duration::from_millis(1 << retry));
    }
    assert!(snapshot.ext().is_max_ts_synced());

    // should NOT get max timestamp not synced error
    check_max_timestamp_not_synced(false);
}

fn expect_locked(err: tikv::storage::Error, key: &[u8], lock_ts: TimeStamp) {
    let lock_info = extract_key_error(&err).take_locked();
    assert_eq!(lock_info.get_key(), key);
    assert_eq!(lock_info.get_lock_version(), lock_ts.into_inner());
}

fn test_async_apply_prewrite_impl<E: Engine, F: KvFormat>(
    storage: &Storage<E, MockLockManager, F>,
    ctx: Context,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: Option<u64>,
    is_pessimistic: bool,
    need_lock: bool,
    use_async_commit: bool,
    expect_async_apply: bool,
) {
    let on_handle_apply = "on_handle_apply";

    let start_ts = TimeStamp::from(start_ts);

    // Acquire the pessimistic lock if needed
    if need_lock {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(key), false)],
                    key.to_vec(),
                    start_ts,
                    0,
                    true,
                    start_ts,
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .unwrap();
    }

    // Prewrite and block it at apply phase.
    fail::cfg(on_handle_apply, "pause").unwrap();
    let (tx, rx) = channel();
    let secondaries = if use_async_commit { Some(vec![]) } else { None };
    if !is_pessimistic {
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(key), value.to_vec())],
                    key.to_vec(),
                    start_ts,
                    0,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    secondaries,
                    false,
                    AssertionLevel::Off,
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
    } else {
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(Key::from_raw(key), value.to_vec()),
                        if need_lock {
                            DoPessimisticCheck
                        } else {
                            SkipPessimisticCheck
                        },
                    )],
                    key.to_vec(),
                    start_ts,
                    0,
                    start_ts,
                    1,
                    0.into(),
                    0.into(),
                    secondaries,
                    false,
                    AssertionLevel::Off,
                    vec![],
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
    }

    if expect_async_apply {
        // The result should be able to be returned.
        let res = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(res.locks.len(), 0);
        assert!(use_async_commit);
        assert!(commit_ts.is_none());
        let min_commit_ts = res.min_commit_ts;
        assert!(
            min_commit_ts > start_ts,
            "min_commit_ts({}) not greater than start_ts({})",
            min_commit_ts,
            start_ts
        );

        // The memory lock is not released so reading will encounter the lock.
        thread::sleep(Duration::from_millis(300));
        let err = block_on(storage.get(ctx.clone(), Key::from_raw(key), min_commit_ts.next()))
            .unwrap_err();
        expect_locked(err, key, start_ts);
        // Commit command will be blocked.
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Commit::new(
                    vec![Key::from_raw(key)],
                    start_ts,
                    min_commit_ts,
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(300)).unwrap_err(),
            RecvTimeoutError::Timeout
        );

        // Continue applying and then the commit command can continue.
        fail::remove(on_handle_apply);
        rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();

        let got_value = block_on(storage.get(ctx, Key::from_raw(key), min_commit_ts.next()))
            .unwrap()
            .0;
        assert_eq!(got_value.unwrap().as_slice(), value);
    } else {
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(300)).unwrap_err(),
            RecvTimeoutError::Timeout
        );

        fail::remove(on_handle_apply);
        let res = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(res.locks.len(), 0);
        assert_eq!(res.min_commit_ts, 0.into());

        // Commit it.
        let commit_ts = commit_ts.unwrap().into();
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::Commit::new(vec![Key::from_raw(key)], start_ts, commit_ts, ctx.clone()),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();

        let got_value = block_on(storage.get(ctx, Key::from_raw(key), commit_ts.next()))
            .unwrap()
            .0;
        assert_eq!(got_value.unwrap().as_slice(), value);
    }
}

#[test]
fn test_async_apply_prewrite() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .async_apply_prewrite(true)
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    test_async_apply_prewrite_impl(
        &storage,
        ctx.clone(),
        b"key",
        b"value1",
        10,
        None,
        false,
        false,
        true,
        true,
    );
    test_async_apply_prewrite_impl(
        &storage,
        ctx.clone(),
        b"key",
        b"value2",
        20,
        None,
        true,
        false,
        true,
        true,
    );
    test_async_apply_prewrite_impl(
        &storage,
        ctx.clone(),
        b"key",
        b"value3",
        30,
        None,
        true,
        true,
        true,
        true,
    );

    test_async_apply_prewrite_impl(
        &storage,
        ctx.clone(),
        b"key",
        b"value1",
        40,
        Some(45),
        false,
        false,
        false,
        false,
    );
    test_async_apply_prewrite_impl(
        &storage,
        ctx.clone(),
        b"key",
        b"value2",
        50,
        Some(55),
        true,
        false,
        false,
        false,
    );
    test_async_apply_prewrite_impl(
        &storage,
        ctx,
        b"key",
        b"value3",
        60,
        Some(65),
        true,
        true,
        false,
        false,
    );
}

#[test]
fn test_async_apply_prewrite_fallback() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .async_apply_prewrite(true)
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    let before_async_apply_prewrite_finish = "before_async_apply_prewrite_finish";
    let on_handle_apply = "on_handle_apply";

    fail::cfg(before_async_apply_prewrite_finish, "return()").unwrap();
    fail::cfg(on_handle_apply, "pause").unwrap();

    let (key, value) = (b"k1", b"v1");
    let (tx, rx) = channel();
    storage
        .sched_txn_command(
            commands::Prewrite::new(
                vec![Mutation::make_put(Key::from_raw(key), value.to_vec())],
                key.to_vec(),
                10.into(),
                0,
                false,
                1,
                0.into(),
                0.into(),
                Some(vec![]),
                false,
                AssertionLevel::Off,
                ctx.clone(),
            ),
            Box::new(move |r| tx.send(r).unwrap()),
        )
        .unwrap();

    assert_eq!(
        rx.recv_timeout(Duration::from_millis(200)).unwrap_err(),
        RecvTimeoutError::Timeout
    );

    fail::remove(on_handle_apply);

    let res = rx.recv().unwrap().unwrap();
    assert!(res.min_commit_ts > 10.into());

    fail::remove(before_async_apply_prewrite_finish);

    let (tx, rx) = channel();
    storage
        .sched_txn_command(
            commands::Commit::new(vec![Key::from_raw(key)], 10.into(), res.min_commit_ts, ctx),
            Box::new(move |r| tx.send(r).unwrap()),
        )
        .unwrap();

    rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
}

fn test_async_apply_prewrite_1pc_impl<E: Engine, F: KvFormat>(
    storage: &Storage<E, MockLockManager, F>,
    ctx: Context,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    is_pessimistic: bool,
) {
    let on_handle_apply = "on_handle_apply";

    let start_ts = TimeStamp::from(start_ts);

    if is_pessimistic {
        let (tx, rx) = channel();
        storage
            .sched_txn_command(
                commands::AcquirePessimisticLock::new(
                    vec![(Key::from_raw(key), false)],
                    key.to_vec(),
                    start_ts,
                    0,
                    true,
                    start_ts,
                    None,
                    false,
                    0.into(),
                    false,
                    false,
                    false,
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
        rx.recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap()
            .unwrap();
    }

    // Prewrite and block it at apply phase.
    fail::cfg(on_handle_apply, "pause").unwrap();
    let (tx, rx) = channel();
    if !is_pessimistic {
        storage
            .sched_txn_command(
                commands::Prewrite::new(
                    vec![Mutation::make_put(Key::from_raw(key), value.to_vec())],
                    key.to_vec(),
                    start_ts,
                    0,
                    false,
                    1,
                    0.into(),
                    0.into(),
                    None,
                    true,
                    AssertionLevel::Off,
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
    } else {
        storage
            .sched_txn_command(
                commands::PrewritePessimistic::new(
                    vec![(
                        Mutation::make_put(Key::from_raw(key), value.to_vec()),
                        DoPessimisticCheck,
                    )],
                    key.to_vec(),
                    start_ts,
                    0,
                    start_ts,
                    1,
                    0.into(),
                    0.into(),
                    None,
                    true,
                    AssertionLevel::Off,
                    vec![],
                    ctx.clone(),
                ),
                Box::new(move |r| tx.send(r).unwrap()),
            )
            .unwrap();
    }

    let res = rx.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(res.locks.len(), 0);
    assert!(res.one_pc_commit_ts > start_ts);
    let commit_ts = res.one_pc_commit_ts;

    let err = block_on(storage.get(ctx.clone(), Key::from_raw(key), commit_ts.next())).unwrap_err();
    expect_locked(err, key, start_ts);

    fail::remove(on_handle_apply);
    // The key may need some time to be applied.
    for retry in 0.. {
        let res = block_on(storage.get(ctx.clone(), Key::from_raw(key), commit_ts.next()));
        match res {
            Ok(v) => {
                assert_eq!(v.0.unwrap().as_slice(), value);
                break;
            }
            Err(e) => expect_locked(e, key, start_ts),
        }

        if retry > 20 {
            panic!("the key is not applied for too long time");
        }
        thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn test_async_apply_prewrite_1pc() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .async_apply_prewrite(true)
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    test_async_apply_prewrite_1pc_impl(&storage, ctx.clone(), b"key", b"value1", 10, false);
    test_async_apply_prewrite_1pc_impl(&storage, ctx, b"key", b"value2", 20, true);
}

#[test]
fn test_atomic_cas_lock_by_latch() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    let latch_acquire_success_fp = "txn_scheduler_acquire_success";
    let latch_acquire_fail_fp = "txn_scheduler_acquire_fail";
    let pending_cas_fp = "txn_commands_compare_and_swap";
    let wakeup_latch_fp = "txn_scheduler_try_to_wake_up";
    let acquire_flag = Arc::new(AtomicBool::new(false));
    let acquire_flag1 = acquire_flag.clone();
    let acquire_flag_fail = Arc::new(AtomicBool::new(false));
    let acquire_flag_fail1 = acquire_flag_fail.clone();
    let wakeup_latch_flag = Arc::new(AtomicBool::new(false));
    let wakeup1 = wakeup_latch_flag.clone();

    fail::cfg(pending_cas_fp, "pause").unwrap();
    fail::cfg_callback(latch_acquire_success_fp, move || {
        acquire_flag1.store(true, Ordering::Release);
    })
    .unwrap();
    fail::cfg_callback(latch_acquire_fail_fp, move || {
        acquire_flag_fail1.store(true, Ordering::Release);
    })
    .unwrap();
    fail::cfg_callback(wakeup_latch_fp, move || {
        wakeup1.store(true, Ordering::Release);
    })
    .unwrap();
    let (cb, f1) = paired_future_callback();
    storage
        .raw_compare_and_swap_atomic(
            ctx.clone(),
            "".to_string(),
            b"key".to_vec(),
            None,
            b"v1".to_vec(),
            0,
            cb,
        )
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    assert!(acquire_flag.load(Ordering::Acquire));
    assert!(!acquire_flag_fail.load(Ordering::Acquire));
    acquire_flag.store(false, Ordering::Release);
    let (cb, f2) = paired_future_callback();
    storage
        .raw_compare_and_swap_atomic(
            ctx.clone(),
            "".to_string(),
            b"key".to_vec(),
            Some(b"v1".to_vec()),
            b"v2".to_vec(),
            0,
            cb,
        )
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    assert!(acquire_flag_fail.load(Ordering::Acquire));
    assert!(!acquire_flag.load(Ordering::Acquire));
    fail::remove(pending_cas_fp);
    let _ = block_on(f1).unwrap();
    let (prev_val, succeed) = block_on(f2).unwrap().unwrap();
    assert!(wakeup_latch_flag.load(Ordering::Acquire));
    assert!(succeed);
    assert_eq!(prev_val, Some(b"v1".to_vec()));
    let f = storage.raw_get(ctx, "".to_string(), b"key".to_vec());
    let ret = block_on(f).unwrap().unwrap();
    assert_eq!(b"v2".to_vec(), ret);
}

#[test]
fn test_before_async_write_deadline() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());
    ctx.max_execution_duration_ms = 200;
    let (tx, rx) = channel();
    fail::cfg("cleanup", "sleep(500)").unwrap();
    storage
        .sched_txn_command(
            commands::Rollback::new(vec![Key::from_raw(b"k")], 10.into(), ctx),
            Box::new(move |res: storage::Result<_>| {
                tx.send(res).unwrap();
            }),
        )
        .unwrap();

    assert!(matches!(
        rx.recv().unwrap(),
        Err(StorageError(box StorageErrorInner::DeadlineExceeded))
    ));
}

#[test]
fn test_deadline_exceeded_on_get_and_batch_get() {
    use tikv_util::time::Instant;
    use tracker::INVALID_TRACKER_TOKEN;

    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster
        .sim
        .read()
        .unwrap()
        .storages
        .get(&1)
        .unwrap()
        .clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    fail::cfg("after-snapshot", "sleep(100)").unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());
    ctx.max_execution_duration_ms = 20;
    let f = storage.get(ctx.clone(), Key::from_raw(b"a"), 1.into());
    assert!(matches!(
        block_on(f),
        Err(StorageError(box StorageErrorInner::DeadlineExceeded))
    ));
    let f = storage.batch_get(ctx.clone(), vec![Key::from_raw(b"a")], 1.into());
    assert!(matches!(
        block_on(f),
        Err(StorageError(box StorageErrorInner::DeadlineExceeded))
    ));

    let consumer = GetConsumer::new();
    let mut get_req = GetRequest::default();
    get_req.set_key(b"a".to_vec());
    get_req.set_version(1_u64);
    get_req.set_context(ctx.clone());
    block_on(storage.batch_get_command(
        vec![get_req],
        vec![1],
        vec![INVALID_TRACKER_TOKEN; 1],
        consumer.clone(),
        Instant::now(),
    ))
    .unwrap();
    let result = consumer.take_data();
    assert_eq!(1, result.len());
    assert!(matches!(
        result[0],
        Err(StorageError(box StorageErrorInner::DeadlineExceeded))
    ));
    fail::remove("after-snapshot");
}

#[test]
fn test_before_propose_deadline() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster.sim.read().unwrap().storages[&1].clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());
    ctx.max_execution_duration_ms = 200;
    let (tx, rx) = channel();
    fail::cfg("pause_on_peer_collect_message", "sleep(500)").unwrap();
    storage
        .sched_txn_command(
            commands::Rollback::new(vec![Key::from_raw(b"k")], 10.into(), ctx),
            Box::new(move |res: storage::Result<_>| {
                tx.send(res).unwrap();
            }),
        )
        .unwrap();
    let res = rx.recv().unwrap();
    assert!(
        matches!(
            res,
            Err(StorageError(box StorageErrorInner::Kv(KvError(
                box KvErrorInner::Request(_),
            ))))
        ),
        "actual: {:?}",
        res
    );
}

#[test]
fn test_resolve_lock_deadline() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let engine = cluster.sim.read().unwrap().storages[&1].clone();
    let storage = TestStorageBuilderApiV1::from_engine_and_lock_mgr(engine, MockLockManager::new())
        .build()
        .unwrap();

    let mut ctx = Context::default();
    ctx.set_region_id(1);
    ctx.set_region_epoch(cluster.get_region_epoch(1));
    ctx.set_peer(cluster.leader_of_region(1).unwrap());

    // One resolve lock batch is 256 keys. So we need to prewrite more than that.
    let mutations = (1i32..300)
        .map(|i| {
            let data = i.to_le_bytes();
            Mutation::make_put(Key::from_raw(&data), data.to_vec())
        })
        .collect();
    let cmd = commands::Prewrite::new(
        mutations,
        1i32.to_le_bytes().to_vec(),
        10.into(),
        1,
        false,
        299,
        15.into(),
        20.into(),
        None,
        false,
        AssertionLevel::Off,
        ctx.clone(),
    );
    let (tx, rx) = channel();
    storage
        .sched_txn_command(
            cmd,
            Box::new(move |res: storage::Result<_>| {
                tx.send(res).unwrap();
            }),
        )
        .unwrap();
    rx.recv().unwrap().unwrap();

    // Resolve lock, this needs two rounds, two process_read and two process_write.
    // So it needs more than 400ms. It will exceed the deadline.
    ctx.max_execution_duration_ms = 300;
    fail::cfg("txn_before_process_read", "1*sleep(100)->sleep(200)").unwrap();
    fail::cfg("txn_before_process_write", "1*sleep(100)->sleep(500)").unwrap();
    let (tx, rx) = channel();
    let mut txn_status = HashMap::default();
    txn_status.insert(TimeStamp::new(10), TimeStamp::new(0));
    storage
        .sched_txn_command(
            commands::ResolveLockReadPhase::new(txn_status, None, ctx),
            Box::new(move |res: storage::Result<_>| {
                tx.send(res).unwrap();
            }),
        )
        .unwrap();
    assert!(matches!(
        rx.recv().unwrap(),
        Err(StorageError(box StorageErrorInner::DeadlineExceeded))
    ));
}

/// Checks if concurrent transaction works correctly during shutdown.
///
/// During shutdown, all pending writes will fail with error so its latch will
/// be released. Then other writes in the latch queue will be continued to be
/// processed, which can break the correctness of latch: underlying command
/// result is always determined, it should be either always success written or
/// never be written.
#[test]
fn test_mvcc_concurrent_commit_and_rollback_at_shutdown() {
    let (mut cluster, mut client, mut ctx) = must_new_cluster_and_kv_client_mul(3);
    let k = b"key".to_vec();
    // Use big value to force it in default cf.
    let v = vec![0; 10240];

    let mut ts = 0;

    // Prewrite
    ts += 1;
    let prewrite_start_version = ts;
    let mut mutation = kvrpcpb::Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_start_version,
    );

    // So all following operation will not be committed by this leader.
    let leader_fp = "before_leader_handle_committed_entries";
    fail::cfg(leader_fp, "pause").unwrap();

    // Commit
    ts += 1;
    let commit_version = ts;
    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx.clone());
    commit_req.start_version = prewrite_start_version;
    commit_req.mut_keys().push(k.clone());
    commit_req.commit_version = commit_version;
    let _commit_resp = client.kv_commit_async(&commit_req).unwrap();

    // Rollback
    let rollback_start_version = prewrite_start_version;
    let mut rollback_req = BatchRollbackRequest::default();
    rollback_req.set_context(ctx.clone());
    rollback_req.start_version = rollback_start_version;
    rollback_req.mut_keys().push(k.clone());
    let _rollback_resp = client.kv_batch_rollback_async(&rollback_req).unwrap();

    // Sleep some time to make sure both commit and rollback are queued in latch.
    thread::sleep(Duration::from_millis(100));
    let shutdown_fp = "after_shutdown_apply";
    fail::cfg_callback(shutdown_fp, move || {
        fail::remove(leader_fp);
        // Sleep some time to ensure all logs can be replicated.
        thread::sleep(Duration::from_millis(300));
    })
    .unwrap();
    let mut leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(leader.get_store_id());

    // So a new leader should be elected.
    cluster.must_put(b"k2", b"v2");
    leader = cluster.leader_of_region(1).unwrap();
    ctx.set_peer(leader.clone());
    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    client = TikvClient::new(channel);

    // The first request is commit, the second is rollback, the first one should
    // succeed.
    ts += 1;
    let get_version = ts;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.key = k;
    get_req.version = get_version;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(
        !get_resp.has_region_error() && !get_resp.has_error(),
        "{:?}",
        get_resp
    );
    assert_eq!(get_resp.value, v);
}

#[test]
fn test_raw_put_deadline() {
    let deadline_fp = "deadline_check_fail";
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = b"k3".to_vec();
    put_req.value = b"v3".to_vec();
    fail::cfg(deadline_fp, "return()").unwrap();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(put_resp.has_region_error(), "{:?}", put_resp);
    must_get_none(&cluster.get_engine(1), b"k3");

    fail::remove(deadline_fp);
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error(), "{:?}", put_resp);
    must_get_equal(&cluster.get_engine(1), b"k3", b"v3");
}
