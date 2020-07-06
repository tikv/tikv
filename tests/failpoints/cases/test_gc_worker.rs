use std::sync::{mpsc::channel, Arc};
use std::thread;
use std::time::Duration;

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::*;
use test_storage::new_raft_engine;
use tikv::server::gc_worker::{GcWorker, GC_MAX_EXECUTING_TASKS};
use tikv::storage;
use tikv_util::{collections::HashMap, HandyRwLock};

#[test]
fn test_gcworker_busy() {
    let snapshot_fp = "raftkv_async_snapshot";
    let (_cluster, engine, ctx) = new_raft_engine(3, "");
    let mut gc_worker = GcWorker::new(engine, None, None, None, Default::default());
    gc_worker.start().unwrap();

    fail::cfg(snapshot_fp, "pause").unwrap();
    let (tx1, rx1) = channel();
    // Schedule `GC_MAX_EXECUTING_TASKS - 1` GC requests.
    for _i in 1..GC_MAX_EXECUTING_TASKS {
        let tx1 = tx1.clone();
        gc_worker
            .gc(
                ctx.clone(),
                1.into(),
                Box::new(move |res: storage::Result<()>| {
                    assert!(res.is_ok());
                    tx1.send(1).unwrap();
                }),
            )
            .unwrap();
    }
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Schedule one more request. So that there is a request being processed and
    // `GC_MAX_EXECUTING_TASKS` requests in queue.
    gc_worker
        .gc(
            ctx,
            1.into(),
            Box::new(move |res: storage::Result<()>| {
                assert!(res.is_ok());
                tx1.send(1).unwrap();
            }),
        )
        .unwrap();

    // Old GC commands are blocked, the new one will get GcWorkerTooBusy error.
    let (tx2, rx2) = channel();
    gc_worker
        .gc(
            Context::default(),
            1.into(),
            Box::new(move |res: storage::Result<()>| {
                match res {
                    Err(storage::Error(box storage::ErrorInner::GcWorkerTooBusy)) => {}
                    res => panic!("expect too busy, got {:?}", res),
                }
                tx2.send(1).unwrap();
            }),
        )
        .unwrap();

    rx2.recv().unwrap();
    fail::remove(snapshot_fp);
    for _ in 0..GC_MAX_EXECUTING_TASKS {
        rx1.recv().unwrap();
    }
}

// In theory, raft can propose conf change as long as there is no pending one. Replicas
// don't apply logs synchronously, so it's possible the old leader is removed before the new
// leader applies all logs.
// In the current implementation, the new leader rejects conf change until it applies all logs.
// It guarantees the correctness of green GC. This test is to prevent breaking it in the
// future.
#[test]
fn test_collect_lock_from_stale_leader() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.pd_client.disable_default_operator();
    let region_id = cluster.run_conf_change();
    let leader = cluster.leader_of_region(region_id).unwrap();

    // Create clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(node_id));
        let client = TikvClient::new(channel);
        clients.insert(node_id, client);
    }

    // Start transferring the region to store 2.
    let new_peer = new_peer(2, 1003);
    cluster.pd_client.must_add_peer(region_id, new_peer.clone());

    // Create the ctx of the first region.
    let leader_client = clients.get(&leader.get_store_id()).unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));

    // Pause the new peer applying so that when it becomes the leader, it doesn't apply all logs.
    let new_leader_apply_fp = "on_handle_apply_1003";
    fail::cfg(new_leader_apply_fp, "pause").unwrap();
    must_kv_prewrite(
        &leader_client,
        ctx,
        vec![new_mutation(Op::Put, b"k1", b"v")],
        b"k1".to_vec(),
        10,
    );

    // Leader election only considers the progress of appending logs, so it can succeed.
    cluster.must_transfer_leader(region_id, new_peer.clone());
    // It shouldn't succeed in the current implementation.
    cluster.pd_client.remove_peer(region_id, leader.clone());
    std::thread::sleep(Duration::from_secs(1));
    cluster.pd_client.must_have_peer(region_id, leader);

    // Must scan the lock from the old leader.
    let locks = must_physical_scan_lock(&leader_client, Context::default(), 100, b"", 10);
    assert_eq!(locks.len(), 1);
    assert_eq!(locks[0].get_key(), b"k1");

    // Can't scan the lock from the new leader.
    let leader_client = clients.get(&new_peer.get_store_id()).unwrap();
    must_register_lock_observer(&leader_client, 100);
    let locks = must_check_lock_observer(&leader_client, 100, true);
    assert!(locks.is_empty());
    let locks = must_physical_scan_lock(&leader_client, Context::default(), 100, b"", 10);
    assert!(locks.is_empty());

    fail::remove(new_leader_apply_fp);
}
