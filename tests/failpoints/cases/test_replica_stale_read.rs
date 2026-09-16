// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use kvproto::{kvrpcpb::Op, metapb::Peer, raft_serverpb::RaftMessage};
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use test_pd_client::TestPdClient;
use test_raftstore::*;
use tikv_util::config::ReadableDuration;

/// A gate the test can park a peer thread on: the producer parks in
/// [`ParkGate::enter`], signalling its arrival first so the test knows it is
/// parked, until the test either grants one permit or opens the gate for good.
struct ParkGate {
    name: &'static str,
    arrived: mpsc::Sender<()>,
    /// `(permits, open)`
    state: Mutex<(usize, bool)>,
    cond: Condvar,
}

impl ParkGate {
    fn new(name: &'static str) -> (Arc<ParkGate>, mpsc::Receiver<()>) {
        let (arrived_tx, arrived_rx) = mpsc::channel();
        (
            Arc::new(ParkGate {
                name,
                arrived: arrived_tx,
                state: Mutex::new((0, false)),
                cond: Condvar::new(),
            }),
            arrived_rx,
        )
    }

    fn enter(&self) {
        let mut state = self.state.lock().unwrap();
        if state.1 {
            return;
        }
        if state.0 > 0 {
            state.0 -= 1;
            return;
        }
        let _ = self.arrived.send(());
        while state.0 == 0 && !state.1 {
            let (guard, timeout) = self
                .cond
                .wait_timeout(state, Duration::from_secs(30))
                .unwrap();
            state = guard;
            assert!(
                !timeout.timed_out(),
                "the test must release gate {}",
                self.name
            );
        }
        if !state.1 {
            state.0 -= 1;
        }
    }

    /// Let exactly one parked producer continue.
    fn permit(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 += 1;
        self.cond.notify_all();
    }

    /// Let the parked producer continue and park no one else.
    fn open(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        state.0 += 1;
        self.cond.notify_all();
    }
}

/// Opens a [`ParkGate`] when it goes out of scope, so a panicking test never
/// leaves a peer thread parked (which would poison the store meta lock it
/// holds).
struct GateGuard(Arc<ParkGate>);

impl GateGuard {
    fn new(gate: Arc<ParkGate>) -> Self {
        GateGuard(gate)
    }
}

impl Drop for GateGuard {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// Drops `MsgHeartbeat` messages that are sent to one store. The store then
/// stops refreshing this follower's election timer without isolating it:
/// replication and `CheckLeader` keep working, and a heartbeat can be let
/// through again on demand.
#[derive(Clone)]
struct DropHeartbeats {
    target_store_ids: Vec<u64>,
    allow: Arc<AtomicBool>,
}

impl Filter for DropHeartbeats {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> raftstore::Result<()> {
        if self.allow.load(Ordering::SeqCst) {
            return Ok(());
        }
        msgs.retain(|msg| {
            msg.get_message().get_msg_type() != MessageType::MsgHeartbeat
                || !self
                    .target_store_ids
                    .contains(&msg.get_to_peer().get_store_id())
        });
        check_messages(msgs)
    }
}

/// `(leader peer id, leader term, region version)` as cached by
/// `RegionReadProgress`.
///
/// This is exactly the tuple `consume_leader_info` compares against the
/// `LeaderInfo` the leader sends, so `leader == 0` means "this replica rejects
/// the leader probe".
fn cached_leader_info(
    cluster: &Cluster<ServerCluster>,
    region_id: u64,
    store_id: u64,
) -> (u64, u64, u64) {
    let info = cluster.store_metas[&store_id]
        .lock()
        .unwrap()
        .region_read_progress
        .get(&region_id)
        .unwrap()
        .dump_leader_info()
        .0;
    (
        info.get_peer_id(),
        info.get_term(),
        info.get_region_epoch().get_version(),
    )
}

fn wait_for_cached_leader(
    cluster: &Cluster<ServerCluster>,
    region_id: u64,
    store_id: u64,
    leader_id: u64,
) -> (u64, u64, u64) {
    let start = Instant::now();
    loop {
        let cached = cached_leader_info(cluster, region_id, store_id);
        if cached.0 == leader_id {
            return cached;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "store {store_id} never cached leader {leader_id}: {cached:?}"
        );
        sleep_ms(10);
    }
}

/// The resolved ts this store published for the region, i.e. the value a
/// `CheckLeader` round settles the region's `ReadState` on.
fn region_resolved_ts(cluster: &Cluster<ServerCluster>, region_id: u64, store_id: u64) -> u64 {
    cluster.store_metas[&store_id]
        .lock()
        .unwrap()
        .region_read_progress
        .get_resolved_ts(&region_id)
        .unwrap()
}

fn prepare_for_stale_read(leader: Peer) -> (Cluster<ServerCluster>, Arc<TestPdClient>, PeerClient) {
    prepare_for_stale_read_before_run(leader, None)
}

fn prepare_for_stale_read_before_run(
    leader: Peer,
    before_run: Option<Box<dyn Fn(&mut Config)>>,
) -> (Cluster<ServerCluster>, Arc<TestPdClient>, PeerClient) {
    let mut cluster = new_server_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    if let Some(f) = before_run {
        f(&mut cluster.cfg);
    };
    cluster.cfg.resolved_ts.enable = true;
    cluster.run();

    cluster.must_transfer_leader(1, leader.clone());
    let leader_client = PeerClient::new(&cluster, 1, leader);

    // There should be no read index message while handling stale read request
    fail::cfg("propose_readindex_from_follower", "panic").unwrap();

    (cluster, pd_client, leader_client)
}

/// Regression test for the stuck resolved-ts in
/// https://github.com/tikv/tikv/issues/19768: a region update must not drop the
/// leadership a replica has cached for resolved-ts.
///
/// The fix is https://github.com/tikv/tikv/pull/20029; this test fails without it.
///
/// The condition it reproduces: one round of a peer carries the PreVote
/// campaign, the region update and a heartbeat in the same batch. The region
/// update then publishes a `leader_id` the replica never lost.
///
/// The region used here has three replicas, and both followers are put through
/// that condition, so on the unfixed code its resolved ts never advances again.
/// Staging is scheduling only: both followers campaign on their own election
/// timers, the region update is a real split `ApplyRes`, and one follower is
/// silenced at a time so the leader keeps its quorum.
#[test]
fn test_region_update_keeps_leader_progress_after_transient_pre_vote() {
    const REGION_ID: u64 = 1;
    const LEADER_STORE_ID: u64 = 1;
    const LEADER_PEER_ID: u64 = 1;
    const FOLLOWER_STORE_ID: u64 = 3;
    let mut cluster = new_server_cluster(0, 4);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.cfg.resolved_ts.enable = true;
    cluster.cfg.resolved_ts.advance_ts_interval = ReadableDuration::millis(100);
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.cfg.raft_store.prevote = true;
    cluster.cfg.raft_store.raft_base_tick_interval = ReadableDuration::millis(10);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration::millis(200);
    cluster.cfg.raft_store.raft_election_timeout_ticks = 30;
    cluster.cfg.raft_store.raft_heartbeat_ticks = 2;
    cluster.run_conf_change();
    pd_client.must_add_peer(REGION_ID, new_peer(2, 2));
    pd_client.must_add_peer(REGION_ID, new_peer(3, 3));
    cluster.must_transfer_leader(REGION_ID, new_peer(1, LEADER_PEER_ID));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");
    wait_for_cached_leader(&cluster, REGION_ID, 2, LEADER_PEER_ID);
    wait_for_cached_leader(&cluster, REGION_ID, FOLLOWER_STORE_ID, LEADER_PEER_ID);

    let (batch_gate_2, batch_arrived_2) = ParkGate::new("batch_2");
    let (batch_gate_3, batch_arrived_3) = ParkGate::new("batch_3");
    let (apply_gate_2, apply_arrived_2) = ParkGate::new("apply_2");
    let (apply_gate_3, apply_arrived_3) = ParkGate::new("apply_3");
    let (region_update_gate, region_update_arrived) = ParkGate::new("region_update");
    for (name, gate) in [
        ("pause_apply_res_of_store_2", apply_gate_2.clone()),
        ("pause_apply_res_of_store_3", apply_gate_3.clone()),
        (
            "set_region_publishes_raw_leader_id",
            region_update_gate.clone(),
        ),
    ] {
        fail::cfg_callback(name, move || gate.enter()).unwrap();
    }
    let _apply_guard_2 = GateGuard::new(apply_gate_2.clone());
    let _apply_guard_3 = GateGuard::new(apply_gate_3.clone());
    let _batch_guard_2 = GateGuard::new(batch_gate_2.clone());
    let _batch_guard_3 = GateGuard::new(batch_gate_3.clone());
    let _region_update_guard = GateGuard::new(region_update_gate.clone());

    // Hold back the apply result of both followers, then drive one real region
    // update.
    let (_, _, version_before) =
        wait_for_cached_leader(&cluster, REGION_ID, LEADER_STORE_ID, LEADER_PEER_ID);
    sleep_ms(300);
    let region = cluster.get_region(b"k1");
    cluster.must_split(&region, b"z");
    for (name, arrived) in [("store 2", &apply_arrived_2), ("store 3", &apply_arrived_3)] {
        arrived
            .recv_timeout(Duration::from_secs(10))
            .unwrap_or_else(|_| panic!("{name} must apply the split"));
    }
    let start = Instant::now();
    while cached_leader_info(&cluster, REGION_ID, LEADER_STORE_ID).2 <= version_before {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the leader never applied the split"
        );
        sleep_ms(10);
    }

    // Latch the two followers one after another, never both at the same time: while
    // one follower is silent (heartbeats cut, so its own election timer reaches its
    // timeout) the other one keeps answering the leader, so the leader never loses
    // its quorum and never has to re-elect. Latching them at the same time would
    // need both election timers to run, which takes longer than the leader's
    // own quorum check, and the term change of the re-election would make
    // `ready.hs()` fire the repair arm and rewrite the entry correctly.
    //
    // Each follower's apply result is only held back until its own election round,
    // so the delay stays within one election timeout, the same order as any
    // scheduling hiccup, rather than being held for the whole test.
    // Phase 1 silences store 3 while store 2 keeps answering.
    let allow_heartbeats_2 = Arc::new(AtomicBool::new(true));
    let allow_heartbeats_3 = Arc::new(AtomicBool::new(false));
    cluster.add_send_filter({
        let allow = allow_heartbeats_2.clone();
        move |_| DropHeartbeats {
            target_store_ids: vec![2],
            allow: allow.clone(),
        }
    });
    cluster.add_send_filter({
        let allow = allow_heartbeats_3.clone();
        move |_| DropHeartbeats {
            target_store_ids: vec![FOLLOWER_STORE_ID],
            allow: allow.clone(),
        }
    });
    fail::cfg_callback("pause_before_collect_peer_msg_on_election", {
        let gate = batch_gate_3.clone();
        move || gate.enter()
    })
    .unwrap();
    // Phase 1: store 3 is silent, store 2 keeps the leader alive.
    batch_arrived_3
        .recv_timeout(Duration::from_secs(10))
        .expect("the follower on store 3 must reach its election timeout");
    sleep_ms(100);
    apply_gate_3.open();
    sleep_ms(50);
    allow_heartbeats_3.store(true, Ordering::SeqCst);
    sleep_ms(200);
    batch_gate_3.open();
    region_update_arrived
        .recv_timeout(Duration::from_secs(3))
        .expect("the region update on store 3 must run while its campaign is pending");
    // The parked region update holds store 3's meta lock, so let it finish before
    // the caches are read.
    region_update_gate.permit();

    // Phase 2: store 3 is healthy again, store 2 is the silent one. The same gate
    // is re-armed for store 2; store 3 keeps receiving heartbeats, so its own
    // election never becomes due and its condition cannot fire here.
    allow_heartbeats_2.store(false, Ordering::SeqCst);
    fail::cfg_callback("pause_before_collect_peer_msg_on_election", {
        let gate = batch_gate_2.clone();
        move || gate.enter()
    })
    .unwrap();
    batch_arrived_2
        .recv_timeout(Duration::from_secs(10))
        .expect("the follower on store 2 must reach its election timeout");
    sleep_ms(100);
    apply_gate_2.open();
    sleep_ms(50);
    allow_heartbeats_2.store(true, Ordering::SeqCst);
    sleep_ms(200);
    batch_gate_2.open();
    region_update_arrived
        .recv_timeout(Duration::from_secs(3))
        .expect("the region update on store 2 must run while its campaign is pending");
    region_update_gate.open();
    fail::remove("pause_before_collect_peer_msg_on_election");
    fail::remove("set_region_publishes_raw_leader_id");
    let cached_2 = cached_leader_info(&cluster, REGION_ID, 2);
    let cached_3 = cached_leader_info(&cluster, REGION_ID, FOLLOWER_STORE_ID);

    fail::remove("pause_apply_res_of_store_2");
    fail::remove("pause_apply_res_of_store_3");
    allow_heartbeats_2.store(true, Ordering::SeqCst);
    allow_heartbeats_3.store(true, Ordering::SeqCst);
    sleep_ms(500);

    cluster.must_put(b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    // Both followers keep serving the region while the poisoned entries stay put.
    let mut latched_samples = 0;
    for _ in 0..20 {
        for store_id in [2, FOLLOWER_STORE_ID] {
            if cached_leader_info(&cluster, REGION_ID, store_id).0 != LEADER_PEER_ID {
                latched_samples += 1;
            }
        }
        sleep_ms(100);
    }

    // With both voters poisoned the leader cannot get a `CheckLeader` quorum at
    // all, while Raft itself is completely healthy: every replica is up, the
    // region keeps committing, and resolved ts still freezes.
    let before = region_resolved_ts(&cluster, REGION_ID, LEADER_STORE_ID);
    let start = Instant::now();
    let mut resolved_ts_advanced = false;
    while start.elapsed() < Duration::from_secs(4) {
        cluster.must_put(b"k3", b"v3");
        must_get_equal(&cluster.get_engine(2), b"k3", b"v3");
        must_get_equal(&cluster.get_engine(3), b"k3", b"v3");
        if region_resolved_ts(&cluster, REGION_ID, LEADER_STORE_ID) > before {
            resolved_ts_advanced = true;
            break;
        }
        sleep_ms(50);
    }
    let leader_version = cached_leader_info(&cluster, REGION_ID, LEADER_STORE_ID).2;

    println!(
        "[latch2] cached_2={cached_2:?} cached_3={cached_3:?} leader_version={leader_version} \
         latched_samples={latched_samples}/40 resolved_ts_advanced={resolved_ts_advanced}"
    );

    assert_eq!(
        cached_2.0, LEADER_PEER_ID,
        "a follower published a raw Raft leader_id into the resolved-ts cache \
         (cached_2={cached_2:?}, cached_3={cached_3:?}, \
         latched_samples={latched_samples}/40, resolved_ts_advanced={resolved_ts_advanced})"
    );
    assert_eq!(
        cached_3.0, LEADER_PEER_ID,
        "a follower published a raw Raft leader_id into the resolved-ts cache \
         (cached_2={cached_2:?}, cached_3={cached_3:?}, \
         latched_samples={latched_samples}/40, resolved_ts_advanced={resolved_ts_advanced})"
    );
    assert!(
        cached_2.2 >= leader_version && cached_3.2 >= leader_version,
        "the region update did not reach both followers: {cached_2:?} {cached_3:?}"
    );
    assert_eq!(
        latched_samples, 0,
        "a follower stayed inconsistent with its leader"
    );
    assert!(
        resolved_ts_advanced,
        "resolved-ts froze although every replica of the region is healthy and keeps \
         committing (cached_2={cached_2:?}, cached_3={cached_3:?})"
    );
}

// Testing how data replication could effect stale read service
#[test]
fn test_stale_read_basic_flow_replicate() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    // Set the `stale_read` flag
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Can read `value1` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Follower 2 can still read `value1`, but can not read `value2` due
    // to it don't have enough data
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
    let resp1 = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp1.get_region_error().has_data_is_not_ready());

    // Leader have up to date data so it can read `value2`
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));

    // clear the `MsgAppend` filter
    cluster.clear_send_filters();

    // Now we can read `value2` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));
}

// Similar to test_stale_read_basic_flow_replicate, but we use 1pc to update.
#[test]
fn test_stale_read_1pc_flow_replicate() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    // Set the `stale_read` flag
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Can read `value1` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));
    // Update `key1`
    leader_client.must_kv_prewrite_one_pc(
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
        get_tso(&pd_client),
    );
    let read_ts = get_tso(&pd_client);
    // wait for advance_resolved_ts.
    sleep_ms(200);
    // Follower 2 can still read `value1`, but can not read `value2` due
    // to it don't have enough data
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
    let resp1 = follower_client2.kv_read(b"key1".to_vec(), read_ts);
    assert!(resp1.get_region_error().has_data_is_not_ready());

    // Leader have up to date data so it can read `value2`
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));

    // clear the `MsgAppend` filter
    cluster.clear_send_filters();

    // Now we can read `value2` with the newest ts
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), get_tso(&pd_client));
}

// Testing how mvcc locks could effect stale read service
#[test]
fn test_stale_read_basic_flow_lock() {
    let (cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Prewrite on `key2` but not commit yet
    let k2_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        k2_prewrite_ts,
    );
    // Update `key1`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Assert `(key1, value2)` can't be read with `commit_ts2` due to it's larger
    // than the `start_ts` of `key2`.
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());
    // Still can read `(key1, value1)` since `commit_ts1` is less than the `key2`
    // lock's `start_ts`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Prewrite on `key3` but not commit yet
    let k3_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value1"[..])],
        b"key3".to_vec(),
        k3_prewrite_ts,
    );
    // Commit on `key2`
    let k2_commit_ts = get_tso(&pd_client);
    leader_client.must_kv_commit(vec![b"key2".to_vec()], k2_prewrite_ts, k2_commit_ts);

    // Although there is still lock on the region, but the min lock is refreshed
    // to the `key3`'s lock, now we can read `(key1, value2)` but not `(key2,
    // value1)`
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    let resp = follower_client2.kv_read(b"key2".to_vec(), k2_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    // Commit on `key3`
    let k3_commit_ts = get_tso(&pd_client);
    leader_client.must_kv_commit(vec![b"key3".to_vec()], k3_prewrite_ts, k3_commit_ts);

    // Now there is not lock on the region, we can read any
    // up to date data
    follower_client2.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
    follower_client2.must_kv_read_equal(b"key3".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that even leader's `apply_index` updated before sync the
// `(apply_index, safe_ts)` item to other replica, the `apply_index` in the
// `(apply_index, safe_ts)` item should not be updated
#[test]
fn test_update_apply_index_before_sync_read_state() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    leader_client.ctx.set_stale_read(true);

    // Stop node 3 to ensure data must replicated to follower 2 before write return
    cluster.stop_node(3);

    // Stop sync `(apply_index, safe_ts)` item to the replica
    let before_sync_replica_read_state = "before_sync_replica_read_state";
    fail::cfg(before_sync_replica_read_state, "return()").unwrap();

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    // Leave a lock on `key2` so the item's `safe_ts` won't be updated
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value2"[..])],
        b"key2".to_vec(),
        get_tso(&pd_client),
    );
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    cluster.run_node(3).unwrap();
    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Write `(key3, value3)` to update the leader `apply_index`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value3"[..])],
        b"key3".to_vec(),
    );

    // Sync `(apply_index, safe_ts)` item to the replica
    fail::remove(before_sync_replica_read_state);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
}

// Testing that if `resolved_ts` updated before `apply_index` update, the
// `safe_ts` won't be updated, hence the leader won't broadcast a wrong
// `(apply_index, safe_ts)` item to other replicas
#[test]
fn test_update_resoved_ts_before_apply_index() {
    let (mut cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    leader_client.ctx.set_stale_read(true);
    follower_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);

    // Return before handling `apply_res`, to stop the leader updating the apply
    // index
    let on_apply_res_fp = "on_apply_res";
    fail::cfg(on_apply_res_fp, "return()").unwrap();
    // Stop replicate data to follower 2
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend),
    ));

    // Write `(key1, value2)`
    let commit_ts2 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
    );

    // Wait `resolved_ts` be updated
    sleep_ms(100);

    // The leader can't handle stale read with `commit_ts2` because its `safe_ts`
    // can't update due to its `apply_index` not update.
    // The request would be handled as a snapshot read on the valid leader peer
    // after fallback.
    let resp = leader_client.kv_read(b"key1".to_vec(), commit_ts2);
    assert_eq!(resp.get_value(), b"value2");
    // The follower can't handle stale read with `commit_ts2` because it don't
    // have enough data
    let resp = follower_client2.kv_read(b"key1".to_vec(), commit_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());

    fail::remove(on_apply_res_fp);
    cluster.clear_send_filters();

    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value2".to_vec(), commit_ts2);
}

// Testing that the new elected leader should initialize the `resolver`
// correctly
#[test]
fn test_new_leader_init_resolver() {
    let (mut cluster, pd_client, mut peer_client1) = prepare_for_stale_read(new_peer(1, 1));
    let mut peer_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    peer_client1.ctx.set_stale_read(true);
    peer_client2.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    let commit_ts1 = peer_client1.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // There are no lock in the region, the `safe_ts` should keep updating by the
    // new leader, so we can read `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(2, 2));
    peer_client1.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Prewrite on `key2` but not commit yet
    peer_client2.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        get_tso(&pd_client),
    );

    // There are locks in the region, the `safe_ts` can't be updated, so we can't
    // read `key1` with the newest ts
    cluster.must_transfer_leader(1, new_peer(1, 1));
    let resp = peer_client2.kv_read(b"key1".to_vec(), get_tso(&pd_client));
    assert!(resp.get_region_error().has_data_is_not_ready());
    // But we can read `key1` with `commit_ts1`
    peer_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), commit_ts1);
}

// Testing that while applying snapshot the follower should reset its `safe_ts`
// to 0 and reject incoming stale read request, then resume the `safe_ts` after
// applying snapshot
#[test]
fn test_stale_read_while_applying_snapshot() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_snapshot)));
    let mut follower_client2 = PeerClient::new(&cluster, 1, new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);

    let k1_commit_ts = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts);

    // Stop replicate data to follower 2
    cluster.add_send_filter(IsolationFilterFactory::new(2));

    // Prewrite on `key3` but not commit yet
    let k2_prewrite_ts = get_tso(&pd_client);
    leader_client.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        k2_prewrite_ts,
    );

    // Compact logs to force requesting snapshot after clearing send filters.
    let gc_limit = cluster.cfg.raft_store.raft_log_gc_count_limit();
    for i in 1..gc_limit * 2 {
        let (k, v) = (
            format!("k{}", i).into_bytes(),
            format!("v{}", i).into_bytes(),
        );
        leader_client.must_kv_write(&pd_client, vec![new_mutation(Op::Put, &k, &v)], k);
    }
    let last_index_on_store_2 = cluster.raft_local_state(1, 2).last_index;
    cluster.wait_log_truncated(1, 1, last_index_on_store_2 + 1);

    // Pasuse before applying snapshot is finish
    let raft_before_applying_snap_finished = "raft_before_applying_snap_finished";
    fail::cfg(raft_before_applying_snap_finished, "pause").unwrap();
    cluster.clear_send_filters();

    // Wait follower 2 start applying snapshot
    cluster.wait_log_truncated(1, 2, last_index_on_store_2 + 1);
    sleep_ms(100);

    // We can't read while applying snapshot and the `safe_ts` should reset to 0
    let resp = follower_client2.kv_read(b"key1".to_vec(), k1_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());
    assert_eq!(
        0,
        resp.get_region_error()
            .get_data_is_not_ready()
            .get_safe_ts()
    );

    // Resume applying snapshot
    fail::remove(raft_before_applying_snap_finished);

    let last_index_on_store_1 = cluster.raft_local_state(1, 1).last_index;
    cluster.wait_last_index(1, 2, last_index_on_store_1, Duration::from_secs(3));

    // We can read `key1` after applied snapshot
    follower_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts);
    // There is still lock on the region, we can't read `key1` with the newest ts
    let resp = follower_client2.kv_read(b"key1".to_vec(), get_tso(&pd_client));
    assert!(resp.get_region_error().has_data_is_not_ready());

    // Commit `key2`
    leader_client.must_kv_commit(vec![b"key2".to_vec()], k2_prewrite_ts, get_tso(&pd_client));
    // We can read `key1` with the newest ts now
    follower_client2.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that after region merged the region's `safe_ts` should reset to
// min(`target_safe_ts`, `source_safe_ts`)
#[test]
fn test_stale_read_while_region_merge() {
    let (mut cluster, pd_client, _) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Write `(key5, value1)`
    target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value1"[..])],
        b"key5".to_vec(),
    );

    let source_leader = cluster.leader_of_region(source.get_id()).unwrap();
    let source_leader = PeerClient::new(&cluster, source.get_id(), source_leader);
    // Prewrite on `key1` but not commit yet
    let k1_prewrite_ts = get_tso(&pd_client);
    source_leader.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
        k1_prewrite_ts,
    );

    // Write `(key5, value2)`
    let k5_commit_ts = target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value2"[..])],
        b"key5".to_vec(),
    );

    // Merge source region into target region, the lock on source region should also
    // merge into the target region and cause the target region's `safe_ts`
    // decrease
    pd_client.must_merge(source.get_id(), target.get_id());

    let mut follower_client2 = PeerClient::new(&cluster, target.get_id(), new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    // We can read `(key5, value1)` with `k1_prewrite_ts`
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value1".to_vec(), k1_prewrite_ts);
    // Can't read `key5` with `k5_commit_ts` because `k1_prewrite_ts` is smaller
    // than `k5_commit_ts`
    let resp = follower_client2.kv_read(b"key5".to_vec(), k5_commit_ts);
    assert!(resp.get_region_error().has_data_is_not_ready());

    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Commit on `key1`
    target_leader.must_kv_commit(vec![b"key1".to_vec()], k1_prewrite_ts, get_tso(&pd_client));
    // We can read `(key5, value2)` now
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value2".to_vec(), get_tso(&pd_client));
}

// Testing that after region merge, the `safe_ts` could be advanced even without
// any incoming write
#[test]
fn test_stale_read_after_merge() {
    let (mut cluster, pd_client, _) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    // Write `(key5, value1)`
    target_leader.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key5"[..], &b"value1"[..])],
        b"key5".to_vec(),
    );

    pd_client.must_merge(source.get_id(), target.get_id());

    let mut follower_client2 = PeerClient::new(&cluster, target.get_id(), new_peer(2, 2));
    follower_client2.ctx.set_stale_read(true);
    // We can read `(key5, value1)` with the newest ts
    follower_client2.must_kv_read_equal(b"key5".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that during the merge, the leader of the source region won't not
// update the `safe_ts` since it can't know when the merge is completed and
// whether there are new kv write into its key range
#[test]
fn test_read_source_region_after_target_region_merged() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    // Write on source region
    let k1_commit_ts1 = leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();
    // Transfer the target region leader to store 1 and the source region leader to
    // store 2
    cluster.must_transfer_leader(target.get_id(), new_peer(1, 1));
    cluster.must_transfer_leader(source.get_id(), find_peer(&source, 2).unwrap().clone());
    // Get the source region follower on store 3
    let mut source_follower_client3 = PeerClient::new(
        &cluster,
        source.get_id(),
        find_peer(&source, 3).unwrap().clone(),
    );
    source_follower_client3.ctx.set_stale_read(true);
    source_follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts1);

    // Pause on source region `prepare_merge` on store 2 and store 3
    let apply_before_prepare_merge_2_3 = "apply_before_prepare_merge_2_3";
    fail::cfg(apply_before_prepare_merge_2_3, "pause").unwrap();

    // Merge source region into target region
    pd_client.must_merge(source.get_id(), target.get_id());

    // Leave a lock on the original source region key range through the target
    // region leader
    let target_leader = PeerClient::new(&cluster, target.get_id(), new_peer(1, 1));
    let k1_prewrite_ts2 = get_tso(&pd_client);
    target_leader.must_kv_prewrite(
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value2"[..])],
        b"key1".to_vec(),
        k1_prewrite_ts2,
    );

    // Wait for the source region leader to update `safe_ts` (if it can)
    sleep_ms(50);

    // We still can read `key1` with `k1_commit_ts1` through source region
    source_follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), k1_commit_ts1);
    // But can't read `key2` with `k1_prewrite_ts2` because the source leader can't
    // update `safe_ts` after source region is merged into target region even
    // though the source leader didn't know the merge is complement
    let resp = source_follower_client3.kv_read(b"key1".to_vec(), k1_prewrite_ts2);
    assert!(resp.get_region_error().has_data_is_not_ready());

    fail::remove(apply_before_prepare_merge_2_3);
}

// Testing that altough the source region's `safe_ts` wont't be updated during
// merge, after merge rollbacked it should resume updating
#[test]
fn test_stale_read_after_rollback_merge() {
    let (mut cluster, pd_client, leader_client) =
        prepare_for_stale_read_before_run(new_peer(1, 1), Some(Box::new(configure_for_merge)));

    // Write on source region
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    cluster.must_split(&cluster.get_region(&[]), b"key3");
    let source = pd_client.get_region(b"key1").unwrap();
    let target = pd_client.get_region(b"key5").unwrap();

    // Trigger merge rollback
    let on_schedule_merge = "on_schedule_merge";
    fail::cfg(on_schedule_merge, "return()").unwrap();
    cluster.must_try_merge(source.get_id(), target.get_id());
    // Change the epoch of target region and the merge will fail
    pd_client.must_remove_peer(target.get_id(), new_peer(3, 3));
    fail::remove(on_schedule_merge);

    // Make sure the rollback is done, it is okey to use raw kv here
    cluster.must_put(b"key2", b"value2");

    let mut source_client3 = PeerClient::new(
        &cluster,
        source.get_id(),
        find_peer(&source, 3).unwrap().clone(),
    );
    source_client3.ctx.set_stale_read(true);
    // the `safe_ts` should resume updating after merge rollback so we can read
    // `key1` with the newest ts
    source_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that the new leader should ignore the pessimistic lock that wrote by
// the previous leader and keep updating the `safe_ts`
#[test]
fn test_new_leader_ignore_pessimistic_lock() {
    let (mut cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));

    // Write (`key1`, `value1`)
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Leave a pessimistic lock on the region
    leader_client.must_kv_pessimistic_lock(b"key2".to_vec(), get_tso(&pd_client));

    // Transfer to a new leader
    cluster.must_transfer_leader(1, new_peer(2, 2));

    let mut follower_client3 = PeerClient::new(&cluster, 1, new_peer(3, 3));
    follower_client3.ctx.set_stale_read(true);
    // The new leader should be able to update `safe_ts` so we can read `key1` with
    // the newest ts
    follower_client3.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that we perform stale read on learner
#[test]
fn test_stale_read_on_learner() {
    let (cluster, pd_client, leader_client) = prepare_for_stale_read(new_peer(1, 1));

    // Write `(key1, value1)`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Replace peer 2 with learner
    pd_client.must_remove_peer(1, new_peer(2, 2));
    pd_client.must_add_peer(1, new_learner_peer(2, 4));
    let mut learner_client2 = PeerClient::new(&cluster, 1, new_learner_peer(2, 4));
    learner_client2.ctx.set_stale_read(true);

    // We can read on the learner with the newst ts
    learner_client2.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}

// Testing that stale read request with a future ts should not update the
// `concurrency_manager`'s `max_ts`
#[test]
fn test_stale_read_future_ts_not_update_max_ts() {
    let (_cluster, pd_client, mut leader_client) = prepare_for_stale_read(new_peer(1, 1));
    leader_client.ctx.set_stale_read(true);

    // Write `(key1, value1)`
    leader_client.must_kv_write(
        &pd_client,
        vec![new_mutation(Op::Put, &b"key1"[..], &b"value1"[..])],
        b"key1".to_vec(),
    );

    // Perform stale read with a future ts, the stale read could be processed
    // falling back to snapshot read on the leader peer.
    let read_ts = get_tso(&pd_client) + 10000000;
    leader_client.must_kv_read_equal(b"key1".to_vec(), b"value1".to_vec(), read_ts);

    // The `max_ts` should not updated by the stale read request, so we can prewrite
    // and commit `async_commit` transaction with a ts that smaller than the
    // `read_ts`
    let prewrite_ts = get_tso(&pd_client);
    assert!(prewrite_ts < read_ts);
    leader_client.must_kv_prewrite_async_commit(
        vec![new_mutation(Op::Put, &b"key2"[..], &b"value1"[..])],
        b"key2".to_vec(),
        prewrite_ts,
    );
    let commit_ts = get_tso(&pd_client);
    assert!(commit_ts < read_ts);
    leader_client.must_kv_commit(vec![b"key2".to_vec()], prewrite_ts, commit_ts);
    leader_client.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), get_tso(&pd_client));

    // Perform stale read with a future ts, the stale read could be processed
    // falling back to snapshot read on the leader peer.
    let read_ts = get_tso(&pd_client) + 10000000;
    leader_client.must_kv_read_equal(b"key2".to_vec(), b"value1".to_vec(), read_ts);

    // The `max_ts` should not updated by the stale read request, so 1pc transaction
    // with a ts that smaller than the `read_ts` should not be fallbacked to 2pc
    let prewrite_ts = get_tso(&pd_client);
    assert!(prewrite_ts < read_ts);
    leader_client.must_kv_prewrite_one_pc(
        vec![new_mutation(Op::Put, &b"key3"[..], &b"value1"[..])],
        b"key3".to_vec(),
        prewrite_ts,
    );
    // `key3` is write as 1pc transaction so we can read `key3` without commit
    leader_client.must_kv_read_equal(b"key3".to_vec(), b"value1".to_vec(), get_tso(&pd_client));
}
