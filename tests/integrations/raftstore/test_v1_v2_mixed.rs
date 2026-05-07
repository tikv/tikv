// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use engine_rocks::{RocksCfOptions, RocksDbOptions};
use engine_traits::{Checkpointer, KvEngine, Peekable, SyncMutable, LARGE_CFS};
use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    raft_serverpb::{RaftMessage, *},
    tikvpb::TikvClient,
};
use raft::eraftpb::{MessageType, Snapshot};
use raftstore::{
    errors::Result,
    store::{snap::TABLET_SNAPSHOT_VERSION, TabletSnapKey, TabletSnapManager},
};
use rand::Rng;
use test_raftstore::{
    new_learner_peer, Direction, Filter, FilterFactory, RegionPacketFilter, Simulator as S1, *,
};
use test_raftstore_v2::{Simulator as S2, WrapFactory};
use tikv::server::tablet_snap::send_snap as send_snap_v2;
use tikv_util::time::Limiter;

struct ForwardFactory {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl FilterFactory for ForwardFactory {
    fn generate(&self, _: u64) -> Vec<Box<dyn Filter>> {
        vec![Box::new(ForwardFilter {
            node_id: self.node_id,
            chain_send: self.chain_send.clone(),
        })]
    }
}

struct ForwardFilter {
    node_id: u64,
    chain_send: Arc<dyn Fn(RaftMessage) + Send + Sync + 'static>,
}

impl Filter for ForwardFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        for m in msgs.drain(..) {
            if self.node_id == m.get_to_peer().get_store_id() {
                (self.chain_send)(m);
            }
        }
        Ok(())
    }
}

fn generate_snap<EK: KvEngine>(
    engine: &WrapFactory<EK>,
    region_id: u64,
    snap_mgr: &TabletSnapManager,
) -> (RaftMessage, TabletSnapKey) {
    let tablet = engine.get_tablet_by_id(region_id).unwrap();
    let region_state = engine.region_local_state(region_id).unwrap().unwrap();
    let apply_state = engine.raft_apply_state(region_id).unwrap().unwrap();
    let raft_state = engine.raft_local_state(region_id).unwrap().unwrap();

    // Construct snapshot by hand
    let mut snapshot = Snapshot::default();
    // use commit term for simplicity
    snapshot
        .mut_metadata()
        .set_term(raft_state.get_hard_state().term + 1);
    snapshot.mut_metadata().set_index(apply_state.applied_index);
    let conf_state = raftstore::store::util::conf_state_from_region(region_state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(region_state.get_region().clone());
    snap_data.set_version(TABLET_SNAPSHOT_VERSION);
    use protobuf::Message;
    snapshot.set_data(snap_data.write_to_bytes().unwrap().into());
    let snap_key = TabletSnapKey::from_region_snap(region_id, 1, &snapshot);
    let checkpointer_path = snap_mgr.tablet_gen_path(&snap_key);
    let mut checkpointer = tablet.new_checkpointer().unwrap();
    checkpointer
        .create_at(checkpointer_path.as_path(), None, 0)
        .unwrap();

    let mut msg = RaftMessage::default();
    msg.region_id = region_id;
    msg.set_to_peer(new_peer(1, 1));
    msg.mut_message().set_snapshot(snapshot);
    msg.mut_message()
        .set_term(raft_state.get_hard_state().commit + 1);
    msg.mut_message().set_msg_type(MessageType::MsgSnapshot);
    msg.set_region_epoch(region_state.get_region().get_region_epoch().clone());

    (msg, snap_key)
}

fn random_long_vec(length: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut value = Vec::with_capacity(1024);
    (0..length).for_each(|_| value.push(rng.gen::<u8>()));
    value
}

#[test]
fn test_v1_receive_snap_from_v2() {
    let test_receive_snap = |key_num| {
        let mut cluster_v1 = test_raftstore::new_server_cluster(1, 1);
        let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);

        cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;

        cluster_v1.run();
        cluster_v2.run();

        let s1_addr = cluster_v1.get_addr(1);
        let region = cluster_v2.get_region(b"");
        let region_id = region.get_id();
        let engine = cluster_v2.get_engine(1);
        let tablet = engine.get_tablet_by_id(region_id).unwrap();

        for i in 0..key_num {
            let k = format!("zk{:04}", i);
            tablet.put(k.as_bytes(), &random_long_vec(1024)).unwrap();
        }

        let snap_mgr = cluster_v2.get_snap_mgr(1);
        let security_mgr = cluster_v2.get_security_mgr();
        let (msg, snap_key) = generate_snap(&engine, region_id, &snap_mgr);
        let limit = Limiter::new(f64::INFINITY);
        let env = Arc::new(Environment::new(1));
        let _ = block_on(async {
            let client =
                TikvClient::new(security_mgr.connect(ChannelBuilder::new(env.clone()), &s1_addr));
            send_snap_v2(client, snap_mgr.clone(), msg.clone(), limit.clone())
                .await
                .unwrap()
        });

        // The snapshot has been received by cluster v1, so check it's completeness
        let snap_mgr = cluster_v1.get_snap_mgr(1);
        let path = snap_mgr
            .tablet_snap_manager()
            .unwrap()
            .final_recv_path(&snap_key);
        let rocksdb = engine_rocks::util::new_engine_opt(
            path.as_path().to_str().unwrap(),
            RocksDbOptions::default(),
            LARGE_CFS
                .iter()
                .map(|&cf| (cf, RocksCfOptions::default()))
                .collect(),
        )
        .unwrap();

        for i in 0..key_num {
            let k = format!("zk{:04}", i);
            assert!(
                rocksdb
                    .get_value_cf("default", k.as_bytes())
                    .unwrap()
                    .is_some()
            );
        }
    };

    // test small snapshot
    test_receive_snap(20);

    // test large snapshot
    test_receive_snap(5000);
}

#[test]
fn test_v1_simple_write() {
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(1, 2);
    let mut cluster_v1 = test_raftstore::new_node_cluster(1, 2);
    cluster_v1.cfg.tikv.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.pd_client.disable_default_operator();
    cluster_v2.pd_client.disable_default_operator();
    let r11 = cluster_v1.run_conf_change();
    let r21 = cluster_v2.run_conf_change();

    cluster_v1.must_put(b"k0", b"v0");
    cluster_v2.must_put(b"k0", b"v0");
    cluster_v1
        .pd_client
        .must_add_peer(r11, new_learner_peer(2, 10));
    cluster_v2
        .pd_client
        .must_add_peer(r21, new_learner_peer(2, 10));
    check_key_in_engine(&cluster_v1.get_engine(2), b"zk0", b"v0");
    check_key_in_engine(&cluster_v2.get_engine(2), b"zk0", b"v0");
    let trans1 = Mutex::new(cluster_v1.sim.read().unwrap().get_router(2).unwrap());
    let trans2 = Mutex::new(cluster_v2.sim.read().unwrap().get_router(1).unwrap());

    let factory1 = ForwardFactory {
        node_id: 1,
        chain_send: Arc::new(move |m| {
            info!("send to trans2"; "msg" => ?m);
            let _ = trans2.lock().unwrap().send_raft_message(Box::new(m));
        }),
    };
    cluster_v1.add_send_filter(factory1);
    let factory2 = ForwardFactory {
        node_id: 2,
        chain_send: Arc::new(move |m| {
            info!("send to trans1"; "msg" => ?m);
            let _ = trans1.lock().unwrap().send_raft_message(m);
        }),
    };
    cluster_v2.add_send_filter(factory2);
    let filter11 = Box::new(
        RegionPacketFilter::new(r11, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .msg_type(MessageType::MsgAppendResponse)
            .msg_type(MessageType::MsgSnapshot)
            .msg_type(MessageType::MsgHeartbeat)
            .msg_type(MessageType::MsgHeartbeatResponse),
    );
    cluster_v1.add_recv_filter_on_node(2, filter11);

    cluster_v2.must_put(b"k1", b"v1");
    assert_eq!(
        cluster_v2.must_get(b"k1").unwrap(),
        "v1".as_bytes().to_vec()
    );
    check_key_in_engine(&cluster_v1.get_engine(2), b"zk1", b"v1");

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}

fn check_key_in_engine<T: Peekable>(engine: &T, key: &[u8], value: &[u8]) {
    for _ in 0..10 {
        if let Ok(Some(vec)) = engine.get_value(key) {
            assert_eq!(vec.to_vec(), value.to_vec());
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    panic!("cannot find key {:?} in engine", key);
}
