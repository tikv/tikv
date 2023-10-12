// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use engine_traits::{Checkpointer, KvEngine, SyncMutable};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    raft_serverpb::{RaftMessage, RaftSnapshotData},
    tikvpb::TikvClient,
};
use mock_engine_store::{
    interfaces_ffi::BaseBuffView, mock_cluster::v1::server::new_server_cluster,
};
use proxy_ffi::{
    domain_impls::TEST_GC_OBJ_MONITOR,
    ffi_gc_rust_ptr,
    interfaces_ffi::{ColumnFamilyType, EngineIteratorSeekType},
    snapshot_reader_impls::{tablet_reader::TabletReader, *},
};
use raft::eraftpb::Snapshot;
use raftstore::store::{snap::TABLET_SNAPSHOT_VERSION, TabletSnapKey, TabletSnapManager};
use rand::Rng;
use test_raftstore::RawEngine;
use test_raftstore_v2::{Simulator as S2, WrapFactory};
use tikv::server::tablet_snap::send_snap as send_snap_v2;
use tikv_util::time::Limiter;

use super::utils::*;
use crate::utils::{sst::*, v1::*};

fn random_long_vec(length: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut value = Vec::with_capacity(1024);
    (0..length).for_each(|_| value.push(rng.gen::<u8>()));
    value
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

fn prepare_snapshot(
    cluster_v1: &mut Cluster<mock_engine_store::mock_cluster::v1::server::ServerCluster>,
    cluster_v2: &mut test_raftstore_v2::Cluster<
        test_raftstore_v2::ServerCluster<engine_rocks::RocksEngine>,
        engine_rocks::RocksEngine,
    >,
    key_num: usize,
) -> PathBuf {
    let s1_addr = cluster_v1.get_addr(1);
    let region = cluster_v2.get_region(b"");
    let region_id = region.get_id();
    let engine = cluster_v2.get_engine(1);
    let tablet = engine.get_tablet_by_id(region_id).unwrap();

    for i in 0..key_num {
        let k = format!("zk{:06}", i);
        tablet.put(k.as_bytes(), &random_long_vec(1024)).unwrap();
        tablet
            .put_cf(CF_LOCK, k.as_bytes(), &random_long_vec(1024))
            .unwrap();
        tablet
            .put_cf(CF_WRITE, k.as_bytes(), &random_long_vec(1024))
            .unwrap();
    }

    let snap_mgr = cluster_v2.get_snap_mgr(1);
    let security_mgr = cluster_v2.get_security_mgr();
    let (msg, snap_key) = generate_snap(&engine, region_id, &snap_mgr);
    let limit = Limiter::new(f64::INFINITY);
    let env = Arc::new(Environment::new(1));
    let _ = block_on(async {
        let client =
            TikvClient::new(security_mgr.connect(ChannelBuilder::new(env.clone()), &s1_addr));
        send_snap_v2(client, snap_mgr.clone(), msg, limit.clone())
            .await
            .unwrap()
    });

    // The snapshot has been received by cluster v1, so check it's completeness
    let snap_mgr = cluster_v1.get_snap_mgr(1);
    snap_mgr
        .tablet_snap_manager()
        .expect("v1 compact tablet snap mgr")
        .final_recv_path(&snap_key)
}

#[test]
fn test_get_snapshot_seek() {
    let mut cluster_v1 = new_server_cluster(1, 1);
    let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);
    cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.run();
    cluster_v2.run();

    let key_count = 100;
    let path = prepare_snapshot(&mut cluster_v1, &mut cluster_v2, key_count);

    let reader = TabletReader::ffi_get_cf_file_reader(
        path.as_path().to_str().unwrap(),
        ColumnFamilyType::Write,
        None,
    );

    unsafe {
        let k = format!("k{:06}", 99);
        let bf = BaseBuffView {
            data: k.as_ptr() as *const _,
            len: k.len() as u64,
        };
        let cf = ColumnFamilyType::Write;
        ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
        let remained = ffi_sst_reader_remained(reader.clone(), cf);
        if remained == 1 {
            ffi_sst_reader_next(reader.clone(), cf);
            let remained = ffi_sst_reader_remained(reader.clone(), cf);
            if remained == 1 {
                ffi_sst_reader_key(reader.clone(), cf);
            }
        }
    }

    unsafe {
        let k = format!("k{:06}", 100);
        let bf = BaseBuffView {
            data: k.as_ptr() as *const _,
            len: k.len() as u64,
        };
        let cf = ColumnFamilyType::Write;
        ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
        let remained = ffi_sst_reader_remained(reader.clone(), cf);
        assert_eq!(remained, 0);
    }

    unsafe {
        let k = format!("k{:06}", 55);
        let bf = BaseBuffView {
            data: k.as_ptr() as *const _,
            len: k.len() as u64,
        };
        let cf = ColumnFamilyType::Write;
        ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Last, bf);
        let remained = ffi_sst_reader_remained(reader.clone(), cf);
        assert_eq!(remained, 0);
    }

    unsafe {
        let k = format!("k{:06}", 55);
        let bf = BaseBuffView {
            data: k.as_ptr() as *const _,
            len: k.len() as u64,
        };
        let cf = ColumnFamilyType::Write;
        ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::First, bf);
        let remained = ffi_sst_reader_remained(reader.clone(), cf);
        assert_eq!(remained, 1);
        let kbf = ffi_sst_reader_key(reader.clone(), cf);
        assert_eq!(kbf.to_slice(), format!("k{:06}", 0).as_bytes());
    }

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}

#[test]
fn test_get_snapshot_split_keys() {
    let mut cluster_v1 = new_server_cluster(1, 1);
    let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);
    cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.run();
    cluster_v2.run();

    let key_count = 10000;
    let path = prepare_snapshot(&mut cluster_v1, &mut cluster_v2, key_count);

    unsafe {
        let reader = TabletReader::ffi_get_cf_file_reader(
            path.as_path().to_str().unwrap(),
            ColumnFamilyType::Write,
            None,
        );
        // If we want to split the range into 2 parts, it should return 1 split key.
        let split_count = 4;
        let res = ffi_get_split_keys(reader.clone(), split_count);
        let maximum = format!("k{:06}", key_count);
        let minimum = format!("k{:06}", 0);
        tikv_util::debug!("minimum split key is {}", minimum);
        tikv_util::debug!("maximum split key is {}", maximum);
        for i in 0..res.len {
            let buff = res.buffs.add(i as usize);
            let slice = (*buff).to_slice();
            // If the snapshot is too small, it will provide worse and less split keys than
            // we want. For example, given a 10000 key snapshot, it generates:
            // 4-keys is zk000000,zk004065,zk008130,zk009999
            // 3-keys is zk004065,zk008130,zk009999
            // 2-keys is zk004065,zk008130
            tikv_util::debug!(
                "the {}-th split key is {}",
                i,
                std::str::from_utf8_unchecked(slice)
            );
            // We don't return the boundary.
            assert!(slice > minimum.as_bytes());
            assert!(slice < maximum.as_bytes());
        }
        assert_eq!(res.len, split_count - 1);
        ffi_gc_rust_ptr(res.inner.ptr, res.inner.type_);
    }
    assert!(TEST_GC_OBJ_MONITOR.valid_clean_rust());

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}

#[test]
fn test_parse_tablet_snapshot() {
    let test_parse_snap = |key_num| {
        let mut cluster_v1 = new_server_cluster(1, 1);
        let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);
        cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;
        cluster_v1.run();
        cluster_v2.run();

        let path = prepare_snapshot(&mut cluster_v1, &mut cluster_v2, key_num);

        let validate = |cf: ColumnFamilyType| unsafe {
            let reader =
                TabletReader::ffi_get_cf_file_reader(path.as_path().to_str().unwrap(), cf, None);

            // SSTReaderPtr is not aware of the data prefix 'z'.
            let k = format!("k{:06}", 5);
            let bf = BaseBuffView {
                data: k.as_ptr() as *const _,
                len: k.len() as u64,
            };
            ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
            for i in 5..key_num {
                let k = format!("k{:06}", i);
                assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 1);
                let kbf = ffi_sst_reader_key(reader.clone(), cf);
                assert_eq!(kbf.to_slice(), k.as_bytes());
                ffi_sst_reader_next(reader.clone(), cf);
            }
            assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 0);

            // If the sst is "empty" to this region. Will not panic, and remained should be
            // false.
            let k = format!("k{:06}", key_num + 10);
            let bf = BaseBuffView {
                data: k.as_ptr() as *const _,
                len: k.len() as u64,
            };
            ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
            assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 0);
        };
        validate(ColumnFamilyType::Default);
        validate(ColumnFamilyType::Write);
        validate(ColumnFamilyType::Lock);
        cluster_v1.shutdown();
        cluster_v2.shutdown();
    };

    test_parse_snap(10);
}

// This test won't run, since we don;t have transport for snapshot data.
// #[test]
fn test_handle_snapshot() {
    let mut cluster_v2 = test_raftstore_v2::new_node_cluster(1, 2);
    let (mut cluster_v1, _) = new_mock_cluster(1, 2);

    cluster_v1.cfg.tikv.raft_store.enable_v2_compatible_learner = true;

    cluster_v1.pd_client.disable_default_operator();
    cluster_v2.pd_client.disable_default_operator();
    let r11 = cluster_v1.run_conf_change();
    let r21 = cluster_v2.run_conf_change();

    let trans1 = Mutex::new(cluster_v1.sim.read().unwrap().get_router(2).unwrap());
    let trans2 = Mutex::new(cluster_v2.sim.read().unwrap().get_router(1).unwrap());

    let filter11 = Box::new(
        RegionPacketFilter::new(r11, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppend)
            .msg_type(MessageType::MsgAppendResponse)
            .msg_type(MessageType::MsgSnapshot),
    );
    cluster_v1.add_recv_filter_on_node(2, filter11);

    cluster_v2.must_put(b"k1", b"v1");
    cluster_v1
        .pd_client
        .must_add_peer(r11, new_learner_peer(2, 10));
    cluster_v2
        .pd_client
        .must_add_peer(r21, new_learner_peer(2, 10));

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

    check_key(&cluster_v1, b"k1", b"v1", None, Some(true), Some(vec![2]));

    cluster_v1.shutdown();
    cluster_v2.shutdown();
}

#[test]
fn test_v1_apply_snap_from_v2() {
    tikv_util::set_panic_hook(true, "./");
    let mut cluster_v1 = new_server_cluster(1, 1);
    let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);
    cluster_v1.cfg.raft_store.enable_v2_compatible_learner = true;
    cluster_v1.cfg.raft_store.snap_mgr_gc_tick_interval = ReadableDuration::millis(200);

    cluster_v1.run();
    cluster_v2.run();

    let region = cluster_v2.get_region(b"");
    cluster_v2.must_split(&region, b"k0010");

    let s1_addr = cluster_v1.get_addr(1);
    let region_id = region.get_id();
    let engine = cluster_v2.get_engine(1);

    for i in 0..50 {
        let k = format!("k{:04}", i);
        cluster_v2.must_put(k.as_bytes(), b"val");
    }
    cluster_v2.flush_data();

    let tablet_snap_mgr = cluster_v2.get_snap_mgr(1);
    let security_mgr = cluster_v2.get_security_mgr();
    let limit = Limiter::new(f64::INFINITY);
    let env = Arc::new(Environment::new(1));

    let (msg, snap_key) = generate_snap(&engine, region_id, &tablet_snap_mgr);
    let _ = block_on(async {
        let client =
            TikvClient::new(security_mgr.connect(ChannelBuilder::new(env.clone()), &s1_addr));
        send_snap_v2(client, tablet_snap_mgr.clone(), msg, limit.clone())
            .await
            .unwrap()
    });

    let snap_mgr = cluster_v1.get_snap_mgr(region_id);
    let path = snap_mgr
        .tablet_snap_manager()
        .as_ref()
        .unwrap()
        .final_recv_path(&snap_key);
    let path_str = path.as_path().to_str().unwrap();

    for i in 11..50 {
        let k = format!("k{:04}", i);
        check_key(
            &cluster_v1,
            k.as_bytes(),
            b"val",
            None,
            Some(true),
            Some(vec![1]),
        );
    }

    // Verify that the tablet snap will be gced
    for _ in 0..10 {
        if !path.exists() {
            cluster_v1.shutdown();
            cluster_v2.shutdown();
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    panic!("tablet snap {:?} still exists", path_str);
}
