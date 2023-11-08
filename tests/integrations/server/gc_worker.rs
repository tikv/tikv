// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{Peekable, CF_WRITE};
use grpcio::{ChannelBuilder, Environment};
use keys::data_key;
use kvproto::{kvrpcpb::*, tikvpb::TikvClient};
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::server::gc_worker::sync_gc;
use tikv_util::HandyRwLock;
use txn_types::Key;

// Since v5.0 GC bypasses Raft, which means GC scans/deletes records with
// `keys::DATA_PREFIX`. This case ensures it's performed correctly.
#[test_case(test_raftstore::must_new_cluster_mul)]
#[test_case(test_raftstore_v2::must_new_cluster_mul)]
fn test_gc_bypass_raft() {
    let (cluster, leader, ctx) = new_cluster(2);
    cluster.pd_client.disable_default_operator();

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    let pk = b"k1".to_vec();
    let value = vec![b'x'; 300];
    let engine = cluster.get_engine(leader_store);

    for &start_ts in &[10, 20, 30, 40] {
        let commit_ts = start_ts + 5;
        let muts = vec![new_mutation(Op::Put, b"k1", &value)];

        must_kv_prewrite(&client, ctx.clone(), muts, pk.clone(), start_ts);
        let keys = vec![pk.clone()];
        must_kv_commit(&client, ctx.clone(), keys, start_ts, commit_ts, commit_ts);

        let key = Key::from_raw(b"k1").append_ts(start_ts.into());
        let key = data_key(key.as_encoded());
        assert!(engine.get_value(&key).unwrap().is_some());

        let key = Key::from_raw(b"k1").append_ts(commit_ts.into());
        let key = data_key(key.as_encoded());
        assert!(engine.get_value_cf(CF_WRITE, &key).unwrap().is_some());
    }

    let node_ids = cluster.get_node_ids();
    for store_id in node_ids {
        let gc_sched = cluster.sim.rl().get_gc_worker(store_id).scheduler();

        let mut region = cluster.get_region(b"a");
        region.set_start_key(b"k1".to_vec());
        region.set_end_key(b"k2".to_vec());
        sync_gc(&gc_sched, region, 200.into()).unwrap();

        let engine = cluster.get_engine(store_id);
        for &start_ts in &[10, 20, 30] {
            let commit_ts = start_ts + 5;
            let key = Key::from_raw(b"k1").append_ts(start_ts.into());
            let key = data_key(key.as_encoded());
            assert!(engine.get_value(&key).unwrap().is_none());

            let key = Key::from_raw(b"k1").append_ts(commit_ts.into());
            let key = data_key(key.as_encoded());
            assert!(engine.get_value_cf(CF_WRITE, &key).unwrap().is_none());
        }
    }
}
