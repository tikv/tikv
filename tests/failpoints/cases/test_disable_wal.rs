// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{MiscExt, CF_LOCK};
use grpcio::{ChannelBuilder, Environment};
use keys::DATA_PREFIX_KEY;
use kvproto::{
    kvrpcpb::{Context, Op},
    tikvpb::TikvClient,
};
use test_raftstore::*;
use tikv_util::{keybuilder::KeyBuilder, HandyRwLock};
use txn_types::Key;

#[test]
fn test_disable_wal_recovery_basic() {
    let mut cluster = new_server_cluster(0, 1);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    let region = cluster.get_region(b"");
    let leader = region.get_peers().iter().find(|p| p.store_id == 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), leader.clone());
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(Arc::clone(&env)).connect(&cluster.sim.rl().get_addr(1));
    let client = TikvClient::new(channel);
    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_peer(region.get_peers()[0].clone());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k1", b"v1")],
        b"k1".to_vec(),
        10,
    );
    must_kv_commit(&client, ctx.clone(), vec![b"k1".to_vec()], 10, 20, 20);
    cluster.get_engine(1).flush_cfs(true).unwrap();
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k2", b"v2")],
        b"k2".to_vec(),
        30,
    );
    must_kv_commit(&client, ctx.clone(), vec![b"k2".to_vec()], 30, 40, 40);

    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    must_kv_read_equal(&client, ctx.clone(), b"k1".to_vec(), b"v1".to_vec(), 50);
    must_kv_read_equal(&client, ctx.clone(), b"k2".to_vec(), b"v2".to_vec(), 50);

    cluster.must_split(&region, b"k3");
    let region = cluster.get_region(b"k3");
    let leader = region.get_peers().iter().find(|p| p.store_id == 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), leader.clone());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k4", b"v4")],
        b"k4".to_vec(),
        60,
    );
    cluster
        .pd_client
        .must_merge(cluster.get_region_id(b"k1"), cluster.get_region_id(b"k3"));
    cluster.stop_node(1);
    let key = Key::from_raw(b"k4").append_ts(60.into());
    let mut key_builder = KeyBuilder::from_slice(key.as_encoded(), 1, 0);
    key_builder.set_prefix(DATA_PREFIX_KEY);
    must_get_cf_none(&cluster.get_engine(1), CF_LOCK, key_builder.as_slice());
    cluster.run_node(1).unwrap();
    let region = cluster.get_region(b"k3");
    let leader = region.get_peers().iter().find(|p| p.store_id == 1).unwrap();
    cluster.must_transfer_leader(region.get_id(), leader.clone());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    must_kv_commit(&client, ctx.clone(), vec![b"k4".to_vec()], 60, 70, 70);
    cluster.must_split(&region, b"k3");
    let schedule_merge_fp = "on_schedule_merge";
    fail::cfg(schedule_merge_fp, "return()").unwrap();
    cluster.must_try_merge(cluster.get_region_id(b"k1"), cluster.get_region_id(b"k3"));
    cluster.get_engine(1).flush_cfs(true).unwrap();
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
    fail::remove(schedule_merge_fp);
    // split to trigger rollback.
    cluster.must_split(&cluster.get_region(b"k3"), b"k4");
    cluster.must_put(b"k1", b"v1");
    cluster.stop_node(1);
    cluster.run_node(1).unwrap();
}
