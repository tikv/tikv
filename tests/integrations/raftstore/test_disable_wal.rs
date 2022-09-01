// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{Iterable, MiscExt, SeqnoPropertiesExt, CF_WRITE, DATA_CFS};
use grpcio::{ChannelBuilder, Environment};
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use kvproto::{
    kvrpcpb::{Context, Op},
    tikvpb::TikvClient,
};
use test_raftstore::*;
use tikv_util::HandyRwLock;

#[test]
fn test_disable_wal_recovery() {
    let mut cluster = new_server_cluster(0, 1);
    // Initialize the cluster.
    cluster.pd_client.disable_default_operator();
    cluster.cfg.raft_store.disable_kv_wal = true;
    cluster.run();
    let region = cluster.get_region(b"");
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
    cluster.get_engine(1).flush_cfs(true).unwrap();
    for cf in DATA_CFS {
        println!(
            "{} get prop after flush: {:?}",
            cf,
            cluster
                .get_engine(1)
                .get_range_seqno_properties_cf(cf, DATA_MIN_KEY, DATA_MAX_KEY)
        );
    }

    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"k3", b"v3")],
        b"k3".to_vec(),
        50,
    );
    cluster.stop_node(1);
    for cf in DATA_CFS {
        println!(
            "{} get prop after stop: {:?}",
            cf,
            cluster
                .get_engine(1)
                .get_range_seqno_properties_cf(cf, DATA_MIN_KEY, DATA_MAX_KEY)
        );
    }
    cluster
        .get_engine(1)
        .scan(CF_WRITE, DATA_MIN_KEY, DATA_MAX_KEY, false, |k, v| {
            println!("key {:?} value {:?}", k, v);
            Ok(true)
        })
        .unwrap();
    cluster.run_node(1).unwrap();
    must_kv_read_equal(&client, ctx.clone(), b"k1".to_vec(), b"v1".to_vec(), 60);
    must_kv_read_equal(&client, ctx.clone(), b"k2".to_vec(), b"v2".to_vec(), 60);
}
