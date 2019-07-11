use std::sync::Arc;

use fail;
use kvproto::kvrpcpb::Context;
use test_raftstore::*;
use tikv::storage::kv::*;
use tikv::storage::Key;
use tikv_util::HandyRwLock;

#[test]
fn test_wait_for_apply_index() {
    let mut cluster = new_server_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k0", b"v0");
    let p2 = new_peer(2, 2);
    cluster.pd_client.must_add_peer(r1, p2.clone());
    let p3 = new_peer(3, 3);
    cluster.pd_client.must_add_peer(r1, p3.clone());
    must_get_equal(&cluster.get_engine(3), b"k0", b"v0");

    let r1 = cluster.get_region(b"k0");
    cluster.must_transfer_leader(r1.get_id(), p2.clone());

    let mut ctx = Context::new();
    ctx.set_region_id(r1.get_id());
    ctx.set_region_epoch(r1.get_region_epoch().clone());
    ctx.set_follower_read(true);

    fail::cfg("on_raft_ready", "pause").unwrap();
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    cluster.must_put(b"k4", b"v4");
    must_get_equal(&cluster.get_engine(2), b"k4", b"v4");
    fail::cfg("on_raft_ready", "off").unwrap();
    // Peer 3 does not apply the value of 'k4' right now
    let p3_storage = cluster.sim.rl().storages[&p3.get_id()].clone();
    ctx.set_peer(p3.clone());
    must_get_none(&cluster.get_engine(3), b"k4");
    assert_has(&ctx, &p3_storage, b"k4", b"v4");
}

fn assert_has<E: Engine>(ctx: &Context, engine: &E, key: &[u8], value: &[u8]) {
    let snapshot = engine.snapshot(ctx).unwrap();
    assert_eq!(
        snapshot
            .get(&Key::from_encoded_slice(key))
            .unwrap()
            .unwrap(),
        value
    );
}
