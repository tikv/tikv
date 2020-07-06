// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use grpcio::{ChannelBuilder, Environment};
use kvproto::{kvrpcpb::*, metapb, tikvpb::TikvClient};
use test_raftstore::*;
use tikv_util::{collections::HashMap, HandyRwLock};

use std::sync::Arc;

#[test]
fn test_physical_scan_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Generate kvs like k10, v10, ts=10; k11, v11, ts=11; ...
    let kv: Vec<_> = (10..20)
        .map(|i| (i, vec![b'k', i as u8], vec![b'v', i as u8]))
        .collect();

    for (ts, k, v) in &kv {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(k.clone());
        mutation.set_value(v.clone());
        must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), *ts);
    }

    let all_locks: Vec<_> = kv
        .into_iter()
        .map(|(ts, k, _)| {
            // Create a LockInfo that matches the prewrite request in `must_kv_prewrite`.
            let mut lock_info = LockInfo::default();
            lock_info.set_primary_lock(k.clone());
            lock_info.set_lock_version(ts);
            lock_info.set_key(k);
            lock_info.set_lock_ttl(3000);
            lock_info.set_lock_type(Op::Put);
            lock_info.set_min_commit_ts(ts + 1);
            lock_info
        })
        .collect();

    let check_result = |got_locks: &[_], expected_locks: &[_]| {
        for i in 0..std::cmp::max(got_locks.len(), expected_locks.len()) {
            assert_eq!(got_locks[i], expected_locks[i], "lock {} mismatch", i);
        }
    };

    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 30, b"", 100),
        &all_locks,
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 15, b"", 100),
        &all_locks[0..=5],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 10, b"", 100),
        &all_locks[0..1],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx.clone(), 9, b"", 100),
        &[],
    );
    check_result(
        &must_physical_scan_lock(&client, ctx, 30, &[b'k', 13], 5),
        &all_locks[3..8],
    );
}

#[test]
fn test_applied_lock_collector() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    // Create all stores' clients.
    let env = Arc::new(Environment::new(1));
    let mut clients = HashMap::default();
    for node_id in cluster.get_node_ids() {
        let channel =
            ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(node_id));
        let client = TikvClient::new(channel);
        clients.insert(node_id, client);
    }

    // Create the ctx of the first region.
    let region = cluster.get_region(b"");
    let region_id = region.get_id();
    let leader_peer = cluster.leader_of_region(region_id).unwrap();
    let leader_store_id = leader_peer.get_store_id();
    let leader_client = clients.get(&leader_store_id).unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader_peer.clone());
    ctx.set_region_epoch(cluster.get_region_epoch(region_id));

    let mut safe_point = 10000;
    // Write 100 locks whose timestamp is less than the safe point.
    let mutations = (0..100)
        .map(|i| new_mutation(Op::Put, format!("key{}", i).as_bytes(), b"v"))
        .collect();
    must_kv_prewrite(&leader_client, ctx.clone(), mutations, b"key0".to_vec(), 10);
    // Make sure all stores applies all logs.
    let wait_for_apply = |cluster: &mut Cluster<_>, region: &metapb::Region| {
        let cluster = &mut *cluster;
        region.get_peers().iter().for_each(|p| {
            let resp = async_read_on_peer(cluster, p.clone(), region.clone(), b"key", true, true)
                .recv()
                .unwrap();
            assert!(!resp.get_header().has_error(), "{:?}", resp);
        });
    };
    wait_for_apply(&mut cluster, &region);

    // Register lock observer at safe point 10000
    clients.iter().for_each(|(_, c)| {
        must_register_lock_observer(c, safe_point);
        assert!(must_check_lock_observer(c, safe_point, true).is_empty());
    });

    // Lock observer shouldn't collect locks after the safe point.
    let key = b"key100";
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, key, b"v")],
        key.to_vec(),
        safe_point + 1,
    );
    wait_for_apply(&mut cluster, &region);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_observer(c, safe_point, true);
        assert!(locks.is_empty(), "{:?}", locks);
    });

    // Lock observer should collect locks before the safe point.
    let key = b"key101";
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, key, b"v")],
        key.to_vec(),
        safe_point - 1,
    );
    wait_for_apply(&mut cluster, &region);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_observer(c, safe_point, true);
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].get_key(), key);
        assert_eq!(locks[0].get_primary_lock(), key);
        assert_eq!(locks[0].get_lock_version(), safe_point - 1);
    });

    // Register lock observer at a stale safe point. It shouldn't affect the observer.
    clients.iter().for_each(|(_, c)| {
        must_register_lock_observer(c, safe_point - 1);
    });
    let key = b"key102";
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, key, b"v")],
        key.to_vec(),
        safe_point,
    );
    wait_for_apply(&mut cluster, &region);
    clients.iter().for_each(|(_, c)| {
        let locks = must_check_lock_observer(c, safe_point, true);
        assert_eq!(locks.len(), 2);
    });

    // Add a new store.
    let store_id = cluster.add_new_engine();
    let channel =
        ChannelBuilder::new(Arc::clone(&env)).connect(cluster.sim.rl().get_addr(store_id));
    let client = TikvClient::new(channel);
    let resp = check_lock_observer(&client, safe_point);
    assert!(!resp.get_error().is_empty());
    must_register_lock_observer(&client, safe_point);

    // Add a new peer. Lock observer should collect all locks.
    let peer = new_peer(store_id, store_id);
    cluster.pd_client.must_add_peer(region_id, peer.clone());
    cluster.pd_client.must_none_pending_peer(peer);
    wait_for_apply(&mut cluster, &region);
    let locks = must_check_lock_observer(&client, safe_point, true);
    assert_eq!(locks.len(), 102);

    // Should be dirty when collects too many locks.
    let mutations = (1024..2048)
        .map(|i| new_mutation(Op::Put, format!("key{}", i).as_bytes(), b"v"))
        .collect();
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        mutations,
        b"key1000".to_vec(),
        10,
    );
    wait_for_apply(&mut cluster, &region);
    clients.insert(store_id, client);
    clients.iter().for_each(|(_, c)| {
        let resp = check_lock_observer(c, safe_point);
        assert!(resp.get_error().is_empty(), "{:?}", resp.get_error());
        assert!(!resp.get_is_clean());
        // MAX_COLLECT_SIZE is 1024.
        assert_eq!(resp.get_locks().len(), 1024);
    });

    // Register lock observer at a later safe point. Lock observer should reset its state.
    safe_point += 1;
    clients.iter().for_each(|(_, c)| {
        must_register_lock_observer(c, safe_point);
        assert!(must_check_lock_observer(c, safe_point, true).is_empty());
    });
    let leader_client = clients.get(&leader_store_id).unwrap();
    must_kv_prewrite(
        &leader_client,
        ctx.clone(),
        vec![new_mutation(Op::Put, b"key103", b"v")],
        b"key103".to_vec(),
        safe_point,
    );
    wait_for_apply(&mut cluster, &region);
    clients.iter().for_each(|(_, c)| {
        // Should collect locks according to the new max ts.
        let locks = must_check_lock_observer(c, safe_point, true);
        assert_eq!(locks.len(), 1, "{:?}", locks);
        // Should remove it with a wrong max ts.
        let resp = remove_lock_observer(c, safe_point - 1);
        assert!(!resp.get_error().is_empty());
        let locks = must_check_lock_observer(c, safe_point, true);
        assert_eq!(locks.len(), 1, "{:?}", locks);
        must_remove_lock_observer(c, safe_point);
        let resp = check_lock_observer(c, safe_point);
        assert!(!resp.get_error().is_empty());
    });
}
