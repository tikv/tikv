// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{Arc, mpsc::channel},
    thread,
    time::Duration,
};

use kvproto::metapb::Region;
use pd_client::PdClient;
use raft::StateRole;
use raftstore::coprocessor::{RangeKey, RegionInfo, RegionInfoAccessor};
use test_raftstore::{Cluster, NodeCluster, configure_for_merge, new_node_cluster};
use test_raftstore_macro::test_case;
use tikv_kv::Engine;
use tikv_util::{
    HandyRwLock,
    store::{find_peer, new_peer},
};

fn dump(c: &RegionInfoAccessor) -> Vec<(Region, StateRole)> {
    let (regions, region_ranges) = c.debug_dump();

    assert_eq!(regions.len(), region_ranges.len());

    let mut res = Vec::new();
    for (end_key, id) in region_ranges {
        let RegionInfo {
            ref region, role, ..
        } = regions[&id];
        assert_eq!(
            end_key,
            RangeKey::from_end_key(region.get_end_key().to_vec())
        );
        assert_eq!(id, region.get_id());
        res.push((region.clone(), role));
    }

    res
}

fn check_region_ranges(regions: &[(Region, StateRole)], ranges: &[(&[u8], &[u8])]) {
    assert_eq!(regions.len(), ranges.len());
    regions
        .iter()
        .zip(ranges.iter())
        .for_each(|((r, _), (start_key, end_key))| {
            assert_eq!(r.get_start_key(), *start_key);
            assert_eq!(r.get_end_key(), *end_key);
        })
}

fn test_region_info_accessor_impl(cluster: &mut Cluster<NodeCluster>, c: &RegionInfoAccessor) {
    for i in 0..9 {
        let k = format!("k{}", i).into_bytes();
        let v = format!("v{}", i).into_bytes();
        cluster.must_put(&k, &v);
    }

    let pd_client = Arc::clone(&cluster.pd_client);

    let init_regions = dump(c);
    check_region_ranges(&init_regions, &[(&b""[..], &b""[..])]);
    assert_eq!(init_regions[0].0, cluster.get_region(b"k1"));

    // Split
    {
        let r1 = cluster.get_region(b"k1");
        cluster.must_split(&r1, b"k1");
        let r2 = cluster.get_region(b"k4");
        cluster.must_split(&r2, b"k4");
        let r3 = cluster.get_region(b"k2");
        cluster.must_split(&r3, b"k2");
        let r4 = cluster.get_region(b"k3");
        cluster.must_split(&r4, b"k3");
    }

    let split_regions = dump(c);
    check_region_ranges(
        &split_regions,
        &[
            (&b""[..], &b"k1"[..]),
            (b"k1", b"k2"),
            (b"k2", b"k3"),
            (b"k3", b"k4"),
            (b"k4", b""),
        ],
    );
    for (ref region, _) in &split_regions {
        if region.get_id() == init_regions[0].0.get_id() {
            assert_ne!(
                region.get_region_epoch(),
                init_regions[0].0.get_region_epoch()
            );
        }
    }

    // Merge from left to right
    pd_client.must_merge(split_regions[1].0.get_id(), split_regions[2].0.get_id());
    let merge_regions = dump(c);
    check_region_ranges(
        &merge_regions,
        &[
            (&b""[..], &b"k1"[..]),
            (b"k1", b"k3"),
            (b"k3", b"k4"),
            (b"k4", b""),
        ],
    );

    // Merge from right to left
    pd_client.must_merge(merge_regions[2].0.get_id(), merge_regions[1].0.get_id());
    let mut merge_regions_2 = dump(c);
    check_region_ranges(
        &merge_regions_2,
        &[(&b""[..], &b"k1"[..]), (b"k1", b"k4"), (b"k4", b"")],
    );

    // Add peer
    let (region1, role1) = merge_regions_2.remove(1);
    assert_eq!(role1, StateRole::Leader);
    assert_eq!(region1.get_peers().len(), 1);
    assert_eq!(region1.get_peers()[0].get_store_id(), 1);

    pd_client.must_add_peer(region1.get_id(), new_peer(2, 100));
    let (region2, role2) = dump(c).remove(1);
    assert_eq!(role2, StateRole::Leader);
    assert_eq!(region2.get_peers().len(), 2);
    assert!(find_peer(&region2, 1).is_some());
    assert!(find_peer(&region2, 2).is_some());

    // Change leader
    pd_client.transfer_leader(
        region2.get_id(),
        find_peer(&region2, 2).unwrap().clone(),
        vec![],
    );
    let mut region3 = Region::default();
    let mut role3 = StateRole::default();
    // Wait for transfer leader finish
    for _ in 0..100 {
        let r = dump(c).remove(1);
        region3 = r.0;
        role3 = r.1;
        if role3 == StateRole::Follower {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(role3, StateRole::Follower);

    // Remove peer
    check_region_ranges(
        &dump(c),
        &[(&b""[..], &b"k1"[..]), (b"k1", b"k4"), (b"k4", b"")],
    );

    pd_client.must_remove_peer(region3.get_id(), find_peer(&region3, 1).unwrap().clone());

    let mut regions_after_removing = Vec::new();
    // It seems region_info_accessor is a little delayed than raftstore...
    for _ in 0..100 {
        regions_after_removing = dump(c);
        if regions_after_removing.len() == 2 {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    check_region_ranges(
        &regions_after_removing,
        &[(&b""[..], &b"k1"[..]), (b"k4", b"")],
    );
}

#[test]
fn test_node_cluster_region_info_accessor() {
    let mut cluster = new_node_cluster(1, 3);
    configure_for_merge(&mut cluster.cfg);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Create a RegionInfoAccessor on node 1
    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_coprocessor_host(Box::new(move |id, host| {
            if id == 1 {
                let c = RegionInfoAccessor::new(host, Arc::new(|| false), Box::new(|| 0));
                tx.send(c).unwrap();
            }
        }));
    cluster.run_conf_change();
    let c = rx.recv().unwrap();
    // We only created it on the node whose id == 1 so we shouldn't receive more
    // than one item.
    assert!(rx.try_recv().is_err());

    test_region_info_accessor_impl(&mut cluster, &c);

    drop(cluster);
    c.stop();
}

#[test_case(test_raftstore::new_server_cluster)]
fn test_raft_engine_seek_region() {
    let mut cluster = new_cluster(0, 3);
    configure_for_snapshot(&mut cluster.cfg);
    cluster.run_conf_change();
    let pd_client = cluster.pd_client.clone();
    for key in [b"a", b"b", b"c", b"d"] {
        let region = pd_client.get_region(key).unwrap();
        cluster.must_split(&region, key);
    }

    let stores = pd_client.get_stores().unwrap();
    assert_eq!(stores.len(), 3);
    let r0 = pd_client.get_region_info(b"").unwrap();
    let r1 = pd_client.get_region_info(b"a").unwrap();
    let r2 = pd_client.get_region_info(b"b").unwrap();
    let r3 = pd_client.get_region_info(b"c").unwrap();
    let r4 = pd_client.get_region_info(b"d").unwrap();

    for (i, r) in [&r0, &r1, &r2, &r3, &r4].iter().enumerate() {
        let leader_store = stores[i % stores.len()].get_id();
        if r.leader.is_none() || r.leader.as_ref().unwrap().store_id != leader_store {
            let leader_peer = find_peer(r, leader_store).unwrap().clone();
            pd_client.transfer_leader(r.get_id(), leader_peer.clone(), vec![]);
            pd_client.region_leader_must_be(r.get_id(), leader_peer);
        }
    }

    let raft_engine = cluster.must_get_raft_engine(stores[0].get_id());
    let (tx, rx) = channel();
    raft_engine
        .seek_region(
            b"b",
            Box::new(move |iter| {
                let mut regions = vec![];
                for info in iter {
                    regions.push(info.clone());
                }
                tx.send(regions).unwrap();
            }),
        )
        .unwrap();

    // seek_region should be available for raft engine
    let scanned_regions = rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .iter()
        .map(|info| (info.region.clone(), info.role))
        .collect::<Vec<_>>();

    assert_eq!(
        scanned_regions,
        vec![
            (r2.region.clone(), StateRole::Follower),
            (r3.region.clone(), StateRole::Leader),
            (r4.region.clone(), StateRole::Follower),
        ]
    );
}
