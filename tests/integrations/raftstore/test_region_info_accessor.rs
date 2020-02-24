// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use keys::data_end_key;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::{RegionInfo, RegionInfoAccessor};
use raftstore::store::util::{find_peer, new_peer};
use std::sync::mpsc::channel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use test_raftstore::{configure_for_merge, new_node_cluster, Cluster, NodeCluster};
use tikv_util::HandyRwLock;

fn dump(c: &RegionInfoAccessor) -> Vec<(Region, StateRole)> {
    let (regions, region_ranges) = c.debug_dump();

    assert_eq!(regions.len(), region_ranges.len());

    let mut res = Vec::new();
    for (end_key, id) in region_ranges {
        let RegionInfo { ref region, role } = regions[&id];
        assert_eq!(end_key, data_end_key(region.get_end_key()));
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
    let merge_regions = dump(&c);
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
    let mut merge_regions_2 = dump(&c);
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
    pd_client.transfer_leader(region2.get_id(), find_peer(&region2, 2).unwrap().clone());
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
    configure_for_merge(&mut cluster);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    // Create a RegionInfoAccessor on node 1
    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_coprocessor_host(Box::new(move |id, host| {
            if id == 1 {
                let c = RegionInfoAccessor::new(host);
                tx.send(c).unwrap();
            }
        }));
    cluster.run_conf_change();
    let c = rx.recv().unwrap();
    c.start();
    // We only created it on the node whose id == 1 so we shouldn't receive more than one item.
    assert!(rx.try_recv().is_err());

    test_region_info_accessor_impl(&mut cluster, &c);

    drop(cluster);
    c.stop();
}
