// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;

use collections::HashMap;
use kvproto::metapb::Region;
use raftstore::coprocessor::{RegionInfoAccessor, RegionInfoProvider};
use test_raftstore::*;
use tikv_util::HandyRwLock;

fn prepare_cluster<T: Simulator>(cluster: &mut Cluster<T>) -> Vec<Region> {
    for i in 0..15 {
        let i = i + b'0';
        let key = vec![b'k', i];
        let value = vec![b'v', i];
        cluster.must_put(&key, &value);
    }

    let end_keys = vec![
        b"k1".to_vec(),
        b"k3".to_vec(),
        b"k5".to_vec(),
        b"k7".to_vec(),
        b"k9".to_vec(),
        b"".to_vec(),
    ];

    let start_keys = vec![
        b"".to_vec(),
        b"k1".to_vec(),
        b"k3".to_vec(),
        b"k5".to_vec(),
        b"k7".to_vec(),
        b"k9".to_vec(),
    ];

    let mut regions = Vec::new();

    for mut key in end_keys.iter().take(end_keys.len() - 1).map(Vec::clone) {
        let region = cluster.get_region(&key);
        cluster.must_split(&region, &key);

        key[1] -= 1;
        let region = cluster.get_region(&key);
        regions.push(region);
    }
    regions.push(cluster.get_region(b"k9"));

    assert_eq!(regions.len(), end_keys.len());
    assert_eq!(regions.len(), start_keys.len());
    for i in 0..regions.len() {
        assert_eq!(regions[i].start_key, start_keys[i]);
        assert_eq!(regions[i].end_key, end_keys[i]);
    }

    // Wait for raftstore to update regions
    thread::sleep(Duration::from_secs(2));
    regions
}

#[test]
fn test_region_collection_seek_region() {
    let mut cluster = new_node_cluster(0, 3);

    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_coprocessor_host(Box::new(move |id, host| {
            let p = RegionInfoAccessor::new(host);
            tx.send((id, p)).unwrap()
        }));

    cluster.run();
    let region_info_providers: HashMap<_, _> = rx.try_iter().collect();
    assert_eq!(region_info_providers.len(), 3);
    let regions = prepare_cluster(&mut cluster);

    for node_id in cluster.get_node_ids() {
        let engine = &region_info_providers[&node_id];

        // Test traverse all regions
        let key = b"".to_vec();
        let (tx, rx) = channel();
        let tx_ = tx.clone();
        engine
            .seek_region(
                &key,
                Box::new(move |infos| {
                    tx_.send(infos.map(|i| i.region.clone()).collect()).unwrap();
                }),
            )
            .unwrap();
        let sought_regions: Vec<_> = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(sought_regions, regions);

        // Test end_key is exclusive
        let (tx, rx) = channel();
        let tx_ = tx.clone();
        engine
            .seek_region(
                b"k1",
                Box::new(move |infos| tx_.send(infos.next().unwrap().region.clone()).unwrap()),
            )
            .unwrap();
        let region = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(region, regions[1]);

        // Test seek from non-starting key
        let tx_ = tx.clone();
        engine
            .seek_region(
                b"k6\xff\xff\xff\xff\xff",
                Box::new(move |infos| tx_.send(infos.next().unwrap().region.clone()).unwrap()),
            )
            .unwrap();
        let region = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(region, regions[3]);
        let tx_ = tx.clone();
        engine
            .seek_region(
                b"\xff\xff\xff\xff\xff\xff\xff\xff",
                Box::new(move |infos| tx_.send(infos.next().unwrap().region.clone()).unwrap()),
            )
            .unwrap();
        let region = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(region, regions[5]);
    }

    for (_, p) in region_info_providers {
        p.stop();
    }
}

#[test]
fn test_region_collection_get_regions_in_range() {
    let mut cluster = new_node_cluster(0, 3);

    let (tx, rx) = channel();
    cluster
        .sim
        .wl()
        .post_create_coprocessor_host(Box::new(move |id, host| {
            let p = RegionInfoAccessor::new(host);
            tx.send((id, p)).unwrap()
        }));

    cluster.run();
    let region_info_providers: HashMap<_, _> = rx.try_iter().collect();
    assert_eq!(region_info_providers.len(), 3);
    let regions = prepare_cluster(&mut cluster);

    for node_id in cluster.get_node_ids() {
        let engine = &region_info_providers[&node_id];

        let result = engine.get_regions_in_range(b"", b"").unwrap();
        assert_eq!(result, regions);

        let result = engine.get_regions_in_range(b"k1", b"k3").unwrap();
        assert_eq!(&result, &regions[1..3]);

        let result = engine.get_regions_in_range(b"k3", b"k8").unwrap();
        assert_eq!(&result, &regions[2..5]);

        let result = engine.get_regions_in_range(b"k6", b"k8").unwrap();
        assert_eq!(&result, &regions[3..5]);

        let result = engine.get_regions_in_range(b"k7", b"k99").unwrap();
        assert_eq!(&result, &regions[4..6]);

        let result = engine.get_regions_in_range(b"k99", b"").unwrap();
        assert_eq!(&result, &regions[5..6]);
    }

    for (_, p) in region_info_providers {
        p.stop();
    }
}
