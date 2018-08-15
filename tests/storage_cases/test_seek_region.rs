// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::thread;
use std::time::Duration;

use test_raftstore::*;
use tikv::raftstore::store::SeekRegionResult;
use tikv::storage::engine::RegionInfoProvider;
use tikv::util::HandyRwLock;

#[test]
fn test_seek_region() {
    // Prepare
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

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

    for node_id in cluster.get_node_ids() {
        let engine = cluster.sim.rl().storages[&node_id].clone();

        // Test traverse all regions
        let mut sought_regions = Vec::new();
        let mut key = b"".to_vec();
        loop {
            let res = engine.seek_region(&key, box |_| true, 100).unwrap();
            match res {
                SeekRegionResult::Found { local_peer, region } => {
                    assert_eq!(local_peer.get_store_id(), node_id);
                    key = region.get_end_key().to_vec();
                    sought_regions.push(region);
                    // Break on the last region
                    if key.is_empty() {
                        break;
                    }
                }
                SeekRegionResult::Ended => break,
                r => panic!("expect getting a region or Ended, but got {:?}", r),
            }
        }
        assert_eq!(sought_regions, regions);

        // Test end_key is exclusive
        let res = engine.seek_region(b"k1", box |_| true, 100).unwrap();
        match res {
            SeekRegionResult::Found { local_peer, region } => {
                assert_eq!(local_peer.get_store_id(), node_id);
                assert_eq!(region, regions[1]);
            }
            r => panic!("expect getting a region, but got {:?}", r),
        }

        // Test exactly reaches limit
        let res = engine
            .seek_region(b"", box |p| p.region().get_end_key() == b"k9", 5)
            .unwrap();
        match res {
            SeekRegionResult::Found { local_peer, region } => {
                assert_eq!(local_peer.get_store_id(), node_id);
                assert_eq!(region, regions[4]);
            }
            r => panic!("expect getting a region, but got {:?}", r),
        }

        // Test exactly exceeds limit
        let res = engine
            .seek_region(b"", box |p| p.region().get_end_key() == b"k9", 4)
            .unwrap();
        match res {
            SeekRegionResult::LimitExceeded { next_key } => {
                assert_eq!(&next_key, b"k7");
            }
            r => panic!("expect getting LimitExceeded, but got {:?}", r),
        }

        // Test seek to the end
        let res = engine.seek_region(b"", box |_| false, 100).unwrap();
        match res {
            SeekRegionResult::Ended => {}
            r => panic!("expect getting Ended, but got {:?}", r),
        }

        // Test exactly to the end
        let res = engine
            .seek_region(b"", box |p| p.region().get_end_key().is_empty(), 6)
            .unwrap();
        match res {
            SeekRegionResult::Found { local_peer, region } => {
                assert_eq!(local_peer.get_store_id(), node_id);
                assert_eq!(region, regions[5]);
            }
            r => panic!("expect getting a region, but got {:?}", r),
        }

        // Test limit exactly reaches end
        let res = engine.seek_region(b"", box |_| false, 6).unwrap();
        match res {
            SeekRegionResult::Ended => {}
            r => panic!("expect getting Ended, but got {:?}", r),
        }

        // Test seek from non-starting key
        let res = engine
            .seek_region(b"k6\xff\xff\xff\xff\xff", box |_| true, 1)
            .unwrap();
        match res {
            SeekRegionResult::Found { local_peer, region } => {
                assert_eq!(local_peer.get_store_id(), node_id);
                assert_eq!(region, regions[3]);
            }
            r => panic!("expect getting a region, but got {:?}", r),
        }
        let res = engine
            .seek_region(b"\xff\xff\xff\xff\xff\xff\xff\xff", box |_| true, 1)
            .unwrap();
        match res {
            SeekRegionResult::Found { local_peer, region } => {
                assert_eq!(local_peer.get_store_id(), node_id);
                assert_eq!(region, regions[5]);
            }
            r => panic!("expect getting a region, but got {:?}", r),
        }
    }
}
