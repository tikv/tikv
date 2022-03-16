// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::{APIVersion, APIV2};
use causal_ts::{BatchTsoProvider, CausalObserver, CausalTsProvider, RegionsCausalManager};
use collections::HashMap;
use engine_rocks::RocksEngine;
use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use raftstore::coprocessor::CoprocessorHost;
use std::collections::HashSet;
use std::sync::{Arc, Once};
use std::time::Duration;
use test_raftstore::{
    configure_for_merge, new_server_cluster, Cluster, ServerCluster, TestPdClient,
};
use tikv_util::HandyRwLock;

static INIT: Once = Once::new();

struct TestSuite {
    pub cluster: Cluster<ServerCluster>,
    pub pd_client: Arc<TestPdClient>,
    pub ts_provider: HashMap<u64, Arc<BatchTsoProvider>>,
}

// fn init() -> TestSuite {
//     INIT.call_once(test_util::setup_for_ci);
//
//     let mut cluster = new_server_cluster(0, 3);
//     configure_for_merge(&mut cluster);
//     let pd_client = cluster.pd_client.clone();
//
//     let mut ts_providers = HashMap::default();
//
//     for id in 1..=cluster.count as u64 {
//         let mut sim = cluster.sim.wl();
//
//         // Disable physical refresh of HLC. Then causal timestamp can be logically advanced only.
//         let ts_provider =
//             Arc::new(block_on(BatchTsoProvider::new_opt(pd_client.clone(), Duration::ZERO, 100)).unwrap());
//         let manager = Arc::new(RegionsCausalManager::default());
//         let ob = CausalObserver::new(manager.clone(), ts_provider.clone());
//
//         let ob_clone = ob.clone();
//         sim.coprocessor_hooks.entry(id).or_default().push(Box::new(
//             move |host: &mut CoprocessorHost<RocksEngine>| {
//                 ob_clone.register_to(host);
//             },
//         ));
//
//         ts_providers.insert(id, ts_provider);
//     }
//
//     TestSuite {
//         cluster,
//         pd_client,
//         ts_provider: ts_providers,
//     }
// }
//
// fn transfer_leader(
//     cluster: &mut Cluster<ServerCluster>,
//     region: &metapb::Region,
//     leader: &metapb::Peer,
// ) -> metapb::Peer {
//     transfer_leader_ext(cluster, region, leader, HashSet::default())
// }
//
// fn transfer_leader_ext(
//     cluster: &mut Cluster<ServerCluster>,
//     region: &metapb::Region,
//     leader: &metapb::Peer,
//     mut excludes: HashSet<u64>,
// ) -> metapb::Peer {
//     let mut new_leader = leader.clone();
//     excludes.insert(leader.get_store_id());
//     for peer in region.peers.iter() {
//         if !excludes.contains(&peer.get_store_id()) {
//             new_leader = peer.clone();
//             break;
//         }
//     }
//     assert_ne!(new_leader.get_id(), leader.get_id());
//     cluster.must_transfer_leader(region.get_id(), new_leader.clone());
//     assert_eq!(
//         new_leader.get_id(),
//         cluster.leader_of_region(region.get_id()).unwrap().get_id()
//     );
//
//     new_leader
// }
//
// #[test]
// fn test_multi_regions_causality() {
//     let mut suite = init();
//     suite.cluster.run();
//
//     let region = suite.pd_client.get_region(b"").unwrap();
//     let leader = suite.cluster.leader_of_region(region.get_id()).unwrap();
//
//     let ts_provider = suite.ts_provider.get(&leader.get_store_id()).unwrap();
//     ts_provider.advance(10.into()).unwrap();
//     let ts = ts_provider.get_ts().unwrap();
//     assert_eq!(ts, 10.into());
//
//     let user_k1: &[u8] = b"rk1";
//     let user_k2: &[u8] = b"rk2";
//     let user_k3: &[u8] = b"rk3";
//     let mut k1 = APIV2::encode_raw_key(user_k1, Some(ts));
//     let k2 = APIV2::encode_raw_key(user_k2, None);
//     let k3 = APIV2::encode_raw_key(user_k3, Some(ts));
//
//     suite.cluster.must_put(k1.as_encoded(), b"v1");
//     suite.cluster.must_put(k3.as_encoded(), b"v3");
//
//     // Transfer leader
//     {
//         let new_leader = transfer_leader(&mut suite.cluster, &region, &leader);
//         // Verify that causal timestamp of follower is advanced when leader transfer on it.
//         let ts = suite
//             .ts_provider
//             .get(&new_leader.get_store_id())
//             .unwrap()
//             .get_ts()
//             .unwrap();
//         assert_eq!(ts, 11.into());
//     }
//
//     // Split region to "left" & "right".
//     {
//         // Advance maximum timestamp of region to 20.
//         k1 = APIV2::encode_raw_key(user_k1, Some(20.into()));
//         suite.cluster.must_put(k1.as_encoded(), b"v1");
//
//         let region = suite.pd_client.get_region(k1.as_encoded()).unwrap();
//         suite.cluster.must_split(&region, k2.as_encoded());
//
//         let left_region = suite.pd_client.get_region(k1.as_encoded()).unwrap();
//         let right_region = suite.pd_client.get_region(k3.as_encoded()).unwrap();
//         assert_ne!(left_region.get_id(), right_region.get_id());
//
//         let left_leader = suite
//             .cluster
//             .leader_of_region(left_region.get_id())
//             .unwrap();
//         let right_leader = suite
//             .cluster
//             .leader_of_region(right_region.get_id())
//             .unwrap();
//
//         // Transfer left & right regions leader to another stores.
//         // Verify timestamp is copied from original region to split regions.
//         let new_left_leader = transfer_leader(&mut suite.cluster, &left_region, &left_leader);
//         let ts = suite
//             .ts_provider
//             .get(&new_left_leader.get_store_id())
//             .unwrap()
//             .get_ts()
//             .unwrap();
//         assert_eq!(ts, 21.into());
//
//         let new_right_leader = transfer_leader_ext(
//             &mut suite.cluster,
//             &right_region,
//             &right_leader,
//             [new_left_leader.get_store_id()].into(),
//         );
//         let ts = suite
//             .ts_provider
//             .get(&new_right_leader.get_store_id())
//             .unwrap()
//             .get_ts()
//             .unwrap();
//         assert_eq!(ts, 21.into());
//     }
//
//     // Merge regions
//     {
//         // Advance maximum timestamp of left region to 30.
//         k1 = APIV2::encode_raw_key(user_k1, Some(30.into()));
//         suite.cluster.must_put(k1.as_encoded(), b"v1");
//
//         let left_region = suite.pd_client.get_region(k1.as_encoded()).unwrap();
//         let right_region = suite.pd_client.get_region(k3.as_encoded()).unwrap();
//         assert_ne!(left_region.get_id(), right_region.get_id());
//
//         suite
//             .pd_client
//             .must_merge(left_region.get_id(), right_region.get_id()); // merge left into right.
//
//         // Verify timestamp is advanced after merge.
//         let right_leader = suite
//             .cluster
//             .leader_of_region(right_region.get_id())
//             .unwrap();
//         let new_right_leader = transfer_leader(&mut suite.cluster, &right_region, &right_leader);
//         let ts = suite
//             .ts_provider
//             .get(&new_right_leader.get_store_id())
//             .unwrap()
//             .get_ts()
//             .unwrap();
//         assert_eq!(ts, 31.into());
//     }
// }
