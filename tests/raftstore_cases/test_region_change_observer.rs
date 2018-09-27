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

use kvproto::metapb::Region;
use std::mem;
use std::sync::mpsc::{channel, sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::time::Duration;
use test_raftstore::{new_node_cluster, new_server_cluster, Cluster, Simulator};
use tikv::raftstore::coprocessor::{
    Coprocessor, ObserverContext, RegionChangeEvent, RegionChangeObserver,
};
use tikv::raftstore::store::util::{find_peer, new_peer};
use tikv::util::HandyRwLock;

struct TestObserver {
    sender: SyncSender<(Region, RegionChangeEvent)>,
}

impl Coprocessor for TestObserver {}

impl RegionChangeObserver for TestObserver {
    fn on_region_changed(&self, ctx: &mut ObserverContext, event: RegionChangeEvent) {
        self.sender.send((ctx.region().clone(), event)).unwrap();
    }
}

fn test_region_change_observer_impl<S: Simulator>(mut cluster: Cluster<S>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let receiver: Receiver<(Region, RegionChangeEvent)>;
    let r1;
    {
        let (tx, rx) = channel();

        cluster
            .sim
            .wl()
            .hook_create_coprocessor_host(box move |id, host| {
                if id == 1 {
                    let (sender, receiver) = sync_channel(10);
                    host.registry
                        .register_region_change_observer(1, box TestObserver { sender });
                    tx.send(receiver).unwrap();
                }
            });
        r1 = cluster.run_conf_change();

        // Only one node has node_id = 1
        receiver = rx.recv().unwrap();
        rx.try_recv().unwrap_err();
    }

    // Init regions
    let event1 = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(event1.1, RegionChangeEvent::New);
    assert_eq!(event1.0.get_id(), r1);
    assert_eq!(event1.0.get_peers().len(), 1);

    // Change peer
    pd_client.must_add_peer(r1, new_peer(2, 10));
    let event2 = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(event2.1, RegionChangeEvent::Update);
    assert_eq!(event2.0.get_id(), r1);
    assert_eq!(event2.0.get_peers().len(), 2);
    assert_ne!(event2.0.get_region_epoch(), event1.0.get_region_epoch());

    // Split
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    cluster.must_split(&event2.0, b"k2");
    let mut event3 = receiver.recv().unwrap();
    let mut event4 = receiver.recv().unwrap();
    // We should receive an `Update` and a `New`. The order of them is not important.
    if event3.1 != RegionChangeEvent::Update {
        mem::swap(&mut event3, &mut event4);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(event3.1, RegionChangeEvent::Update);
    assert_eq!(event3.0.get_id(), r1);
    assert_ne!(event3.0.get_region_epoch(), event2.0.get_region_epoch());
    let r2 = event4.0.get_id();
    assert_ne!(r2, r1);
    assert_eq!(event4.1, RegionChangeEvent::New);
    if event3.0.get_start_key().is_empty() {
        assert_eq!(event3.0.get_end_key(), b"k2");
        assert_eq!(event4.0.get_start_key(), b"k2");
        assert!(event4.0.get_end_key().is_empty());
    } else {
        assert_eq!(event3.0.get_start_key(), b"k2");
        assert!(event3.0.get_end_key().is_empty());
        assert!(event4.0.get_start_key().is_empty());
        assert_eq!(event4.0.get_end_key(), b"k2");
    }

    // Merge
    pd_client.must_merge(event3.0.get_id(), event4.0.get_id());
    // An `Update` produced by PrepareMerge. Ignore it.
    assert_eq!(receiver.recv().unwrap().1, RegionChangeEvent::Update);
    let mut event5 = receiver.recv().unwrap();
    let mut event6 = receiver.recv().unwrap();
    // We should receive an `Update` and a `Destroy`. The order of them is not important.
    if event5.1 != RegionChangeEvent::Update {
        mem::swap(&mut event5, &mut event6);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(event5.1, RegionChangeEvent::Update);
    assert!(event5.0.get_start_key().is_empty());
    assert!(event5.0.get_end_key().is_empty());
    assert_eq!(event6.1, RegionChangeEvent::Destroy);
    if event5.0.get_id() == event3.0.get_id() {
        assert_eq!(event6.0.get_id(), event4.0.get_id());
        assert_ne!(event5.0.get_region_epoch(), event3.0.get_region_epoch());
    } else {
        assert_eq!(event5.0.get_id(), event4.0.get_id());
        assert_eq!(event6.0.get_id(), event3.0.get_id());
        assert_ne!(event5.0.get_region_epoch(), event4.0.get_region_epoch());
    }

    // Move out from this node
    // After last time calling "must_add_peer", this region must have two peers
    assert_eq!(event5.0.get_peers().len(), 2);
    let r = event5.0.get_id();

    pd_client.must_remove_peer(r, find_peer(&event5.0, 1).unwrap().clone());
    // TODO: Why is there an `Update`?
    assert_eq!(receiver.recv().unwrap().1, RegionChangeEvent::Update);
    let event7 = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(event7.1, RegionChangeEvent::Destroy);
    assert_eq!(event7.0.get_id(), r);

    pd_client.must_add_peer(r, new_peer(1, 2333));
    let event8 = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(event8.1, RegionChangeEvent::New);
    assert_eq!(event8.0.get_id(), r);
    assert_eq!(find_peer(&event8.0, 1).unwrap().get_id(), 2333);

    // No more messages
    receiver.recv_timeout(Duration::from_secs(1)).unwrap_err();
}

#[test]
fn test_server_region_change_observer() {
    let cluster = new_server_cluster(1, 3);
    test_region_change_observer_impl(cluster);
}

#[test]
fn test_node_region_change_observer() {
    let cluster = new_node_cluster(1, 3);
    test_region_change_observer_impl(cluster);
}
