// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    sync::{
        mpsc::{channel, sync_channel, Receiver, SyncSender},
        Arc,
    },
    time::Duration,
};

use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::{
    coprocessor::{
        BoxRegionChangeObserver, Coprocessor, ObserverContext, RegionChangeEvent,
        RegionChangeObserver,
    },
    store::util::{find_peer, new_peer},
};
use test_raftstore::{new_node_cluster, Cluster, NodeCluster};
use tikv_util::HandyRwLock;

#[derive(Clone)]
struct TestObserver {
    sender: SyncSender<(Region, RegionChangeEvent)>,
}

impl Coprocessor for TestObserver {}

impl RegionChangeObserver for TestObserver {
    fn on_region_changed(
        &self,
        ctx: &mut ObserverContext<'_>,
        event: RegionChangeEvent,
        _: StateRole,
    ) {
        self.sender.send((ctx.region().clone(), event)).unwrap();
    }
}

fn test_region_change_observer_impl(mut cluster: Cluster<NodeCluster>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let receiver: Receiver<(Region, RegionChangeEvent)>;
    let r1;
    {
        let (tx, rx) = channel();

        cluster
            .sim
            .wl()
            .post_create_coprocessor_host(Box::new(move |id, host| {
                if id == 1 {
                    let (sender, receiver) = sync_channel(10);
                    host.registry.register_region_change_observer(
                        1,
                        BoxRegionChangeObserver::new(TestObserver { sender }),
                    );
                    tx.send(receiver).unwrap();
                }
            }));
        r1 = cluster.run_conf_change();

        // Only one node has node_id = 1
        receiver = rx.recv().unwrap();
        rx.try_recv().unwrap_err();
    }

    // Init regions
    let init_region_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(init_region_event.1, RegionChangeEvent::Create);
    assert_eq!(init_region_event.0.get_id(), r1);
    assert_eq!(init_region_event.0.get_peers().len(), 1);

    // Change peer
    pd_client.must_add_peer(r1, new_peer(2, 10));
    let add_peer_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(add_peer_event.1, RegionChangeEvent::Update);
    assert_eq!(add_peer_event.0.get_id(), r1);
    assert_eq!(add_peer_event.0.get_peers().len(), 2);
    assert_ne!(
        add_peer_event.0.get_region_epoch(),
        init_region_event.0.get_region_epoch()
    );

    // Split
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k2", b"v2");
    cluster.must_put(b"k3", b"v3");
    cluster.must_split(&add_peer_event.0, b"k2");
    let mut split_update = receiver.recv().unwrap();
    let mut split_create = receiver.recv().unwrap();
    // We should receive an `Update` and a `Create`. The order of them is not important.
    if split_update.1 != RegionChangeEvent::Update {
        mem::swap(&mut split_update, &mut split_create);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(split_update.1, RegionChangeEvent::Update);
    assert_eq!(split_update.0.get_id(), r1);
    assert_ne!(
        split_update.0.get_region_epoch(),
        add_peer_event.0.get_region_epoch()
    );
    let r2 = split_create.0.get_id();
    assert_ne!(r2, r1);
    assert_eq!(split_create.1, RegionChangeEvent::Create);
    if split_update.0.get_start_key().is_empty() {
        assert_eq!(split_update.0.get_end_key(), b"k2");
        assert_eq!(split_create.0.get_start_key(), b"k2");
        assert!(split_create.0.get_end_key().is_empty());
    } else {
        assert_eq!(split_update.0.get_start_key(), b"k2");
        assert!(split_update.0.get_end_key().is_empty());
        assert!(split_create.0.get_start_key().is_empty());
        assert_eq!(split_create.0.get_end_key(), b"k2");
    }

    // Merge
    pd_client.must_merge(split_update.0.get_id(), split_create.0.get_id());
    // An `Update` produced by PrepareMerge. Ignore it.
    assert_eq!(receiver.recv().unwrap().1, RegionChangeEvent::Update);
    let mut merge_update = receiver.recv().unwrap();
    let mut merge_destroy = receiver.recv().unwrap();
    // We should receive an `Update` and a `Destroy`. The order of them is not important.
    if merge_update.1 != RegionChangeEvent::Update {
        mem::swap(&mut merge_update, &mut merge_destroy);
    }
    // No more events
    receiver.try_recv().unwrap_err();
    assert_eq!(merge_update.1, RegionChangeEvent::Update);
    assert!(merge_update.0.get_start_key().is_empty());
    assert!(merge_update.0.get_end_key().is_empty());
    assert_eq!(merge_destroy.1, RegionChangeEvent::Destroy);
    if merge_update.0.get_id() == split_update.0.get_id() {
        assert_eq!(merge_destroy.0.get_id(), split_create.0.get_id());
        assert_ne!(
            merge_update.0.get_region_epoch(),
            split_update.0.get_region_epoch()
        );
    } else {
        assert_eq!(merge_update.0.get_id(), split_create.0.get_id());
        assert_eq!(merge_destroy.0.get_id(), split_update.0.get_id());
        assert_ne!(
            merge_update.0.get_region_epoch(),
            split_create.0.get_region_epoch()
        );
    }

    // Move out from this node
    // After last time calling "must_add_peer", this region must have two peers
    assert_eq!(merge_update.0.get_peers().len(), 2);
    let r = merge_update.0.get_id();

    pd_client.must_remove_peer(r, find_peer(&merge_update.0, 1).unwrap().clone());

    let remove_peer_update = receiver.recv().unwrap();
    // After being removed from the region's peers, an update is triggered at first.
    assert_eq!(remove_peer_update.1, RegionChangeEvent::Update);
    assert!(find_peer(&remove_peer_update.0, 1).is_none());

    let remove_peer_destroy = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(remove_peer_destroy.1, RegionChangeEvent::Destroy);
    assert_eq!(remove_peer_destroy.0.get_id(), r);

    pd_client.must_add_peer(r, new_peer(1, 2333));
    let add_peer_event = receiver.recv().unwrap();
    receiver.try_recv().unwrap_err();
    assert_eq!(add_peer_event.1, RegionChangeEvent::Create);
    assert_eq!(add_peer_event.0.get_id(), r);
    assert_eq!(find_peer(&add_peer_event.0, 1).unwrap().get_id(), 2333);

    // No more messages
    receiver.recv_timeout(Duration::from_secs(1)).unwrap_err();
}

#[test]
fn test_region_change_observer() {
    let cluster = new_node_cluster(1, 3);
    test_region_change_observer_impl(cluster);
}
