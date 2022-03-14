// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::FromIterator;
use std::sync::{Arc, Condvar, Mutex};

use futures::executor::block_on;
use pd_client::PdClient;
use raftstore::store::util::find_peer;
use test_raftstore::*;

#[allow(clippy::mutex_atomic)]
#[test]
fn test_unsafe_recover_send_report() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    configure_for_lease_read(&mut cluster, None, None);

    // Makes the leadership definite.
    let store2_peer = find_peer(&region, nodes[1]).unwrap().to_owned();
    cluster.must_transfer_leader(region.get_id(), store2_peer);
    cluster.put(b"random_key1", b"random_val1").unwrap();

    // Blocks the raft apply process on store 1 entirely .
    let apply_triggered_pair = Arc::new((Mutex::new(false), Condvar::new()));
    let apply_triggered_pair2 = Arc::clone(&apply_triggered_pair);
    let apply_released_pair = Arc::new((Mutex::new(false), Condvar::new()));
    let apply_released_pair2 = Arc::clone(&apply_released_pair);
    fail::cfg_callback("on_handle_apply_store_1", move || {
        {
            let (lock, cvar) = &*apply_triggered_pair2;
            let mut triggered = lock.lock().unwrap();
            *triggered = true;
            cvar.notify_one();
        }
        {
            let (lock2, cvar2) = &*apply_released_pair2;
            let mut released = lock2.lock().unwrap();
            while !*released {
                released = cvar2.wait(released).unwrap();
            }
        }
    })
    .unwrap();

    // Mannually makes an update, and wait for the apply to be triggered, to simulate "some entries are commited but not applied" scenario.
    cluster.put(b"random_key2", b"random_val2").unwrap();
    {
        let (lock, cvar) = &*apply_triggered_pair;
        let mut triggered = lock.lock().unwrap();
        while !*triggered {
            triggered = cvar.wait(triggered).unwrap();
        }
    }

    // Makes the group lose its quorum.
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);

    // Triggers the unsafe recovery store reporting process.
    pd_client.must_set_require_report(true);
    cluster.must_send_store_heartbeat(nodes[0]);

    // No store report is sent, since there are peers have unapplied entries.
    for _ in 0..20 {
        assert_eq!(pd_client.must_get_store_reported(&nodes[0]), 0);
        sleep_ms(100);
    }

    // Unblocks the apply process.
    {
        let (lock2, cvar2) = &*apply_released_pair;
        let mut released = lock2.lock().unwrap();
        *released = true;
        cvar2.notify_all();
    }

    // Store reports are sent once the entries are applied.
    let mut reported = false;
    for _ in 0..20 {
        if pd_client.must_get_store_reported(&nodes[0]) > 0 {
            reported = true;
            break;
        }
        sleep_ms(100);
    }
    assert_eq!(reported, true);
    fail::remove("on_handle_apply_store_1");
}
