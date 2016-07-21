// Copyright 2016 PingCAP, Inc.
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

use std::sync::Arc;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::util::*;
use tikv::pd::PdClient;
use super::pd::TestPdClient;

fn check_available<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    let engine = cluster.get_engine(1);

    let stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_region_count(), 2);

    let value = vec![0;1024];
    for i in 0..1000 {
        let last_available = stats.get_available();
        cluster.must_put(format!("k{}", i).as_bytes(), &value);
        engine.flush(true).unwrap();
        sleep_ms(20);

        let stats = pd_client.get_store_stats(1).unwrap();
        // Because the available is for disk size, even we add data
        // other process may reduce data too. so here we try to
        // check available size changed.
        if stats.get_available() != last_available {
            return;
        }
    }

    panic!("available not changed")
}

fn test_simple_store_stats<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();

    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = 20;
    cluster.run();

    // wait store reports stats.
    for _ in 0..100 {
        sleep_ms(20);

        if pd_client.get_store_stats(1).is_some() {
            break;
        }
    }

    let engine = cluster.get_engine(1);
    engine.flush(true).unwrap();
    let last_stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(last_stats.get_region_count(), 1);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"").unwrap();
    cluster.must_split(&region, b"k2");
    engine.flush(true).unwrap();

    // wait report region count after split
    for _ in 0..100 {
        sleep_ms(20);

        let stats = pd_client.get_store_stats(1).unwrap();
        if stats.get_region_count() == 2 {
            break;
        }
    }

    let stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_region_count(), 2);

    check_available(cluster);
}

#[test]
fn test_node_simple_store_stats() {
    let mut cluster = new_node_cluster(0, 1);
    test_simple_store_stats(&mut cluster);
}

#[test]
fn test_server_simple_store_stats() {
    let mut cluster = new_server_cluster(0, 1);
    test_simple_store_stats(&mut cluster);
}

#[test]
fn test_server_store_snap_stats() {
    let mut cluster = new_server_cluster(0, 2);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = 600000;

    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    let r1 = cluster.run_conf_change();

    // make a big snapshot
    for i in 0..2 * 1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    pd_client.must_add_peer(r1, new_peer(2, 2));

    must_detect_snap(&pd_client);

    // wait snapshot finish.
    sleep_ms(100);

    // remove the peer so we can't do any snapshot now.
    pd_client.must_remove_peer(r1, new_peer(2, 2));
    cluster.must_put(b"k2", b"v2");

    must_not_detect_snap(&pd_client);
}

fn must_detect_snap(pd_client: &Arc<TestPdClient>) {
    for _ in 0..200 {
        sleep_ms(10);

        if let Some(stats) = pd_client.get_store_stats(1) {
            if stats.get_sending_snap_count() > 0 || stats.get_receiving_snap_count() > 0 {
                return;
            }
        }
    }

    panic!("must detect snapshot sending/receiving");
}

fn must_not_detect_snap(pd_client: &Arc<TestPdClient>) {
    for _ in 0..200 {
        sleep_ms(10);

        if let Some(stats) = pd_client.get_store_stats(1) {
            if stats.get_sending_snap_count() == 0 && stats.get_receiving_snap_count() == 0 {
                return;
            }
        }
    }

    panic!("must not detect snapshot sending/receiving");
}
