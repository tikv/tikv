// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use pd_client::PdClient;
use test_raftstore::*;
use tikv_util::{config::ReadableDuration, time::Instant};

/// Wait until f returns true.
/// Deadline and interval are in milliseconds.
fn must_wait_until<F>(deadline: u64, interval: u64, mut f: F)
where
    F: FnMut() -> bool,
{
    let timer = Instant::now();
    loop {
        if timer.saturating_elapsed().as_millis() > deadline as u128 {
            panic!("wait timeout");
        }
        if f() {
            return;
        }
        sleep_ms(interval);
    }
}

/// Store should send heartbeats to PD just after it starts.
#[test]
fn test_store_heartbeat_after_start() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(2000);
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    // The store needs not to wait for a round of tick, and it should report
    // heartbeat just after it starts.
    must_wait_until(1000, 50, || -> bool {
        pd_client.get_store(1).unwrap().get_last_heartbeat() != 0
    });
}
