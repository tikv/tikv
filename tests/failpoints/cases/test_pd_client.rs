// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::Future;
use futures_cpupool::Builder;
use kvproto::metapb::*;
use pd_client::{PdClient, RegionInfo, RegionStat, RpcClient};
use test_pd::{mocker::*, util::*, Server as MockServer};
use tikv_util::config::ReadableDuration;

use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

fn new_test_server_and_client(
    update_interval: ReadableDuration,
) -> (MockServer<Service>, RpcClient) {
    let server = MockServer::new(1);
    let eps = server.bind_addrs();
    let client = new_client_with_update_interval(eps, None, update_interval);
    (server, client)
}

macro_rules! request {
    ($client: ident => block_on($func: tt($($arg: expr),*))) => {
        (stringify!($func), {
            let client = $client.clone();
            Box::new(move || {
                let _ = client.$func($($arg),*).wait();
            })
        })
    };
    ($client: ident => $func: tt($($arg: expr),*)) => {
        (stringify!($func), {
            let client = $client.clone();
            Box::new(move || {
                let _ = client.$func($($arg),*);
            })
        })
    };
}

#[test]
fn test_pd_client_deadlock() {
    let (_server, client) = new_test_server_and_client(ReadableDuration::millis(100));
    let client = Arc::new(client);
    let leader_client_reconnect_fp = "leader_client_reconnect";

    // It contains all interfaces of PdClient except region_heartbeat which is a stream-future.
    let test_funcs: Vec<(_, Box<dyn FnOnce() + Send>)> = vec![
        request!(client => reconnect()),
        request!(client => get_cluster_id()),
        request!(client => bootstrap_cluster(Store::default(), Region::default())),
        request!(client => is_cluster_bootstrapped()),
        request!(client => alloc_id()),
        request!(client => put_store(Store::default())),
        request!(client => get_store(0)),
        request!(client => get_all_stores(false)),
        request!(client => get_cluster_config()),
        request!(client => get_region(b"")),
        request!(client => get_region_info(b"")),
        request!(client => block_on(get_region_by_id(0))),
        request!(client => block_on(ask_split(Region::default()))),
        request!(client => block_on(ask_batch_split(Region::default(), 1))),
        request!(client => block_on(store_heartbeat(Default::default()))),
        request!(client => block_on(report_batch_split(vec![]))),
        request!(client => scatter_region(RegionInfo::new(Region::default(), None))),
        request!(client => block_on(get_gc_safe_point())),
        request!(client => get_store_stats(0)),
        request!(client => get_operator(0)),
        request!(client => block_on(get_tso())),
    ];

    for (name, func) in test_funcs {
        fail::cfg(leader_client_reconnect_fp, "pause").unwrap();
        // Wait for the PD client thread blocking on the fail point.
        // The GLOBAL_RECONNECT_INTERVAL is 0.1s so sleeps 0.2s here.
        thread::sleep(Duration::from_millis(200));

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            func();
            tx.send(()).unwrap();
        });
        // Remove the fail point to let the PD client thread go on.
        fail::remove(leader_client_reconnect_fp);
        // The REQUEST_RECONNECT_INTERVAL is 1s so waits 1.5s here. Not required for version 5.0 or higher.
        if rx.recv_timeout(Duration::from_millis(1500)).is_err() {
            panic!("PdClient::{}() hangs", name);
        }
        handle.join().unwrap();
    }

    // Handle region_heartbeat specially which is a stream-future.
    let poller = Builder::new()
        .pool_size(1)
        .name_prefix(thd_name!("poller"))
        .create();
    let (tx, rx) = mpsc::channel();
    poller
        .spawn(client.handle_region_heartbeat_response(1, move |resp| {
            tx.send(resp).unwrap();
        }))
        .forget();
    poller
        .spawn(client.region_heartbeat(
            0,
            Region::default(),
            Peer::default(),
            RegionStat::default(),
        ))
        .forget();
    rx.recv_timeout(Duration::from_millis(3000)).unwrap();
}

// Reconnection will be speed limited.
#[test]
fn test_reconnect_limit() {
    let leader_client_reconnect_fp = "leader_client_reconnect";
    let (_server, client) = new_test_server_and_client(ReadableDuration::secs(100));

    // The GLOBAL_RECONNECT_INTERVAL is 0.1s so sleeps 0.2s here.
    thread::sleep(Duration::from_millis(200));

    // The first reconnection will succeed, and the last_update will not be updated.
    fail::cfg(leader_client_reconnect_fp, "return").unwrap();
    client.reconnect().unwrap();
    // The subsequent reconnection will be cancelled.
    for _ in 0..10 {
        let ret = client.reconnect();
        assert!(format!("{:?}", ret.unwrap_err()).contains("cancel reconnection"));
    }

    fail::remove(leader_client_reconnect_fp);
}
