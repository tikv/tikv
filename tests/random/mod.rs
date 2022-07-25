// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::atomic::AtomicU16, thread::sleep, time::Duration};

use futures::executor::block_on;
use pd_client::PdClient;
use rand::{Rng, RngCore};
use test_cloud_server::ServerCluster;
use tikv_util::{config::ReadableSize, info, time::Instant};

static NODE_ALLOCATOR: AtomicU16 = AtomicU16::new(1);

pub(crate) fn alloc_node_id() -> u16 {
    let node_id = NODE_ALLOCATOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    info!("allocated node_id {}", node_id);
    node_id
}

#[test]
fn test_random_workload() {
    test_util::init_log_for_test();
    let nodes = vec![
        alloc_node_id(),
        alloc_node_id(),
        alloc_node_id(),
        alloc_node_id(),
        alloc_node_id(),
    ];
    let mut cluster = ServerCluster::new(nodes.clone(), |_, conf| {
        conf.coprocessor.region_split_size = ReadableSize::kb(128);
    });
    cluster.wait_region_replicated(&[], 3);
    let concurrency = 4usize;
    let timeout = Duration::from_secs(30);
    let mut handles = vec![];
    for _ in 0..concurrency {
        let mut client = cluster.new_client();
        let handle = std::thread::spawn(move || {
            let start_time = Instant::now();
            let mut rng = rand::thread_rng();
            while start_time.saturating_elapsed() < timeout {
                let i = rng.gen_range(0..10000usize);
                client.put_kv(i..(i + 10), i_to_key, i_to_val);
            }
        });
        handles.push(handle);
    }
    let start_time = Instant::now();
    let pd_client = cluster.get_pd_client();
    while start_time.saturating_elapsed() < timeout {
        let ts = block_on(pd_client.get_tso()).unwrap();
        sleep(Duration::from_millis(1000));
        pd_client.set_gc_safe_point(ts.into_inner());
    }
    for handle in handles {
        handle.join().unwrap();
    }
    for &node_id in &nodes {
        let engine = cluster.get_kvengine(node_id);
        let stats = engine.get_all_shard_stats();
        info!("node {} shard stats {:?}", node_id, &stats);
    }
    cluster.stop();
}

fn i_to_key(i: usize) -> Vec<u8> {
    format!("key{:08}", i).into_bytes()
}

fn i_to_val(i: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut buf = vec![0; i % 512 + 1];
    rng.fill_bytes(&mut buf);
    buf
}
