// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::atomic::AtomicU16, thread::sleep, time::Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use futures::executor::block_on;
use pd_client::PdClient;
use rand::{Rng, RngCore};
use test_cloud_server::{ServerCluster, try_wait};
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
    let timeout = Duration::from_secs(60);
    let write_counter = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    for _ in 0..concurrency {
        let mut client = cluster.new_client();
        let write_count = write_counter.clone();
        let handle = std::thread::spawn(move || {
            let start_time = Instant::now();
            let mut rng = rand::thread_rng();
            while start_time.saturating_elapsed() < timeout {
                let i = rng.gen_range(0..10000usize);
                client.put_kv(i..(i + 10), i_to_key, i_to_val);
                write_count.fetch_add(10, Ordering::SeqCst);
            }
        });
        handles.push(handle);
    }
    let start_time = Instant::now();
    let pd_client = cluster.get_pd_client();
    let scheduler = cluster.new_region_scheduler();
    let mut move_count = 0;
    while start_time.saturating_elapsed() < timeout {
        let ts = block_on(pd_client.get_tso()).unwrap();
        sleep(Duration::from_millis(1000));
        pd_client.set_gc_safe_point(ts.into_inner());
        scheduler.move_random_region();
        move_count += 1;
    }
    for handle in handles {
        handle.join().unwrap();
    }
    if !try_wait(|| {
        let data_stats = cluster.get_data_stats();
        data_stats.check_data().is_ok()
    }, 10) {
        cluster.get_data_stats().check_data().unwrap();
    }
    cluster.stop();
    let total_write_count = write_counter.load(Ordering::SeqCst);
    let region_number = pd_client.get_regions_number();
    info!("total_write_count {}, region number {}, move count {}",
        total_write_count, region_number, move_count);
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
