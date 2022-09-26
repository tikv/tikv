// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{
        atomic::{AtomicU16, AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    thread::{sleep, JoinHandle},
    time::Duration,
};

use futures::executor::block_on;
use pd_client::PdClient;
use rand::{Rng, RngCore};
use test_cloud_server::{client::ClusterClient, scheduler::Scheduler, try_wait, ServerCluster};
use tikv::config::TiKvConfig;
use tikv_util::{
    config::{ReadableDuration, ReadableSize},
    info,
    time::Instant,
};

static NODE_ALLOCATOR: AtomicU16 = AtomicU16::new(1);
static WRITE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static MOVE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static TRANSFER_COUNTER: AtomicUsize = AtomicUsize::new(0);
const TIMEOUT: Duration = Duration::from_secs(60);
const CONCURRENCY: usize = 4;

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
    let update_conf_fn = |_, conf: &mut TiKvConfig| {
        conf.coprocessor.region_split_size = ReadableSize::kb(128);
        conf.raft_store.peer_stale_state_check_interval = ReadableDuration::secs(1);
        conf.raft_store.abnormal_leader_missing_duration = ReadableDuration::secs(3);
        conf.raft_store.max_leader_missing_duration = ReadableDuration::secs(5);
    };
    let mut cluster = ServerCluster::new(nodes.clone(), update_conf_fn);
    cluster.wait_region_replicated(&[], 3);
    cluster.get_pd_client().disable_default_operator();
    let mut handles = vec![];
    for i in 0..CONCURRENCY {
        handles.push(spawn_write(i, cluster.new_client()));
    }
    let two_nodes_down = Arc::new(RwLock::new(()));
    handles.push(spawn_move(cluster.new_scheduler(), two_nodes_down.clone()));
    handles.push(spawn_transfer(cluster.new_scheduler()));
    let start_time = Instant::now();
    let pd_client = cluster.get_pd_client();
    while start_time.saturating_elapsed() < TIMEOUT {
        let ts = block_on(pd_client.get_tso()).unwrap();
        let mut rng = rand::thread_rng();
        let node_idx = rng.gen_range(0..nodes.len());
        let node_id = nodes[node_idx];
        info!("stop node {}", node_id);
        cluster.stop_node(node_id);
        info!("finish stop node {}", node_id);
        let mut node2_id = 0;
        let mut guard = None;
        if node_idx == 0 {
            guard = Some(two_nodes_down.write().unwrap());
            let node2_idx = rng.gen_range(1..nodes.len());
            node2_id = nodes[node2_idx];
            info!("stop node {}", node2_id);
            cluster.stop_node(node2_id);
            info!("finish stop node {}", node_id);
        }
        let sleep_sec = rng.gen_range(1..5);
        sleep(Duration::from_secs(sleep_sec));
        info!("start node {}", node_id);
        cluster.start_node(node_id, update_conf_fn);
        if node2_id > 0 {
            info!("start node {}", node2_id);
            cluster.start_node(node2_id, update_conf_fn);
            guard = None;
        }
        sleep(Duration::from_secs(5));
        pd_client.set_gc_safe_point(ts.into_inner());
    }
    info!("stop node thread exit");
    for handle in handles {
        handle.join().unwrap();
    }
    if !try_wait(
        || {
            let data_stats = cluster.get_data_stats();
            data_stats.check_data().is_ok()
        },
        10,
    ) {
        cluster.get_data_stats().check_data().unwrap();
    }
    let mut client = cluster.new_client();
    client.verify_data_with_ref_store();
    cluster.stop();
    let total_write_count = WRITE_COUNTER.load(Ordering::SeqCst);
    let total_move_count = MOVE_COUNTER.load(Ordering::SeqCst);
    let total_transfer_count = TRANSFER_COUNTER.load(Ordering::SeqCst);
    let region_number = pd_client.get_regions_number();
    info!(
        "total_write_count {}, region number {}, move count {}, transfer count {}",
        total_write_count, region_number, total_move_count, total_transfer_count,
    );
}

fn spawn_write(idx: usize, mut client: ClusterClient) -> JoinHandle<()> {
    std::thread::spawn(move || {
        // Make sure each write thread don't conflict with others.
        let begin = idx * 2000;
        let end = begin + 2000 - 10;
        let start_time = Instant::now();
        let mut rng = rand::thread_rng();
        while start_time.saturating_elapsed() < TIMEOUT {
            let i = rng.gen_range(begin..end);
            client.put_kv(i..(i + 10), i_to_key, i_to_val);
            WRITE_COUNTER.fetch_add(10, Ordering::SeqCst);
        }
        info!("write thread {} exit", idx);
    })
}

fn spawn_move(scheduler: Scheduler, two_node_down: Arc<RwLock<()>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < TIMEOUT {
            sleep(Duration::from_millis(1000));
            let guard = two_node_down.read().unwrap();
            scheduler.move_random_region();
            drop(guard);
            MOVE_COUNTER.fetch_add(1, Ordering::SeqCst);
        }
        info!("move thread exit");
    })
}

fn spawn_transfer(scheduler: Scheduler) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let start_time = Instant::now();
        while start_time.saturating_elapsed() < TIMEOUT {
            sleep(Duration::from_millis(100));
            if scheduler.transfer_random_leader() {
                TRANSFER_COUNTER.fetch_add(1, Ordering::SeqCst);
            }
        }
        info!("transfer thread exit");
    })
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
