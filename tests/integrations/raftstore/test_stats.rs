// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::kvrpcpb::*;
use pd_client::PdClient;
use std::sync::*;
use std::thread;
use std::time::Duration;
use test_raftstore::*;
use tikv_util::config::*;

use futures::{executor::block_on, SinkExt, StreamExt};
use grpcio::*;

use kvproto::tikvpb::*;

use txn_types::Key;

fn check_available<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    let engine = cluster.get_engine(1);
    let raft_engine = cluster.get_raft_engine(1);

    let stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(stats.get_region_count(), 2);

    let value = vec![0; 1024];
    for i in 0..1000 {
        let last_available = stats.get_available();
        cluster.must_put(format!("k{}", i).as_bytes(), &value);
        raft_engine.flush(true).unwrap();
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
    let pd_client = Arc::clone(&cluster.pd_client);

    cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(20);
    cluster.run();

    // wait store reports stats.
    for _ in 0..100 {
        sleep_ms(20);

        if pd_client.get_store_stats(1).is_some() {
            break;
        }
    }

    let engine = cluster.get_engine(1);
    let raft_engine = cluster.get_raft_engine(1);
    raft_engine.flush(true).unwrap();
    engine.flush(true).unwrap();
    let last_stats = pd_client.get_store_stats(1).unwrap();
    assert_eq!(last_stats.get_region_count(), 1);

    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");

    let region = pd_client.get_region(b"").unwrap();
    cluster.must_split(&region, b"k2");
    raft_engine.flush(true).unwrap();
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
fn test_store_heartbeat_report_hotspots() {
    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    let (mut cluster, client, _) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
    });
    let (k1, v1) = (b"k1".to_vec(), b"v1".to_vec());
    let (k3, v3) = (b"k3".to_vec(), b"v3".to_vec());
    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k2");
    cluster.must_put(b"k1", b"v1");
    cluster.must_put(b"k3", b"v3");
    let left = cluster.get_region(b"k1");
    let mut left_ctx = Context::default();
    left_ctx.set_region_id(left.id);
    left_ctx.set_peer(left.get_peers()[0].clone());
    left_ctx.set_region_epoch(cluster.get_region_epoch(left.id));
    let right = cluster.get_region(b"k3");
    let mut right_ctx = Context::default();
    right_ctx.set_region_id(right.id);
    right_ctx.set_peer(right.get_peers()[0].clone());
    right_ctx.set_region_epoch(cluster.get_region_epoch(right.id));

    // raw get k1 100 times
    for _i in 0..100 {
        let mut get_req = RawGetRequest::default();
        get_req.set_context(left_ctx.clone());
        get_req.key = k1.clone();
        let get_resp = client.raw_get(&get_req).unwrap();
        assert_eq!(get_resp.value, v1);
    }
    // raw get k3 10 times
    for _i in 0..10 {
        let mut get_req = RawGetRequest::default();
        get_req.set_context(right_ctx.clone());
        get_req.key = k3.clone();
        let get_resp = client.raw_get(&get_req).unwrap();
        assert_eq!(get_resp.value, v3);
    }
    sleep_ms(50);
    let store_id = 1;
    let hot_peers = cluster.pd_client.get_store_hotspots(store_id).unwrap();
    let peer_stat = hot_peers.get(&left.id).unwrap();
    assert!(peer_stat.get_read_keys() > 0);
    assert!(peer_stat.get_read_bytes() > 0);
    fail::remove("mock_tick_interval");
    fail::remove("mock_hotspot_threshold");
}

type Query = dyn Fn(Context, &Cluster<ServerCluster>, TikvClient, u64, u64, Vec<u8>);

#[test]
fn test_query_stats() {
    let raw_get: Box<Query> = Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
        let mut req = RawGetRequest::default();
        req.set_context(ctx.clone());
        req.key = start_key.clone();
        client.raw_get(&req).unwrap();
        assert!(check_split_key(cluster, start_key));
    });
    let raw_batch_get: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = RawBatchGetRequest::default();
            req.set_context(ctx.clone());
            req.set_keys(protobuf::RepeatedField::from(vec![start_key.clone()]));
            client.raw_batch_get(&req).unwrap();
            assert!(check_split_key(cluster, start_key));
        });
    let raw_scan: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = RawScanRequest::default();
            req.set_context(ctx.clone());
            req.start_key = start_key.clone();
            req.end_key = vec![];
            client.raw_scan(&req).unwrap();
            assert!(check_split_key(cluster, start_key));
        });
    let raw_batch_scan: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = RawBatchScanRequest::default();
            let mut key_range = KeyRange::default();
            key_range.set_start_key(start_key.clone());
            key_range.set_end_key(vec![]);
            req.set_context(ctx.clone());
            req.set_ranges(protobuf::RepeatedField::from(vec![key_range]));
            client.raw_batch_scan(&req).unwrap();
            assert!(check_split_key(cluster, start_key));
        });
    let get: Box<Query> = Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
        let mut req = GetRequest::default();
        req.set_context(ctx.clone());
        req.key = start_key.clone();
        client.kv_get(&req).unwrap();
        assert!(check_split_key(
            cluster,
            Key::from_raw(&start_key).as_encoded().to_vec()
        ));
    });
    let batch_get: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = BatchGetRequest::default();
            req.set_context(ctx.clone());
            req.set_keys(protobuf::RepeatedField::from(vec![start_key.clone()]));
            client.kv_batch_get(&req).unwrap();
            assert!(check_split_key(
                cluster,
                Key::from_raw(&start_key).as_encoded().to_vec()
            ));
        });
    let scan: Box<Query> = Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
        let mut req = ScanRequest::default();
        req.set_context(ctx.clone());
        req.start_key = start_key.clone();
        req.end_key = vec![];
        client.kv_scan(&req).unwrap();
        assert!(check_split_key(
            cluster,
            Key::from_raw(&start_key).as_encoded().to_vec()
        ));
    });
    let scan_lock: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = ScanLockRequest::default();
            req.set_context(ctx.clone());
            req.start_key = start_key.clone();
            req.end_key = vec![];
            client.kv_scan_lock(&req).unwrap();
            assert!(check_split_key(
                cluster,
                Key::from_raw(&start_key).as_encoded().to_vec()
            ));
        });
    let get_key_ttl: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let mut req = RawGetKeyTtlRequest::default();
            req.set_context(ctx.clone());
            req.key = start_key.clone();
            client.raw_get_key_ttl(&req).unwrap();
            assert!(check_split_key(cluster, start_key));
        });
    let raw_batch_get_command: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let get_command: Box<GenRequest> = Box::new(|ctx, start_key| {
                let mut get_req = RawGetRequest::default();
                get_req.set_context(ctx.clone());
                get_req.key = start_key.clone();
                let mut req = BatchCommandsRequestRequest::new();
                req.set_raw_get(get_req);
                return req;
            });
            batch_commands(&ctx, &client, get_command, &start_key);
            assert!(check_split_key(cluster, start_key));
        });
    let batch_get_command: Box<Query> =
        Box::new(|ctx, cluster, client, _store_id, _region_id, start_key| {
            let get_command: Box<GenRequest> = Box::new(|ctx, start_key| {
                let mut get_req = GetRequest::default();
                get_req.set_context(ctx.clone());
                get_req.key = start_key;
                let mut req = BatchCommandsRequestRequest::new();
                req.set_get(get_req);
                return req;
            });
            batch_commands(&ctx, &client, get_command, &start_key);
            assert!(check_split_key(
                cluster,
                Key::from_raw(&start_key).as_encoded().to_vec()
            ));
        });
    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    fail::cfg("mock_collect_interval", "return(0)").unwrap();
    test_query_num(raw_get, true);
    test_query_num(raw_batch_get, true);
    test_query_num(raw_scan, true);
    test_query_num(raw_batch_scan, true);
    test_query_num(get, false);
    test_query_num(batch_get, false);
    test_query_num(scan, false);
    test_query_num(scan_lock, false);
    test_query_num(get_key_ttl, true);
    test_query_num(raw_batch_get_command, true);
    test_query_num(batch_get_command, false);
    fail::remove("mock_tick_interval");
    fail::remove("mock_hotspot_threshold");
    fail::remove("mock_collect_interval");
}

fn raw_put(
    _cluster: &Cluster<ServerCluster>,
    client: &TikvClient,
    ctx: &Context,
    _store_id: u64,
    key: Vec<u8>,
) {
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = key;
    put_req.value = b"v2".to_vec();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());
}

fn put(
    cluster: &Cluster<ServerCluster>,
    client: &TikvClient,
    ctx: &Context,
    _store_id: u64,
    key: Vec<u8>,
) {
    // Prewrite
    let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
    {
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.key = key.clone();
        mutation.value = b"v2".to_vec();
        let mut prewrite_req = PrewriteRequest::default();
        prewrite_req.set_context(ctx.clone());
        prewrite_req.set_mutations(vec![mutation].into_iter().collect());
        prewrite_req.primary_lock = key.clone();
        prewrite_req.start_version = start_ts.into_inner();
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
        assert!(
            !prewrite_resp.has_region_error(),
            "{:?}",
            prewrite_resp.get_region_error()
        );
        assert!(
            prewrite_resp.errors.is_empty(),
            "{:?}",
            prewrite_resp.get_errors()
        );
    }
    // Commit
    {
        let commit_ts = block_on(cluster.pd_client.get_tso()).unwrap();
        let mut commit_req = CommitRequest::default();
        commit_req.set_context(ctx.clone());
        commit_req.start_version = start_ts.into_inner();
        commit_req.set_keys(vec![key.clone()].into_iter().collect());
        commit_req.commit_version = commit_ts.into_inner();
        let commit_resp = client.kv_commit(&commit_req).unwrap();
        assert!(
            !commit_resp.has_region_error(),
            "{:?}",
            commit_resp.get_region_error()
        );
        assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());
    }
}

fn test_query_num(query: Box<Query>, enable_ttl: bool) {
    let (cluster, client, ctx) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
        cluster.cfg.split.qps_threshold = 0;
        cluster.cfg.split.split_balance_score = 2.0;
        cluster.cfg.split.split_contained_score = 2.0;
        cluster.cfg.split.detect_times = 1;
        cluster.cfg.split.sample_threshold = 0;
        cluster.cfg.storage.enable_ttl = enable_ttl;
    });

    let k = b"key".to_vec();
    let store_id = 1;
    if enable_ttl {
        raw_put(&cluster, &client, &ctx, store_id, k.clone());
    } else {
        put(&cluster, &client, &ctx, store_id, k.clone());
    }
    let region_id = cluster.get_region_id(&k);
    query(
        ctx.clone(),
        &cluster,
        client.clone(),
        store_id,
        region_id,
        k.clone(),
    );
}

fn check_split_key(cluster: &Cluster<ServerCluster>, k: Vec<u8>) -> bool {
    let start = std::time::SystemTime::now();
    loop {
        sleep_ms(10);
        let region_num = cluster.pd_client.get_regions_number();
        if region_num == 2 {
            let region = cluster.pd_client.get_region(&k).unwrap();
            assert_eq!(k, region.get_start_key());
            return true;
        }
        if start.elapsed().unwrap().as_secs() > 5 {
            return false;
        }
    }
}

type GenRequest = dyn Fn(&Context, Vec<u8>) -> BatchCommandsRequestRequest;

fn batch_commands(
    ctx: &Context,
    client: &TikvClient,
    gen_request: Box<GenRequest>,
    start_key: &Vec<u8>,
) {
    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..100 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            let req = gen_request(ctx, start_key.clone());
            batch_req.mut_requests().push(req);
            batch_req.mut_request_ids().push(i);
        }
        block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    }
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 1k requests to the server, so we should get 1k responses.
        let mut count = 0;
        for x in block_on(
            receiver
                .map(move |b| b.unwrap().get_responses().len())
                .collect::<Vec<usize>>(),
        ) {
            count += x;
            if count == 1000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(10)).unwrap();
}
