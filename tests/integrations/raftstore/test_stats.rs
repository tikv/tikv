// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::pdpb::QueryKind;

use futures::executor::block_on;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb_grpc::TikvClient;
use pd_client::PdClient;
use raftstore::store::QueryStats;
use test_raftstore::*;
use tikv_util::config::*;

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
    fail::cfg("on_report_vec_too_large", "panic").unwrap();
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
    fail::remove("mock_hotspot_capacity");
    fail::remove("mock_tick_interval");
    fail::remove("mock_hotspot_threshold");
}

type Query = dyn Fn(Context, &Cluster<ServerCluster>, TikvClient, u64, u64, Vec<u8>);

#[test]
fn test_query_stats() {
    let raw_get: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = RawGetRequest::default();
        req.set_context(ctx.clone());
        req.key = start_key;
        client.raw_get(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Get,
            1
        ));
    });
    let raw_batch_get: Box<Query> =
        Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
            let mut req = RawBatchGetRequest::default();
            req.set_context(ctx.clone());
            req.set_keys(protobuf::RepeatedField::from(vec![start_key]));
            client.raw_batch_get(&req).unwrap();
            assert!(check_query_num_read(
                cluster,
                store_id,
                region_id,
                QueryKind::Get,
                1
            ));
        });
    let raw_scan: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = RawScanRequest::default();
        req.set_context(ctx.clone());
        req.start_key = start_key;
        req.end_key = vec![];
        client.raw_scan(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Scan,
            1
        ));
    });
    let raw_batch_scan: Box<Query> =
        Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
            let mut req = RawBatchScanRequest::default();
            let mut key_range = KeyRange::default();
            key_range.set_start_key(start_key);
            key_range.set_end_key(vec![]);
            req.set_context(ctx.clone());
            req.set_ranges(protobuf::RepeatedField::from(vec![key_range]));
            client.raw_batch_scan(&req).unwrap();
            assert!(check_query_num_read(
                cluster,
                store_id,
                region_id,
                QueryKind::Scan,
                1
            ));
        });
    let get: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = GetRequest::default();
        req.set_context(ctx.clone());
        req.key = start_key;
        client.kv_get(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Get,
            1
        ));
    });
    let batch_get: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = BatchGetRequest::default();
        req.set_context(ctx.clone());
        req.set_keys(protobuf::RepeatedField::from(vec![start_key]));
        client.kv_batch_get(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Get,
            1
        ));
    });
    let scan: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = ScanRequest::default();
        req.set_context(ctx.clone());
        req.start_key = start_key;
        req.end_key = vec![];
        client.kv_scan(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Scan,
            1
        ));
    });
    let scan_lock: Box<Query> = Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
        let mut req = ScanLockRequest::default();
        req.set_context(ctx.clone());
        req.start_key = start_key;
        req.end_key = vec![];
        client.kv_scan_lock(&req).unwrap();
        assert!(check_query_num_read(
            cluster,
            store_id,
            region_id,
            QueryKind::Scan,
            1
        ));
    });
    let get_key_ttl: Box<Query> =
        Box::new(|ctx, cluster, client, store_id, region_id, start_key| {
            let mut req = RawGetKeyTtlRequest::default();
            req.set_context(ctx.clone());
            req.key = start_key;
            client.raw_get_key_ttl(&req).unwrap(); //todo test enable ttl
            assert!(check_query_num_read(
                cluster,
                store_id,
                region_id,
                QueryKind::Get,
                1
            ));
        });

    fail::cfg("mock_hotspot_threshold", "return(0)").unwrap();
    fail::cfg("mock_tick_interval", "return(0)").unwrap();
    test_query_num(raw_get);
    test_query_num(raw_batch_get);
    test_query_num(raw_scan);
    test_query_num(raw_batch_scan);
    test_query_num(get);
    test_query_num(batch_get);
    test_query_num(scan);
    test_query_num(scan_lock);
    test_query_num(get_key_ttl);
    fail::remove("mock_tick_interval");
    fail::remove("mock_hotspot_threshold");
}

fn test_query_num(query: Box<Query>) {
    let (cluster, client, ctx) = must_new_and_configure_cluster_and_kv_client(|cluster| {
        cluster.cfg.raft_store.pd_store_heartbeat_tick_interval = ReadableDuration::millis(50);
        cluster.cfg.storage.enable_ttl = true;
    });
    let (k, v) = (b"key".to_vec(), b"v2".to_vec());
    let store_id = 1;

    let raw_put = |key| {
        let mut put_req = RawPutRequest::default();
        put_req.set_context(ctx.clone());
        put_req.key = key;
        put_req.value = v.clone();
        let put_resp = client.raw_put(&put_req).unwrap();
        assert!(!put_resp.has_region_error());
        assert!(put_resp.error.is_empty());
        assert!(check_query_num_write(&cluster, store_id, QueryKind::Put, 1));
    };
    let put = |key: Vec<u8>| {
        // Prewrite
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
        {
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.key = key.clone();
            mutation.value = v.clone();
            let mut prewrite_req = PrewriteRequest::default();
            prewrite_req.set_context(ctx.clone());
            prewrite_req.set_mutations(vec![mutation].into_iter().collect());
            prewrite_req.primary_lock = k.clone();
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
        assert!(check_query_num_write(&cluster, store_id, QueryKind::Put, 1));
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
        assert!(check_query_num_write(&cluster, store_id, QueryKind::Put, 1));
    };

    raw_put(k.clone());
    put(k.clone());
    let region_id = cluster.get_region_id(&k);
    query(
        ctx.clone(),
        &cluster,
        client.clone(),
        store_id,
        region_id,
        k.clone(),
    );

    // Raw Delete
    let mut delete_req = RawDeleteRequest::default();
    delete_req.set_context(ctx.clone());
    delete_req.key = k.clone();
    client.raw_delete(&delete_req).unwrap();
    assert!(check_query_num_write(
        &cluster,
        store_id,
        QueryKind::Delete,
        1
    ));

    // DeleteRange
    let mut delete_req = DeleteRangeRequest::default();
    delete_req.set_context(ctx.clone());
    delete_req.set_start_key(k.clone());
    delete_req.set_end_key(vec![]);
    client.kv_delete_range(&delete_req).unwrap();
    assert!(check_query_num_write(
        &cluster,
        store_id,
        QueryKind::DeleteRange,
        1
    ));

    raw_put(k.clone());
    put(k.clone());
    // Raw DeleteRange
    let mut delete_req = RawDeleteRangeRequest::default();
    delete_req.set_context(ctx.clone());
    delete_req.set_start_key(k.clone());
    delete_req.set_end_key(vec![]);
    client.raw_delete_range(&delete_req).unwrap();
    assert!(check_query_num_write(
        &cluster,
        store_id,
        QueryKind::DeleteRange,
        1
    ));
}

fn check_query_num_read(
    cluster: &Cluster<ServerCluster>,
    store_id: u64,
    region_id: u64,
    kind: QueryKind,
    expect: u64,
) -> bool {
    let start = std::time::SystemTime::now();
    loop {
        sleep_ms(10);
        if let Some(hot_peers) = cluster.pd_client.get_store_hotspots(store_id) {
            let peer_stat = hot_peers.get(&region_id).unwrap();
            let query_stat = peer_stat.get_query_stats();
            if QueryStats::get_query_num(query_stat, kind) == expect {
                return true;
            }
        }
        if start.elapsed().unwrap().as_secs() > 5 {
            return false;
        }
    }
}

fn check_query_num_write(
    cluster: &Cluster<ServerCluster>,
    store_id: u64,
    kind: QueryKind,
    expect: u64,
) -> bool {
    let start = std::time::SystemTime::now();
    loop {
        sleep_ms(10);
        if let Some(hb) = cluster.pd_client.get_store_stats(store_id) {
            if QueryStats::get_query_num(hb.get_query_stats(), kind) >= expect {
                return true;
            }
        }
        if start.elapsed().unwrap().as_secs() > 5 {
            return false;
        }
    }
}
