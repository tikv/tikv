// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use concurrency_manager::ConcurrencyManager;
use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError, Sender};
use grpcio::{ChannelBuilder, Environment};
use kvproto::{coprocessor, kvrpcpb::*, resource_usage_agent::ResourceUsageRecord, tikvpb::*};
use protobuf::Message;
use resource_metering::ResourceTagFactory;
use test_coprocessor::{DAGSelect, ProductTable, Store};
use test_raftstore::*;
use test_util::alloc_port;
use tidb_query_datatype::codec::Datum;
use tikv::{
    config::CoprReadPoolConfig,
    coprocessor::{readpool_impl, Endpoint},
    read_pool::ReadPool,
    storage::{Engine, RocksEngine},
};
use tikv_util::{
    config::ReadableDuration, quota_limiter::QuotaLimiter, thread_group::GroupProperties,
    HandyRwLock,
};
use tipb::SelectResponse;

use crate::resource_metering::test_suite::MockReceiverServer;

#[test]
#[ignore = "the case is unstable, ref #11765"]
pub fn test_read_keys() {
    // Create & start receiver server.
    let (tx, rx) = unbounded();
    let mut server = MockReceiverServer::new(tx);
    let port = alloc_port();
    let env = Arc::new(Environment::new(1));
    server.start_server(port, env.clone());

    // Create cluster.
    let (_cluster, client, mut ctx) = new_cluster(port, env);

    // Set resource group tag for enable resource metering.
    ctx.set_resource_group_tag("TEST-TAG".into());

    let mut ts = 0;

    // Write 10 key-value pairs.
    for n in 0..10 {
        let n = n.to_string().into_bytes();
        let (k, v) = (n.clone(), n);

        // Prewrite.
        ts += 1;
        let prewrite_start_version = ts;
        let mut mutation = Mutation::default();
        mutation.set_op(Op::Put);
        mutation.set_key(k.clone());
        mutation.set_value(v.clone());
        must_kv_prewrite(
            &client,
            ctx.clone(),
            vec![mutation],
            k.clone(),
            prewrite_start_version,
        );

        // Commit.
        ts += 1;
        let commit_version = ts;
        must_kv_commit(
            &client,
            ctx.clone(),
            vec![k.clone()],
            prewrite_start_version,
            commit_version,
            commit_version,
        );
    }

    // PointGet
    ts += 1;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.set_key(b"0".to_vec());
    get_req.set_version(ts);
    let _ = client.kv_get(&get_req).unwrap(); // trigger thread register
    std::thread::sleep(Duration::from_secs(2));
    recv_read_keys(&rx);
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(!get_resp.has_error());
    let scan_detail_v2 = get_resp.get_exec_details_v2().get_scan_detail_v2();
    assert_eq!(scan_detail_v2.get_total_versions(), 1);
    assert_eq!(scan_detail_v2.get_processed_versions(), 1);
    assert!(scan_detail_v2.get_processed_versions_size() > 0);
    assert_eq!(get_resp.value, b"0".to_vec());

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 1);

    // Scan 0 ~ 4.
    ts += 1;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.set_start_key(b"0".to_vec());
    scan_req.set_limit(5);
    scan_req.set_version(ts);
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.pairs.len(), 5);

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 5);

    // Scan 0 ~ 9.
    ts += 1;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.set_start_key(b"0".to_vec());
    scan_req.set_limit(100);
    scan_req.set_version(ts);
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.pairs.len(), 10);

    // Wait & receive & assert.
    assert_eq!(must_recv_read_keys(&rx), 10);

    // Shutdown receiver server.
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        server.shutdown_server().await;
    });
}

fn new_cluster(port: u16, env: Arc<Environment>) -> (Cluster<ServerCluster>, TikvClient, Context) {
    let (cluster, leader, ctx) = must_new_and_configure_cluster(|cluster| {
        cluster.cfg.resource_metering.receiver_address = format!("127.0.0.1:{}", port);
        cluster.cfg.resource_metering.precision = ReadableDuration::millis(100);
        cluster.cfg.resource_metering.report_receiver_interval = ReadableDuration::millis(400);
    });
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);
    (cluster, client, ctx)
}

fn must_recv_read_keys(rx: &Receiver<Vec<ResourceUsageRecord>>) -> u32 {
    const MAX_WAIT_SECS: u32 = 30;
    let duration = Duration::from_secs(1);
    for _ in 0..MAX_WAIT_SECS {
        std::thread::sleep(duration);
        let read_keys = recv_read_keys(rx);
        if read_keys > 0 {
            return read_keys;
        }
    }
    panic!("no read_keys");
}

fn recv_read_keys(rx: &Receiver<Vec<ResourceUsageRecord>>) -> u32 {
    let mut total = 0;
    while let Ok(records) = rx.try_recv() {
        for r in &records {
            total += r
                .get_record()
                .get_items()
                .iter()
                .map(|item| item.read_keys)
                .sum::<u32>();
        }
    }
    total
}

#[test]
#[ignore = "the case is unstable, ref #11765"]
fn test_read_keys_coprocessor() {
    // Start resource metering.
    let mut cfg = resource_metering::Config::default();
    cfg.precision = ReadableDuration::millis(100);
    cfg.report_receiver_interval = ReadableDuration::millis(400);

    let (_, collector_reg_handle, resource_tag_factory, recorder_worker) =
        resource_metering::init_recorder(cfg.precision.as_millis());
    let (_, data_sink_reg_handle, reporter_worker) =
        resource_metering::init_reporter(cfg, collector_reg_handle);

    let data_sink = MockDataSink::new();
    let _reg_guard = data_sink_reg_handle.register(Box::new(data_sink.clone()));

    // Init data.
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];
    let product = ProductTable::new();
    let endpoint = init_coprocessor_with_data(&product, &data, resource_tag_factory);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // Do DAG select to register runtime thread.
    let mut req = DAGSelect::from(&product).build();
    let mut ctx = Context::default();
    ctx.set_resource_group_tag("TEST-TAG".into());
    req.set_context(ctx);
    runtime.block_on(handle_select(&endpoint, req.clone()));

    // Clear current result.
    let _ = data_sink.wait_read_keys(Duration::from_secs(3));

    // Do DAG select again.
    runtime.block_on(handle_select(&endpoint, req));

    // Wait & receive & assert.
    assert_eq!(
        data_sink.wait_read_keys(Duration::from_secs(30)).unwrap(),
        4
    );

    // Cleanup.
    reporter_worker.stop_worker();
    recorder_worker.stop_worker();
}

fn init_coprocessor_with_data(
    tbl: &ProductTable,
    vals: &[(i64, Option<&str>, i64)],
    tag_factory: ResourceTagFactory,
) -> Endpoint<RocksEngine> {
    let mut store = Store::default();
    store.begin();
    for &(id, name, count) in vals {
        store
            .insert_into(tbl)
            .set(&tbl["id"], Datum::I64(id))
            .set(&tbl["name"], name.map(str::as_bytes).into())
            .set(&tbl["count"], Datum::I64(count))
            .execute_with_ctx(Context::default());
    }
    store.commit_with_ctx(Context::default());
    tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
    let pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
        &CoprReadPoolConfig::default_for_test(),
        store.get_engine(),
    ));
    let cm = ConcurrencyManager::new(1.into());
    Endpoint::new(
        &tikv::server::Config::default(),
        pool.handle(),
        cm,
        tag_factory,
        Arc::new(QuotaLimiter::default()),
    )
}

async fn handle_select<E>(copr: &Endpoint<E>, req: coprocessor::Request) -> SelectResponse
where
    E: Engine,
{
    let resp = copr
        .parse_and_handle_unary_request(req, None)
        .await
        .consume();
    assert!(!resp.get_data().is_empty(), "{:?}", resp);
    let mut sel_resp = SelectResponse::default();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

#[derive(Clone)]
struct MockDataSink {
    tx: Sender<u32>,
    rx: Receiver<u32>,
}

impl MockDataSink {
    fn new() -> Self {
        let (tx, rx) = unbounded();
        Self { tx, rx }
    }

    fn wait_read_keys(&self, timeout: Duration) -> Result<u32, RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }
}

impl resource_metering::DataSink for MockDataSink {
    fn try_send(
        &mut self,
        records: Arc<Vec<ResourceUsageRecord>>,
    ) -> resource_metering::error::Result<()> {
        let mut read_keys = 0;
        for r in records.iter() {
            read_keys += r
                .get_record()
                .get_items()
                .iter()
                .map(|item| item.read_keys)
                .sum::<u32>();
        }
        self.tx.send(read_keys).unwrap();
        Ok(())
    }
}
