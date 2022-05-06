// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    future::Future,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use concurrency_manager::ConcurrencyManager;
use futures::{executor::block_on, StreamExt};
use kvproto::kvrpcpb::Context;
use test_coprocessor::{DAGSelect, Insert, ProductTable, Store};
use tidb_query_datatype::codec::Datum;
use tikv::{
    config::CoprReadPoolConfig,
    coprocessor::{readpool_impl, Endpoint},
    read_pool::ReadPool,
    storage::RocksEngine,
};
use tikv_util::{
    config::ReadableDuration, quota_limiter::QuotaLimiter, thread_group::GroupProperties,
};
use txn_types::{Key, TimeStamp};

use crate::resource_metering::test_suite::TestSuite;

#[test]
pub fn test_prewrite() {
    let tag = "tag_prewrite";

    let (test_suite, mut store, _) = setup_test_suite();
    fail::cfg_callback("scheduler_process", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!(fail::remove("scheduler_process"));

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let mut ctx = Context::default();
    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute_with_ctx(ctx);

    assert!(block_on(jh).unwrap());
}

#[test]
pub fn test_commit() {
    let tag = "tag_commit";

    let (test_suite, mut store, _) = setup_test_suite();
    fail::cfg_callback("scheduler_process", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!(fail::remove("scheduler_process"));

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute();

    let mut ctx = Context::default();
    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
    store.commit_with_ctx(ctx);

    assert!(block_on(jh).unwrap());
}

#[test]
pub fn test_reschedule_coprocessor() {
    let tag = "tag_coprocessor";

    let (test_suite, mut store, endpoint) = setup_test_suite();
    fail::cfg("copr_reschedule", "return").unwrap();
    fail::cfg_callback("scanner_next", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!({
        fail::remove("scanner_next");
        fail::remove("copr_reschedule");
    });

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute();
    store.commit();

    let mut req = DAGSelect::from(&table).build();
    let mut ctx = Context::default();
    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
    req.set_context(ctx);
    assert!(
        !block_on(endpoint.parse_and_handle_unary_request(req, None))
            .consume()
            .get_data()
            .is_empty()
    );

    assert!(block_on(jh).unwrap());
}

#[test]
fn test_get() {
    let tag = "tag_get";

    let (test_suite, mut store, _) = setup_test_suite();
    fail::cfg_callback("point_getter_get", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!(fail::remove("point_getter_get"));

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute();
    store.commit();

    let storage = store.get_storage();
    for (k, v) in store.export() {
        let mut ctx = Context::default();
        ctx.set_resource_group_tag(tag.as_bytes().to_vec());
        assert_eq!(
            storage
                .get(ctx, &Key::from_raw(&k), TimeStamp::max())
                .unwrap()
                .0
                .unwrap()
                .as_slice(),
            v.as_slice()
        );
    }

    assert!(block_on(jh).unwrap());
}

#[test]
fn test_batch_get() {
    let tag = "tag_batch_get";

    let (test_suite, mut store, _) = setup_test_suite();
    fail::cfg_callback("point_getter_get", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!(fail::remove("point_getter_get"));

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute();
    store.commit();

    let storage = store.get_storage();
    let kvs = store.export();

    let mut ctx = Context::default();
    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
    let keys = kvs
        .iter()
        .map(|(k, _)| Key::from_raw(k))
        .collect::<Vec<_>>();
    let res = storage.batch_get(ctx, &keys, TimeStamp::max()).unwrap().0;

    let expected_values = kvs.iter().map(|(_, v)| v);
    let returned_values = res.iter().map(|kv| &kv.as_ref().unwrap().1);
    for (expected, returned) in expected_values.zip(returned_values) {
        assert_eq!(expected, returned);
    }
    assert!(block_on(jh).unwrap());
}

#[test]
fn test_batch_get_command() {
    let tag = "tag_batch_get_command";

    let (test_suite, mut store, _) = setup_test_suite();
    fail::cfg_callback("point_getter_get", || cpu_load(Duration::from_millis(100))).unwrap();
    defer!(fail::remove("point_getter_get"));

    let jh = test_suite
        .rt
        .spawn(require_cpu_time_not_zero(&test_suite, tag));

    let table = ProductTable::new();
    let insert = prepare_insert(&mut store, &table);
    insert.execute();
    store.commit();

    let storage = store.get_storage();
    let kvs = store.export();

    let mut ctx = Context::default();
    ctx.set_resource_group_tag(tag.as_bytes().to_vec());
    let keys = kvs.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>();
    let res = storage.batch_get_command(ctx, &keys, u64::MAX).unwrap();

    let expected_values = kvs.iter().map(|(_, v)| v);
    let returned_values = res.iter().map(|kv| kv.as_ref().unwrap());
    for (expected, returned) in expected_values.zip(returned_values) {
        assert_eq!(expected, returned);
    }
    assert!(block_on(jh).unwrap());
}

fn setup_test_suite() -> (TestSuite, Store<RocksEngine>, Endpoint<RocksEngine>) {
    let test_suite = TestSuite::new(resource_metering::Config {
        report_receiver_interval: ReadableDuration::secs(3),
        precision: ReadableDuration::secs(1),
        ..Default::default()
    });
    let store = Store::from_storage(test_suite.get_storage());
    tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
    let pool = ReadPool::from(readpool_impl::build_read_pool_for_test(
        &CoprReadPoolConfig::default_for_test(),
        store.get_engine(),
    ));
    let cm = ConcurrencyManager::new(1.into());
    let endpoint = Endpoint::new(
        &Default::default(),
        pool.handle(),
        cm,
        test_suite.get_tag_factory(),
        Arc::new(QuotaLimiter::default()),
    );
    (test_suite, store, endpoint)
}

fn prepare_insert<'a>(
    store: &'a mut Store<RocksEngine>,
    table: &'a ProductTable,
) -> Insert<'a, RocksEngine> {
    store.begin();
    store
        .insert_into(table)
        .set(&table["id"], Datum::I64(1))
        .set(&table["name"], Datum::Bytes(b"name:0".to_vec()))
        .set(&table["count"], Datum::I64(2))
}

fn require_cpu_time_not_zero(
    test_suite: &TestSuite,
    tag: &'static str,
) -> impl Future<Output = bool> {
    let (client, stream) = test_suite.subscribe();
    async move {
        let _client = client;
        let tags = stream
            .filter(|record| {
                let record = record.as_ref().unwrap().clone();
                async move { record.get_record().get_resource_group_tag() == tag.as_bytes() }
            })
            .take(1);
        tags.collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|record| {
                record
                    .as_ref()
                    .unwrap()
                    .get_record()
                    .get_items()
                    .iter()
                    .map(|item| item.cpu_time_ms)
                    .sum::<u32>()
            })
            .sum::<u32>()
            > 0
    }
}

fn cpu_load(duration: Duration) {
    static A: AtomicU64 = AtomicU64::new(0);

    let now = Instant::now();
    while now.elapsed() < duration {
        for _ in 0..100000 {
            A.fetch_add(1, Ordering::SeqCst);
        }
    }
}
