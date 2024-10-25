// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(test)]

use std::sync::Arc;

use criterion::*;
use engine_rocks::{util::new_engine, RocksEngine};
use engine_traits::{
    CacheRegion, KvEngine, Mutable, RegionCacheEngine, WriteBatch, WriteBatchExt, CF_DEFAULT,
    CF_WRITE, DATA_CFS,
};
use futures::future::ready;
use in_memory_engine::{BackgroundRunner, *};
use keys::{DATA_MAX_KEY, DATA_MIN_KEY};
use pd_client::PdClient;
use raftstore::coprocessor::config::SPLIT_SIZE;
use rand::{thread_rng, RngCore};
use tikv_util::config::VersionTrack;
use txn_types::{Key, TimeStamp, Write, WriteType};

/// Benches the performace of background load region
fn bench_load_region(c: &mut Criterion) {
    for value_size in [32, 128, 512, 4096] {
        bench_with_args(c, 128, value_size, SPLIT_SIZE.0 as usize, 100);
    }
}

fn bench_with_args(
    c: &mut Criterion,
    key_size: usize,
    value_size: usize,
    region_size: usize,
    mvcc_amp_thres: usize,
) {
    use std::time::Duration;

    let rocks_engine = prepare_data(key_size, value_size, region_size, mvcc_amp_thres);
    let mut group = c.benchmark_group("load_region");
    // the bench is slow and workload is not useful.
    group.warm_up_time(Duration::from_millis(1)).sample_size(10);
    group.bench_function(format!("value size {}", value_size), |b| {
        b.iter_with_large_drop(|| {
            load_region(&rocks_engine);
        })
    });
}

fn prepare_data(
    key_size: usize,
    value_size: usize,
    region_size: usize,
    mvcc_amp_thres: usize,
) -> RocksEngine {
    let path = tempfile::Builder::new()
        .prefix("bench_load")
        .tempdir()
        .unwrap();
    let path_str = path.path().to_str().unwrap();
    let rocks_engine = new_engine(path_str, DATA_CFS).unwrap();

    // prepare for test data
    let mut r = thread_rng();
    let mut wb = rocks_engine.write_batch();
    let mut ts_count = 1;
    let count = (region_size + key_size + value_size - 1) / (key_size + value_size);
    let mut key = vec![0u8; key_size];
    r.fill_bytes(&mut key[..key_size - 8]);
    let mut key_version_count = 0;
    let mut mvcc_version = r.next_u32() as usize % (mvcc_amp_thres * 2) + 1;
    for _i in 0..count {
        if key_version_count >= mvcc_version {
            r.fill_bytes(&mut key[..key_size - 8]);
            mvcc_version = r.next_u32() as usize % (mvcc_amp_thres * 2) + 1;
            key_version_count = 0;
        }

        let ts = TimeStamp::new(ts_count);
        let k = keys::data_key(Key::from_raw(&key).append_ts(ts).as_encoded());
        let mut value = vec![0u8; value_size];
        r.fill_bytes(&mut value);

        let v = if value_size <= 256 {
            Some(value)
        } else {
            wb.put_cf(CF_DEFAULT, &k, &value).unwrap();
            None
        };
        let w = Write::new(WriteType::Put, ts, v);
        wb.put_cf(CF_WRITE, &k, &w.as_ref().to_bytes()).unwrap();

        key_version_count += 1;
        ts_count += 1;
    }

    wb.write().unwrap();

    rocks_engine
}

fn load_region(rocks_engine: &RocksEngine) {
    let mut engine = RegionCacheMemoryEngine::new(InMemoryEngineContext::new_for_tests(Arc::new(
        VersionTrack::new(InMemoryEngineConfig::config_for_test()),
    )));
    engine.set_disk_engine(rocks_engine.clone());
    let memory_controller = engine.memory_controller();

    // do load
    let config = Arc::new(VersionTrack::new(InMemoryEngineConfig::config_for_test()));
    let (worker, _) = BackgroundRunner::new(
        engine.core().clone(),
        memory_controller.clone(),
        None,
        config,
        Arc::new(MockTsPdClient::new()),
        None,
    );

    let region = CacheRegion::new(1, 1, DATA_MIN_KEY, DATA_MAX_KEY);
    engine.load_region(region.clone()).unwrap();
    // update region state to loading to avoid background load.
    engine.must_set_region_state(1, RegionState::Loading);

    let snapshot = Arc::new(rocks_engine.snapshot());
    worker.run_load_region(region, snapshot);
}

struct MockTsPdClient {
    ts: TimeStamp,
}

impl MockTsPdClient {
    fn new() -> Self {
        // use now to build a big enough timestamp to ensure gc can run.
        let now = TimeStamp::physical_now();
        Self {
            ts: TimeStamp::compose(now, 0),
        }
    }
}

impl PdClient for MockTsPdClient {
    fn get_tso(&self) -> pd_client::PdFuture<txn_types::TimeStamp> {
        Box::pin(ready(Ok(self.ts)))
    }
}

criterion_group!(benches, bench_load_region);
criterion_main!(benches);
