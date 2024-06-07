use std::{
    any::Any,
    sync::Arc,
    time::{Instant},
};

use engine_rocks::RocksEngine;

use external_storage::{BackendConfig, BlobStore, S3Storage, WalkExternalStorage};
use futures::stream::{self, StreamExt, TryStreamExt};
use kvproto::brpb::{Gcs, StorageBackend, S3};

use super::{
    compaction::CollectCompaction,
    execute::{Execution},
    storage::{LoadFromExt},
};
use crate::{
    compact::{
        compaction::{CollectCompactionConfig, CompactLogExt, CompactWorker},
        statistic::{CompactStatistic, LoadStatistic},
        storage::StreamyMetaStorage,
    },
    compact_logs::ExecHooks,
};

#[tokio::test]
#[ignore]
async fn playground() {
    let mut backend = StorageBackend::new();
    let mut s3 = S3::new();
    s3.endpoint = "http://10.2.7.193:9000".to_owned();
    s3.force_path_style = true;
    s3.access_key = "minioadmin".to_owned();
    s3.secret_access_key = "minioadmin".to_owned();
    s3.bucket = "astro".to_owned();
    s3.prefix = "tpcc-1000-incr".to_owned();
    backend.set_s3(s3);
    let storage = external_storage::create_storage(&backend, BackendConfig::default()).unwrap()
        as Box<dyn Any>;
    let storage = storage.downcast::<BlobStore<S3Storage>>().unwrap();
    let now = Instant::now();

    let mut ext = LoadFromExt::default();
    ext.max_concurrent_fetch = 128;
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.logs).map(Ok).left_stream(),
        Err(err) => stream::once(futures::future::err(err)).right_stream(),
    });
    let collect = CollectCompaction::new(
        stream,
        CollectCompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
        },
    );
    println!("{:?}", now.elapsed());
    let compaction = collect
        .try_filter(|_f| futures::future::ready(true))
        .next()
        .await
        .unwrap();
    println!("{:?}", now.elapsed());
    println!("{:?}", compaction);
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();
    let arc_store = Arc::from(*storage);
    let mut compact_worker = CompactWorker::<RocksEngine>::inplace(Arc::clone(&arc_store) as _);
    let mut load_stat = LoadStatistic::default();
    let mut compact_stat = CompactStatistic::default();
    let c_ext = CompactLogExt {
        load_statistic: Some(&mut load_stat),
        compact_statistic: Some(&mut compact_stat),
        max_load_concurrency: 32,
    };
    compact_worker
        .compact_ext(compaction.unwrap(), c_ext)
        .await
        .unwrap();

    println!("{:?}\n{:?}", load_stat, compact_stat);
    let mut file = std::fs::File::create("/tmp/pprof.svg").unwrap();
    guard
        .report()
        .build()
        .unwrap()
        .flamegraph(&mut file)
        .unwrap();
}

#[test]
fn cli_playground() {
    let mut backend = StorageBackend::new();
    let mut s3 = S3::new();
    s3.endpoint = "http://10.2.7.193:9000".to_owned();
    s3.force_path_style = true;
    s3.access_key = "minioadmin".to_owned();
    s3.secret_access_key = "minioadmin".to_owned();
    s3.bucket = "astro".to_owned();
    s3.prefix = "tpcc-1000-incr".to_owned();
    backend.set_s3(s3);

    let exec = Execution {
        from_ts: 0,
        until_ts: u64::MAX,
        max_concurrent_compaction: 16,
        external_storage: backend,
    };
    #[derive(Default)]
    struct EventuallyStatistic {
        load_stat: LoadStatistic,
        compact_stat: CompactStatistic,
    }
    impl ExecHooks for EventuallyStatistic {
        fn after_compaction_end(
            &mut self,
            _cid: crate::compact_logs::CId,
            lst: LoadStatistic,
            cst: CompactStatistic,
        ) {
            self.load_stat.merge_with(&lst);
            self.compact_stat.merge_with(&cst);
        }

        fn after_finish(&mut self) {
            println!("All compcations done.");
            println!("{:?}\n{:?}", self.load_stat, self.compact_stat);
        }
    }

    exec.run(EventuallyStatistic::default()).unwrap();
}

#[tokio::test]
async fn gcloud() {
    let mut backend = StorageBackend::new();
    let mut gcs = Gcs::new();
    gcs.bucket = "br-datasets".to_owned();
    gcs.prefix = "pitr-compaction-test/log".to_owned();
    gcs.credentials_blob = String::from_utf8(
        std::fs::read("/root/.config/gcloud/application_default_credentials.json").unwrap(),
    )
    .unwrap();
    backend.set_gcs(gcs);
    let storage =
        external_storage::create_walkable_storage(&backend, BackendConfig::default()).unwrap();

    let now = Instant::now();
    let mut ext = LoadFromExt::default();
    ext.max_concurrent_fetch = 128;
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.logs).map(Ok).left_stream(),
        Err(err) => stream::once(futures::future::err(err)).right_stream(),
    });
    let mut coll = CollectCompaction::new(
        stream,
        CollectCompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
        },
    );
    let compaction = coll.try_next().await;
    println!("{:?}", compaction);
    println!("{:?}", now.elapsed());
    let mut load_stat = LoadStatistic::default();
    let mut compact_stat = CompactStatistic::default();
    let c_ext = CompactLogExt {
        load_statistic: Some(&mut load_stat),
        compact_statistic: Some(&mut compact_stat),
        max_load_concurrency: 32,
    };
    drop(coll);
    let arc_store: Arc<dyn WalkExternalStorage> = Arc::from(storage);
    let mut compact_worker = CompactWorker::<RocksEngine>::inplace(Arc::clone(&arc_store) as _);
    compact_worker
        .compact_ext(compaction.unwrap().unwrap(), c_ext)
        .await
        .unwrap();
    println!("{:?}\n{:?}", load_stat, compact_stat);
}
