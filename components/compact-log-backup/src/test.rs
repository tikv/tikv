use std::{any::Any, collections::HashMap, sync::Arc, time::Instant};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, BlobStore, FullFeaturedStorage, S3Storage};
use futures::stream::{self, StreamExt, TryStreamExt};
use kvproto::brpb::{self, Gcs, StorageBackend, S3};

use crate::{
    compaction::{
        collector::{CollectCompaction, CollectCompactionConfig},
        exec::{CompactLogExt, SingleCompactionExec},
        meta::{CompactionRunConfig, CompactionRunInfoBuilder},
    },
    execute::{ExecHooks, Execution},
    statistic::{CompactStatistic, LoadStatistic},
    storage::{LoadFromExt, StreamyMetaStorage},
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
    // Stream<Output = Result<LogFileMeta>>
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
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
    let mut compactions = collect.try_collect::<Vec<_>>().await.unwrap();
    println!("{:?}", now.elapsed());
    let guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .unwrap();
    println!("{}", compactions.len());
    let compaction = compactions.swap_remove(0);
    println!("{:?}", compaction);
    let arc_store = Arc::from(*storage);
    let compact_worker = SingleCompactionExec::<RocksEngine>::inplace(Arc::clone(&arc_store) as _);
    let mut load_stat = LoadStatistic::default();
    let mut compact_stat = CompactStatistic::default();
    let c_ext = CompactLogExt {
        load_statistic: Some(&mut load_stat),
        compact_statistic: Some(&mut compact_stat),
        max_load_concurrency: 32,
        ..Default::default()
    };
    compact_worker.compact_ext(compaction, c_ext).await.unwrap();

    println!("{:?}\n{:?}", load_stat, compact_stat);
    let mut file = std::fs::File::create("/tmp/pprof.svg").unwrap();
    guard
        .report()
        .build()
        .unwrap()
        .flamegraph(&mut file)
        .unwrap();
}

#[tokio::test]
async fn playground_no_pref() {
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
    let _now = Instant::now();

    let mut ext = LoadFromExt::default();
    ext.max_concurrent_fetch = 128;
    // Stream<Output = Result<LogFileMeta>>
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
        Err(err) => stream::once(futures::future::err(err)).right_stream(),
    });
    let collect = CollectCompaction::new(
        stream,
        CollectCompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
        },
    );

    let compactions = collect.try_collect::<Vec<_>>().await.unwrap();
    let mut sources = HashMap::<String, Vec<brpb::Span>>::new();
    let mut compacted_files = CompactionRunInfoBuilder::new(CompactionRunConfig {
        from_ts: 0,
        until_ts: u64::MAX,
        name: "test".to_owned(),
    });
    for compact in &compactions {
        compacted_files.add_compaction(compact.clone());
        for source in &compact.inputs {
            if sources.contains_key(source.id.name.as_ref()) {
                sources
                    .get_mut(source.id.name.as_ref())
                    .unwrap()
                    .push(source.id.span());
            } else {
                sources.insert(source.id.name.to_string(), vec![source.id.span()]);
            }
        }
    }
    let minimal_size = sources.iter().fold(0, |mut sum, (k, v)| {
        sum += k.len();
        sum += v.len() * std::mem::size_of::<(u64, u64)>();
        sum
    });
    let maximum_size = sources.iter().fold(0, |mut sum, (k, v)| {
        sum += k.len() * v.len();
        sum += v.len() * std::mem::size_of::<(u64, u64)>();
        sum
    });
    let compaction_size = compactions.iter().fold(0, |sum, com| sum + com.size);
    let hash = compactions.iter().fold(0, |hash, c| hash ^ c.crc64());
    let expired = compacted_files
        .find_expiring_files(storage.as_ref())
        .await
        .unwrap();
    let to_delete = expired.to_delete().count();
    let segments = expired.spans().count();
    println!("full = {} / segmented = {}", to_delete, segments);
    dbg!(expired.spans().take(15).collect::<Vec<_>>());

    println!(
        "{} ~ {} total {} files, {} compactions totally {} hash {}",
        minimal_size,
        maximum_size,
        sources.len(),
        compactions.len(),
        compaction_size,
        hash
    );
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
        fn after_a_compaction_end(
            &mut self,
            _cid: crate::execute::CId,
            lst: LoadStatistic,
            cst: CompactStatistic,
        ) {
            self.load_stat += lst;
            self.compact_stat += cst;
        }

        fn after_execution_finished(&mut self) {
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
        external_storage::create_full_featured_storage(&backend, BackendConfig::default()).unwrap();

    let now = Instant::now();
    let mut ext = LoadFromExt::default();
    ext.max_concurrent_fetch = 128;
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
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
        ..Default::default()
    };
    drop(coll);
    let arc_store: Arc<dyn FullFeaturedStorage> = Arc::from(storage);
    let compact_worker = SingleCompactionExec::<RocksEngine>::inplace(Arc::clone(&arc_store) as _);
    compact_worker
        .compact_ext(compaction.unwrap().unwrap(), c_ext)
        .await
        .unwrap();
    println!("{:?}\n{:?}", load_stat, compact_stat);
}

#[tokio::test]
async fn gcloud_count() {
    test_util::init_log_for_test();
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
        external_storage::create_full_featured_storage(&backend, BackendConfig::default()).unwrap();

    let n = StreamyMetaStorage::count_objects(storage.as_ref())
        .await
        .unwrap();
    println!("{n}");
}
