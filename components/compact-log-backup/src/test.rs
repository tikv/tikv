// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{any::Any, collections::HashMap, sync::Arc, time::Instant};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, BlobStore, IterableExternalStorage, S3Storage};
use futures::stream::{self, StreamExt, TryStreamExt};
use kvproto::brpb::{self, Gcs, StorageBackend, S3};
use tikv_util::config::ReadableSize;
use tokio::{io::AsyncWriteExt, signal::unix::SignalKind};

use crate::{
    compaction::{
        collector::{CollectSubcompaction, CollectSubcompactionConfig},
        exec::{SubcompactExt, SubcompactionExec},
        meta::CompactionRunInfoBuilder,
        SubcompactionResult,
    },
    execute::{hooks::SaveMeta, Execution, ExecutionConfig},
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
    s3.prefix = "tpcc-1000-incr-with-crc64".to_owned();
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
    let collect = CollectSubcompaction::new(
        stream,
        CollectSubcompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
            subcompaction_size_threshold: ReadableSize::mb(128).0,
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
    let compaction = compactions.pop().unwrap();
    println!("{:?}", compaction);
    let arc_store = Arc::from(*storage);
    let compact_worker =
        SubcompactionExec::<RocksEngine>::default_config(Arc::clone(&arc_store) as _);
    let _load_stat = LoadStatistic::default();
    let _compact_stat = CompactStatistic::default();
    let c_ext = SubcompactExt {
        max_load_concurrency: 32,
        ..Default::default()
    };
    let res = compact_worker.run(compaction, c_ext).await.unwrap();

    println!("{:?}", res);
    println!("{:?}", res.verify_checksum());
    let mut file = std::fs::File::create("/tmp/pprof.svg").unwrap();
    guard
        .report()
        .build()
        .unwrap()
        .flamegraph(&mut file)
        .unwrap();
}

fn start_async_backtrace() {
    tracing_active_tree::init();

    let sigusr1_handler = async {
        let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
        while signal.recv().await.is_some() {
            let file_name = "/tmp/compact-sst.dump".to_owned();
            let res = async {
                let mut file = tokio::fs::File::create(&file_name).await?;
                file.write_all(&tracing_active_tree::layer::global().fmt_bytes())
                    .await
            }
            .await;
            match res {
                Ok(_) => eprintln!("dumped to {}", file_name),
                Err(err) => eprintln!("failed to dump because {}", err),
            }
        }
    };

    eprintln!("handler started, pid = {:?}", std::thread::current().id());
    tokio::spawn(sigusr1_handler);
}

#[tokio::test]
#[ignore]
async fn playground_no_pref() {
    start_async_backtrace();

    let mut backend = StorageBackend::new();
    let mut s3 = S3::new();
    s3.endpoint = "http://10.2.7.193:9000".to_owned();
    s3.force_path_style = true;
    s3.access_key = "minioadmin".to_owned();
    s3.secret_access_key = "minioadmin".to_owned();
    s3.bucket = "astro".to_owned();
    s3.prefix = "tpcc-1000-incr-with-crc64".to_owned();
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
    let collect = CollectSubcompaction::new(
        stream,
        CollectSubcompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
            subcompaction_size_threshold: ReadableSize::mb(128).0,
        },
    );

    let compactions = collect.try_collect::<Vec<_>>().await.unwrap();
    let mut sources = HashMap::<String, Vec<brpb::Span>>::new();
    let mut compacted_files = CompactionRunInfoBuilder::default();
    println!("{:?}", compactions[0]);
    for compact in &compactions {
        compacted_files.add_subcompaction(&SubcompactionResult::of(compact.clone()));
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
    let min_ts = compactions.iter().map(|c| c.input_min_ts).min().unwrap();
    let max_ts = compactions.iter().map(|c| c.input_max_ts).max().unwrap();

    let mig = compacted_files.migration(storage.as_ref()).await.unwrap();
    for em in &mig.edit_meta {
        println!("====={}=====", em.path);
        println!("{:?}", em.delete_physical_files);
        println!("{:?}", em.delete_logical_files);
    }

    println!(
        "{} ~ {} total {} files, {} compactions totally {} hash {} {}~{}",
        minimal_size,
        maximum_size,
        sources.len(),
        compactions.len(),
        compaction_size,
        hash,
        min_ts,
        max_ts
    );
}

#[test]
#[ignore = "manual test"]
fn cli_playground() {
    let mut backend = StorageBackend::new();
    let mut s3 = S3::new();
    s3.endpoint = "http://10.2.7.193:9000".to_owned();
    s3.force_path_style = true;
    s3.access_key = "minioadmin".to_owned();
    s3.secret_access_key = "minioadmin".to_owned();
    s3.bucket = "astro".to_owned();
    s3.prefix = "tpcc-1000-incr-with-crc64".to_owned();
    backend.set_s3(s3);

    let cfg = ExecutionConfig {
        from_ts: 450751747746168891,
        until_ts: 450756116281266182,
        compression: engine_traits::SstCompressionType::Lz4,
        compression_level: None,
    };
    let exec = Execution {
        out_prefix: cfg.recommended_prefix("fivloit"),
        cfg,
        max_concurrent_compaction: 16,
        external_storage: backend,
        db: None,
    };

    exec.run(SaveMeta::default()).unwrap();
}

#[tokio::test]
#[ignore = "manual test"]
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
        external_storage::create_iterable_storage(&backend, BackendConfig::default()).unwrap();

    let now = Instant::now();
    let mut ext = LoadFromExt::default();
    ext.max_concurrent_fetch = 128;
    let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
    let stream = meta.flat_map(|file| match file {
        Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
        Err(err) => stream::once(futures::future::err(err)).right_stream(),
    });
    let mut coll = CollectSubcompaction::new(
        stream,
        CollectSubcompactionConfig {
            compact_from_ts: 0,
            compact_to_ts: u64::MAX,
            subcompaction_size_threshold: ReadableSize::mb(128).0,
        },
    );
    let compaction = coll.try_next().await;
    println!("{:?}", compaction);
    println!("{:?}", now.elapsed());
    let c_ext = SubcompactExt {
        max_load_concurrency: 32,
        ..Default::default()
    };
    drop(coll);
    let arc_store: Arc<dyn IterableExternalStorage> = Arc::from(storage);
    let compact_worker =
        SubcompactionExec::<RocksEngine>::default_config(Arc::clone(&arc_store) as _);
    let result = compact_worker
        .run(compaction.unwrap().unwrap(), c_ext)
        .await
        .unwrap();
    println!("{:?}\n{:?}", result.load_stat, result.compact_stat);
}

#[tokio::test]
#[ignore = "manual test"]
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
        external_storage::create_iterable_storage(&backend, BackendConfig::default()).unwrap();

    let n = StreamyMetaStorage::count_objects(storage.as_ref())
        .await
        .unwrap();
    println!("{n}");
}
