use std::{any::Any, time::Instant};

use external_storage::{BackendConfig, BlobStore, S3Storage};
use futures::stream::{self, StreamExt, TryStreamExt};
use kvproto::brpb::{StorageBackend, S3};

use super::{
    compaction::CollectCompaction,
    storage::{CompactStorage, LoadFromExt, MetaStorage},
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
    let meta = MetaStorage::load_from_ext(storage.as_ref(), ext).await;
    let mut collect = CollectCompaction::new(
        stream::iter(meta.unwrap().files)
            .flat_map(|file| stream::iter(file.logs.into_iter()))
            .map(Ok),
    );
    println!("{:?}", now.elapsed());
    let compaction = collect.try_collect::<Vec<_>>().await.unwrap();
    println!("{:?}", now.elapsed());
    println!("{:?}", compaction[0]);
    println!("{:?}", compaction.len());
    println!(
        "{:?}",
        compaction.iter().map(|f| f.source.len()).sum::<usize>()
    );
}
