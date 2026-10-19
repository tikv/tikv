// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![cfg(test)]

use std::{
    collections::{HashSet, hash_map::RandomState},
    iter::FromIterator,
};

use kvproto::brpb::{Noop, S3, StorageBackend};
use tokio_stream::StreamExt;

use super::{MetadataClient, StreamTask, keys::MetaKey};
use crate::{
    errors::Result,
    metadata::{MetadataEvent, store::SlashEtcStore},
};

pub fn test_meta_cli() -> MetadataClient<SlashEtcStore> {
    MetadataClient::new(SlashEtcStore::default(), 42)
}

pub fn simple_task(name: &str) -> StreamTask {
    let mut task = StreamTask::default();
    task.info.set_name(name.to_owned());
    task.info.set_start_ts(1);
    task.info.set_end_ts(1000);
    let mut storage = StorageBackend::new();
    storage.set_noop(Noop::new());
    task.info.set_storage(storage);
    task.info.set_table_filter(vec!["*.*".to_owned()].into());
    task
}

// Maybe we can make it more generic...?
// But there isn't a AsIter trait :(
fn assert_range_matches(real: Vec<(Vec<u8>, Vec<u8>)>, expected: &[(&[u8], &[u8])]) {
    assert!(
        real.iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .eq(expected.iter().copied()),
        "range not match: {:?} vs {:?}",
        real,
        expected,
    );
}

#[tokio::test]
async fn test_basic() -> Result<()> {
    let cli = test_meta_cli();
    let name = "simple";
    let task = simple_task(name);
    let ranges: &[(&[u8], &[u8])] = &[(b"1", b"2"), (b"4", b"5"), (b"6", b"8"), (b"8", b"9")];
    cli.insert_task_with_range(&task, ranges).await?;
    let remote_ranges = cli.ranges_of_task(name).await?.inner;
    assert_range_matches(remote_ranges, ranges);

    Ok(())
}

#[tokio::test]
async fn test_s3_encryption_context_reload() -> Result<()> {
    for context in ["", "eyJ0ZW5hbnQiOiJ0ZW5hbnQtYSJ9"] {
        let cli = test_meta_cli();
        let mut task = simple_task("s3_context");
        let mut s3 = S3::new();
        s3.set_bucket("backup-bucket".to_owned());
        s3.set_sse_kms_encryption_context(context.to_owned());
        task.info.mut_storage().set_s3(s3);
        let assert_context = |task: &StreamTask| {
            assert_eq!(
                task.info
                    .get_storage()
                    .get_s3()
                    .get_sse_kms_encryption_context(),
                context,
            );
        };

        let revision = cli.get_tasks().await?.revision;
        let watcher = cli.events_from(revision).await?;
        cli.insert_task_with_range(&task, &[]).await?;
        watcher.cancel.await;
        let events = watcher.stream.collect::<Vec<_>>().await;
        assert_eq!(events.len(), 1);
        match &events[0] {
            MetadataEvent::AddTask { task } => assert_context(task),
            event => panic!("expected a task addition, got {:?}", event),
        }

        // A fresh client reloads the protobuf bytes persisted in the mock store.
        let reloaded = MetadataClient::new(cli.meta_store.clone(), 43);
        drop(cli);
        let tasks = reloaded.get_tasks().await?.inner;
        assert_eq!(tasks.len(), 1);
        assert_context(&tasks[0]);
        assert!(!tasks[0].is_paused);
        let name = task.info.get_name();
        assert_context(&reloaded.get_task(name).await?.unwrap());

        reloaded.pause(name).await?;
        let paused = reloaded.get_task(name).await?.unwrap();
        assert!(paused.is_paused);
        assert_context(&paused);
        reloaded.resume(name).await?;
        let resumed = reloaded.get_task(name).await?.unwrap();
        assert!(!resumed.is_paused);
        assert_context(&resumed);
    }
    Ok(())
}

fn task_matches(expected: &[StreamTask], real: &[StreamTask]) {
    assert_eq!(
        expected.len(),
        real.len(),
        "task not match: {:?} vs {:?}",
        expected,
        real
    );
    let name_set =
        HashSet::<_, RandomState>::from_iter(expected.iter().map(|t| t.info.name.clone()));
    let real = HashSet::<_, RandomState>::from_iter(real.iter().map(|t| t.info.name.clone()));
    assert!(
        name_set == real,
        "task not match: {:?} vs {:?}",
        name_set,
        real
    );
}

#[tokio::test]
async fn test_watch() -> Result<()> {
    let cli = test_meta_cli();
    let task = simple_task("simple_1");
    cli.insert_task_with_range(&task, &[]).await?;
    let initial_task_set = cli.get_tasks().await?;
    task_matches(initial_task_set.inner.as_slice(), &[task]);
    let watcher = cli.events_from(initial_task_set.revision + 1).await?;
    let task2 = simple_task("simple_2");
    cli.insert_task_with_range(&task2, &[]).await?;
    cli.remove_task("simple_1").await?;
    watcher.cancel.await;
    let events = watcher.stream.collect::<Vec<_>>().await;
    assert_eq!(
        events,
        vec![
            MetadataEvent::AddTask { task: task2 },
            MetadataEvent::RemoveTask {
                task: "simple_1".to_owned()
            }
        ]
    );
    Ok(())
}

#[tokio::test]
async fn test_progress() -> Result<()> {
    let cli = test_meta_cli();
    let task = simple_task("simple_1");
    cli.insert_task_with_range(&task, &[]).await?;
    let progress = cli.global_progress_of_task(&task.info.name).await?;
    assert_eq!(progress, task.info.start_ts);
    cli.set_local_task_checkpoint(&task.info.name, 42).await?;
    let progress = cli.global_progress_of_task(&task.info.name).await?;
    assert_eq!(progress, 42);
    cli.set_local_task_checkpoint(&task.info.name, 43).await?;
    let progress = cli.global_progress_of_task(&task.info.name).await?;
    assert_eq!(progress, 43);
    let other_store = MetadataClient::new(cli.meta_store.clone(), 43);
    let progress = other_store
        .get_local_task_checkpoint(&task.info.name)
        .await?;
    assert_eq!(progress.into_inner(), task.info.start_ts);

    Ok(())
}

#[test]
fn test_storage_checkpoint_of() {
    let task_name = "simple_task";
    let store_id: u64 = 5;
    let key = MetaKey::storage_checkpoint_of(task_name, store_id);
    assert_eq!(
        &key.0,
        "/tidb/br-stream/storage-checkpoint/simple_task/5".as_bytes()
    );
}

#[tokio::test]
async fn test_set_storage_checkpoint() -> Result<()> {
    let cli = test_meta_cli();
    let task = simple_task("simple_3");
    let storage_checkpoint_ts: u64 = 12345;

    // set storage checkpoint to metadata
    cli.set_storage_checkpoint(task.info.get_name(), storage_checkpoint_ts)
        .await?;
    // get storage checkpoint from metadata
    let ts = cli.get_storage_checkpoint(task.info.get_name()).await?;
    assert_eq!(ts.into_inner(), storage_checkpoint_ts);
    Ok(())
}

#[tokio::test]
async fn test_init() -> Result<()> {
    let cli = test_meta_cli();
    let mut task = simple_task("simple_2");
    cli.insert_task_with_range(&task, &[]).await?;
    task.info.set_start_ts(42);
    // Init task should set the checkpoint.
    cli.init_task(&task.info).await?;
    let progress = cli.global_progress_of_task(&task.info.name).await?;
    assert_eq!(progress, 42);
    cli.set_local_task_checkpoint(&task.info.name, 43).await?;

    // Init task again shouldn't roll back checkpoint.
    cli.init_task(&task.info).await?;
    let progress = cli.global_progress_of_task(&task.info.name).await?;
    assert_eq!(progress, 43);

    Ok(())
}
