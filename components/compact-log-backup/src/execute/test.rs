// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine_rocks::RocksEngine;
use external_storage::ExternalStorage;
use futures::{future::FutureExt, stream::TryStreamExt};
use kvproto::brpb::StorageBackend;
use tokio::sync::mpsc::Sender;

use super::{Execution, ExecutionConfig};
use crate::{
    ErrorKind,
    compaction::SubcompactionResult,
    errors::OtherErrExt,
    exec_hooks::{
        checkpoint::Checkpoint, consistency::StorageConsistencyGuard, save_meta::SaveMeta,
        skip_small_compaction::SkipSmallCompaction,
    },
    execute::hooking::{CId, ExecHooks, SubcompactionFinishCtx},
    storage::LOCK_PREFIX,
    test_util::{CompactInMem, KvGen, LogFileBuilder, TmpStorage, gen_step},
};

#[derive(Clone)]
struct CompactionSpy(Sender<SubcompactionResult>);

impl ExecHooks for CompactionSpy {
    async fn after_a_subcompaction_end(
        &mut self,
        _cid: super::hooking::CId,
        res: super::hooking::SubcompactionFinishCtx<'_>,
    ) -> crate::Result<()> {
        self.0
            .send(res.result.clone())
            .map(|res| res.adapt_err())
            .await
    }
}

fn gen_builder(cm: &mut HashMap<usize, CompactInMem>, batch: i64, num: i64) -> Vec<LogFileBuilder> {
    let mut result = vec![];
    for (n, i) in (num * batch..num * (batch + 1)).enumerate() {
        let it = cm
            .entry(n)
            .or_default()
            .tap_on(KvGen::new(gen_step(1, i, num), move |_| {
                vec![42u8; (n + 1) * 12]
            }))
            .take(200);
        let mut b = LogFileBuilder::new(|v| v.region_id = n as u64);
        for kv in it {
            b.add_encoded(&kv.key, &kv.value)
        }
        result.push(b);
    }
    result
}

pub fn create_compaction(st: StorageBackend) -> Execution {
    Execution::<RocksEngine> {
        cfg: ExecutionConfig {
            from_ts: 0,
            until_ts: u64::MAX,
            compression: engine_traits::SstCompressionType::Lz4,
            compression_level: None,
            prefetch_buffer_count: 128,
            prefetch_running_count: 128,
        },
        max_concurrent_subcompaction: 3,
        external_storage: st,
        db: None,
        out_prefix: "test-output".to_owned(),
    }
}

#[tokio::test]
async fn test_exec_simple() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();

    for i in 0..3 {
        st.build_flush(
            &format!("{}.log", i),
            &format!("v1/backupmeta/{}.meta", i),
            gen_builder(&mut cm, i, 10),
        )
        .await;
    }

    let exec = create_compaction(st.backend());

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let bg_exec = tokio::task::spawn_blocking(move || {
        exec.run((SaveMeta::default(), CompactionSpy(tx))).unwrap()
    });
    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id() as usize;
        st.verify_result(item, cm.remove(&rid).unwrap());
    }
    bg_exec.await.unwrap();

    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let (id, mig) = migs.pop().unwrap();
    assert_eq!(id, 1);
    assert_eq!(mig.edit_meta.len(), 3);
    for meta in mig.edit_meta.iter() {
        assert!(meta.all_data_files_compacted);
        assert!(meta.destruct_self);
    }
    assert_eq!(mig.compactions.len(), 1);
    let subc = st
        .load_subcompactions(mig.compactions[0].get_artifacts())
        .await
        .unwrap();
    assert_eq!(subc.len(), 10);
}

#[tokio::test]
async fn test_checkpointing() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();

    for i in 0..3 {
        st.build_flush(
            &format!("{}.log", i),
            &format!("v1/backupmeta/{}.meta", i),
            gen_builder(&mut cm, i, 15),
        )
        .await;
    }

    #[derive(Clone)]
    struct AbortEvery3TimesAndRecordFinishCount(Arc<AtomicU64>);

    const ERR_MSG: &str = "nameless you. back to where you from";

    impl ExecHooks for AbortEvery3TimesAndRecordFinishCount {
        async fn after_a_subcompaction_end(
            &mut self,
            cid: CId,
            _res: SubcompactionFinishCtx<'_>,
        ) -> crate::Result<()> {
            if cid.0 == 4 {
                Err(crate::ErrorKind::Other(ERR_MSG.to_owned()).into())
            } else {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
    }

    let be = st.backend();
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let cnt = Arc::new(AtomicU64::default());
    let cloneable_hooks = (
        AbortEvery3TimesAndRecordFinishCount(cnt.clone()),
        CompactionSpy(tx),
    );
    let hooks = move || {
        (
            (SaveMeta::default(), Checkpoint::default()),
            cloneable_hooks.clone(),
        )
    };
    let bg_exec = tokio::task::spawn_blocking(move || {
        while let Err(err) = create_compaction(be.clone()).run(hooks()) {
            if !err.kind.to_string().contains(ERR_MSG) {
                return Err(err);
            }
        }
        Ok(())
    });

    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id() as usize;
        st.verify_result(item, cm.remove(&rid).unwrap());
    }
    bg_exec.await.unwrap().unwrap();

    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let (id, mig) = migs.pop().unwrap();
    assert_eq!(id, 1);
    assert_eq!(mig.edit_meta.len(), 3);
    for meta in mig.edit_meta.iter() {
        assert!(meta.all_data_files_compacted);
        assert!(meta.destruct_self);
    }
    assert_eq!(mig.compactions.len(), 1);
    let subc = st
        .load_subcompactions(mig.compactions[0].get_artifacts())
        .await
        .unwrap();
    assert_eq!(subc.len(), 15);
    assert_eq!(cnt.load(Ordering::SeqCst), 15);
}

async fn put_checkpoint(storage: &dyn ExternalStorage, store: u64, cp: u64) {
    let pfx = format!("v1/global_checkpoint/{}.ts", store);
    let content = futures::io::Cursor::new(cp.to_le_bytes());
    storage.write(&pfx, content.into(), 8).await.unwrap();
}

async fn load_locks(storage: &dyn ExternalStorage) -> Vec<String> {
    storage
        .iter_prefix(LOCK_PREFIX)
        .map_ok(|v| v.key)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_consistency_guard() {
    let st = TmpStorage::create();
    let strg = st.storage().as_ref();
    put_checkpoint(strg, 1, 42).await;

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 43;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap_err());

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 41;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap());

    put_checkpoint(strg, 2, 40).await;

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 41;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap());

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 39;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap());

    put_checkpoint(strg, 2, 49).await;
    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 43;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap());
}

#[tokio::test]
async fn test_locking() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();
    st.build_flush("0.log", "v1/backupmeta/0.meta", gen_builder(&mut cm, 0, 15))
        .await;
    put_checkpoint(st.storage().as_ref(), 1, u64::MAX).await;

    struct Blocking<F>(Option<F>);
    impl<F: Future + Unpin + 'static> ExecHooks for Blocking<F> {
        async fn after_a_subcompaction_end(
            &mut self,
            _cid: CId,
            _res: SubcompactionFinishCtx<'_>,
        ) -> crate::Result<()> {
            if let Some(fut) = self.0.take() {
                fut.await;
            }
            Ok(())
        }
    }

    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let exec = create_compaction(st.backend());

    let (ptx, prx) = tokio::sync::oneshot::channel::<()>();
    let run = Box::pin(async move {
        ptx.send(()).unwrap();
        rx.await.unwrap();
    });

    let hooks = (Blocking(Some(run)), StorageConsistencyGuard::default());
    let hnd = tokio::task::spawn_blocking(move || exec.run(hooks));

    prx.await.unwrap();
    let l = load_locks(st.storage().as_ref()).await;
    assert_eq!(l.len(), 1, "it is {:?}", l);
    tx.send(()).unwrap();
    hnd.await.unwrap().unwrap();

    let l = load_locks(st.storage().as_ref()).await;
    assert_eq!(l.len(), 0, "it is {:?}", l);
}

#[tokio::test]
async fn test_abort_unlocking() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();
    st.build_flush("0.log", "v1/backupmeta/0.meta", gen_builder(&mut cm, 0, 15))
        .await;

    struct Abort;
    impl ExecHooks for Abort {
        async fn after_a_subcompaction_end(
            &mut self,
            _cid: CId,
            _res: SubcompactionFinishCtx<'_>,
        ) -> crate::Result<()> {
            Err(ErrorKind::Other("Journey ends here.".to_owned()))?
        }
    }

    let exec = create_compaction(st.backend());
    let hooks = (Abort, StorageConsistencyGuard::default());

    tokio::task::spawn_blocking(move || exec.run(hooks))
        .await
        .unwrap()
        .unwrap_err();
    let l = load_locks(st.storage().as_ref()).await;
    assert_eq!(l.len(), 0, "it is {:?}", l);
}

#[tokio::test]
async fn test_filter_out_small_compactions() {
    let st = TmpStorage::create();
    let mut cm = HashMap::new();
    st.build_flush("0.log", "v1/backupmeta/0.meta", gen_builder(&mut cm, 0, 15))
        .await;

    let exec = create_compaction(st.backend());

    tokio::task::spawn_blocking(move || {
        exec.run((SkipSmallCompaction::new(27800), SaveMeta::default()))
    })
    .await
    .unwrap()
    .unwrap();

    let migs = st.load_migrations().await.unwrap();
    let c_path = &migs[0].1.compactions[0];
    let cs = st.load_subcompactions(&c_path.artifacts).await.unwrap();
    assert_eq!(cs.len(), 8, "{:?}", cs);
    for c in &cs {
        assert!(c.get_meta().get_size() >= 27800, "{:?}", c.get_meta());
    }
}
<<<<<<< HEAD
=======

#[tokio::test]
async fn test_store_id_path_validation_only_runs_in_shard_mode() {
    let log_path = "v1/20260320/12/7/7100-00000000-0000-0000-0000-000000000042.log";
    let meta_path = "v1/backupmeta/not_backupmeta_format.meta";
    let shard = ShardConfig::new(1, 2).unwrap();

    let st_unsharded = TmpStorage::create();
    st_unsharded
        .build_flush(log_path, meta_path, gen_store_builders(7))
        .await;
    run_exec(st_unsharded.backend(), None, "sharding_bad_store_id_path").await;

    let st_sharded = TmpStorage::create();
    st_sharded
        .build_flush(log_path, meta_path, gen_store_builders(7))
        .await;
    let err = run_exec_err(st_sharded.backend(), shard, "sharding_bad_store_id_path").await;
    assert!(
        err.kind
            .to_string()
            .contains("cannot parse backup metadata path")
    );
    assert!(err.kind.to_string().contains(meta_path));
}

#[tokio::test]
async fn test_sharding_rejects_path_and_metadata_store_id_mismatch() {
    let shard = ShardConfig::new(1, 2).unwrap();
    let store_id = (1..=16)
        .find(|&store_id| shard.contains_store_id(store_id))
        .unwrap();
    let (meta_path, log_path) = store_paths(store_id);

    let st = TmpStorage::create();
    st.build_flush(&log_path, &meta_path, gen_store_builders(store_id))
        .await;
    set_metadata_store_id(&st, &meta_path, store_id + 1).await;

    let err = run_exec_err(st.backend(), shard, "sharding_store_id_mismatch").await;
    assert!(
        err.kind
            .to_string()
            .contains("backup metadata store id mismatch")
    );
}

#[tokio::test]
async fn test_sharding_skips_non_matching_meta_before_reading() {
    let shard = ShardConfig::new(1, 2).unwrap();
    let mut included_store_id = None;
    let mut excluded_store_id = None;
    for store_id in 1..=16 {
        if shard.contains_store_id(store_id) {
            included_store_id.get_or_insert(store_id);
        } else {
            excluded_store_id.get_or_insert(store_id);
        }
        if included_store_id.is_some() && excluded_store_id.is_some() {
            break;
        }
    }

    let included_store_id = included_store_id.unwrap();
    let excluded_store_id = excluded_store_id.unwrap();
    let (good_meta_path, good_log_path) = store_paths(included_store_id);
    let (bad_meta_path, _) = store_paths(excluded_store_id);

    let st = TmpStorage::create();
    st.build_flush(
        &good_log_path,
        &good_meta_path,
        gen_store_builders(included_store_id),
    )
    .await;
    st.storage()
        .write(
            &bad_meta_path,
            futures::io::Cursor::new(vec![0xFF]).into(),
            1,
        )
        .await
        .unwrap();

    run_exec(
        st.backend(),
        Some(shard),
        "sharding_skip_non_matching_meta_before_reading",
    )
    .await;
    assert!(!st.load_migrations().await.unwrap().is_empty());
}

#[tokio::test]
async fn test_sharding_by_store_and_union_matches_unsharded() {
    let st_sharded = TmpStorage::create();
    let st_unsharded = TmpStorage::create();

    let meta_path_to_store_id = populate_stores(&st_sharded, 1..=6u64).await;
    let _ = populate_stores(&st_unsharded, 1..=6u64).await;

    let shard1 = ShardConfig::new(1, 2).unwrap();
    let shard2 = ShardConfig::new(2, 2).unwrap();

    let out_prefix1 = run_exec(st_sharded.backend(), Some(shard1), "sharding_test").await;
    let out_prefix2 = run_exec(st_sharded.backend(), Some(shard2), "sharding_test").await;

    assert_ne!(out_prefix1, out_prefix2);

    let mut mig_by_out_prefix = load_migrations_by_out_prefix(&st_sharded).await;
    assert_eq!(mig_by_out_prefix.len(), 2);

    let mig1 = mig_by_out_prefix
        .remove(&out_prefix1)
        .expect("missing shard1 migration");
    let mig2 = mig_by_out_prefix
        .remove(&out_prefix2)
        .expect("missing shard2 migration");
    assert!(mig_by_out_prefix.is_empty());

    for (mig, include_shard, exclude_shard) in [(&mig1, shard1, shard2), (&mig2, shard2, shard1)] {
        for edit in &mig.edit_meta {
            let store_id = *meta_path_to_store_id
                .get(edit.get_path())
                .expect("unknown meta edit path");
            assert!(include_shard.contains_store_id(store_id));
            assert!(!exclude_shard.contains_store_id(store_id));
        }
    }

    let out_prefix_all = run_exec(st_unsharded.backend(), None, "sharding_test").await;
    let mut mig_by_out_prefix_all = load_migrations_by_out_prefix(&st_unsharded).await;
    assert_eq!(mig_by_out_prefix_all.len(), 1);
    let mig_all = mig_by_out_prefix_all
        .remove(&out_prefix_all)
        .expect("missing unsharded migration");

    let mut union = meta_edits_by_path(&mig1);
    for (path, edit) in meta_edits_by_path(&mig2) {
        let prev = union.insert(path.clone(), edit);
        assert!(prev.is_none(), "duplicate meta edit for {}", path);
    }
    let expected = meta_edits_by_path(&mig_all);

    assert_eq!(union.len(), expected.len());
    for (path, expected_edit) in expected {
        let union_edit = union.get(&path).unwrap();
        assert_eq!(union_edit.destruct_self, expected_edit.destruct_self);
        assert_eq!(
            union_edit.all_data_files_compacted,
            expected_edit.all_data_files_compacted
        );
        assert_eq!(hash_meta_edit(union_edit), hash_meta_edit(&expected_edit));
    }
}
>>>>>>> e0628046b6 (backup-stream: add flag into metadata name tag (#19609))
