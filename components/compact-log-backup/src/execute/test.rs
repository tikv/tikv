// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    future::Future,
    ops::RangeInclusive,
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

use super::{Execution, ExecutionConfig, ShardConfig};
use crate::{
    ErrorKind,
    compaction::SubcompactionResult,
    errors::OtherErrExt,
    exec_hooks::{
        checkpoint::Checkpoint, consistency::StorageConsistencyGuard, save_meta::SaveMeta,
        skip_small_compaction::SkipSmallCompaction,
    },
    execute::hooking::{CId, ExecHooks, SubcompactionFinishCtx},
    storage::{LOCK_PREFIX, hash_meta_edit},
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
            shard: None,
            from_ts: 0,
            until_ts: u64::MAX,
            compression: engine_traits::SstCompressionType::Lz4,
            compression_level: None,
            prefetch_buffer_count: 128,
            prefetch_running_count: 128,
            input_cache: None,
        },
        max_concurrent_subcompaction: 3,
        external_storage: st,
        db: None,
        out_prefix: "test-output".to_owned(),
    }
}

fn gen_store_builders(store_id: u64) -> Vec<LogFileBuilder> {
    (0..4)
        .map(|n| {
            let it = KvGen::new(
                gen_step(store_id as i64, store_id as i64 * 1000 + n as i64, 7),
                move |_| vec![store_id as u8; 16],
            )
            .take(200);
            let mut b = LogFileBuilder::new(|v| v.region_id = store_id * 100 + n as u64);
            for kv in it {
                b.add_encoded(&kv.key, &kv.value)
            }
            b
        })
        .collect()
}

async fn populate_stores(st: &TmpStorage, stores: RangeInclusive<u64>) -> HashMap<String, u64> {
    let mut meta_path_to_store_id = HashMap::<String, u64>::new();
    for store_id in stores {
        let meta_path = format!("v1/backupmeta/store_{store_id}.meta");
        let log_path = format!("store_{store_id}.log");
        st.build_flush_with_store_id(
            store_id,
            &log_path,
            &meta_path,
            gen_store_builders(store_id),
        )
        .await;
        meta_path_to_store_id.insert(meta_path, store_id);
    }
    meta_path_to_store_id
}

async fn run_exec(st: StorageBackend, shard: Option<ShardConfig>, name: &str) -> String {
    let mut exec = create_compaction(st);
    exec.cfg.shard = shard;
    exec.out_prefix = exec.cfg.recommended_prefix(name);
    let out_prefix = exec.out_prefix.clone();
    tokio::task::spawn_blocking(move || exec.run(SaveMeta::default()))
        .await
        .unwrap()
        .unwrap();
    out_prefix
}

fn out_prefix_from_artifacts(artifacts: &str) -> &str {
    artifacts.strip_suffix("/metas").unwrap_or(artifacts)
}

async fn load_migrations_by_out_prefix(
    st: &TmpStorage,
) -> HashMap<String, kvproto::brpb::Migration> {
    let mut migs = st
        .load_migrations()
        .await
        .unwrap()
        .into_iter()
        .filter(|(id, _)| *id > 0)
        .collect::<Vec<_>>();
    migs.sort_by_key(|(id, _)| *id);

    let mut mig_by_out_prefix = HashMap::<String, kvproto::brpb::Migration>::new();
    for (_id, mig) in migs {
        let artifacts = mig
            .compactions
            .get(0)
            .map(|c| c.get_artifacts())
            .unwrap_or_default();
        let out_prefix = out_prefix_from_artifacts(artifacts).to_owned();
        mig_by_out_prefix.insert(out_prefix, mig);
    }
    mig_by_out_prefix
}

fn meta_edits_by_path(mig: &kvproto::brpb::Migration) -> HashMap<String, kvproto::brpb::MetaEdit> {
    let mut by_path = HashMap::new();
    for edit in &mig.edit_meta {
        by_path.insert(edit.get_path().to_owned(), edit.clone());
    }
    by_path
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

    let artifacts = mig.compactions[0].get_artifacts();
    let mut cmeta_objects = 0usize;
    let mut s = st.storage().iter_prefix(artifacts);
    while let Some(obj) = s.try_next().await.unwrap() {
        if obj.key.ends_with(".cmeta") {
            cmeta_objects += 1;
        }
    }
    assert!(
        cmeta_objects < subc.len(),
        "expected cmeta batching to reduce object count, got {} objects for {} subcompactions",
        cmeta_objects,
        subc.len()
    );
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

    for edit in &mig1.edit_meta {
        let store_id = *meta_path_to_store_id
            .get(edit.get_path())
            .expect("unknown meta edit path");
        assert!(shard1.contains_store_id(store_id));
        assert!(!shard2.contains_store_id(store_id));
    }
    for edit in &mig2.edit_meta {
        let store_id = *meta_path_to_store_id
            .get(edit.get_path())
            .expect("unknown meta edit path");
        assert!(shard2.contains_store_id(store_id));
        assert!(!shard1.contains_store_id(store_id));
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
