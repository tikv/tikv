// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    ops::RangeInclusive,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, ExternalStorage, UnpinReader};
use futures::{
    future::FutureExt,
    io::{AsyncReadExt, Cursor},
    stream::TryStreamExt,
};
use kvproto::brpb::StorageBackend;
use protobuf::Message;
use tokio::sync::mpsc::Sender;

use super::{Execution, ExecutionConfig, ShardConfig};
use crate::{
    ErrorKind,
    compaction::{META_OUT_REL, SubcompactionResult},
    errors::OtherErrExt,
    exec_hooks::{
        checkpoint::Checkpoint, consistency::StorageConsistencyGuard, save_meta::SaveMeta,
        skip_small_compaction::SkipSmallCompaction,
    },
    execute::hooking::{AfterFinishCtx, BeforeStartCtx, CId, ExecHooks, SubcompactionFinishCtx},
    storage::{LOCK_PREFIX, hash_meta_edit},
    test_util::{CompactInMem, KvGen, LogFileBuilder, TmpStorage, gen_min_max, gen_step},
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
            shift_ts: 0,
            calculate_shift_ts: false,
            minimal_compaction_size: 0,
            from_ts: 0,
            until_ts: u64::MAX,
            compression: engine_traits::SstCompressionType::Lz4,
            compression_level: None,
            prefetch_buffer_count: 128,
            prefetch_running_count: 128,
            physical_file_cache_capacity: 0,
        },
        max_concurrent_subcompaction: 3,
        external_storage: st,
        backend_config: BackendConfig {
            gcp_v2_enable: true,
            ..Default::default()
        },
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

fn store_paths(store_id: u64) -> (String, String) {
    let base_ts = store_id * 1_000;
    let meta_path = format!(
        "v1/backupmeta/{:016X}{:016X}-d{:016X}l{:016X}u{:016X}.meta",
        base_ts + 300,
        store_id,
        base_ts + 100,
        base_ts + 100,
        base_ts + 300,
    );
    let log_path = format!(
        "v1/20260320/12/{store_id}/{}-00000000-0000-0000-0000-000000000042.log",
        base_ts + 100
    );
    (meta_path, log_path)
}

async fn populate_stores(st: &TmpStorage, stores: RangeInclusive<u64>) -> HashMap<String, u64> {
    let mut meta_path_to_store_id = HashMap::<String, u64>::new();
    for store_id in stores {
        let (meta_path, log_path) = store_paths(store_id);
        st.build_flush(&log_path, &meta_path, gen_store_builders(store_id))
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

async fn run_exec_err(st: StorageBackend, shard: ShardConfig, name: &str) -> crate::Error {
    let mut exec = create_compaction(st);
    exec.cfg.shard = Some(shard);
    exec.out_prefix = exec.cfg.recommended_prefix(name);
    tokio::task::spawn_blocking(move || exec.run(SaveMeta::default()))
        .await
        .unwrap()
        .unwrap_err()
}

async fn set_metadata_store_id(st: &TmpStorage, meta_path: &str, store_id: u64) {
    let mut content = vec![];
    st.storage()
        .read(meta_path)
        .read_to_end(&mut content)
        .await
        .unwrap();
    let mut meta = protobuf::parse_from_bytes::<kvproto::brpb::Metadata>(&content).unwrap();
    meta.store_id = store_id as i64;
    let content = meta.write_to_bytes().unwrap();
    let len = content.len() as u64;
    st.storage()
        .write(meta_path, futures::io::Cursor::new(content).into(), len)
        .await
        .unwrap();
}

fn out_prefix_from_artifacts(artifacts: &str) -> &str {
    artifacts.strip_suffix("/metas").unwrap_or(artifacts)
}

async fn load_migrations_by_out_prefix(
    st: &TmpStorage,
) -> HashMap<String, kvproto::brpb::Migration> {
    st.load_migrations()
        .await
        .unwrap()
        .into_iter()
        .filter(|(id, _)| *id > 0)
        .map(|(_id, mig)| {
            let artifacts = mig
                .compactions
                .first()
                .map(|c| c.get_artifacts())
                .unwrap_or_default();
            (out_prefix_from_artifacts(artifacts).to_owned(), mig)
        })
        .collect()
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

    let mut expected_cmeta_files_total_size = 0;
    let mut stream = st.storage().iter_prefix(mig.compactions[0].get_artifacts());
    while let Some(item) = stream.try_next().await.unwrap() {
        if item.key.ends_with(".cmeta") {
            let mut content = vec![];
            st.storage()
                .read(&item.key)
                .read_to_end(&mut content)
                .await
                .unwrap();
            expected_cmeta_files_total_size += content.len() as u64;
        }
    }
    assert_eq!(
        mig.compactions[0].get_generated_cmeta_files_total_size(),
        expected_cmeta_files_total_size
    );

    let expected_sst_files_total_size: u64 = subc
        .iter()
        .flat_map(|subcompaction| subcompaction.get_sst_outputs())
        .map(|file| file.get_size())
        .sum();
    assert_eq!(
        mig.compactions[0].get_generated_sst_files_total_size(),
        expected_sst_files_total_size
    );
}

#[tokio::test]
async fn test_checkpointing_reuses_only_committed_batches() {
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
    struct AbortAfterNFinishes {
        seen: Arc<AtomicU64>,
        abort_at: u64,
    }

    const ERR_MSG: &str = "nameless you. back to where you from";

    impl ExecHooks for AbortAfterNFinishes {
        async fn after_a_subcompaction_end(
            &mut self,
            _cid: CId,
            _res: SubcompactionFinishCtx<'_>,
        ) -> crate::Result<()> {
            let seen = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
            if seen >= self.abort_at {
                Err(crate::ErrorKind::Other(ERR_MSG.to_owned()).into())
            } else {
                Ok(())
            }
        }
    }

    let failure_count = 3;
    let run_failed_attempt = |with_checkpoint| {
        tokio::task::spawn_blocking({
            let be = st.backend();
            move || {
                let mut exec = create_compaction(be);
                exec.max_concurrent_subcompaction = 1;
                let checkpoint = Checkpoint::default();
                let save_meta = SaveMeta::default()
                    .with_batch_limits(2, usize::MAX)
                    .with_checkpointed_file_sizes(checkpoint.file_sizes());
                let abort = AbortAfterNFinishes {
                    seen: Arc::new(AtomicU64::new(0)),
                    abort_at: 5,
                };
                if with_checkpoint {
                    exec.run(((save_meta, checkpoint), abort))
                } else {
                    exec.run((save_meta, abort))
                }
            }
        })
    };

    for failed_attempt in 0..failure_count {
        let err = run_failed_attempt(failed_attempt != 0)
            .await
            .unwrap()
            .unwrap_err();
        assert!(err.kind.to_string().contains(ERR_MSG));
    }
    assert!(st.load_migrations().await.unwrap().is_empty());

    let committed_before = st
        .load_subcompactions(&format!("test-output/{}", META_OUT_REL))
        .await
        .unwrap();
    let committed_regions = committed_before
        .iter()
        .map(|subc| subc.get_meta().get_region_id())
        .collect::<HashSet<_>>();
    assert!(!committed_regions.is_empty());
    assert!(committed_regions.len() < 15);

    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let executed_count = tokio::task::spawn_blocking({
        let be = st.backend();
        move || {
            let mut exec = create_compaction(be);
            exec.max_concurrent_subcompaction = 1;
            let checkpoint = Checkpoint::default();
            let save_meta = SaveMeta::default()
                .with_batch_limits(2, usize::MAX)
                .with_checkpointed_file_sizes(checkpoint.file_sizes());
            exec.run(((save_meta, checkpoint), CompactionSpy(tx)))
                .unwrap();
        }
    });

    let mut executed_regions = HashSet::new();
    while let Some(item) = rx.recv().await {
        let rid = item.meta.get_meta().get_region_id();
        assert!(!committed_regions.contains(&rid));
        executed_regions.insert(rid);
        st.verify_result(item, cm.remove(&(rid as usize)).unwrap());
    }
    executed_count.await.unwrap();

    assert_eq!(executed_regions.len() + committed_regions.len(), 15);

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

    let mut expected_cmeta_files_total_size = 0;
    let mut stream = st.storage().iter_prefix(mig.compactions[0].get_artifacts());
    while let Some(item) = stream.try_next().await.unwrap() {
        if item.key.ends_with(".cmeta") {
            let mut content = vec![];
            st.storage()
                .read(&item.key)
                .read_to_end(&mut content)
                .await
                .unwrap();
            expected_cmeta_files_total_size += content.len() as u64;
        }
    }
    assert_eq!(
        mig.compactions[0].get_generated_cmeta_files_total_size(),
        expected_cmeta_files_total_size
    );

    let expected_sst_files_total_size: u64 = subc
        .iter()
        .flat_map(|subcompaction| subcompaction.get_sst_outputs())
        .map(|file| file.get_size())
        .sum();
    assert_eq!(
        mig.compactions[0].get_generated_sst_files_total_size(),
        expected_sst_files_total_size
    );
}

async fn put_checkpoint(storage: &dyn ExternalStorage, store: u64, cp: u64) {
    let pfx = format!("v1/global_checkpoint/{}.ts", store);
    let content = futures::io::Cursor::new(cp.to_le_bytes());
    storage.write(&pfx, content.into(), 8).await.unwrap();
}

async fn put_crr_checkpoint(storage: &dyn ExternalStorage, cp: u64) {
    let content = format!(r#"{{"last_checkpoint":{}}}"#, cp).into_bytes();
    storage
        .write(
            "crr-checkpoint/resume-state.json",
            futures::io::Cursor::new(content.clone()).into(),
            content.len() as u64,
        )
        .await
        .unwrap();
}

async fn load_locks(storage: &dyn ExternalStorage) -> Vec<String> {
    storage
        .iter_prefix(LOCK_PREFIX)
        .map_ok(|v| v.key)
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_load_until_ts_from_checkpoint_prefers_crr_checkpoint() {
    let st = TmpStorage::create();
    let storage = st.storage().as_ref();
    put_checkpoint(storage, 1, 42).await;

    assert_eq!(
        super::load_until_ts_from_checkpoint(storage).await.unwrap(),
        42
    );

    put_crr_checkpoint(storage, 40).await;
    assert_eq!(
        super::load_until_ts_from_checkpoint(storage).await.unwrap(),
        40
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_consistency_guard_prefers_crr_checkpoint() {
    let st = TmpStorage::create();
    let strg = st.storage().as_ref();
    put_checkpoint(strg, 1, 49).await;
    put_crr_checkpoint(strg, 40).await;

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 41;
    let c = StorageConsistencyGuard::default();
    tokio::task::block_in_place(|| exec.run(c).unwrap_err());

    let mut exec = create_compaction(st.backend());
    exec.cfg.until_ts = 39;
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
async fn test_comments_record_execution_config_for_coverage() {
    let shard = ShardConfig::new(1, 2).unwrap();
    let store_id = (1..=16)
        .find(|&store_id| shard.contains_store_id(store_id))
        .unwrap();
    let (meta_path, log_path) = store_paths(store_id);

    let st = TmpStorage::create();
    st.build_flush(&log_path, &meta_path, gen_store_builders(store_id))
        .await;

    let mut exec = create_compaction(st.backend());
    exec.cfg.shard = Some(shard);
    exec.cfg.from_ts = 0x20;
    exec.cfg.calculate_shift_ts = true;
    exec.cfg.minimal_compaction_size = 42;
    exec.out_prefix = exec.cfg.recommended_prefix("comments_config");

    tokio::task::spawn_blocking(move || exec.run(SaveMeta::default()))
        .await
        .unwrap()
        .unwrap();

    let migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let comments = migs[0].1.compactions[0].get_comments();
    let comments: serde_json::Value = serde_json::from_str(comments).unwrap();
    let config = &comments["config"];

    assert_eq!(config["from-ts"].as_u64(), Some(0x20));
    assert_eq!(config["until-ts"].as_u64(), Some(u64::MAX));
    assert_eq!(config["shift-ts"].as_u64(), Some(0x20));
    assert_eq!(config["cal-shift-ts"].as_bool(), Some(true));
    assert_eq!(config["minimal-compaction-size"].as_u64(), Some(42));
    assert_eq!(config["shard"]["index"].as_u64(), Some(1));
    assert_eq!(config["shard"]["total"].as_u64(), Some(2));
}

#[tokio::test]
async fn test_empty_execution_still_writes_migration_with_comments() {
    let st = TmpStorage::create();
    let mut exec = create_compaction(st.backend());
    exec.cfg.shard = Some(ShardConfig::new(1, 2).unwrap());
    exec.cfg.from_ts = 0x20;
    exec.cfg.until_ts = 0x80;
    exec.cfg.calculate_shift_ts = true;
    exec.cfg.minimal_compaction_size = 42;
    exec.out_prefix = exec.cfg.recommended_prefix("empty_comments_config");

    tokio::task::spawn_blocking(move || exec.run(SaveMeta::default()))
        .await
        .unwrap()
        .unwrap();

    let migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    let mig = &migs[0].1;
    assert!(mig.edit_meta.is_empty());
    assert_eq!(mig.compactions.len(), 1);

    let compaction = &mig.compactions[0];
    assert_eq!(compaction.get_generated_cmeta_files_total_size(), 0);
    assert_eq!(compaction.get_generated_sst_files_total_size(), 0);

    let comments: serde_json::Value = serde_json::from_str(compaction.get_comments()).unwrap();
    let config = &comments["config"];
    assert_eq!(config["from-ts"].as_u64(), Some(0x20));
    assert_eq!(config["until-ts"].as_u64(), Some(0x80));
    assert_eq!(config["shift-ts"].as_u64(), Some(0x20));
    assert_eq!(config["cal-shift-ts"].as_bool(), Some(true));
    assert_eq!(config["minimal-compaction-size"].as_u64(), Some(42));
    assert_eq!(config["shard"]["index"].as_u64(), Some(1));
    assert_eq!(config["shard"]["total"].as_u64(), Some(2));
}

#[tokio::test]
async fn test_empty_migration_does_not_scan_backup_metadata() {
    let st = TmpStorage::create();
    let bad_meta = vec![0xFF];
    st.storage()
        .write(
            "v1/backupmeta/bad.meta",
            futures::io::Cursor::new(bad_meta.clone()).into(),
            bad_meta.len() as u64,
        )
        .await
        .unwrap();

    let mut exec = create_compaction(st.backend());
    exec.cfg.shard = Some(ShardConfig::new(1, 2).unwrap());
    let storage: Arc<dyn ExternalStorage> = st.storage().clone();
    let handle = tokio::runtime::Handle::current();
    let mut save_meta = SaveMeta::default();

    save_meta
        .before_execution_started(BeforeStartCtx {
            async_rt: &handle,
            this: &exec,
            meta_count: 1,
            storage: storage.as_ref(),
        })
        .await
        .unwrap();
    save_meta
        .after_execution_finished(AfterFinishCtx {
            async_rt: &handle,
            this: &exec,
            storage: &storage,
        })
        .await
        .unwrap();

    let migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    assert!(migs[0].1.edit_meta.is_empty());
    assert_eq!(migs[0].1.compactions.len(), 1);
}

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

const WINDOW_FROM: u64 = 1000;
const WINDOW_UNTIL: u64 = 2000;

#[derive(Clone, Copy)]
enum NameFormat {
    Tagged,
    Legacy,
    Unparsable,
}

/// A flush of one store, with the ts range and the min begin ts of default CF
/// its metadata name carries.
struct WindowFlush {
    id: u64,
    format: NameFormat,
    min_begin_ts: u64,
    files: Vec<(&'static str, bool, u64, u64)>,
}

impl WindowFlush {
    fn min_ts(&self) -> u64 {
        self.files.iter().map(|f| f.2).min().unwrap()
    }

    fn max_ts(&self) -> u64 {
        self.files.iter().map(|f| f.3).max().unwrap()
    }

    fn intersects_window(&self) -> bool {
        self.max_ts() >= WINDOW_FROM && self.min_ts() <= WINDOW_UNTIL
    }

    /// Whether the compaction reads its metadata.
    fn meta_in_window(&self, shift_ts: u64) -> bool {
        matches!(self.format, NameFormat::Unparsable)
            || (self.max_ts() >= shift_ts && self.min_ts() <= WINDOW_UNTIL)
    }

    /// Whether the compaction takes any of its files, which are kept from
    /// `shift_ts` in default CF and from `WINDOW_FROM` in write CF.
    fn has_compacted_files(&self, shift_ts: u64) -> bool {
        self.files.iter().any(|&(cf, is_meta, min_ts, max_ts)| {
            let from_ts = if cf == "default" {
                shift_ts
            } else {
                WINDOW_FROM
            };
            !is_meta && max_ts >= from_ts && min_ts <= WINDOW_UNTIL
        })
    }

    fn meta_path(&self) -> String {
        let (min_ts, max_ts) = (self.min_ts(), self.max_ts());
        match self.format {
            NameFormat::Tagged => format!(
                "v1/backupmeta/{:016X}{:016X}-d{:016X}l{:016X}u{:016X}p{:016X}.meta",
                max_ts, self.id, self.min_begin_ts, min_ts, max_ts, 1
            ),
            NameFormat::Legacy => format!(
                "v1/backupmeta/{:016X}-{:016X}-{:016X}-{:016X}.meta",
                max_ts, self.min_begin_ts, min_ts, max_ts
            ),
            NameFormat::Unparsable => format!("v1/backupmeta/flush-{}.meta", self.id),
        }
    }

    /// A name no ts window can exclude, which gives `--cal-shift-ts` the
    /// same shift ts as [`Self::meta_path`].
    fn unfiltered_meta_path(&self) -> String {
        if matches!(self.format, NameFormat::Unparsable) {
            return self.meta_path();
        }
        let min_begin_ts = if self.intersects_window() {
            self.min_begin_ts
        } else {
            0
        };
        format!(
            "v1/backupmeta/{:016X}{:016X}-d{:016X}l{:016X}u{:016X}.meta",
            self.max_ts(),
            self.id,
            min_begin_ts,
            0,
            u64::MAX
        )
    }

    fn builders(&self) -> Vec<LogFileBuilder> {
        let id = self.id;
        self.files
            .iter()
            .enumerate()
            .map(|(n, &(cf, is_meta, min_ts, max_ts))| {
                LogFileBuilder::from_iter(
                    KvGen::new(
                        gen_min_max(id as i64, n as i64 * 10, n as i64 * 10 + 5, min_ts, max_ts),
                        move |_| vec![id as u8; 16],
                    ),
                    |v| {
                        v.cf = cf;
                        v.is_meta = is_meta;
                        v.region_id = id;
                    },
                )
            })
            .collect()
    }

    async fn save(&self, st: &TmpStorage, meta_path: &str) {
        let log_path = format!("v1/20260320/12/{}/flush.log", self.id);
        st.build_flush(&log_path, meta_path, self.builders()).await;
    }
}

fn window_flushes(cal_shift_ts: bool) -> Vec<WindowFlush> {
    use NameFormat::*;
    let flush = |id, format, min_begin_ts, files| WindowFlush {
        id,
        format,
        min_begin_ts,
        files,
    };
    let mut flushes = vec![
        flush(
            1,
            Tagged,
            900,
            vec![("write", false, 1200, 1300), ("default", false, 1100, 1150)],
        ),
        flush(
            2,
            Legacy,
            800,
            vec![("write", false, 1500, 1600), ("default", false, 1400, 1450)],
        ),
        // Default CF records of transactions committed in the window, older than `from`.
        flush(
            3,
            Tagged,
            850,
            vec![("default", false, 850, 950), ("write", false, 860, 990)],
        ),
        flush(4, Legacy, 820, vec![("default", false, 820, 980)]),
        flush(5, Tagged, 1700, vec![("default", true, 1700, 1800)]),
        flush(
            6,
            Legacy,
            500,
            vec![("write", false, 500, 600), ("default", false, 500, 550)],
        ),
        flush(
            7,
            Tagged,
            2500,
            vec![("write", false, 2500, 2600), ("default", false, 2500, 2550)],
        ),
        flush(8, Legacy, 2200, vec![("default", false, 2200, 2300)]),
        // Only meta KV files: marked all data files compacted only when in the window.
        flush(9, Legacy, 300, vec![("default", true, 300, 400)]),
        flush(10, Tagged, 3000, vec![("default", true, 3000, 3100)]),
    ];
    if !cal_shift_ts {
        flushes.push(flush(11, Unparsable, 0, vec![("write", false, 100, 200)]));
    }
    flushes
}

fn normalized_subcompactions(subcs: Vec<kvproto::brpb::LogFileSubcompaction>) -> Vec<Vec<u8>> {
    let mut out = subcs
        .into_iter()
        .map(|subc| {
            let mut meta = subc.get_meta().clone();
            meta.mut_sources().sort_by(|a, b| a.path.cmp(&b.path));
            for source in meta.mut_sources().iter_mut() {
                source.mut_spans().sort_by_key(|s| s.offset);
            }
            meta.write_to_bytes().unwrap()
        })
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn normalized_meta_edits(
    mig: &kvproto::brpb::Migration,
    rename: impl Fn(&str) -> String,
) -> BTreeMap<String, kvproto::brpb::MetaEdit> {
    mig.edit_meta
        .iter()
        .map(|edit| {
            let mut edit = edit.clone();
            edit.set_path(rename(edit.get_path()));
            edit.mut_delete_physical_files().sort();
            edit.mut_delete_logical_files()
                .sort_by(|a, b| a.path.cmp(&b.path));
            for file in edit.mut_delete_logical_files().iter_mut() {
                file.mut_spans().sort_by_key(|s| s.offset);
            }
            (edit.get_path().to_owned(), edit)
        })
        .collect()
}

async fn run_window_exec(st: &TmpStorage, cal_shift_ts: bool) -> kvproto::brpb::Migration {
    let mut exec = create_compaction(st.backend());
    exec.cfg.from_ts = WINDOW_FROM;
    exec.cfg.until_ts = WINDOW_UNTIL;
    exec.cfg.calculate_shift_ts = cal_shift_ts;
    exec.out_prefix = exec.cfg.recommended_prefix("window");
    exec.run_with_storage_async(st.storage().clone(), SaveMeta::default())
        .await
        .unwrap();
    let mut migs = st.load_migrations().await.unwrap();
    assert_eq!(migs.len(), 1);
    migs.pop().unwrap().1
}

async fn check_window_matches_unfiltered(cal_shift_ts: bool) {
    let flushes = window_flushes(cal_shift_ts);
    let st = TmpStorage::create();
    let st_unfiltered = TmpStorage::create();
    let mut path_of_unfiltered = HashMap::new();
    for flush in &flushes {
        flush.save(&st, &flush.meta_path()).await;
        flush
            .save(&st_unfiltered, &flush.unfiltered_meta_path())
            .await;
        path_of_unfiltered.insert(flush.unfiltered_meta_path(), flush.meta_path());
    }

    let shift_ts = if cal_shift_ts { 800 } else { WINDOW_FROM };
    for flush in flushes.iter().filter(|f| !f.meta_in_window(shift_ts)) {
        st.storage()
            .write(
                &flush.meta_path(),
                UnpinReader(Box::new(Cursor::new(vec![0xFF]))),
                1,
            )
            .await
            .unwrap();
    }

    let mig = run_window_exec(&st, cal_shift_ts).await;
    let unfiltered_mig = run_window_exec(&st_unfiltered, cal_shift_ts).await;

    for flush in &flushes {
        let in_window = flush.meta_in_window(shift_ts);
        let edit = mig
            .edit_meta
            .iter()
            .find(|e| e.get_path() == flush.meta_path());
        let has_data = flush.files.iter().any(|f| !f.1);
        if !has_data {
            assert_eq!(
                edit.map(|e| e.all_data_files_compacted),
                in_window.then_some(true),
                "flush {}",
                flush.id
            );
        } else {
            assert_eq!(
                edit.is_some(),
                flush.has_compacted_files(shift_ts),
                "flush {}",
                flush.id
            );
        }
    }

    let mut expected_edits =
        normalized_meta_edits(&unfiltered_mig, |p| path_of_unfiltered[p].clone());
    expected_edits.retain(|path, _| {
        flushes
            .iter()
            .any(|f| &f.meta_path() == path && f.meta_in_window(shift_ts))
    });
    assert_eq!(
        normalized_meta_edits(&mig, |p| p.to_owned()),
        expected_edits
    );
    let (c, uc) = (&mig.compactions[0], &unfiltered_mig.compactions[0]);
    assert_eq!(c.artifacts_hash, uc.artifacts_hash);
    assert_eq!(c.input_min_ts, uc.input_min_ts);
    assert_eq!(c.input_max_ts, uc.input_max_ts);
    assert_eq!(
        normalized_subcompactions(st.load_subcompactions(c.get_artifacts()).await.unwrap()),
        normalized_subcompactions(
            st_unfiltered
                .load_subcompactions(uc.get_artifacts())
                .await
                .unwrap()
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_window_skips_metas_and_matches_unfiltered() {
    check_window_matches_unfiltered(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_window_with_shift_ts_skips_metas_and_matches_unfiltered() {
    check_window_matches_unfiltered(true).await;
}
