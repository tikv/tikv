use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, FullFeaturedStorage};
use futures::stream::{self, StreamExt};
use kvproto::brpb::StorageBackend;
use tokio::{io::AsyncWriteExt, runtime::Handle, signal::unix::SignalKind};
use tracing::{trace_span, Instrument};
use tracing_active_tree::{frame, root};
use txn_types::TimeStamp;

use super::{
    compaction::{
        collector::{CollectCompaction, CollectCompactionConfig},
        exec::{CompactLogExt, SingleCompactionExec},
        Compaction,
    },
    statistic::{CollectCompactionStatistic, LoadMetaStatistic},
    storage::{LoadFromExt, StreamyMetaStorage},
};
use crate::{
    errors::Result,
    statistic::{CompactStatistic, LoadStatistic},
};

pub struct Execution {
    pub from_ts: u64,
    pub until_ts: u64,
    pub max_concurrent_compaction: u64,
    pub external_storage: StorageBackend,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct CId(u64);

impl std::fmt::Display for CId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub struct BeforeStartCtx<'a> {
    async_rt: &'a Handle,
    est_meta_size: u64,
}

pub trait ExecHooks: 'static {
    fn before_a_compaction_start(&mut self, _c: &Compaction, _cid: CId) {}
    fn after_a_compaction_end(&mut self, _cid: CId, _lst: LoadStatistic, _cst: CompactStatistic) {}

    fn before_execution_started(&mut self, _cx: &BeforeStartCtx<'_>) {}
    fn after_execution_finished(&mut self) {}

    fn update_load_meta_stat(&mut self, _stat: LoadMetaStatistic) {}
    fn update_collect_compaction_stat(&mut self, _stat: CollectCompactionStatistic) {}
}

pub struct NoHooks;

impl ExecHooks for NoHooks {}

#[derive(Default)]
pub struct LogToTerm {
    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,
    load_meta_stat: LoadMetaStatistic,
    collect_stat: CollectCompactionStatistic,
    meta_len: u64,
}

impl ExecHooks for LogToTerm {
    fn before_a_compaction_start(&mut self, c: &Compaction, cid: CId) {
        println!(
            "[{}] spawning compaction. cid: {}, cf: {}, input_min_ts: {}, input_max_ts: {}, source: {}, size: {}, region_id: {}",
            TimeStamp::physical_now(),
            cid.0,
            c.cf,
            c.input_min_ts,
            c.input_max_ts,
            c.inputs.len(),
            c.size,
            c.region_id
        );
    }

    fn after_a_compaction_end(&mut self, cid: CId, lst: LoadStatistic, cst: CompactStatistic) {
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;
        self.load_stat += lst.clone();
        self.compact_stat += cst.clone();

        println!(
            "[{}] finishing compaction. meta: {}/{}, p_bytes: {}, cid: {}, load_stat: {:?}, compact_stat: {:?}, speed: {:.2} KB./s, total_take: {:?}, global_load_meta_stat: {:?}",
            TimeStamp::physical_now(),
            self.load_meta_stat.meta_files_in,
            self.meta_len,
            self.collect_stat.bytes_in - self.collect_stat.bytes_out,
            cid.0,
            lst,
            cst,
            speed,
            total_take,
            self.load_meta_stat,
        );
    }

    fn after_execution_finished(&mut self) {
        println!(
            "[{}] All compcations done. load_stat: {:?}, compact_stat: {:?}",
            TimeStamp::physical_now(),
            self.load_stat,
            self.compact_stat
        );
    }

    fn update_load_meta_stat(&mut self, stat: LoadMetaStatistic) {
        self.load_meta_stat += stat;
    }

    fn before_execution_started(&mut self, cx: &BeforeStartCtx<'_>) {
        tracing_active_tree::init();

        let sigusr1_handler = async {
            let mut signal = tokio::signal::unix::signal(SignalKind::user_defined1()).unwrap();
            while let Some(_) = signal.recv().await {
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

        cx.async_rt.spawn(sigusr1_handler);
        self.meta_len = cx.est_meta_size;
    }

    fn update_collect_compaction_stat(&mut self, stat: CollectCompactionStatistic) {
        self.collect_stat += stat
    }
}

impl Execution {
    pub fn run(self, hooks: impl ExecHooks) -> Result<()> {
        let storage = external_storage::create_full_featured_storage(
            &self.external_storage,
            BackendConfig::default(),
        )?;
        let storage: Arc<dyn FullFeaturedStorage> = Arc::from(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let locked_hooks = Mutex::new(hooks);

        let all_works = async move {
            let mut ext = LoadFromExt::default();
            let next_compaction = trace_span!("next_compaction");
            ext.max_concurrent_fetch = 128;
            ext.on_update_stat = Some(Box::new(|stat| {
                locked_hooks.lock().unwrap().update_load_meta_stat(stat)
            }));
            ext.loading_content_span = Some(trace_span!(
                parent: next_compaction.clone(),
                "load_meta_file_names"
            ));

            let total = StreamyMetaStorage::count_objects(storage.as_ref()).await?;
            let cx = BeforeStartCtx {
                est_meta_size: total,
                async_rt: &tokio::runtime::Handle::current(),
            };
            locked_hooks.lock().unwrap().before_execution_started(&cx);
            let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
            let stream = meta.flat_map(|file| match file {
                Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
                Err(err) => stream::once(futures::future::err(err)).right_stream(),
            });
            let mut compact_stream = CollectCompaction::new(
                stream,
                CollectCompactionConfig {
                    compact_from_ts: self.from_ts,
                    compact_to_ts: self.until_ts,
                },
            );
            let mut pending = VecDeque::new();
            let mut id = 0;

            while let Some(c) = compact_stream
                .next()
                .instrument(next_compaction.clone())
                .await
            {
                locked_hooks
                    .lock()
                    .unwrap()
                    .update_collect_compaction_stat(compact_stream.take_statistic());

                let c = c?;
                let cid = CId(id);
                locked_hooks
                    .lock()
                    .unwrap()
                    .before_a_compaction_start(&c, cid);

                id += 1;

                let compact_worker =
                    SingleCompactionExec::<RocksEngine>::inplace(Arc::clone(&storage) as _);
                let compact_work = async move {
                    let mut load_statistic = LoadStatistic::default();
                    let mut compact_statistic = CompactStatistic::default();
                    let mut ext = CompactLogExt::default();
                    ext.compact_statistic = Some(&mut compact_statistic);
                    ext.load_statistic = Some(&mut load_statistic);
                    ext.max_load_concurrency = 32;
                    compact_worker.compact_ext(c, ext).await?;
                    Result::Ok((load_statistic, compact_statistic))
                };
                let join_handle = tokio::spawn(root!(compact_work));
                pending.push_back((join_handle, cid));

                if pending.len() >= self.max_concurrent_compaction as _ {
                    let (join, cid) = pending.pop_front().unwrap();
                    let (lst, cst) = frame!("wait"; join).await.unwrap()?;
                    locked_hooks
                        .lock()
                        .unwrap()
                        .after_a_compaction_end(cid, lst, cst);
                }
            }
            drop(next_compaction);

            for (join, cid) in pending {
                let (lst, cst) = frame!("final_wait"; join).await.unwrap()?;
                locked_hooks
                    .lock()
                    .unwrap()
                    .after_a_compaction_end(cid, lst, cst);
            }

            locked_hooks
                .lock()
                .unwrap()
                .update_collect_compaction_stat(compact_stream.take_statistic());
            locked_hooks.lock().unwrap().after_execution_finished();
            Result::Ok(())
        };
        runtime.block_on(frame!(all_works))
    }
}
