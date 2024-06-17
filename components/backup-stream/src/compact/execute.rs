use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, WalkExternalStorage};
use futures::stream::{self, StreamExt};
use kvproto::brpb::StorageBackend;
use tikv_util::info;
use tokio::{
    io::AsyncWriteExt,
    runtime::Handle,
    signal::{self, unix::SignalKind},
};
use tracing::{instrument::Instrumented, span, trace_span, Instrument, Span};
use tracing_active_tree::{frame, root};
use txn_types::TimeStamp;

use super::{
    compaction::{
        CollectCompaction, CollectCompactionConfig, CompactLogExt, CompactWorker, Compaction,
    },
    statistic::LoadMetaStatistic,
    storage::{LoadFromExt, StreamyMetaStorage},
};
use crate::compact::{
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

pub trait ExecHooks: 'static {
    fn before_compaction_start(&mut self, _c: &Compaction, _cid: CId) {}
    fn after_compaction_end(&mut self, _cid: CId, _lst: LoadStatistic, _cst: CompactStatistic) {}
    fn after_finish(&mut self) {}

    fn after_runtime_start(&mut self, _h: &Handle) {}

    fn update_load_meta_stat(&mut self, _stat: LoadMetaStatistic) {}
}

pub struct NoHooks;

impl ExecHooks for NoHooks {}

#[derive(Default)]
pub struct LogToTerm {
    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,
    load_meta_stat: LoadMetaStatistic,
}

impl ExecHooks for LogToTerm {
    fn before_compaction_start(&mut self, c: &Compaction, cid: CId) {
        println!(
            "[{}] spawning compaction. cid: {}, cf: {}, input_min_ts: {}, input_max_ts: {}, source: {}, size: {}, region_id: {}",
            TimeStamp::physical_now(),
            cid.0,
            c.cf,
            c.input_min_ts,
            c.input_max_ts,
            c.source.len(),
            c.size,
            c.region_id
        );
    }

    fn after_compaction_end(&mut self, cid: CId, lst: LoadStatistic, cst: CompactStatistic) {
        let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
        let total_take =
            cst.load_duration + cst.sort_duration + cst.save_duration + cst.write_sst_duration;
        let speed = logical_input_size as f64 / total_take.as_millis() as f64;
        self.load_stat += lst.clone();
        self.compact_stat += cst.clone();

        println!(
            "[{}] finishing compaction. cid: {}, load_stat: {:?}, compact_stat: {:?}, speed: {:.2} KB./s, total_take: {:?}, global_load_meta_stat: {:?}",
            TimeStamp::physical_now(),
            cid.0,
            lst,
            cst,
            speed,
            total_take,
            self.load_meta_stat,
        );
    }

    fn after_finish(&mut self) {
        println!(
            "[{}] All compcations done. load_stat: {:?}, compact_stat: {:?}",
            TimeStamp::physical_now(),
            self.load_stat,
            self.compact_stat
        );
    }

    fn after_runtime_start(&mut self, h: &Handle) {
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

        h.spawn(sigusr1_handler);
    }

    fn update_load_meta_stat(&mut self, stat: LoadMetaStatistic) {
        self.load_meta_stat += stat;
    }
}

impl Execution {
    pub fn run(self, hooks: impl ExecHooks) -> Result<()> {
        let storage = external_storage::create_walkable_storage(
            &self.external_storage,
            BackendConfig::default(),
        )?;
        let storage: Arc<dyn WalkExternalStorage> = Arc::from(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let locked_hooks = Mutex::new(hooks);
        locked_hooks
            .lock()
            .unwrap()
            .after_runtime_start(runtime.handle());

        let all_works = async move {
            let mut ext = LoadFromExt::default();
            ext.max_concurrent_fetch = 128;
            ext.on_update_stat = Some(Box::new(|stat| {
                locked_hooks.lock().unwrap().update_load_meta_stat(stat)
            }));

            let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
            let stream = meta.flat_map(|file| match file {
                Ok(file) => stream::iter(file.logs).map(Ok).left_stream(),
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

            let span = trace_span!("poll_meta_stream");
            while let Some(c) = compact_stream.next().instrument(span.clone()).await {
                let c = c?;
                let cid = CId(id);
                locked_hooks
                    .lock()
                    .unwrap()
                    .before_compaction_start(&c, cid);
                id += 1;

                let mut compact_worker =
                    CompactWorker::<RocksEngine>::inplace(Arc::clone(&storage) as _);
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
                        .after_compaction_end(cid, lst, cst);
                }
            }
            drop(span);

            for (join, cid) in pending {
                let (lst, cst) = frame!("final_wait"; join).await.unwrap()?;
                locked_hooks
                    .lock()
                    .unwrap()
                    .after_compaction_end(cid, lst, cst);
            }

            locked_hooks.lock().unwrap().after_finish();
            Result::Ok(())
        };
        runtime.block_on(frame!(all_works))
    }
}
