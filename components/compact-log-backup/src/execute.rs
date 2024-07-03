use std::{
    borrow::Cow,
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
};

use chrono::{DateTime, Duration, Local};
use engine_rocks::RocksEngine;
pub use engine_traits::SstCompressionType;
use engine_traits::SstExt;
use external_storage::{BackendConfig, FullFeaturedStorage};
use futures::stream::{self, StreamExt};
use hooks::{AfterFinishCtx, BeforeStartCtx, CId, ExecHooks};
use kvproto::brpb::StorageBackend;
use tokio::{io::AsyncWriteExt, runtime::Handle, signal::unix::SignalKind};
use tracing::{trace_span, Instrument};
use tracing_active_tree::{frame, root};
use txn_types::TimeStamp;

use super::{
    compaction::{
        collector::{CollectSubcompaction, CollectSubcompactionConfig},
        exec::{SubcompactExt, SubcompactionExec},
        Subcompaction,
    },
    statistic::{CollectCompactionStatistic, LoadMetaStatistic},
    storage::{LoadFromExt, StreamyMetaStorage},
};
use crate::{
    compaction::{exec::SingleCompactionArg, meta::CompactionRunInfoBuilder, SubcompactionResult},
    errors::{Result, TraceResultExt},
    statistic::{CompactStatistic, LoadStatistic},
    util,
};

pub struct ExecutionConfig {
    pub from_ts: u64,
    pub until_ts: u64,
    pub compression: SstCompressionType,
    pub compression_level: Option<i32>,
}

impl ExecutionConfig {
    pub fn recommended_prefix(&self, name: &str) -> String {
        let mut hasher = crc64fast::Digest::new();
        hasher.write(name.as_bytes());
        hasher.write(&self.from_ts.to_le_bytes());
        hasher.write(&self.until_ts.to_le_bytes());
        hasher.write(&util::compression_type_to_u8(self.compression).to_le_bytes());
        hasher.write(&self.compression_level.unwrap_or(0).to_le_bytes());

        format!("{}_{}", name, util::aligned_u64(hasher.sum64()))
    }
}

pub struct Execution<DB: SstExt = RocksEngine> {
    pub cfg: ExecutionConfig,

    pub max_concurrent_compaction: u64,
    pub external_storage: StorageBackend,
    pub db: Option<DB>,
    pub out_prefix: String,
}

pub struct NoHooks;

impl ExecHooks for NoHooks {}

pub mod hooks {

    use tokio::runtime::Handle;

    use super::Execution;
    use crate::{
        compaction::{Subcompaction, SubcompactionResult},
        statistic::{CollectCompactionStatistic, LoadMetaStatistic},
    };

    #[derive(PartialEq, Eq, Debug, Clone, Copy)]
    pub struct CId(pub u64);

    impl std::fmt::Display for CId {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Clone, Copy)]
    pub struct BeforeStartCtx<'a> {
        pub async_rt: &'a Handle,
        pub est_meta_size: u64,
        pub this: &'a Execution,
    }

    #[derive(Clone)]
    pub struct AfterFinishCtx {
        pub async_rt: Handle,
        pub comments: String,
    }

    pub trait ExecHooks: 'static {
        fn before_a_compaction_start(&mut self, _c: &Subcompaction, _cid: CId) {}
        fn after_a_compaction_end(&mut self, _cid: CId, _res: &SubcompactionResult) {}

        fn before_execution_started(&mut self, _cx: BeforeStartCtx<'_>) {}
        fn after_execution_finished(&mut self, _cx: &mut AfterFinishCtx) {}

        fn update_load_meta_stat(&mut self, _stat: &LoadMetaStatistic) {}
        fn update_collect_compaction_stat(&mut self, _stat: &CollectCompactionStatistic) {}
    }
}

#[derive(Default)]
pub struct LogToTerm {
    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,
    load_meta_stat: LoadMetaStatistic,
    collect_stat: CollectCompactionStatistic,
    begin: Option<DateTime<Local>>,
    meta_len: u64,
}

impl ExecHooks for LogToTerm {
    fn before_a_compaction_start(&mut self, c: &Subcompaction, cid: CId) {
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

    fn after_a_compaction_end(&mut self, cid: CId, res: &SubcompactionResult) {
        let lst = &res.load_stat;
        let cst = &res.compact_stat;
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

    fn after_execution_finished(&mut self, cx: &mut AfterFinishCtx) {
        let now = Local::now();
        cx.comments += &format!(
            "start_time: {}\n",
            self.begin
                .map(|v| v.to_rfc3339())
                .as_deref()
                .unwrap_or("unknown")
        );
        cx.comments += &format!("end_time: {}\n", now.to_rfc3339());
        cx.comments += &format!(
            "taken: {}\n",
            self.begin.map(|v| now - v).unwrap_or(Duration::zero())
        );
        cx.comments += &format!("exec_by: {:?}\n", tikv_util::sys::hostname());
        cx.comments += &format!("load_stat: {:?}\n", self.load_stat);
        cx.comments += &format!("compact_stat: {:?}\n", self.compact_stat);
        cx.comments += &format!("load_meta_stat: {:?}\n", self.load_meta_stat);
        cx.comments += &format!("collect_stat: {:?}\n", self.collect_stat);

        println!("[{}] All compcations done", TimeStamp::physical_now(),);
        println!("{}", cx.comments);
    }

    fn update_load_meta_stat(&mut self, stat: &LoadMetaStatistic) {
        self.load_meta_stat += stat.clone();
    }

    fn before_execution_started(&mut self, cx: BeforeStartCtx<'_>) {
        tracing_active_tree::init();

        self.begin = Some(Local::now());
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

    fn update_collect_compaction_stat(&mut self, stat: &CollectCompactionStatistic) {
        self.collect_stat += stat.clone()
    }
}

impl Execution {
    fn gen_name(&self) -> String {
        let compaction_name = Path::new(&self.out_prefix)
            .file_name()
            .map(|v| v.to_string_lossy())
            .unwrap_or(Cow::Borrowed("unknown"));
        let pid = tikv_util::sys::thread::thread_id();
        let hostname = tikv_util::sys::hostname();
        format!(
            "{}#{}@{}",
            compaction_name,
            pid,
            hostname.as_deref().unwrap_or("unknown")
        )
    }

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
                locked_hooks.lock().unwrap().update_load_meta_stat(&stat)
            }));
            ext.loading_content_span = Some(trace_span!(
                parent: next_compaction.clone(),
                "load_meta_file_names"
            ));

            let total = StreamyMetaStorage::count_objects(storage.as_ref()).await?;
            let mut run_info = CompactionRunInfoBuilder::new();
            run_info.mut_meta().set_name(self.gen_name());
            run_info.mut_meta().set_compaction_from_ts(self.cfg.from_ts);
            run_info
                .mut_meta()
                .set_compaction_until_ts(self.cfg.until_ts);
            let cx = BeforeStartCtx {
                est_meta_size: total,
                async_rt: &tokio::runtime::Handle::current(),
                this: &self,
            };
            locked_hooks.lock().unwrap().before_execution_started(cx);
            let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext);
            let stream = meta.flat_map(|file| match file {
                Ok(file) => stream::iter(file.into_logs()).map(Ok).left_stream(),
                Err(err) => stream::once(futures::future::err(err)).right_stream(),
            });
            let mut compact_stream = CollectSubcompaction::new(
                stream,
                CollectSubcompactionConfig {
                    compact_from_ts: self.cfg.from_ts,
                    compact_to_ts: self.cfg.until_ts,
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
                    .update_collect_compaction_stat(&compact_stream.take_statistic());

                let c = c?;
                let cid = CId(id);
                locked_hooks
                    .lock()
                    .unwrap()
                    .before_a_compaction_start(&c, cid);

                id += 1;

                let compact_args = SingleCompactionArg {
                    out_prefix: Some(Path::new(&self.out_prefix).to_owned()),
                    db: self.db.clone(),
                    storage: Arc::clone(&storage) as _,
                };
                let compact_worker = SubcompactionExec::from(compact_args);
                let compact_work = async move {
                    let mut ext = SubcompactExt::default();
                    ext.max_load_concurrency = 32;
                    ext.compression = self.cfg.compression;
                    ext.compression_level = self.cfg.compression_level;
                    let res = compact_worker.run(c, ext).await.trace_err()?;
                    res.verify_checksum()
                        .annotate(format_args!("the compaction is {:?}", res.origin))?;
                    Result::Ok(res)
                };
                let join_handle = tokio::spawn(root!(compact_work));
                pending.push_back((join_handle, cid));

                if pending.len() >= self.max_concurrent_compaction as _ {
                    let (join, cid) = pending.pop_front().unwrap();
                    let cres = frame!("wait"; join).await.unwrap()?;
                    locked_hooks
                        .lock()
                        .unwrap()
                        .after_a_compaction_end(cid, &cres);
                    run_info.add_compaction(&cres);
                }
            }
            drop(next_compaction);

            locked_hooks
                .lock()
                .unwrap()
                .update_collect_compaction_stat(&compact_stream.take_statistic());

            for (join, cid) in pending {
                let cres = frame!("final_wait"; join).await.unwrap()?;
                locked_hooks
                    .lock()
                    .unwrap()
                    .after_a_compaction_end(cid, &cres);
                run_info.add_compaction(&cres);
            }
            let mut cx = AfterFinishCtx {
                async_rt: Handle::current(),
                comments: String::new(),
            };
            locked_hooks
                .lock()
                .unwrap()
                .after_execution_finished(&mut cx);
            run_info.mut_meta().set_comments(cx.comments);

            run_info
                .write_migration(storage.as_ref())
                .await
                .trace_err()?;

            Result::Ok(())
        };
        runtime.block_on(frame!(all_works))
    }
}
