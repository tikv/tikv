use std::{collections::VecDeque, sync::Arc};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, WalkExternalStorage};
use futures::stream::{self, StreamExt};
use kvproto::brpb::StorageBackend;
use tikv_util::info;
use txn_types::TimeStamp;

use super::{
    compaction::{
        CollectCompaction, CollectCompactionConfig, CompactLogExt, CompactWorker, Compaction,
    },
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
}

pub struct NoHooks;

impl ExecHooks for NoHooks {}

#[derive(Default)]
pub struct LogToTerm {
    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,
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
        self.load_stat.merge_with(&lst);
        self.compact_stat.merge_with(&cst);

        println!(
            "[{}] finishing compaction. cid: {}, load_stat: {:?}, compact_stat: {:?}, speed: {:.2} KB./s, total_take: {:?}",
            TimeStamp::physical_now(),
            cid.0,
            lst,
            cst,
            speed,
            total_take
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
}

impl Execution {
    pub fn run(self, mut hooks: impl ExecHooks) -> Result<()> {
        let storage = external_storage::create_walkable_storage(
            &self.external_storage,
            BackendConfig::default(),
        )?;
        let storage: Arc<dyn WalkExternalStorage> = Arc::from(storage);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let all_works = async move {
            let mut ext = LoadFromExt::default();
            ext.max_concurrent_fetch = 128;
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
            while let Some(c) = compact_stream.next().await {
                let c = c?;
                let cid = CId(id);
                hooks.before_compaction_start(&c, cid);
                id += 1;

                let mut compact_worker =
                    CompactWorker::<RocksEngine>::inplace(Arc::clone(&storage) as _);
                let compact_work = async move {
                    let mut load_statistic = LoadStatistic::default();
                    let mut compact_statistic = CompactStatistic::default();
                    let mut ext = CompactLogExt::default();
                    ext.compact_statistic = Some(&mut compact_statistic);
                    ext.load_statistic = Some(&mut load_statistic);
                    ext.max_load_concurrency = 128;
                    compact_worker.compact_ext(c, ext).await?;
                    Result::Ok((load_statistic, compact_statistic))
                };
                let join_handle = tokio::spawn(compact_work);
                pending.push_back((join_handle, cid));

                if pending.len() >= self.max_concurrent_compaction as _ {
                    let (join, cid) = pending.pop_front().unwrap();
                    let (lst, cst) = join.await.unwrap()?;
                    hooks.after_compaction_end(cid, lst, cst);
                }
            }

            for (join, cid) in pending {
                let (lst, cst) = join.await.unwrap()?;
                hooks.after_compaction_end(cid, lst, cst);
            }

            hooks.after_finish();
            Result::Ok(())
        };
        runtime.block_on(all_works)
    }
}
