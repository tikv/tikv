use std::{collections::VecDeque, sync::Arc};

use engine_rocks::RocksEngine;
use external_storage::{BackendConfig, WalkExternalStorage};
use futures::stream::{self, StreamExt};
use kvproto::brpb::StorageBackend;
use tikv_util::sys::SysQuota;

use super::{
    compaction::{CollectCompaction, CollectCompactionConfig, CompactLogExt, CompactWorker},
    storage::{LoadFromExt, StreamyMetaStorage},
};
use crate::compact::{
    errors::Result,
    statistic::{CompactStatistic, LoadStatistic},
};

pub struct Execution {
    pub from_ts: u64,
    pub until_ts: u64,
    pub external_storage: StorageBackend,
}

impl Execution {
    pub fn run(self) -> Result<()> {
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
            let meta = StreamyMetaStorage::load_from_ext(storage.as_ref(), ext).await;
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
            while let Some(c) = compact_stream.next().await {
                let c = c?;
                println!(
                    "Spawning compaction @{} from {} to {} input {} region {}",
                    c.cf,
                    c.input_min_ts,
                    c.input_max_ts,
                    c.source.len(),
                    c.region_id
                );
                let mut compact_worker =
                    CompactWorker::<RocksEngine>::inplace(Arc::clone(&storage) as _);
                pending.push_back(tokio::spawn(async move {
                    let mut load_statistic = LoadStatistic::default();
                    let mut compact_statistic = CompactStatistic::default();
                    let mut ext = CompactLogExt::default();
                    ext.compact_statistic = Some(&mut compact_statistic);
                    ext.load_statistic = Some(&mut load_statistic);
                    compact_worker.compact_ext(c, ext).await?;
                    Result::Ok((load_statistic, compact_statistic))
                }));
                if pending.len() as f64 > 2.0 * SysQuota::cpu_cores_quota() {
                    let (lst, cst) = pending.pop_front().unwrap().await.unwrap()?;
                    let logical_input_size = lst.logical_key_bytes_in + lst.logical_value_bytes_in;
                    let total_take = cst.load_duration
                        + cst.sort_duration
                        + cst.save_duration
                        + cst.write_sst_duration;
                    let speed = logical_input_size / total_take.as_secs();
                    println!(
                        "finishing compaction input {} take {:?} speed {} bytes per sec",
                        logical_input_size, total_take, speed
                    );
                }
            }

            println!("All compactions done.");
            Result::Ok(())
        };
        runtime.block_on(all_works)
    }
}
