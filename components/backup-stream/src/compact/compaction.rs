use std::{collections::HashMap, marker::PhantomData, sync::Arc, task::ready};

use engine_traits::{
    CfName, ExternalSstFileInfo, SstCompressionType, SstExt, SstWriter, SstWriterBuilder,
};
use external_storage::ExternalStorage;
use futures::io::AllowStdIo;
use tikv_util::{
    codec::stream_event::Iterator as KvStreamIter, config::ReadableSize, time::Instant,
};
use tokio_stream::Stream;

use super::{
    errors::Result,
    source::{Record, Source},
    statistic::{CompactStatistic, LoadStatistic},
    storage::{LogFile, LogFileId},
    util::{Cooperate, ExecuteAllExt},
};

#[derive(Debug)]
pub struct Compaction {
    pub source: Vec<LogFileId>,
    pub size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub input_max_ts: u64,
    pub input_min_ts: u64,
    pub compact_from_ts: u64,
    pub compact_to_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
}

struct UnformedCompaction {
    size: u64,
    files: Vec<LogFileId>,
    min_ts: u64,
    max_ts: u64,
    min_key: Arc<[u8]>,
    max_key: Arc<[u8]>,
}

#[pin_project::pin_project]
pub struct CollectCompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Compaction>>,

    collector: CompactionCollector,
}

pub struct CollectCompactionConfig {
    pub compact_from_ts: u64,
    pub compact_to_ts: u64,
}

impl<S: Stream<Item = Result<LogFile>>> CollectCompaction<S> {
    pub fn new(s: S, cfg: CollectCompactionConfig) -> Self {
        CollectCompaction {
            inner: s,
            last_compactions: None,
            collector: CompactionCollector {
                cfg,
                items: HashMap::new(),
                compaction_size_threshold: ReadableSize::mb(128).0,
            },
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq, Clone, Copy)]
struct CompactionCollectKey {
    cf: &'static str,
    region_id: u64,
}

struct CompactionCollector {
    items: HashMap<CompactionCollectKey, UnformedCompaction>,
    compaction_size_threshold: u64,
    cfg: CollectCompactionConfig,
}

impl CompactionCollector {
    fn add_new_file(&mut self, file: LogFile) -> Option<Compaction> {
        use std::collections::hash_map::Entry;
        let key = CompactionCollectKey {
            region_id: file.region_id,
            cf: file.cf,
        };

        // Skip out-of-range files and schema meta files.
        // Meta files need to have a simpler format so other languages can easily open
        // and rewrite it.
        if file.is_meta
            || file.max_ts < self.cfg.compact_from_ts
            || file.min_ts > self.cfg.compact_to_ts
        {
            return None;
        }

        match self.items.entry(key) {
            Entry::Occupied(mut o) => {
                let key = *o.key();
                let u = o.get_mut();
                u.files.push(file.id);
                u.size += file.real_size;
                u.min_ts = u.min_ts.min(file.min_ts);
                u.max_ts = u.max_ts.max(file.max_ts);
                if u.max_key < file.max_key {
                    u.max_key = file.max_key;
                }
                if u.min_key > file.min_key {
                    u.min_key = file.min_key;
                }

                if u.size > self.compaction_size_threshold {
                    let c = Compaction {
                        source: std::mem::take(&mut u.files),
                        region_id: key.region_id,
                        cf: key.cf,
                        size: u.size,
                        input_min_ts: u.min_ts,
                        input_max_ts: u.max_ts,
                        min_key: u.min_key.clone(),
                        max_key: u.max_key.clone(),
                        compact_from_ts: self.cfg.compact_from_ts,
                        compact_to_ts: self.cfg.compact_to_ts,
                    };
                    o.remove();
                    return Some(c);
                }
            }
            Entry::Vacant(v) => {
                let u = UnformedCompaction {
                    size: file.real_size,
                    files: vec![file.id],
                    min_ts: file.min_ts,
                    max_ts: file.max_ts,
                    min_key: file.min_key.clone(),
                    max_key: file.max_key.clone(),
                };
                v.insert(u);
            }
        }
        None
    }

    fn take_pending_compactions(&mut self) -> impl Iterator<Item = Compaction> + '_ {
        self.items.drain().map(|(key, c)| Compaction {
            source: c.files,
            region_id: key.region_id,
            size: c.size,
            cf: key.cf,
            input_max_ts: c.max_ts,
            input_min_ts: c.min_ts,
            min_key: c.min_key,
            max_key: c.max_key,
            compact_from_ts: self.cfg.compact_from_ts,
            compact_to_ts: self.cfg.compact_to_ts,
        })
    }
}

impl<S: Stream<Item = Result<LogFile>>> Stream for CollectCompaction<S> {
    type Item = Result<Compaction>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(finalize) = this.last_compactions {
                return finalize.pop().map(Ok).into();
            }

            let item = ready!(this.inner.as_mut().poll_next(cx));
            match item {
                None => {
                    *this.last_compactions =
                        Some(this.collector.take_pending_compactions().collect())
                }
                Some(Err(err)) => return Some(Err(err.attach_current_frame())).into(),
                Some(Ok(item)) => {
                    if let Some(comp) = this.collector.add_new_file(item) {
                        return Some(Ok(comp)).into();
                    }
                }
            }
        }
    }
}

pub struct CompactWorker<DB> {
    source: Source,
    output: Arc<dyn ExternalStorage>,
    co: Cooperate,

    // Note: maybe use the TiKV config to construct a DB?
    _great_phantom: PhantomData<DB>,
}

#[derive(Default)]
pub struct CompactLogExt<'a> {
    pub load_statistic: Option<&'a mut LoadStatistic>,
    pub compact_statistic: Option<&'a mut CompactStatistic>,
    pub max_load_concurrency: usize,
}

impl<'a> CompactLogExt<'a> {
    fn with_compact_stat(&mut self, f: impl FnOnce(&mut CompactStatistic)) {
        if let Some(stat) = &mut self.compact_statistic {
            f(stat)
        }
    }

    fn with_load_stat(&mut self, f: impl FnOnce(&mut LoadStatistic)) {
        if let Some(stat) = &mut self.load_statistic {
            f(stat)
        }
    }
}

impl<DB> CompactWorker<DB> {
    pub fn inplace(storage: Arc<dyn ExternalStorage>) -> Self {
        Self {
            source: Source::new(Arc::clone(&storage)),
            output: storage,
            co: Cooperate::new(4096),
            _great_phantom: PhantomData,
        }
    }
}

impl<DB: SstExt> CompactWorker<DB>
where
    <<DB as SstExt>::SstWriter as SstWriter>::ExternalSstFileReader: 'static,
{
    const COMPRESSION: Option<SstCompressionType> = Some(SstCompressionType::Lz4);

    async fn pick_and_sort(
        &mut self,
        c: &Compaction,
        items: impl Iterator<Item = Vec<Record>>,
    ) -> Vec<Record> {
        let mut flatten_items = items
            .into_iter()
            .flat_map(|v| v.into_iter())
            .filter(|v| {
                v.ts()
                    .map(|ts| ts >= c.compact_from_ts && ts < c.compact_to_ts)
                    .unwrap_or(false)
            })
            .collect::<Vec<_>>();
        flatten_items.sort_unstable_by(|k1, k2| k1.cmp_key(&k2));
        tokio::task::yield_now().await;
        flatten_items.dedup_by(|k1, k2| k1.cmp_key(&k2) == std::cmp::Ordering::Equal);
        flatten_items
    }

    async fn load(
        &mut self,
        c: &Compaction,
        ext: &mut CompactLogExt<'_>,
    ) -> Result<impl Iterator<Item = Vec<Record>>> {
        let mut eext = ExecuteAllExt::default();
        let load_stat = ext.load_statistic.is_some();
        eext.max_concurrency = ext.max_load_concurrency;

        let items = super::util::execute_all_ext(
            c.source
                .iter()
                .cloned()
                .map(|f| {
                    let source = &self.source;
                    Box::pin(async move {
                        let mut out = vec![];
                        let mut stat = LoadStatistic::default();
                        source
                            .load(f, load_stat.then_some(&mut stat), |k, v| {
                                out.push(Record {
                                    key: k.to_owned(),
                                    value: v.to_owned(),
                                })
                            })
                            .await?;
                        Result::Ok((out, stat))
                    })
                })
                .collect(),
            eext,
        )
        .await?;

        let mut result = Vec::with_capacity(items.len());
        for (item, stat) in items {
            ext.with_load_stat(|s| s.merge_with(&stat));
            result.push(item);
        }
        Ok(result.into_iter())
    }

    async fn write_sst(
        &mut self,
        cf: CfName,
        sorted_items: impl Iterator<Item = Record>,
        ext: &mut CompactLogExt<'_>,
    ) -> Result<(u64, impl std::io::Read + 'static)> {
        let mut w = <DB as SstExt>::SstWriterBuilder::new()
            .set_cf(cf)
            .set_compression_type(Self::COMPRESSION)
            .set_in_memory(true)
            .build(&"in-mem.sst")?;

        for item in sorted_items {
            self.co.step().await;
            w.put(&item.key, &item.value)?;
            ext.with_compact_stat(|stat| {
                stat.logical_key_bytes_out += item.key.len() as u64;
                stat.logical_value_bytes_out += item.value.len() as u64;
            })
        }
        let (info, out) = w.finish_read()?;
        ext.with_compact_stat(|stat| {
            stat.keys_out += info.num_entries();
            stat.physical_bytes_out += info.file_size();
        });

        Ok((info.file_size(), out))
    }

    pub async fn compact_ext(&mut self, c: Compaction, mut ext: CompactLogExt<'_>) -> Result<()> {
        let mut eext = ExecuteAllExt::default();
        eext.max_concurrency = ext.max_load_concurrency;

        let begin = Instant::now();
        let items = self.load(&c, &mut ext).await?;
        ext.with_compact_stat(|stat| stat.load_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let sorted_items = self.pick_and_sort(&c, items).await;
        ext.with_compact_stat(|stat| stat.sort_duration += begin.saturating_elapsed());

        if sorted_items.is_empty() {
            ext.with_compact_stat(|stat| stat.empty_generation += 1);
            return Ok(());
        }

        let begin = Instant::now();
        let (size, out) = self
            .write_sst(c.cf, sorted_items.into_iter(), &mut ext)
            .await?;
        ext.with_compact_stat(|stat| stat.write_sst_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let out_name = format!(
            "compact-out/{}-{}-{}-{}.sst",
            c.input_min_ts, c.input_max_ts, c.cf, c.region_id
        );
        self.output
            .write(
                &out_name,
                external_storage::UnpinReader(Box::new(AllowStdIo::new(out))),
                size,
            )
            .await?;
        ext.with_compact_stat(|stat| stat.save_duration += begin.saturating_elapsed());
        Ok(())
    }
}
