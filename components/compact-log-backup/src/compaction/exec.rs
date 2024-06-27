use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use engine_traits::{
    CfName, ExternalSstFileInfo, SstCompressionType, SstExt, SstWriter, SstWriterBuilder,
};
use external_storage::{ExternalStorage, UnpinReader};
use file_system::Sha256Reader;
use futures::{
    future::TryFutureExt,
    io::{AllowStdIo, Cursor},
};
use kvproto::brpb::{self, LogFileCompactionMeta};
use tikv_util::{
    codec::stream_event::Iterator as KvStreamIter,
    retry_expr,
    stream::{JustRetry, RetryExt},
    time::Instant,
};

use super::{Compaction, CompactionResult};
use crate::{
    errors::{OtherErrExt, Result, TraceResultExt},
    source::{Record, Source},
    statistic::{CompactStatistic, LoadStatistic},
    storage::{COMPACTION_METADATA_PREFIX, COMPACTION_OUT_PREFIX},
    util,
    util::{Cooperate, ExecuteAllExt},
};

pub struct SingleCompactionExec<DB> {
    source: Source,
    output: Arc<dyn ExternalStorage>,
    co: Cooperate,
    out_prefix: PathBuf,

    // Note: maybe use the TiKV config to construct a DB?
    _great_phantom: PhantomData<DB>,
}

pub struct CompactLogExt<'a> {
    pub load_statistic: Option<&'a mut LoadStatistic>,
    pub compact_statistic: Option<&'a mut CompactStatistic>,
    pub max_load_concurrency: usize,
    pub compression: SstCompressionType,
    pub compression_level: Option<i32>,
}

impl<'a> Default for CompactLogExt<'a> {
    fn default() -> Self {
        Self {
            load_statistic: Default::default(),
            compact_statistic: Default::default(),
            max_load_concurrency: Default::default(),
            compression: SstCompressionType::Lz4,
            compression_level: None,
        }
    }
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

impl<DB> SingleCompactionExec<DB> {
    pub fn inplace(storage: Arc<dyn ExternalStorage>) -> Self {
        Self::with_output_prefix(storage, COMPACTION_OUT_PREFIX)
    }

    pub fn with_output_prefix(storage: Arc<dyn ExternalStorage>, pfx: impl AsRef<Path>) -> Self {
        Self {
            source: Source::new(Arc::clone(&storage)),
            output: storage,
            co: Cooperate::new(4096),
            _great_phantom: PhantomData,
            out_prefix: pfx.as_ref().to_owned(),
        }
    }
}

struct WrittenSst<S> {
    content: S,
    meta: kvproto::brpb::File,
    physical_size: u64,
}

#[derive(Default)]
struct ChecksumDiff {
    removed_key: u64,
    decreaed_size: u64,
    crc64xor_diff: u64,
}

impl<DB: SstExt> SingleCompactionExec<DB>
where
    <<DB as SstExt>::SstWriter as SstWriter>::ExternalSstFileReader: 'static,
{
    fn update_checksum_diff(a: &Record, b: &Record, diff: &mut ChecksumDiff) {
        assert_eq!(a, b);

        diff.removed_key += 1;
        diff.decreaed_size += (a.key.len() + a.value.len()) as u64;
        let mut d = crc64fast::Digest::new();
        d.write(&a.key);
        d.write(&a.value);
        diff.crc64xor_diff ^= d.sum64();
    }

    #[tracing::instrument(skip_all)]
    async fn process_input(
        &mut self,
        items: impl Iterator<Item = Vec<Record>>,
    ) -> (Vec<Record>, ChecksumDiff) {
        let mut flatten_items = items
            .into_iter()
            .flat_map(|v| v.into_iter())
            .collect::<Vec<_>>();
        tokio::task::yield_now().await;
        flatten_items.sort_unstable_by(|k1, k2| k1.cmp_key(&k2));
        tokio::task::yield_now().await;
        let mut diff = ChecksumDiff::default();
        flatten_items.dedup_by(|k1, k2| {
            if k1.key == k2.key {
                Self::update_checksum_diff(k1, k2, &mut diff);
                true
            } else {
                false
            }
        });
        (flatten_items, diff)
    }

    #[tracing::instrument(skip_all)]
    async fn load(
        &mut self,
        c: &Compaction,
        ext: &mut CompactLogExt<'_>,
    ) -> Result<impl Iterator<Item = Vec<Record>>> {
        let mut eext = ExecuteAllExt::default();
        let load_stat = ext.load_statistic.is_some();
        eext.max_concurrency = ext.max_load_concurrency;

        let items = super::util::execute_all_ext(
            c.inputs
                .iter()
                .cloned()
                .map(|f| {
                    let source = &self.source;
                    Box::pin(async move {
                        let mut out = vec![];
                        let mut stat = LoadStatistic::default();
                        source
                            .load(f.id, load_stat.then_some(&mut stat), |k, v| {
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
            ext.with_load_stat(|s| *s += stat);
            result.push(item);
        }
        Ok(result.into_iter())
    }

    /// write the `sorted_items` to a in-mem SST.
    ///
    /// # Panics
    ///
    /// For now, if the `sorted_items` is empty, it will panic.
    /// But it is reasonable to return an error in this scenario if needed.
    #[tracing::instrument(skip_all, fields(name=%name))]
    async fn write_sst(
        &mut self,
        name: &str,
        cf: CfName,
        sorted_items: &[Record],
        ext: &mut CompactLogExt<'_>,
    ) -> Result<WrittenSst<<DB::SstWriter as SstWriter>::ExternalSstFileReader>> {
        let mut wb = <DB as SstExt>::SstWriterBuilder::new()
            .set_cf(cf)
            .set_compression_type(Some(ext.compression))
            .set_in_memory(true);
        if let Some(level) = ext.compression_level {
            wb = wb.set_compression_level(level);
        }
        let mut w = wb.build(name)?;
        let mut meta = kvproto::brpb::File::default();
        meta.set_start_key(sorted_items[0].key.clone());
        meta.set_end_key(sorted_items.last().unwrap().key.clone());
        meta.set_cf(cf.to_owned());
        meta.name = name.to_owned();
        meta.end_version = u64::MAX;

        for item in sorted_items {
            self.co.step().await;
            let mut d = crc64fast::Digest::new();
            d.write(&item.key);
            d.write(&item.value);
            let ts = item.ts().trace_err()?;
            meta.crc64xor ^= d.sum64();
            meta.start_version = meta.start_version.min(ts);
            meta.end_version = meta.end_version.max(ts);
            w.put(&item.key, &item.value)?;
            ext.with_compact_stat(|stat| {
                stat.logical_key_bytes_out += item.key.len() as u64;
                stat.logical_value_bytes_out += item.value.len() as u64;
            });
            meta.total_kvs += 1;
            meta.total_bytes += item.key.len() as u64 + item.value.len() as u64;
        }
        let (info, out) = w.finish_read()?;
        ext.with_compact_stat(|stat| {
            stat.keys_out += info.num_entries();
            stat.physical_bytes_out += info.file_size();
        });

        let result = WrittenSst {
            content: out,
            meta,
            physical_size: info.file_size(),
        };

        Ok(result)
    }

    #[tracing::instrument(skip_all, fields(name=%sst.meta.name))]
    async fn upload_compaction_artifact(
        &mut self,
        c: &Compaction,
        sst: &mut WrittenSst<<DB::SstWriter as SstWriter>::ExternalSstFileReader>,
    ) -> Result<LogFileCompactionMeta> {
        use engine_traits::ExternalSstFileReader;
        sst.content.reset()?;
        let (rd, hasher) = Sha256Reader::new(&mut sst.content).adapt_err()?;
        self.output
            .write(
                &sst.meta.name,
                external_storage::UnpinReader(Box::new(AllowStdIo::new(rd))),
                sst.physical_size,
            )
            .await?;
        sst.meta.sha256 = hasher.lock().unwrap().finish().adapt_err()?.to_vec();
        let mut meta = brpb::LogFileCompactionMeta::new();
        meta.set_compaction(c.brpb_compaction());
        meta.set_output(vec![sst.meta.clone()].into());
        let meta_name = format!(
            "{}_{}_{}.cmeta",
            util::aligned_u64(c.input_min_ts),
            util::aligned_u64(c.input_max_ts),
            util::aligned_u64(c.crc64())
        );
        let meta_name = self.out_prefix.join(meta_name).display().to_string();
        let mut meta_bytes = vec![];
        protobuf::Message::write_to_vec(&meta, &mut meta_bytes)?;
        self.output
            .write(
                &meta_name,
                UnpinReader(Box::new(Cursor::new(&meta_bytes))),
                meta_bytes.len() as u64,
            )
            .await?;
        Ok(meta)
    }

    #[tracing::instrument(skip_all, fields(c=%c))]
    pub async fn compact_ext(
        mut self,
        c: Compaction,
        mut ext: CompactLogExt<'_>,
    ) -> Result<CompactionResult> {
        let mut eext = ExecuteAllExt::default();
        eext.max_concurrency = ext.max_load_concurrency;
        let mut result = CompactionResult::default();
        for input in &c.inputs {
            if input.crc64xor == 0 {
                result.expected_crc64 = None;
            }
            result.expected_crc64.as_mut().map(|v| *v ^= input.crc64xor);
            result.expected_keys += input.num_of_entries;
            result.expected_size += input.key_value_size;
        }

        let begin = Instant::now();
        let items = self.load(&c, &mut ext).await?;
        ext.with_compact_stat(|stat| stat.load_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        let (sorted_items, cdiff) = self.process_input(items).await;
        ext.with_compact_stat(|stat| stat.sort_duration += begin.saturating_elapsed());
        result
            .expected_crc64
            .as_mut()
            .map(|v| *v ^= cdiff.crc64xor_diff);
        result.expected_keys -= cdiff.removed_key;
        result.expected_size -= cdiff.decreaed_size;

        if sorted_items.is_empty() {
            ext.with_compact_stat(|stat| stat.empty_generation += 1);
            return Ok(result);
        }

        let out_name = format!(
            "{}/{}-{}-{}-{}.sst",
            COMPACTION_OUT_PREFIX, c.input_min_ts, c.input_max_ts, c.cf, c.region_id
        );
        let begin = Instant::now();
        assert!(!sorted_items.is_empty());
        let mut sst = self
            .write_sst(&out_name, c.cf, sorted_items.as_slice(), &mut ext)
            .await?;

        ext.with_compact_stat(|stat| stat.write_sst_duration += begin.saturating_elapsed());

        let begin = Instant::now();
        result.meta =
            retry_expr! { self.upload_compaction_artifact(&c, &mut sst).map_err(JustRetry) }
                .await
                .map_err(|err| err.0)?;
        ext.with_compact_stat(|stat| stat.save_duration += begin.saturating_elapsed());

        return Ok(result);
    }
}
