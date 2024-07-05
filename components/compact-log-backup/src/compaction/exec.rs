use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use engine_traits::{
    CfName, ExternalSstFileInfo, SstCompressionType, SstExt, SstWriter, SstWriterBuilder,
    DATA_KEY_PREFIX_LEN,
};
use external_storage::{ExternalStorage, UnpinReader};
use file_system::Sha256Reader;
use futures::{
    future::TryFutureExt,
    io::{AllowStdIo, Cursor},
};
use kvproto::brpb::{self, LogFileSubcompaction};
use tikv_util::{
    retry_expr,
    stream::{JustRetry, RetryExt},
    time::Instant,
};

use super::{Subcompaction, SubcompactionResult};
use crate::{
    compaction::SST_OUT_REL,
    errors::{OtherErrExt, Result, TraceResultExt},
    source::{Record, Source},
    statistic::{CompactStatistic, LoadStatistic},
    storage::COMPACTION_OUT_PREFIX,
    util::{self, Cooperate, ExecuteAllExt},
};

pub struct SubcompactionExec<DB> {
    source: Source,
    output: Arc<dyn ExternalStorage>,
    co: Cooperate,
    out_prefix: PathBuf,

    load_stat: LoadStatistic,
    compact_stat: CompactStatistic,

    db: Option<DB>,
}

pub struct SubcompactExt {
    pub max_load_concurrency: usize,
    pub compression: SstCompressionType,
    pub compression_level: Option<i32>,
}

impl<'a> Default for SubcompactExt {
    fn default() -> Self {
        Self {
            max_load_concurrency: Default::default(),
            compression: SstCompressionType::Lz4,
            compression_level: None,
        }
    }
}

pub struct SingleCompactionArg<DB> {
    pub out_prefix: Option<PathBuf>,
    pub db: Option<DB>,
    pub storage: Arc<dyn ExternalStorage>,
}

impl<DB> From<SingleCompactionArg<DB>> for SubcompactionExec<DB> {
    fn from(value: SingleCompactionArg<DB>) -> Self {
        Self {
            source: Source::new(Arc::clone(&value.storage)),
            output: value.storage,
            co: Cooperate::new(4096),
            out_prefix: value
                .out_prefix
                .unwrap_or_else(|| Path::new(COMPACTION_OUT_PREFIX).to_owned()),
            db: value.db,
            load_stat: Default::default(),
            compact_stat: Default::default(),
        }
    }
}

impl<DB> SubcompactionExec<DB> {
    #[cfg(test)]
    pub fn default_config(storage: Arc<dyn ExternalStorage>) -> Self {
        Self::from(SingleCompactionArg {
            storage,
            out_prefix: None,
            db: None,
        })
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

impl<DB: SstExt> SubcompactionExec<DB>
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
        c: &Subcompaction,
        ext: &mut SubcompactExt,
    ) -> Result<impl Iterator<Item = Vec<Record>>> {
        let mut eext = ExecuteAllExt::default();
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
                            .load(f.id, Some(&mut stat), |k, v| {
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
            self.load_stat += stat;
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
        ext: &mut SubcompactExt,
    ) -> Result<WrittenSst<<DB::SstWriter as SstWriter>::ExternalSstFileReader>> {
        let mut wb = <DB as SstExt>::SstWriterBuilder::new()
            .set_cf(cf)
            .set_compression_type(Some(ext.compression))
            .set_in_memory(true);
        if let Some(db) = self.db.as_ref() {
            wb = wb.set_db(db);
        }
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

        let mut data_key = keys::DATA_PREFIX_KEY.to_vec();
        for item in sorted_items {
            self.co.step().await;

            let mut d = crc64fast::Digest::new();
            d.write(&item.key);
            d.write(&item.value);
            let ts = item.ts().trace_err()?;
            meta.crc64xor ^= d.sum64();
            meta.start_version = meta.start_version.min(ts);
            meta.end_version = meta.end_version.max(ts);

            // NOTE: We may need to check whether the key is already a data key here once we
            // are going to support compact SSTs.
            data_key.truncate(DATA_KEY_PREFIX_LEN);
            data_key.extend(&item.key);
            w.put(&data_key, &item.value)?;

            self.compact_stat.logical_key_bytes_out += item.key.len() as u64;
            self.compact_stat.logical_value_bytes_out += item.value.len() as u64;
            meta.total_kvs += 1;
            meta.total_bytes += item.key.len() as u64 + item.value.len() as u64;
        }
        let (info, out) = w.finish_read()?;
        self.compact_stat.keys_out += info.num_entries();
        self.compact_stat.physical_bytes_out += info.file_size();

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
        c: &Subcompaction,
        sst: &mut WrittenSst<<DB::SstWriter as SstWriter>::ExternalSstFileReader>,
    ) -> Result<LogFileSubcompaction> {
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
        let mut meta = brpb::LogFileSubcompaction::new();
        meta.set_meta(c.pb_meta());
        meta.set_sst_outputs(vec![sst.meta.clone()].into());
        Ok(meta)
    }

    #[tracing::instrument(skip_all, fields(c=%c))]
    pub async fn run(
        mut self,
        c: Subcompaction,
        mut ext: SubcompactExt,
    ) -> Result<SubcompactionResult> {
        let mut result = SubcompactionResult::of(c);
        let c = &result.origin;
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
        self.compact_stat.load_duration += begin.saturating_elapsed();

        let begin = Instant::now();
        let (sorted_items, cdiff) = self.process_input(items).await;
        self.compact_stat.sort_duration += begin.saturating_elapsed();
        result
            .expected_crc64
            .as_mut()
            .map(|v| *v ^= cdiff.crc64xor_diff);
        result.expected_keys -= cdiff.removed_key;
        result.expected_size -= cdiff.decreaed_size;

        if sorted_items.is_empty() {
            self.compact_stat.empty_generation += 1;
            return Ok(result);
        }

        let out_name = self
            .out_prefix
            .join(SST_OUT_REL)
            .join(format!(
                "{}_{}_{}_{}.sst",
                util::aligned_u64(c.input_min_ts),
                util::aligned_u64(c.input_max_ts),
                c.cf,
                c.region_id
            ))
            .display()
            .to_string();
        let begin = Instant::now();
        assert!(!sorted_items.is_empty());
        let mut sst = self
            .write_sst(&out_name, c.cf, sorted_items.as_slice(), &mut ext)
            .await?;

        self.compact_stat.write_sst_duration += begin.saturating_elapsed();

        let begin = Instant::now();
        result.meta =
            retry_expr! { self.upload_compaction_artifact(&c, &mut sst).map_err(JustRetry) }
                .await
                .map_err(|err| err.0)?;
        self.compact_stat.save_duration += begin.saturating_elapsed();

        result.compact_stat = self.compact_stat;
        result.load_stat = self.load_stat;

        return Ok(result);
    }
}
