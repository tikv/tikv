use std::{
    collections::{BTreeSet, HashMap},
    marker::PhantomData,
    pin::Pin,
    process::Output,
    sync::Arc,
    task::ready,
};

use async_compression::futures::write::ZstdDecoder;
use engine_traits::{
    ExternalSstFileInfo, SstCompressionType, SstExt, SstMetaInfo, SstWriter, SstWriterBuilder,
};
use external_storage::ExternalStorage;
use futures::io::{AllowStdIo, AsyncReadExt, AsyncWriteExt, Cursor};
use tikv_util::{
    codec::{
        self,
        stream_event::{self, Iterator as KvStreamIter},
    },
    config::ReadableSize,
    stream::block_on_external_io,
};
use tokio::{io::AsyncRead, sync::mpsc::Receiver};
use tokio_stream::Stream;

use super::{
    errors::Result,
    storage::{LogFile, LogFileId},
    util::{Cooperate, ExecuteAllExt},
};

#[derive(Debug)]
pub struct Compaction {
    pub source: Vec<LogFileId>,
    pub size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub max_ts: u64,
    pub min_ts: u64,
}

struct UnformedCompaction {
    size: u64,
    files: Vec<LogFileId>,
    min_ts: u64,
    max_ts: u64,
}

#[pin_project::pin_project]
pub struct CollectCompaction<S: Stream<Item = Result<LogFile>>> {
    #[pin]
    inner: S,
    last_compactions: Option<Vec<Compaction>>,

    collector: CompactionCollector,
}

impl<S: Stream<Item = Result<LogFile>>> CollectCompaction<S> {
    pub fn new(s: S) -> Self {
        CollectCompaction {
            inner: s,
            last_compactions: None,
            collector: CompactionCollector {
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
}

impl CompactionCollector {
    fn add_new_file(&mut self, file: LogFile) -> Option<Compaction> {
        use std::collections::hash_map::Entry;
        let key = CompactionCollectKey {
            region_id: file.region_id,
            cf: file.cf,
        };
        match self.items.entry(key) {
            Entry::Occupied(mut o) => {
                let key = *o.key();
                let u = o.get_mut();
                u.files.push(file.id);
                u.size += file.real_size;
                u.min_ts = u.min_ts.min(file.min_ts);
                u.max_ts = u.max_ts.max(file.max_ts);

                if u.size > self.compaction_size_threshold {
                    let c = Compaction {
                        source: std::mem::take(&mut u.files),
                        region_id: key.region_id,
                        cf: key.cf,
                        size: u.size,
                        min_ts: u.min_ts,
                        max_ts: u.max_ts,
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
            max_ts: c.max_ts,
            min_ts: c.min_ts,
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

#[derive(Clone)]
struct Source {
    inner: Arc<dyn ExternalStorage>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Record {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl Source {
    async fn load(&self, id: LogFileId, output: &mut Vec<Record>) -> Result<()> {
        let mut content = vec![];
        let mut decompress = ZstdDecoder::new(Cursor::new(&mut content));
        let source = self.inner.read_part(&id.name, id.offset, id.length);
        futures::io::copy(source, &mut decompress).await?;
        decompress.flush().await?;
        drop(decompress);

        let mut co = Cooperate::new(4096);
        let iter = stream_event::EventIterator::new(&content);
        while iter.valid() {
            co.step().await;
            output.push(Record {
                key: iter.key().to_owned(),
                value: iter.value().to_owned(),
            })
        }
        Ok(())
    }
}

struct CompactWorker<DB> {
    rx: Receiver<Compaction>,

    source: Source,
    output: Arc<dyn ExternalStorage>,
    max_load_concurrency: usize,
    co: Cooperate,

    // Note: maybe use the TiKV config to construct a DB?
    _great_phantom: PhantomData<DB>,
}

impl<DB: SstExt> CompactWorker<DB>
where
    <<DB as SstExt>::SstWriter as SstWriter>::ExternalSstFileReader: 'static,
{
    const COMPRESSION: Option<SstCompressionType> = Some(SstCompressionType::Lz4);

    async fn merge_and_sort(&mut self, items: Vec<Vec<Record>>) -> Vec<Record> {
        #[derive(PartialEq, Eq)]
        struct KeyRecord(Record);

        impl PartialOrd for KeyRecord {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(Ord::cmp(self, other))
            }
        }

        impl Ord for KeyRecord {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.key.cmp(&other.0.key)
            }
        }

        let mut result = BTreeSet::new();
        let flatten_items = items.into_iter().flat_map(|v| v.into_iter());
        for item in flatten_items {
            self.co.step().await;
            result.insert(KeyRecord(item));
        }
        result.into_iter().map(|f| f.0).collect()
    }

    async fn compact(&mut self, c: Compaction) -> Result<()> {
        let mut ext = ExecuteAllExt::default();
        ext.max_concurrency = self.max_load_concurrency;
        let items = super::util::execute_all_ext(
            c.source
                .into_iter()
                .map(|f| {
                    let source = &self.source;
                    Box::pin(async move {
                        let mut out = vec![];
                        source.load(f, &mut out).await?;
                        Result::Ok(out)
                    })
                })
                .collect(),
            ext,
        )
        .await?;
        let sorted_items = self.merge_and_sort(items).await;

        let out_name = format!("{}-{}-{}.sst", c.region_id, c.min_ts, c.max_ts);
        let mut w = <DB as SstExt>::SstWriterBuilder::new()
            .set_cf(c.cf)
            .set_compression_type(Self::COMPRESSION)
            .set_in_memory(true)
            .build(&out_name)?;

        for item in sorted_items {
            self.co.step().await;
            w.put(&item.key, &item.value)?;
        }

        let (info, out) = w.finish_read()?;

        self.output
            .write(
                &out_name,
                external_storage::UnpinReader(Box::new(AllowStdIo::new(out))),
                info.file_size(),
            )
            .await?;
        Ok(())
    }
}
