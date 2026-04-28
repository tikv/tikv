// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{HashMap, hash_map::Entry},
    pin::{Pin, pin},
    sync::Arc,
};

use async_compression::futures::write::ZstdDecoder;
use bytes::Bytes;
use cloud::blob::read_to_end;
use external_storage::ExternalStorage;
use futures::io::{AsyncWriteExt, Cursor};
use futures_io::AsyncWrite;
use kvproto::brpb;
use prometheus::core::{Atomic, AtomicU64};
use protobuf::Chars;
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{JustRetry, RetryExt, retry_all_ext},
};
use txn_types::Key;

use super::{statistic::LoadStatistic, util::Cooperate};
use crate::{compaction::Input, errors::Result};

type SpanKey = (u64, u64);

/// The manager of fetching log files from remote for compacting.
#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ExternalStorage>,
    physical_file_cache: Option<Arc<PhysicalFileCache>>,
}

impl Source {
    #[cfg(test)]
    pub fn new(inner: Arc<dyn ExternalStorage>) -> Self {
        Self {
            inner,
            physical_file_cache: None,
        }
    }

    pub fn new_with_cache(
        inner: Arc<dyn ExternalStorage>,
        physical_file_cache: Option<Arc<PhysicalFileCache>>,
    ) -> Self {
        Self {
            inner,
            physical_file_cache,
        }
    }
}

struct CacheEntry {
    remaining_spans: HashMap<SpanKey, usize>,
    content: Option<Bytes>,
    loading: bool,
    notify: Arc<tokio::sync::Notify>,
}

impl CacheEntry {
    fn new() -> Self {
        Self {
            remaining_spans: HashMap::new(),
            content: None,
            loading: false,
            notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    fn register(&mut self, offset: u64, length: u64) {
        *self.remaining_spans.entry((offset, length)).or_default() += 1;
    }

    fn take_span(&mut self, offset: u64, length: u64) -> bool {
        match self.remaining_spans.entry((offset, length)) {
            Entry::Occupied(mut entry) if *entry.get() > 1 => {
                *entry.get_mut() -= 1;
                false
            }
            Entry::Occupied(entry) => {
                entry.remove();
                self.remaining_spans.is_empty()
            }
            Entry::Vacant(_) => self.remaining_spans.is_empty(),
        }
    }
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<Chars, CacheEntry>,
    reserved_bytes: u64,
}

/// A bounded cache for raw physical log files.
///
/// The cache stores each physical object as one `Bytes` allocation. Logical
/// files are returned as zero-copy slices, and registered spans are consumed
/// once they are taken. When the last span of a physical file is taken, the
/// entry is removed immediately so the backing allocation can be released as
/// soon as outstanding slices finish decompression.
pub struct PhysicalFileCache {
    capacity: u64,
    state: tokio::sync::Mutex<CacheState>,
}

impl PhysicalFileCache {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity,
            state: tokio::sync::Mutex::new(CacheState::default()),
        }
    }

    pub async fn register_inputs(&self, inputs: &[Input]) {
        let mut state = self.state.lock().await;
        for input in inputs {
            let entry = state
                .entries
                .entry(input.id.name.clone())
                .or_insert_with(CacheEntry::new);
            entry.register(input.id.offset, input.id.length);
        }
    }

    async fn take_or_reserve(&self, input: &Input) -> CacheDecision {
        if input.physical_file_size > self.capacity {
            self.take_registered_span(input).await;
            return CacheDecision::Bypass;
        }
        loop {
            let mut state = self.state.lock().await;
            let reserved_bytes = state.reserved_bytes;
            let entry = state
                .entries
                .entry(input.id.name.clone())
                .or_insert_with(CacheEntry::new);

            if let Some(content) = entry.content.clone() {
                let start = input.id.offset as usize;
                let end = start + input.id.length as usize;
                let part = content.slice(start..end);
                if entry.take_span(input.id.offset, input.id.length) {
                    state.entries.remove(&input.id.name);
                    state.reserved_bytes = state
                        .reserved_bytes
                        .saturating_sub(input.physical_file_size);
                    drop(state);
                }
                return CacheDecision::Ready(part);
            }
            if entry.loading {
                let notify = Arc::clone(&entry.notify);
                let notified = notify.notified();
                drop(state);
                notified.await;
                continue;
            }
            let physical_size = input.physical_file_size;
            if reserved_bytes + physical_size <= self.capacity {
                entry.loading = true;
                state.reserved_bytes += physical_size;
                return CacheDecision::Download;
            }
            if entry.take_span(input.id.offset, input.id.length) {
                state.entries.remove(&input.id.name);
            }
            return CacheDecision::Bypass;
        }
    }

    async fn take_registered_span(&self, input: &Input) {
        let mut state = self.state.lock().await;
        if let Some(entry) = state.entries.get_mut(&input.id.name) {
            if entry.take_span(input.id.offset, input.id.length) {
                state.entries.remove(&input.id.name);
            }
        }
    }

    async fn finish_download(&self, input: &Input, content: Bytes) -> Bytes {
        let mut state = self.state.lock().await;
        let start = input.id.offset as usize;
        let end = start + input.id.length as usize;
        let (part, remove, notify) = {
            let entry = state
                .entries
                .entry(input.id.name.clone())
                .or_insert_with(CacheEntry::new);
            entry.loading = false;
            entry.content = Some(content.clone());
            let notify = Arc::clone(&entry.notify);
            let part = content.slice(start..end);
            let remove = entry.take_span(input.id.offset, input.id.length);
            (part, remove, notify)
        };
        if remove {
            state.entries.remove(&input.id.name);
            state.reserved_bytes = state
                .reserved_bytes
                .saturating_sub(input.physical_file_size);
        }
        drop(state);
        notify.notify_waiters();
        part
    }

    async fn fail_download(&self, input: &Input) {
        let mut state = self.state.lock().await;
        let mut notify = None;
        if let Some(entry) = state.entries.get_mut(&input.id.name) {
            entry.loading = false;
            notify = Some(Arc::clone(&entry.notify));
        }
        state.reserved_bytes = state
            .reserved_bytes
            .saturating_sub(input.physical_file_size);
        drop(state);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }
}

enum CacheDecision {
    Ready(Bytes),
    Download,
    Bypass,
}

/// A record from log files.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
    #[inline(always)]
    pub fn cmp_key(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }

    pub fn ts(&self) -> Result<u64> {
        let ts = Key::decode_ts_from(&self.key)?.into_inner();
        Ok(ts)
    }
}

impl Source {
    /// Load the content of an input.
    #[tracing::instrument(skip_all)]
    pub async fn load_remote(
        &self,
        input: Input,
        stat: &mut Option<&mut LoadStatistic>,
    ) -> Result<Vec<u8>> {
        if let Some(cache) = &self.physical_file_cache {
            return self.load_remote_with_cache(input, cache, stat).await;
        }
        self.load_remote_part(input, stat).await
    }

    async fn load_remote_part(
        &self,
        input: Input,
        stat: &mut Option<&mut LoadStatistic>,
    ) -> Result<Vec<u8>> {
        let error_during_downloading = Arc::new(AtomicU64::new(0));
        let counter = error_during_downloading.clone();
        let ext = RetryExt::default()
            .with_fail_hook(move |_: &JustRetry<std::io::Error>| counter.inc_by(1));
        let fetch = || {
            let storage = self.inner.clone();
            let id = input.id.clone();
            let compression = input.compression;
            let file_real_size = input.file_real_size;
            async move {
                let mut content = Vec::with_capacity(file_real_size as _);
                let item = pin!(Cursor::new(&mut content));
                let mut decompress = decompress(compression, item)?;
                let source = storage.read_part(&id.name, id.offset, id.length);
                let n = futures::io::copy(source, &mut decompress).await?;
                decompress.flush().await?;
                drop(decompress);
                std::io::Result::Ok((content, n))
            }
        };
        let (content, size) = retry_all_ext(fetch, ext).await?;
        if let Some(stat) = stat.as_mut() {
            stat.physical_bytes_in += size;
            stat.error_during_downloading += error_during_downloading.get();
        }
        Ok(content)
    }

    async fn load_remote_with_cache(
        &self,
        input: Input,
        cache: &PhysicalFileCache,
        stat: &mut Option<&mut LoadStatistic>,
    ) -> Result<Vec<u8>> {
        match cache.take_or_reserve(&input).await {
            CacheDecision::Ready(part) => {
                self.decode_compressed_bytes(input.compression, input.file_real_size, part)
                    .await
            }
            CacheDecision::Bypass => self.load_remote_part(input, stat).await,
            CacheDecision::Download => {
                let raw = self.load_physical_file(&input, stat).await;
                match raw {
                    Ok(raw) => {
                        let part = cache.finish_download(&input, raw).await;
                        self.decode_compressed_bytes(input.compression, input.file_real_size, part)
                            .await
                    }
                    Err(err) => {
                        cache.fail_download(&input).await;
                        Err(err)
                    }
                }
            }
        }
    }

    async fn load_physical_file(
        &self,
        input: &Input,
        stat: &mut Option<&mut LoadStatistic>,
    ) -> Result<Bytes> {
        let error_during_downloading = Arc::new(AtomicU64::new(0));
        let counter = error_during_downloading.clone();
        let ext = RetryExt::default()
            .with_fail_hook(move |_: &JustRetry<std::io::Error>| counter.inc_by(1));
        let fetch = || {
            let storage = self.inner.clone();
            let name = input.id.name.clone();
            async move {
                let mut content = Vec::new();
                let n = read_to_end(storage.read(&name), &mut content).await?;
                std::io::Result::Ok((Bytes::from(content), n))
            }
        };
        let (content, size) = retry_all_ext(fetch, ext).await?;
        if let Some(stat) = stat.as_mut() {
            stat.physical_bytes_in += size;
            stat.error_during_downloading += error_during_downloading.get();
        }
        Ok(content)
    }

    async fn decode_compressed_bytes(
        &self,
        compression: brpb::CompressionType,
        uncompressed_size: u64,
        input: Bytes,
    ) -> Result<Vec<u8>> {
        let mut content = Vec::with_capacity(uncompressed_size as _);
        let item = pin!(Cursor::new(&mut content));
        let mut decompress = decompress(compression, item)?;
        let source = Cursor::new(input);
        futures::io::copy(source, &mut decompress).await?;
        decompress.flush().await?;
        drop(decompress);
        Ok(content)
    }

    /// Load key value pairs from remote.
    #[tracing::instrument(skip_all, fields(id=?input.id))]
    pub async fn load(
        &self,
        input: Input,
        mut stat: Option<&mut LoadStatistic>,
        mut on_key_value: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        let content = self.load_remote(input, &mut stat).await?;

        let mut co = Cooperate::default();
        let mut iter = stream_event::EventIterator::new(&content);
        while let Some((k, v)) = iter.get_next()? {
            co.step().await;
            on_key_value(k, v);
            if let Some(stat) = stat.as_mut() {
                stat.keys_in += 1;
                stat.logical_key_bytes_in += iter.key().len() as u64;
                stat.logical_value_bytes_in += iter.value().len() as u64;
            }
        }
        if let Some(stat) = stat.as_mut() {
            stat.files_in += 1;
        }
        Ok(())
    }
}

fn decompress(
    compression: brpb::CompressionType,
    input: Pin<&mut (impl AsyncWrite + Send)>,
) -> std::io::Result<impl AsyncWrite + Send + '_> {
    match compression {
        kvproto::brpb::CompressionType::Zstd => Ok(ZstdDecoder::new(input)),
        compress => Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!("the compression type ({:?}) isn't supported", compress),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{PhysicalFileCache, Source};
    use crate::{
        compaction::{Input, Subcompaction},
        statistic::LoadStatistic,
        storage::{LogFile, MetaFile},
        test_util::{KvGen, LogFileBuilder, TmpStorage, gen_adjacent_with_ts},
    };

    const NUM_FLUSH: usize = 2;
    const NUM_REGION: usize = 5;
    const NUM_KV: usize = 10;

    async fn construct_storage(st: &TmpStorage) -> Vec<MetaFile> {
        let gen_builder = |batch, num_kv, num_region| {
            (0..num_region).map(move |v| {
                let it = KvGen::new(
                    gen_adjacent_with_ts(1, v * num_kv, batch).take(num_kv),
                    move |_| format!("v@{batch}").into_bytes(),
                )
                .take(num_kv);

                let mut b = LogFileBuilder::new(|b| b.region_id = v as u64);
                for kv in it {
                    b.add_encoded(&kv.key, &kv.value)
                }
                b
            })
        };
        let mut mfs = vec![];
        for i in 0..NUM_FLUSH {
            let mf = st
                .build_flush(
                    &format!("{i}.l"),
                    &format!("{i}.m"),
                    gen_builder(i as u64, NUM_KV, NUM_REGION),
                )
                .await;
            mfs.push(mf);
        }
        mfs
    }

    fn as_input(l: &LogFile) -> Input {
        Subcompaction::singleton(l.clone()).inputs.pop().unwrap()
    }

    #[tokio::test]
    async fn test_loading() {
        let st = TmpStorage::create();
        let m = construct_storage(&st).await;

        let so = Source::new(st.storage().clone());
        for epoch in 0..NUM_FLUSH {
            for seg in 0..NUM_REGION {
                let input = as_input(&m[epoch].physical_files[0].files[seg]);
                let mut i = 0;
                let mut stat = LoadStatistic::default();
                so.load(input, Some(&mut stat), |k, v| {
                    assert_eq!(
                        k,
                        crate::test_util::sow((1, (seg * NUM_KV + i) as i64, epoch as u64))
                    );
                    assert_eq!(v, format!("v@{epoch}").as_bytes());
                    i += 1;
                })
                .await
                .unwrap();
                assert_eq!(stat.files_in, 1);
                assert_eq!(stat.keys_in, 10);
                assert_eq!(stat.logical_key_bytes_in, 350);
                assert_eq!(stat.logical_value_bytes_in, 30);
                assert_eq!(stat.error_during_downloading, 0);
            }
        }
    }

    #[tokio::test]
    async fn test_loading_with_physical_file_cache() {
        let st = TmpStorage::create();
        let m = construct_storage(&st).await;

        let physical = &m[0].physical_files[0];
        let inputs = physical.files.iter().map(as_input).collect::<Vec<_>>();
        let cache = Arc::new(PhysicalFileCache::new(physical.size));
        cache.register_inputs(&inputs).await;

        let so = Source::new_with_cache(st.storage().clone(), Some(cache));
        let mut loaded_physical_bytes = 0;
        for input in inputs {
            let mut stat = LoadStatistic::default();
            so.load(input, Some(&mut stat), |_, _| {}).await.unwrap();
            loaded_physical_bytes += stat.physical_bytes_in;
        }

        assert_eq!(loaded_physical_bytes, physical.size);
    }
}
