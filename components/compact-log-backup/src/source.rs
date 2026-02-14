// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    pin::{Pin, pin},
    path::Path,
    sync::Arc,
};

use async_compression::futures::write::ZstdDecoder;
use external_storage::ExternalStorage;
use futures::io::{AsyncWriteExt, Cursor};
use futures_io::AsyncWrite;
use kvproto::brpb;
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{JustRetry, RetryExt, retry_all_ext},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use txn_types::Key;

use super::{statistic::LoadStatistic, util::Cooperate};
use crate::{compaction::Input, errors::Result, input_cache::LocalObjectCache};

/// The manager of fetching log files from remote for compacting.
#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ExternalStorage>,
    cache: Option<Arc<LocalObjectCache>>,
}

impl Source {
    pub fn new(inner: Arc<dyn ExternalStorage>) -> Self {
        Self { inner, cache: None }
    }

    pub fn with_cache(inner: Arc<dyn ExternalStorage>, cache: Arc<LocalObjectCache>) -> Self {
        Self {
            inner,
            cache: Some(cache),
        }
    }
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
        let error_during_downloading = Arc::new(AtomicU64::new(0));
        let counter = error_during_downloading.clone();
        let ext = RetryExt::default()
            .with_fail_hook(move |_: &JustRetry<std::io::Error>| counter.inc_by(1));

        let fetch = || {
            let storage = self.inner.clone();
            let id = input.id.clone();
            let compression = input.compression;
            let cache = self.cache.clone();
            async move {
                let (content, remote_bytes_in, access_stat) = match cache {
                    Some(cache) => {
                        let (local_path, access_stat) =
                            cache.get_or_fetch(storage.as_ref(), &id.name).await?;
                        let compressed_span = read_span(&local_path, id.offset, id.length).await?;
                        let mut content = Vec::with_capacity(id.length as _);
                        let item = pin!(Cursor::new(&mut content));
                        let mut decompress = decompress(compression, item)?;
                        let mut source = Cursor::new(compressed_span);
                        let _ = futures::io::copy(&mut source, &mut decompress).await?;
                        decompress.flush().await?;
                        drop(decompress);
                        (content, access_stat.remote_read_bytes, Some(access_stat))
                    }
                    None => {
                        let mut content = Vec::with_capacity(id.length as _);
                        let item = pin!(Cursor::new(&mut content));
                        let mut decompress = decompress(compression, item)?;
                        let source = storage.read_part(&id.name, id.offset, id.length);
                        let n = futures::io::copy(source, &mut decompress).await?;
                        decompress.flush().await?;
                        drop(decompress);
                        (content, n, None)
                    }
                };
                std::io::Result::Ok((content, remote_bytes_in, access_stat))
            }
        };

        let (content, remote_bytes_in, access_stat) = retry_all_ext(fetch, ext).await?;
        if let Some(stat) = stat.as_mut() {
            stat.physical_bytes_in += remote_bytes_in;
            stat.error_during_downloading += error_during_downloading.get();
            match access_stat {
                Some(access_stat) => {
                    stat.cache_hit += access_stat.cache_hit;
                    stat.cache_miss += access_stat.cache_miss;
                    stat.cache_inflight_wait += access_stat.cache_inflight_wait;
                    stat.cache_evicted_files += access_stat.cache_evicted_files;
                    stat.cache_evicted_bytes += access_stat.cache_evicted_bytes;
                    stat.remote_read_calls += access_stat.remote_read_calls;
                }
                None => {
                    stat.remote_read_part_calls += 1;
                }
            }
        }
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

async fn read_span(path: &Path, offset: u64, length: u64) -> std::io::Result<Vec<u8>> {
    let len = usize::try_from(length).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "span length too large")
    })?;
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut buf = vec![0; len];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use async_trait::async_trait;
    use external_storage::{BlobObject, ExternalStorage};
    use futures::stream;
    use kvproto::brpb;
    use protobuf::Chars;

    use super::Source;
    use crate::{
        compaction::{Input, Subcompaction},
        input_cache::LocalObjectCache,
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

    struct CountingStorage {
        name: &'static str,
        obj_name: String,
        obj_bytes: Vec<u8>,
        read_calls: AtomicU64,
        read_part_calls: AtomicU64,
    }

    impl CountingStorage {
        fn new(obj_name: String, obj_bytes: Vec<u8>) -> Self {
            Self {
                name: "counting-storage",
                obj_name,
                obj_bytes,
                read_calls: AtomicU64::new(0),
                read_part_calls: AtomicU64::new(0),
            }
        }

        fn reads(&self) -> u64 {
            self.read_calls.load(Ordering::Relaxed)
        }

        fn read_parts(&self) -> u64 {
            self.read_part_calls.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ExternalStorage for CountingStorage {
        fn name(&self) -> &'static str {
            self.name
        }

        fn url(&self) -> std::io::Result<url::Url> {
            Ok(url::Url::parse("mem://counting-storage").unwrap())
        }

        async fn write(
            &self,
            _name: &str,
            _reader: external_storage::UnpinReader<'_>,
            _content_length: u64,
        ) -> std::io::Result<()> {
            Err(external_storage::unimplemented())
        }

        fn read(&self, name: &str) -> external_storage::ExternalData<'_> {
            assert_eq!(name, self.obj_name.as_str());
            self.read_calls.fetch_add(1, Ordering::Relaxed);
            Box::new(futures::io::Cursor::new(self.obj_bytes.clone()))
        }

        fn read_part(&self, name: &str, off: u64, len: u64) -> external_storage::ExternalData<'_> {
            assert_eq!(name, self.obj_name.as_str());
            self.read_part_calls.fetch_add(1, Ordering::Relaxed);
            let off = off as usize;
            let len = len as usize;
            let end = off + len;
            Box::new(futures::io::Cursor::new(self.obj_bytes[off..end].to_vec()))
        }

        fn iter_prefix(
            &self,
            _prefix: &str,
        ) -> futures::stream::LocalBoxStream<'_, std::io::Result<BlobObject>> {
            Box::pin(stream::empty())
        }

        fn delete(&self, _name: &str) -> futures::future::LocalBoxFuture<'_, std::io::Result<()>> {
            Box::pin(async { Err(external_storage::unimplemented()) })
        }
    }

    fn make_input(name: &str, offset: u64, length: u64) -> Input {
        Input {
            id: crate::storage::LogFileId {
                name: Chars::from(name),
                offset,
                length,
            },
            compression: brpb::CompressionType::Zstd,
            crc64xor: 0,
            key_value_size: 0,
            num_of_entries: 0,
        }
    }

    fn zstd_frame(data: &[u8]) -> Vec<u8> {
        zstd::stream::encode_all(std::io::Cursor::new(data), 0).unwrap()
    }

    #[tokio::test]
    async fn test_input_cache_dedup_remote_reads() {
        let seg1 = zstd_frame(b"segment-1");
        let seg2 = zstd_frame(b"segment-2");
        let obj = [seg1.as_slice(), seg2.as_slice()].concat();
        let off2 = seg1.len() as u64;

        let storage: Arc<CountingStorage> = Arc::new(CountingStorage::new("obj".to_owned(), obj));
        let tmp = tempdir::TempDir::new("compact-log-input-cache").unwrap();
        let cache = Arc::new(
            LocalObjectCache::new(tmp.path().to_path_buf(), 64 * 1024 * 1024, 4)
                .await
                .unwrap(),
        );
        let source = Source::with_cache(storage.clone(), cache);

        let input1 = make_input("obj", 0, seg1.len() as u64);
        let input2 = make_input("obj", off2, seg2.len() as u64);

        let s1 = source.clone();
        let t1 = tokio::spawn(async move {
            let mut stat: Option<&mut LoadStatistic> = None;
            s1.load_remote(input1, &mut stat).await.unwrap()
        });
        let s2 = source.clone();
        let t2 = tokio::spawn(async move {
            let mut stat: Option<&mut LoadStatistic> = None;
            s2.load_remote(input2, &mut stat).await.unwrap()
        });

        let (out1, out2) = tokio::join!(t1, t2);
        assert_eq!(out1.unwrap(), b"segment-1");
        assert_eq!(out2.unwrap(), b"segment-2");
        assert_eq!(storage.reads(), 1);
        assert_eq!(storage.read_parts(), 0);
    }

    #[tokio::test]
    async fn test_input_cache_disabled_uses_read_part() {
        let seg1 = zstd_frame(b"segment-1");
        let seg2 = zstd_frame(b"segment-2");
        let obj = [seg1.as_slice(), seg2.as_slice()].concat();
        let off2 = seg1.len() as u64;

        let storage: Arc<CountingStorage> = Arc::new(CountingStorage::new("obj".to_owned(), obj));
        let source = Source::new(storage.clone());

        let input1 = make_input("obj", 0, seg1.len() as u64);
        let input2 = make_input("obj", off2, seg2.len() as u64);

        let mut stat: Option<&mut LoadStatistic> = None;
        assert_eq!(
            source.load_remote(input1, &mut stat).await.unwrap(),
            b"segment-1"
        );
        let mut stat: Option<&mut LoadStatistic> = None;
        assert_eq!(
            source.load_remote(input2, &mut stat).await.unwrap(),
            b"segment-2"
        );

        assert_eq!(storage.reads(), 0);
        assert_eq!(storage.read_parts(), 2);
    }
}
