// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    pin::{pin, Pin},
    sync::Arc,
};

use async_compression::futures::write::ZstdDecoder;
use external_storage::BlobStorage;
use futures::io::{AsyncWriteExt, Cursor};
use futures_io::AsyncWrite;
use kvproto::brpb;
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{retry_all_ext, JustRetry, RetryExt},
};
use txn_types::Key;

use super::{statistic::LoadStatistic, util::Cooperate};
use crate::{compaction::Input, errors::Result};

/// The manager of fetching log files from remote for compacting.
#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn BlobStorage>,
}

impl Source {
    pub fn new(inner: Arc<dyn BlobStorage>) -> Self {
        Self { inner }
    }
}

/// A record from log files.
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Record {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Record {
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
            async move {
                let mut content = Vec::with_capacity(id.length as _);
                let item = pin!(Cursor::new(&mut content));
                let mut decompress = decompress(compression, item)?;
                let source = storage.get_part(&id.name, id.offset, id.length);
                let n = futures::io::copy(source, &mut decompress).await?;
                decompress.flush().await?;
                drop(decompress);
                std::io::Result::Ok((content, n))
            }
        };
        let (content, size) = retry_all_ext(fetch, ext).await?;
        stat.as_mut().map(|stat| {
            stat.physical_bytes_in += size;
            stat.error_during_downloading += error_during_downloading.get();
        });
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

        let mut co = Cooperate::new(4096);
        let mut iter = stream_event::EventIterator::new(&content);
        loop {
            if !iter.valid() {
                break;
            }

            iter.next()?;
            co.step().await;
            on_key_value(iter.key(), iter.value());
            stat.as_mut().map(|stat| {
                stat.keys_in += 1;
                stat.logical_key_bytes_in += iter.key().len() as u64;
                stat.logical_value_bytes_in += iter.value().len() as u64;
            });
        }
        stat.as_mut().map(|stat| stat.files_in += 1);
        Ok(())
    }
}

fn decompress<'a>(
    compression: brpb::CompressionType,
    input: Pin<&'a mut (impl AsyncWrite + Send)>,
) -> std::io::Result<impl AsyncWrite + Send + 'a> {
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
    use super::Source;
    use crate::{
        compaction::{Input, Subcompaction},
        statistic::LoadStatistic,
        storage::{LogFile, MetaFile},
        test_util::{gen_adjacent_with_ts, KvGen, LogFileBuilder, TmpStorage},
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
}
