use std::sync::Arc;

use async_compression::futures::write::ZstdDecoder;
use external_storage::ExternalStorage;
use futures::io::{AsyncWriteExt, Cursor};
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{retry_ext, RetryError, RetryExt},
};
use txn_types::Key;

use super::{statistic::LoadStatistic, storage::LogFileId, util::Cooperate};
use crate::compact::errors::Result;

#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ExternalStorage>,
}

impl Source {
    pub fn new(inner: Arc<dyn ExternalStorage>) -> Self {
        Self { inner }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
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

struct RetryIo(std::io::Error);

impl RetryError for RetryIo {
    fn is_retryable(&self) -> bool {
        self.0.kind() == std::io::ErrorKind::Interrupted
    }
}

impl From<std::io::Error> for RetryIo {
    fn from(e: std::io::Error) -> Self {
        RetryIo(e)
    }
}

impl Source {
    pub async fn load(
        &self,
        id: LogFileId,
        mut stat: Option<&mut LoadStatistic>,
        mut on_key_value: impl FnMut(&[u8], &[u8]),
    ) -> Result<()> {
        let error_during_downloading = Arc::new(AtomicU64::new(0));
        let counter = error_during_downloading.clone();
        let ext = RetryExt::default().with_fail_hook(move |_err| counter.inc_by(1));
        let fetch = || {
            let storage = self.inner.clone();
            let id = id.clone();
            async move {
                let mut content = vec![];
                let mut decompress = ZstdDecoder::new(Cursor::new(&mut content));
                let source = storage.read_part(&id.name, id.offset, id.length);
                let n = futures::io::copy(source, &mut decompress).await?;
                decompress.flush().await?;
                std::result::Result::<_, RetryIo>::Ok((content, n))
            }
        };
        let (content, size) = retry_ext(fetch, ext).await.map_err(|err| err.0)?;
        stat.as_mut().map(|stat| {
            stat.physical_bytes_in += size;
            stat.error_during_downloading += error_during_downloading.get();
        });

        let mut co = Cooperate::new(4096);
        let mut iter = stream_event::EventIterator::new(&content);
        iter.next()?;
        while iter.valid() {
            co.step().await;
            on_key_value(iter.key(), iter.value());
            stat.as_mut().map(|stat| {
                stat.keys_in += 1;
                stat.logical_key_bytes_in += iter.key().len() as u64;
                stat.logical_value_bytes_in += iter.value().len() as u64;
            });
            iter.next()?;
        }
        stat.as_mut().map(|stat| stat.files_in += 1);
        Ok(())
    }
}
