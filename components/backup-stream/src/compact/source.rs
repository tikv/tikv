use std::{
    alloc::Layout,
    collections::HashMap,
    io::{Read, Seek},
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use async_compression::futures::write::ZstdDecoder;
use bytes::BytesMut;
use dashmap::{mapref::entry::Entry, DashMap};
use external_storage::ExternalStorage;
use futures::{
    future::{Remote, TryFutureExt},
    io::{AllowStdIo, AsyncWriteExt, Cursor},
};
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    codec::stream_event::{self, Iterator},
    stream::{retry, retry_ext, RetryError, RetryExt},
};
use tokio::sync::OnceCell;
use txn_types::Key;

use super::{statistic::LoadStatistic, storage::LogFileId, util::Cooperate};
use crate::compact::errors::Result;

#[derive(Clone)]
pub struct Source {
    inner: Arc<dyn ExternalStorage>,
    cache_manager: Option<Arc<CacheManager>>,
}

struct CacheManager {
    base: PathBuf,
    files: Mutex<HashMap<Arc<str>, Arc<OnceCell<PathBuf>>>>,
}

impl Source {
    pub fn new(inner: Arc<dyn ExternalStorage>) -> Self {
        Self {
            inner,
            cache_manager: None,
        }
    }

    pub fn with_cache(
        inner: Arc<dyn ExternalStorage>,
        cache_prefix: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            inner,
            cache_manager: Some(Arc::new(CacheManager::new(cache_prefix)?)),
        })
    }
}

impl CacheManager {
    fn new(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let base = path.as_ref().join(format!(
            "compact-log-cache-{}",
            tikv_util::sys::thread::process_id()
        ));
        std::fs::create_dir_all(&base)?;
        Ok(Self {
            base,
            files: Default::default(),
        })
    }

    async fn load_file(
        &self,
        id: LogFileId,
        storage: &Arc<dyn ExternalStorage>,
    ) -> std::io::Result<Vec<u8>> {
        loop {
            // (net_io, error)
            let stat = Arc::new((AtomicU64::new(0), AtomicU64::new(0)));
            let stat_ref = Arc::clone(&stat);
            let ext = RetryExt::default().with_fail_hook(move |_err| stat_ref.0.inc_by(1));
            let fetch = || {
                let storage = storage.clone();
                let id = id.clone();
                let stat = Arc::clone(&stat);
                async move {
                    let path = self.base.join(id.name.as_ref());
                    let local = std::fs::File::create(&path)?;
                    let mut decompress = ZstdDecoder::new(AllowStdIo::new(local));
                    let source = storage.read(&id.name);
                    let n = futures::io::copy(source, &mut decompress).await?;
                    stat.1.inc_by(n);
                    decompress.flush().await?;
                    std::result::Result::<_, RetryIo>::Ok(path)
                }
            };

            let mut files = self.files.lock().unwrap();
            if let Some(path_cell) = files.get(&id.name) {
                let path_cell = Arc::clone(&path_cell);
                drop(files);

                let path = path_cell
                    .get_or_try_init(|| retry_ext(fetch, ext).map_err(|err| err.0))
                    .await?;
                let mut f = std::fs::File::options()
                    .read(true)
                    .write(false)
                    .open(path)?;
                let mut v = vec![0u8; id.length as usize];
                f.seek(futures_io::SeekFrom::Start(id.offset))?;
                f.read_exact(&mut v[..])?;
                return Ok(v);
            }

            files.insert(Arc::clone(&id.name), Arc::default());
        }
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
