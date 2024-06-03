use std::{
    collections::VecDeque, future::Future, pin::Pin, process::Output, sync::Arc, task::ready,
};

use chrono::format::Item;
use external_storage::{BlobObject, ExternalStorage, WalkBlobStorage};
use futures::{
    future::{BoxFuture, FusedFuture, FutureExt, TryFutureExt},
    io::AsyncReadExt,
    stream::{self, StreamExt, TryStream},
};
use tidb_query_datatype::codec::mysql::Time;
use tokio_stream::Stream;
use txn_types::TimeStamp;

use super::{
    errors::{Error, Result},
    util::select_vec,
};
use crate::utils;

pub trait CompactStorage: WalkBlobStorage + ExternalStorage {}

impl<T: WalkBlobStorage + ExternalStorage> CompactStorage for T {}

const METADATA_PREFIX: &'static str = "v1/backupmeta";

#[derive(Debug)]
pub struct MetaStorage {
    pub files: Vec<MetaFile>,
}

#[derive(Debug)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub logs: Vec<LogFile>,
}

#[derive(Debug)]

pub struct LogFile {
    pub id: LogFileId,
    pub real_size: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub min_ts: u64,
    pub max_ts: u64,
    pub min_start_ts: u64,
}

pub struct LogFileId {
    pub name: Arc<str>,
    pub offset: u64,
    pub length: u64,
}

impl std::fmt::Debug for LogFileId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Id")
            .field(&self.name)
            .field(&format_args!("@{}+{}", self.offset, self.length))
            .finish()
    }
}

pub struct LoadFromExt {
    pub from_ts: Option<TimeStamp>,
    pub to_ts: Option<TimeStamp>,
    pub max_concurrent_fetch: usize,
}

impl Default for LoadFromExt {
    fn default() -> Self {
        Self {
            from_ts: Default::default(),
            to_ts: Default::default(),
            max_concurrent_fetch: 16,
        }
    }
}

impl MetaStorage {
    pub async fn load_from_ext(s: &dyn CompactStorage, ext: LoadFromExt) -> Result<Self> {
        let mut files = s.walk(&METADATA_PREFIX);
        let mut result = MetaStorage { files: vec![] };
        let mut pending_futures = vec![];
        while let Some(file) = files.next().await {
            pending_futures.push(Box::pin(MetaFile::load_from(s, file?)));
            if pending_futures.len() >= ext.max_concurrent_fetch {
                result.files.push(select_vec(&mut pending_futures).await?);
            }
        }
        for fut in pending_futures {
            result.files.push(fut.await?);
        }
        Ok(result)
    }
}

pub struct StreamyMetaStorage<'a> {
    prefetch:
        VecDeque<Prefetch<Pin<Box<dyn Future<Output = Result<MetaFile>> + 'a>>, Result<MetaFile>>>,
    files: Pin<Box<dyn Stream<Item = std::io::Result<BlobObject>> + 'a>>,
    ext_storage: &'a dyn ExternalStorage,
    ext: LoadFromExt,
}

#[pin_project::pin_project(project = ProjCachedFuse)]
enum Prefetch<F: Future<Output = T>, T> {
    Polling(#[pin] F),
    Ready(T),
}

impl<F: Future<Output = T>, T> Prefetch<F, T> {
    fn must_cached(self) -> T {
        match self {
            Prefetch::Ready(v) => v,
            _ => panic!("must_cached call but the future not ready"),
        }
    }
}

impl<F: Future<Output = T>, T> Future for Prefetch<F, T> {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().project() {
            ProjCachedFuse::Polling(fut) => {
                let resolved = ready!(fut.poll(cx));
                unsafe {
                    // SAFETY: we won't poll it anymore.
                    *self.get_unchecked_mut() = Prefetch::Ready(resolved);
                }
                ().into()
            }
            ProjCachedFuse::Ready(_) => std::task::Poll::Pending,
        }
    }
}

impl<F: Future<Output = T>, T> FusedFuture for Prefetch<F, T> {
    fn is_terminated(&self) -> bool {
        match self {
            Prefetch::Polling(_) => false,
            Prefetch::Ready(_) => true,
        }
    }
}

impl<'a> Stream for StreamyMetaStorage<'a> {
    type Item = Result<MetaFile>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            if self.prefetch.len() > self.ext.max_concurrent_fetch {
                for fut in &mut self.prefetch {
                    if !fut.is_terminated() {
                        let _ = fut.poll_unpin(cx);
                    }
                }
                if self.prefetch[0].is_terminated() {
                    return Some(self.prefetch.pop_front().unwrap().must_cached()).into();
                }
            }

            let input = ready!(self.files.poll_next_unpin(cx));
            let storage = self.ext_storage;
            match input {
                Some(Ok(load)) => self.prefetch.push_back(Prefetch::Polling(
                    MetaFile::load_from(storage, load).boxed_local(),
                )),
                Some(Err(err)) => return Some(Err(err.into())).into(),
                None => return None.into(),
            }
        }
    }
}

impl<'a> StreamyMetaStorage<'a> {
    pub async fn load_from_ext(s: &'a dyn CompactStorage, ext: LoadFromExt) -> Self {
        let files = s.walk(&METADATA_PREFIX);
        Self {
            prefetch: VecDeque::new(),
            files,
            ext_storage: s,
            ext,
        }
    }
}

impl MetaFile {
    async fn load_from(s: &dyn ExternalStorage, blob: BlobObject) -> Result<Self> {
        use protobuf::Message;

        let mut content = vec![];
        s.read(&blob.key)
            .read_to_end(&mut content)
            .await
            .map_err(|err| Error::from(err).message(format!("reading {}", blob.key)))?;
        let mut meta_file = kvproto::brpb::Metadata::new();
        meta_file.merge_from_bytes(&content)?;
        let mut log_files = vec![];

        for group in meta_file.get_file_groups() {
            let name = Arc::from(group.path.clone().into_boxed_str());
            for log_file in group.get_data_files_info() {
                log_files.push(LogFile {
                    id: LogFileId {
                        name: Arc::clone(&name),
                        offset: log_file.range_offset,
                        length: log_file.range_length,
                    },
                    real_size: log_file.length,
                    region_id: log_file.region_id as _,
                    cf: utils::cf_name(&log_file.cf),
                    max_ts: log_file.max_ts,
                    min_ts: log_file.min_ts,
                    min_start_ts: log_file.min_begin_ts_in_default_cf,
                })
            }
        }

        let result = Self {
            name: Arc::from(blob.key.to_owned().into_boxed_str()),
            logs: log_files,
        };
        Ok(result)
    }
}
