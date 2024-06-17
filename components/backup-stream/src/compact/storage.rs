use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use external_storage::{BlobObject, ExternalStorage, WalkBlobStorage, WalkExternalStorage};
use futures::{
    future::{FusedFuture, FutureExt, TryFutureExt},
    io::AsyncReadExt,
    stream::{Fuse, FusedStream, StreamExt},
};
use tokio_stream::Stream;

use super::errors::{Error, Result};
use crate::utils;

const METADATA_PREFIX: &'static str = "v1/backupmeta";

#[derive(Debug)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub logs: Vec<LogFile>,
    pub min_ts: u64,
    pub max_ts: u64,
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
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
    pub is_meta: bool,
}

#[derive(Clone)]
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
    pub max_concurrent_fetch: usize,
}

impl Default for LoadFromExt {
    fn default() -> Self {
        Self {
            max_concurrent_fetch: 16,
        }
    }
}

pub struct StreamyMetaStorage<'a> {
    prefetch: VecDeque<Prefetch<Pin<Box<dyn Future<Output = Result<MetaFile>> + 'a>>>>,
    ext_storage: &'a dyn ExternalStorage,
    ext: LoadFromExt,

    files: Fuse<Pin<Box<dyn Stream<Item = std::io::Result<BlobObject>> + 'a>>>,
}

#[pin_project::pin_project(project = ProjPrefetch)]
enum Prefetch<F: Future> {
    Polling(#[pin] F),
    Ready(<F as Future>::Output),
}

impl<F: Future> Prefetch<F> {
    fn must_fetch(self) -> <F as Future>::Output {
        match self {
            Prefetch::Ready(v) => v,
            _ => panic!("must_cached call but the future not ready"),
        }
    }

    fn new(f: F) -> Self {
        Self::Polling(f)
    }
}

impl<F: Future> Future for Prefetch<F> {
    type Output = ();

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.as_mut().project() {
            ProjPrefetch::Polling(fut) => {
                let resolved = ready!(fut.poll(cx));
                unsafe {
                    // SAFETY: we won't poll it anymore.
                    *self.get_unchecked_mut() = Prefetch::Ready(resolved);
                }
                ().into()
            }
            ProjPrefetch::Ready(_) => std::task::Poll::Pending,
        }
    }
}

impl<F: Future> FusedFuture for Prefetch<F> {
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
        if self.prefetch.is_empty() {
            return self.poll_fetch_or_finish(cx);
        }

        let first_result = self.poll_first_prefetch(cx);
        match first_result {
            Poll::Ready(item) => Some(item).into(),
            Poll::Pending => self.poll_fetch_or_finish(cx),
        }
    }
}

impl<'a> StreamyMetaStorage<'a> {
    fn register_prefetch(&mut self, cx: &mut Context<'_>, load: BlobObject) {
        let mut fut = Prefetch::new(MetaFile::load_from(self.ext_storage, load).boxed_local());
        // start the execution of this future.
        let poll = fut.poll_unpin(cx);
        if poll.is_ready() {
            // We need to check this in next run.
            cx.waker().wake_by_ref();
        }
        self.prefetch.push_back(fut);
    }

    fn poll_fetch_or_finish(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<MetaFile>>> {
        loop {
            // No more space for prefetching.
            if self.prefetch.len() >= self.ext.max_concurrent_fetch {
                return Poll::Pending;
            }
            if self.files.is_terminated() {
                if self.prefetch.is_empty() {
                    return None.into();
                } else {
                    return Poll::Pending;
                }
            }
            match self.files.next().poll_unpin(cx) {
                Poll::Ready(Some(load)) => {
                    self.register_prefetch(cx, load?);
                }
                Poll::Ready(None) | Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_first_prefetch(&mut self, cx: &mut Context<'_>) -> Poll<Result<MetaFile>> {
        for fut in &mut self.prefetch {
            if !fut.is_terminated() {
                let _ = fut.poll_unpin(cx);
            }
        }
        if self.prefetch[0].is_terminated() {
            let file = self.prefetch.pop_front().unwrap().must_fetch();
            file.into()
        } else {
            Poll::Pending
        }
    }

    pub fn load_from_ext(s: &'a dyn WalkExternalStorage, ext: LoadFromExt) -> Self {
        let files = s.walk(&METADATA_PREFIX).fuse();
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
        let min_ts = meta_file.min_ts;
        let max_ts = meta_file.max_ts;

        for mut group in meta_file.take_file_groups().into_iter() {
            let name = Arc::from(group.path.clone().into_boxed_str());
            for mut log_file in group.take_data_files_info().into_iter() {
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
                    max_key: Arc::from(log_file.take_end_key().into_boxed_slice()),
                    min_key: Arc::from(log_file.take_start_key().clone().into_boxed_slice()),
                    is_meta: log_file.is_meta,
                    min_start_ts: log_file.min_begin_ts_in_default_cf,
                })
            }
        }
        drop(meta_file);

        let result = Self {
            name: Arc::from(blob.key.to_owned().into_boxed_str()),
            logs: log_files,
            min_ts,
            max_ts,
        };
        Ok(result)
    }
}
