// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::VecDeque,
    future::Future,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use derive_more::Display;
use external_storage::{
    BlobObject, ExternalStorage, FullFeaturedStorage, UnpinReader, WalkBlobStorage,
};
use futures::{
    future::{FusedFuture, FutureExt, TryFutureExt},
    io::{AsyncReadExt, Cursor},
    stream::{Fuse, FusedStream, StreamExt, TryStreamExt},
};
use kvproto::brpb::{self, FileType, Migration};
use prometheus::core::{Atomic, AtomicU64};
use tikv_util::{
    retry_expr,
    stream::{JustRetry, RetryExt},
    time::Instant,
};
use tokio_stream::Stream;
use tracing::{span::Entered, Span};
use tracing_active_tree::frame;

use super::{
    errors::{Error, Result},
    statistic::LoadMetaStatistic,
};
use crate::{errors::ErrorKind, util};

pub const METADATA_PREFIX: &'static str = "v1/backupmeta";
pub const COMPACTION_OUT_PREFIX: &'static str = "compaction_out";
pub const MIGRATION_PREFIX: &'static str = "v1/migrations";

#[derive(Debug)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub physical_files: Vec<PhysicalLogFile>,
    pub min_ts: u64,
    pub max_ts: u64,
}

impl From<brpb::Metadata> for MetaFile {
    fn from(value: brpb::Metadata) -> Self {
        Self::from_file(Arc::from(":memory:"), value)
    }
}

impl MetaFile {
    pub fn from_file(name: Arc<str>, mut meta_file: brpb::Metadata) -> Self {
        let mut log_files = vec![];
        let min_ts = meta_file.min_ts;
        let max_ts = meta_file.max_ts;

        // NOTE: perhaps we also need consider non-grouped backup meta here?
        for mut group in meta_file.take_file_groups().into_iter() {
            let name = Arc::from(group.path.clone().into_boxed_str());
            let mut g = PhysicalLogFile {
                size: group.length,
                name: Arc::clone(&name),
                files: vec![],
            };
            for log_file in group.take_data_files_info().into_iter() {
                g.files.push(LogFile::from_pb(Arc::clone(&name), log_file))
            }
            log_files.push(g);
        }
        drop(meta_file);

        let result = Self {
            name,
            physical_files: log_files,
            min_ts,
            max_ts,
        };
        result
    }
}

impl MetaFile {
    pub fn into_logs(self) -> impl Iterator<Item = LogFile> {
        self.physical_files
            .into_iter()
            .flat_map(|g| g.files.into_iter())
    }
}

#[derive(Debug)]
pub struct PhysicalLogFile {
    pub size: u64,
    pub name: Arc<str>,
    pub files: Vec<LogFile>,
}

#[derive(Debug, Clone)]

pub struct LogFile {
    pub id: LogFileId,
    pub file_real_size: u64,
    pub number_of_entries: i64,
    pub crc64xor: u64,
    pub region_id: u64,
    pub cf: &'static str,
    pub min_ts: u64,
    pub max_ts: u64,
    pub min_start_ts: u64,
    pub min_key: Arc<[u8]>,
    pub max_key: Arc<[u8]>,
    pub is_meta: bool,
    pub ty: FileType,
    pub compression: brpb::CompressionType,
    pub table_id: i64,
    pub resolved_ts: u64,
    pub sha256: Arc<[u8]>,
}

impl LogFile {
    pub fn hacky_key_value_size(&self) -> u64 {
        const HEADER_SIZE_PER_ENTRY: u64 = std::mem::size_of::<u32>() as u64 * 2;
        self.file_real_size - HEADER_SIZE_PER_ENTRY * self.number_of_entries as u64
    }
}

#[derive(Clone, Display, Eq, PartialEq)]
#[display(fmt = "{}@{}+{}", name, offset, length)]
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
    pub loading_content_span: Option<Span>,
}

impl LoadFromExt {
    fn enter_load_span(&self) -> Option<Entered> {
        if let Some(span) = &self.loading_content_span {
            Some(span.enter())
        } else {
            None
        }
    }
}

impl<'a> Default for LoadFromExt {
    fn default() -> Self {
        Self {
            max_concurrent_fetch: 16,
            loading_content_span: None,
        }
    }
}

pub struct StreamyMetaStorage<'a> {
    prefetch: VecDeque<
        Prefetch<Pin<Box<dyn Future<Output = Result<(MetaFile, LoadMetaStatistic)>> + 'a>>>,
    >,
    ext_storage: &'a dyn ExternalStorage,
    ext: LoadFromExt,
    stat: LoadMetaStatistic,

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
    fn poll_fetch_or_finish(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<MetaFile>>> {
        loop {
            // No more space for prefetching.
            if self.prefetch.len() >= self.ext.max_concurrent_fetch {
                return Poll::Pending;
            }
            if self.files.is_terminated() {
                self.ext.loading_content_span.take();
                if self.prefetch.is_empty() {
                    return None.into();
                } else {
                    return Poll::Pending;
                }
            }
            match {
                let _enter = self.ext.enter_load_span();
                self.files.next().poll_unpin(cx)
            } {
                Poll::Ready(Some(load)) => {
                    let mut fut =
                        Prefetch::new(MetaFile::load_from(self.ext_storage, load?).boxed_local());
                    // start the execution of this future.
                    let poll = fut.poll_unpin(cx);
                    if poll.is_ready() {
                        // We need to check this in next run.
                        cx.waker().wake_by_ref();
                    }
                    self.stat.prefetch_task_emitted += 1;
                    self.prefetch.push_back(fut);
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
            match file {
                Ok((file, stat)) => {
                    self.stat += stat;
                    self.stat.meta_files_in += 1;
                    self.stat.prefetch_task_finished += 1;
                    Ok(file).into()
                }
                Err(err) => Err(err.attach_current_frame()).into(),
            }
        } else {
            Poll::Pending
        }
    }

    pub fn take_statistic(&mut self) -> LoadMetaStatistic {
        std::mem::take(&mut self.stat)
    }

    pub fn load_from_ext(s: &'a dyn FullFeaturedStorage, ext: LoadFromExt) -> Self {
        let files = s.walk(&METADATA_PREFIX).fuse();
        Self {
            prefetch: VecDeque::new(),
            files,
            ext_storage: s,
            ext,
            stat: LoadMetaStatistic::default(),
        }
    }

    pub async fn count_objects(s: &'a dyn FullFeaturedStorage) -> std::io::Result<u64> {
        let mut n = 0;
        let mut items = s.walk(&METADATA_PREFIX);
        while let Some(_) = items.try_next().await? {
            n += 1
        }
        Ok(n)
    }
}

impl MetaFile {
    #[tracing::instrument(skip_all, fields(blob=%blob))]
    async fn load_from(
        s: &dyn ExternalStorage,
        blob: BlobObject,
    ) -> Result<(Self, LoadMetaStatistic)> {
        use protobuf::Message;

        let mut stat = LoadMetaStatistic::default();
        let begin = Instant::now();

        let error_cnt = Arc::new(AtomicU64::new(0));
        let error_cnt2 = Arc::clone(&error_cnt);
        let ext = RetryExt::default().with_fail_hook(move |_err| {
            error_cnt.inc_by(1);
        });

        let loading_file = tikv_util::stream::retry_all_ext(
            || async {
                let mut content = vec![];
                let n = s.read(&blob.key).read_to_end(&mut content).await?;
                std::io::Result::Ok((n, content))
            },
            ext,
        );
        let (n, content) = frame!(loading_file)
            .await
            .map_err(|err| Error::from(err).message(format_args!("reading {}", blob.key)))?;
        stat.physical_bytes_loaded += n as u64;
        stat.error_during_downloading += error_cnt2.get();

        let mut meta_file = kvproto::brpb::Metadata::new();
        meta_file.merge_from_bytes(&content)?;
        let name = Arc::from(blob.key.into_boxed_str());
        let result = Self::from_file(name, meta_file);

        stat.physical_data_files_in += result.physical_files.len() as u64;
        stat.logical_data_files_in += result
            .physical_files
            .iter()
            .map(|v| v.files.len() as u64)
            .sum::<u64>();
        stat.load_file_duration += begin.saturating_elapsed();

        Ok((result, stat))
    }
}

impl LogFile {
    fn from_pb(host_file: Arc<str>, mut pb_info: brpb::DataFileInfo) -> Self {
        Self {
            id: LogFileId {
                name: host_file,
                offset: pb_info.range_offset,
                length: pb_info.range_length,
            },
            file_real_size: pb_info.length,
            region_id: pb_info.region_id as _,
            cf: util::cf_name(&pb_info.cf),
            max_ts: pb_info.max_ts,
            min_ts: pb_info.min_ts,
            max_key: Arc::from(pb_info.take_end_key().into_boxed_slice()),
            min_key: Arc::from(pb_info.take_start_key().into_boxed_slice()),
            is_meta: pb_info.is_meta,
            min_start_ts: pb_info.min_begin_ts_in_default_cf,
            ty: pb_info.r_type,
            crc64xor: pb_info.crc64xor,
            number_of_entries: pb_info.number_of_entries,
            sha256: Arc::from(pb_info.take_sha256().into_boxed_slice()),
            resolved_ts: pb_info.resolved_ts,
            table_id: pb_info.table_id,
            compression: pb_info.compression_type,
        }
    }

    pub fn into_pb(self) -> brpb::DataFileInfo {
        let mut pb = brpb::DataFileInfo::new();
        pb.range_offset = self.id.offset;
        pb.range_length = self.id.length;
        pb.length = self.file_real_size;
        pb.region_id = self.region_id as _;
        pb.cf = self.cf.to_owned();
        pb.max_ts = self.max_ts;
        pb.min_ts = self.min_ts;
        pb.set_end_key(self.max_key.to_vec());
        pb.set_start_key(self.min_key.to_vec());
        pb.is_meta = self.is_meta;
        pb.min_begin_ts_in_default_cf = self.min_start_ts;
        pb.r_type = self.ty;
        pb.crc64xor = self.crc64xor;
        pb.number_of_entries = self.number_of_entries;
        pb.set_sha256(self.sha256.to_vec());
        pb.resolved_ts = self.resolved_ts;
        pb.table_id = self.table_id;
        pb.compression_type = self.compression;
        pb
    }
}

pub struct MigartionStorageWrapper<'a> {
    storage: &'a dyn FullFeaturedStorage,
    migartions_prefix: &'a str,
}

impl<'a> MigartionStorageWrapper<'a> {
    pub fn new(storage: &'a dyn FullFeaturedStorage) -> Self {
        Self {
            storage,
            migartions_prefix: &MIGRATION_PREFIX,
        }
    }

    pub async fn write(&self, migration: Migration) -> Result<()> {
        use protobuf::Message;
        let id = self.largest_id().await?;
        // Note: perhaps we need to verify that there isn't concurrency writing in the
        // future.
        let name = name_of_migration(id + 1, &migration);
        let bytes = migration.write_to_bytes()?;
        retry_expr!(
            self.storage
                .write(
                    &format!("{}/{}", self.migartions_prefix, name),
                    UnpinReader(Box::new(Cursor::new(&bytes))),
                    bytes.len() as u64
                )
                .map_err(|err| JustRetry(err))
        )
        .await
        .map_err(|err| err.0)?;
        Ok(())
    }

    pub async fn largest_id(&self) -> Result<u64> {
        self.storage
            .walk(&self.migartions_prefix)
            .err_into::<Error>()
            .map(|v| {
                v.and_then(|v| match id_of_migration(&v.key) {
                    Some(v) => Ok(v),
                    None => Err(Error::from(ErrorKind::Other(format!(
                        "the file {} cannot be parsed as a migration",
                        v
                    )))),
                })
            })
            .try_fold(u64::MIN, |val, new| futures::future::ok(val.max(new)))
            .await
    }
}

pub fn name_of_migration(id: u64, m: &Migration) -> String {
    format!("{:08}_{:016X}.mgrt", id, hash_migration(m))
}

pub fn id_of_migration(name: &str) -> Option<u64> {
    let file_name = Path::new(name).file_name()?.to_string_lossy();

    if file_name.len() < 8 {
        return None;
    }
    file_name[..8].parse::<u64>().ok()
}

pub fn hash_migration(m: &Migration) -> u64 {
    let mut crc64 = 0;
    for compaction in m.compactions.iter() {
        crc64 ^= compaction.artifactes_hash;
    }
    for meta_edit in m.edit_meta.iter() {
        crc64 ^= hash_meta_edit(meta_edit);
    }
    crc64 ^ m.truncated_to
}

pub fn hash_meta_edit(meta_edit: &brpb::MetaEdit) -> u64 {
    let mut crc64 = 0;
    for df in meta_edit.delete_physical_files.iter() {
        let mut digest = crc64fast::Digest::new();
        digest.write(df.as_bytes());
        crc64 ^= digest.sum64();
    }
    for spans in meta_edit.delete_logical_files.iter() {
        let mut crc = crc64fast::Digest::new();
        crc.write(spans.get_path().as_bytes());
        for span in spans.get_spans() {
            let mut crc = crc.clone();
            crc.write(&span.offset.to_le_bytes());
            crc.write(&span.length.to_le_bytes());
            crc64 ^= crc.sum64();
        }
    }
    let mut crc = crc64fast::Digest::new();
    crc.write(&[meta_edit.destruct_self as u8]);
    crc64 ^ crc.sum64()
}
