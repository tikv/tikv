// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque, hash_map::Entry},
    future::Future,
    ops::{Deref, Not},
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};

use cloud::blob::read_to_end;
use derive_more::Display;
use external_storage::{BlobObject, ExternalStorage, UnpinReader};
use futures::{
    future::{FusedFuture, FutureExt, TryFutureExt},
    io::Cursor,
    stream::{Fuse, FusedStream, StreamExt, TryStreamExt},
};
use kvproto::{
    brpb::{self, DataFileInfo, FileType, MetaEdit, Metadata, Migration},
    metapb::RegionEpoch,
};
use prometheus::core::{Atomic, AtomicU64};
use protobuf::{Chars, ProtobufEnum};
use tikv_util::{
    info, retry_expr,
    stream::{JustRetry, RetryExt},
};
use tokio::time::Instant;
use tokio_stream::Stream;
use tracing::{Span, span::Entered};
use tracing_active_tree::frame;

use super::{
    errors::{Error, Result},
    statistic::LoadMetaStatistic,
};
use crate::{OtherErrExt, compaction::EpochHint, errors::ErrorKind, util};

pub const METADATA_PREFIX: &str = "v1/backupmeta";
pub const DEFAULT_COMPACTION_OUT_PREFIX: &str = "v1/compaction_out";
pub const MIGRATION_PREFIX: &str = "v1/migrations";
pub const LOCK_PREFIX: &str = "v1/LOCK";

/// The in-memory presentation of the message [`brpb::Metadata`].
#[derive(Debug, PartialEq, Eq)]
pub struct MetaFile {
    pub name: Arc<str>,
    pub physical_files: Vec<PhysicalLogFile>,
    pub min_ts: u64,
    pub max_ts: u64,
}

impl From<Metadata> for MetaFile {
    fn from(value: Metadata) -> Self {
        Self::from_file(Arc::from(":memory:"), value)
    }
}

impl MetaFile {
    pub fn from_file(name: Arc<str>, mut meta_file: Metadata) -> Self {
        let mut log_files = vec![];
        let min_ts = meta_file.min_ts;
        let max_ts = meta_file.max_ts;

        // NOTE: perhaps we also need consider non-grouped backup meta here?
        for mut group in meta_file.take_file_groups().into_iter() {
            let mut g = PhysicalLogFile {
                size: group.length,
                name: group.path.clone(),
                files: Vec::with_capacity(group.data_files_info.len()),
            };
            for log_file in group.take_data_files_info().into_iter() {
                g.files.push(LogFile::from_pb(group.path.clone(), log_file))
            }
            log_files.push(g);
        }

        Self {
            name,
            physical_files: log_files,
            min_ts,
            max_ts,
        }
    }
}

impl MetaFile {
    pub fn into_logs(self) -> impl Iterator<Item = LogFile> {
        self.physical_files
            .into_iter()
            .flat_map(|g| g.files.into_iter())
    }
}

/// The in-memory presentation of the message [`brpb::DataFileGroup`].
#[derive(Debug, PartialEq, Eq)]
pub struct PhysicalLogFile {
    pub size: u64,
    pub name: Chars,
    pub files: Vec<LogFile>,
}

/// An [`RegionEpoch`] without protocol buffer fields and comparable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Epoch {
    pub version: u64,
    pub conf_ver: u64,
}

impl From<RegionEpoch> for Epoch {
    fn from(value: RegionEpoch) -> Self {
        Self {
            version: value.version,
            conf_ver: value.conf_ver,
        }
    }
}

impl From<Epoch> for RegionEpoch {
    fn from(value: Epoch) -> Self {
        let mut v = Self::new();
        v.version = value.version;
        v.conf_ver = value.conf_ver;
        v
    }
}

/// The in-memory presentation of the message [`brpb::DataFileInfo`].
/// The difference is that all `Vec<u8>` are replaced with `Arc<[u8]>` to save
/// memory.
#[derive(Debug, Clone, PartialEq, Eq)]

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
    pub min_key: bytes::Bytes,
    pub max_key: bytes::Bytes,
    pub region_start_key: Option<bytes::Bytes>,
    pub region_end_key: Option<bytes::Bytes>,
    pub region_epoches: Option<Arc<[Epoch]>>,
    pub is_meta: bool,
    pub ty: FileType,
    pub compression: brpb::CompressionType,
    pub table_id: i64,
    pub resolved_ts: u64,
    pub sha256: bytes::Bytes,
}

impl LogFile {
    pub fn hacky_key_value_size(&self) -> u64 {
        const HEADER_SIZE_PER_ENTRY: u64 = std::mem::size_of::<u32>() as u64 * 2;
        self.file_real_size - HEADER_SIZE_PER_ENTRY * self.number_of_entries as u64
    }

    pub fn epoch_hints(&self) -> impl Iterator<Item = EpochHint> + '_ {
        self.region_epoches.iter().flat_map(|epoches| {
            self.region_start_key.iter().flat_map(|sk| {
                self.region_end_key.iter().flat_map(|ek| {
                    epoches.iter().map(|v| EpochHint {
                        region_epoch: *v,
                        start_key: sk.clone(),
                        end_key: ek.clone(),
                    })
                })
            })
        })
    }
}

/// The identity of a log file.
/// A log file can be located in the storage with this.
#[derive(Clone, Display, Eq, PartialEq, Hash)]
#[display(fmt = "{}@{}+{}", name, offset, length)]
pub struct LogFileId {
    pub name: Chars,
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

/// Extra config for loading metadata.
pub struct LoadFromExt<'a> {
    /// The [`tracing::Span`] of loading remote tasks.
    /// This span will be entered when fetching the remote tasks.
    /// This span will be closed when all metadata loaded.
    pub loading_content_span: Option<Span>,
    /// The prefix of metadata in the external storage.
    /// By default it is `v1/backupmeta`.
    pub meta_prefix: &'a str,
    /// Max number of running tasks to fetch metadatas
    pub prefetch_running_count: usize,
    /// Max number of spawning tasks to fetch metadatas
    pub prefetch_buffer_count: usize,
}

impl LoadFromExt<'_> {
    fn enter_load_span(&self) -> Option<Entered<'_>> {
        self.loading_content_span.as_ref().map(|span| span.enter())
    }
}

impl Default for LoadFromExt<'_> {
    fn default() -> Self {
        Self {
            loading_content_span: None,
            meta_prefix: METADATA_PREFIX,
            prefetch_running_count: 128,
            prefetch_buffer_count: 1024,
        }
    }
}

/// The storage of log backup.
///
/// For now, it supports load all metadata only, by consuming the stream.
/// [`StreamyMetaStorage`] is a [`Stream`] that yields `Result<MetaFile>`.
pub struct StreamMetaStorage<'a> {
    // NOTE: we want to keep the order of incoming meta files, so calls with the same argument can
    // generate the same compactions.
    prefetch: VecDeque<
        Prefetch<
            Pin<Box<dyn Future<Output = Result<(MetaFile, LoadMetaStatistic)>> + Send + 'static>>,
        >,
    >,
    ext_storage: Arc<dyn ExternalStorage>,
    ext: LoadFromExt<'a>,
    stat: LoadMetaStatistic,

    files: Fuse<Pin<Box<dyn Stream<Item = std::io::Result<BlobObject>> + 'a>>>,

    skip_map: MetaEditFilters,

    running_fetch_tasks: usize,
}

/// A future that stores its result for future use when completed.
///
/// This wraps a [`Future`](std::future::Future) yields `T` to a future yields
/// nothing. Once the future is terminaled (resolved), the content can then be
/// fetch by `must_fetch`.
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

impl Stream for StreamMetaStorage<'_> {
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
            Poll::Ready(item) => {
                let result = item.map(|mut meta| {
                    let sm = &self.skip_map;
                    let skipped = sm.apply_to(&mut meta);
                    self.stat.log_filtered_out_by_migration += skipped as u64;
                    meta
                });
                let _ = self.poll_fetch_or_finish(cx);
                Poll::Ready(Some(result))
            }
            Poll::Pending => self.poll_fetch_or_finish(cx),
        }
    }
}

impl<'a> StreamMetaStorage<'a> {
    /// Poll the next event.
    fn poll_fetch_or_finish(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<MetaFile>>> {
        loop {
            // No more space for prefetching.
            if self.running_fetch_tasks >= self.ext.prefetch_running_count
                || self.prefetch.len() >= self.ext.prefetch_buffer_count
            {
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
            let res = {
                let _enter = self.ext.enter_load_span();
                self.files.next().poll_unpin(cx)
            };
            match res {
                Poll::Ready(Some(load)) => {
                    let load = load?;
                    if self.skip_map.should_fully_skip(&load.key) {
                        info!("Skipping a metadata by migration."; "name" => %load.key);
                        self.stat.meta_filtered_out_by_migration += 1;
                        continue;
                    }
                    let storage = Arc::clone(&self.ext_storage);
                    let handle = tokio::spawn(MetaFile::load_from_owned(storage, load));
                    let mut fut = Prefetch::new(async move { handle.await.unwrap() }.boxed());
                    // start the execution of this future.
                    let poll = fut.poll_unpin(cx);
                    if poll.is_ready() {
                        // We need to check this in next run.
                        cx.waker().wake_by_ref();
                    }
                    self.stat.prefetch_task_emitted += 1;
                    self.prefetch.push_back(fut);
                    self.running_fetch_tasks += 1;
                }
                Poll::Ready(None) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_first_prefetch(&mut self, cx: &mut Context<'_>) -> Poll<Result<MetaFile>> {
        self.running_fetch_tasks = 0;
        for fut in &mut self.prefetch {
            if !fut.is_terminated() && fut.poll_unpin(cx) == Poll::Pending {
                self.running_fetch_tasks += 1;
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

    /// Streaming metadata from an external storage.
    /// Defaultly this will fetch metadata from `v1/backupmeta`, you may
    /// override this in `ext`.
    pub async fn load_from_ext(
        s: &'a Arc<dyn ExternalStorage>,
        ext: LoadFromExt<'a>,
    ) -> Result<Self> {
        let files = s.iter_prefix(ext.meta_prefix).fuse();
        let mig_ext = MigrationStorageWrapper::new(s);
        let skip_map = MetaEditFilters::from_migrations(mig_ext.load().await?);
        Ok(Self {
            prefetch: VecDeque::new(),
            files,
            ext_storage: Arc::clone(s),
            ext,
            stat: LoadMetaStatistic::default(),
            skip_map,
            running_fetch_tasks: 0,
        })
    }

    /// Count the number of the metadata prefix.
    pub async fn count_objects(s: &'a dyn ExternalStorage) -> std::io::Result<u64> {
        let mut n = 0;
        // NOTE: should we allow user to specify the prefix?
        let mut items = s.iter_prefix(METADATA_PREFIX);
        while items.try_next().await?.is_some() {
            n += 1
        }
        Ok(n)
    }
}

impl MetaFile {
    async fn load_from_owned(
        s: Arc<dyn ExternalStorage>,
        blob: BlobObject,
    ) -> Result<(Self, LoadMetaStatistic)> {
        Self::load_from(s.as_ref(), blob).await
    }

    #[tracing::instrument(skip_all, fields(blob=%blob))]
    async fn load_from(
        s: &dyn ExternalStorage,
        blob: BlobObject,
    ) -> Result<(Self, LoadMetaStatistic)> {
        use protobuf::{CodedInputStream, Message};

        let _t = crate::statistic::prom::COMPACT_LOG_BACKUP_READ_META_DURATION.start_coarse_timer();

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
                let n = read_to_end(s.read(&blob.key), &mut content).await?;
                std::io::Result::Ok((n, bytes::Bytes::from(content)))
            },
            ext,
        );
        let (n, content) = frame!(loading_file)
            .await
            .map_err(|err| Error::from(err).message(format_args!("reading {}", blob.key)))?;
        stat.physical_bytes_loaded += n;
        stat.error_during_downloading += error_cnt2.get();

        let mut meta_file = Metadata::new();
        meta_file.merge_from(&mut CodedInputStream::from_carllerche_bytes(&content))?;
        let name = Arc::from(blob.key.into_boxed_str());
        let result = Self::from_file(name, meta_file);

        stat.physical_data_files_in += result.physical_files.len() as u64;
        stat.logical_data_files_in += result
            .physical_files
            .iter()
            .map(|v| v.files.len() as u64)
            .sum::<u64>();
        stat.load_file_duration += begin.elapsed();

        Ok((result, stat))
    }
}

impl LogFile {
    fn from_pb(host_file: Chars, mut pb_info: DataFileInfo) -> Self {
        let region_epoches = pb_info.region_epoch.is_empty().not().then(|| {
            pb_info
                .region_epoch
                .iter()
                .cloned()
                .map(From::from)
                .collect()
        });
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
            max_key: pb_info.take_end_key(),
            min_key: pb_info.take_start_key(),
            region_start_key: pb_info
                .region_epoch
                .is_empty()
                .not()
                .then(|| pb_info.take_region_start_key()),
            region_end_key: pb_info
                .region_epoch
                .is_empty()
                .not()
                .then(|| pb_info.take_region_end_key()),
            is_meta: pb_info.is_meta,
            min_start_ts: pb_info.min_begin_ts_in_default_cf,
            ty: pb_info.r_type,
            crc64xor: pb_info.crc64xor,
            number_of_entries: pb_info.number_of_entries,
            sha256: pb_info.take_sha256(),
            resolved_ts: pb_info.resolved_ts,
            table_id: pb_info.table_id,
            compression: pb_info.compression_type,
            region_epoches,
        }
    }

    pub fn into_pb(self) -> brpb::DataFileInfo {
        let mut pb = brpb::DataFileInfo::new();
        pb.range_offset = self.id.offset;
        pb.range_length = self.id.length;
        pb.length = self.file_real_size;
        pb.region_id = self.region_id as _;
        pb.cf = self.cf.into();
        pb.max_ts = self.max_ts;
        pb.min_ts = self.min_ts;
        pb.set_end_key(self.max_key);
        pb.set_start_key(self.min_key);
        pb.is_meta = self.is_meta;
        pb.min_begin_ts_in_default_cf = self.min_start_ts;
        pb.r_type = self.ty;
        pb.crc64xor = self.crc64xor;
        pb.number_of_entries = self.number_of_entries;
        pb.set_sha256(self.sha256);
        pb.resolved_ts = self.resolved_ts;
        pb.table_id = self.table_id;
        pb.compression_type = self.compression;
        pb.set_region_start_key(self.region_start_key.unwrap_or_default());
        pb.set_region_end_key(self.region_end_key.unwrap_or_default());
        pb.set_region_epoch(
            self.region_epoches
                .map(|v| v.iter().cloned().map(From::from).collect())
                .unwrap_or_default(),
        );
        pb
    }
}

#[derive(derive_more::Deref, derive_more::DerefMut, Debug)]
/// A migration with version and creator info.
/// Preferring use this instead of directly create `Migration`.
pub struct VersionedMigration(Migration);

impl From<Migration> for VersionedMigration {
    fn from(mut mig: Migration) -> Self {
        // The last version we know.
        mig.set_version(*brpb::MigrationVersion::values().last().unwrap());
        mig.set_creator(format!(
            "tikv;commit={};branch={}",
            option_env!("TIKV_BUILD_GIT_HASH").unwrap_or("UNKNOWN"),
            option_env!("TIKV_BUILD_GIT_BRANCH").unwrap_or("UNKNOWN"),
        ));
        Self(mig)
    }
}

impl Default for VersionedMigration {
    fn default() -> Self {
        Self::from(Migration::default())
    }
}

#[derive(Debug)]
struct MetaEditFilters(HashMap<String, MetaEditFilter>);

impl MetaEditFilters {
    fn from_migrations(migs: impl IntoIterator<Item = Migration>) -> Self {
        let iter = migs
            .into_iter()
            .flat_map(|mut m| m.take_edit_meta().into_iter());
        let mut m: HashMap<String, MetaEditFilter> = HashMap::with_capacity(iter.size_hint().0);
        iter.for_each(|em| {
            match m.entry(em.path.clone()) {
                Entry::Occupied(mut o) => o.get_mut().merge_from_meta_edit(em),
                Entry::Vacant(v) => {
                    v.insert(MetaEditFilter::from_meta_edit(em));
                }
            };
        });
        Self(m)
    }

    fn should_fully_skip(&self, meta: &str) -> bool {
        self.0
            .get(meta)
            .map(|v| v.all_data_files_compacted || v.destructed_self)
            .unwrap_or(false)
    }

    /// Apply the meta edition to a meta file and returns how many log files are
    /// deleted.
    fn apply_to(&self, meta: &mut MetaFile) -> usize {
        let mut deleted = 0;
        if let Some(meta_edit) = self.0.get(meta.name.as_ref()) {
            info!("Applying meta edition to a meta file."; "edition" => ?meta_edit, "meta" => %meta.name);
            meta.physical_files.retain_mut(|v| {
                let before = v.files.len();
                v.files.retain(|f| meta_edit.should_retain(&f.id));
                deleted += before - v.files.len();
                !v.files.is_empty()
            })
        }
        deleted
    }
}

/// The same content as [`kvproto::brpb::MetaEdit`], but provides some utility
/// to apply directly in the memory content.
#[derive(Debug)]
struct MetaEditFilter {
    // Full deleted files
    full_files: HashSet<String>,
    // FileName -> Offset
    segments: HashMap<String, BTreeSet<u64>>,
    destructed_self: bool,
    all_data_files_compacted: bool,
}

impl MetaEditFilter {
    fn from_meta_edit(mut em: MetaEdit) -> Self {
        let mut this = Self {
            full_files: Default::default(),
            segments: Default::default(),
            destructed_self: em.destruct_self,
            all_data_files_compacted: em.all_data_files_compacted,
        };
        this.full_files
            .extend(em.take_delete_physical_files().into_iter());
        for deletion in em.get_delete_logical_files() {
            if !this.segments.contains_key(deletion.get_path()) {
                this.segments
                    .insert(deletion.get_path().to_owned(), Default::default());
            }

            this.segments
                .get_mut(deletion.get_path())
                .unwrap()
                .extend(deletion.get_spans().iter().map(|span| span.offset));
        }

        this
    }

    fn merge_from_meta_edit(&mut self, mut em: MetaEdit) {
        self.destructed_self = self.destructed_self || em.destruct_self;
        self.all_data_files_compacted =
            self.all_data_files_compacted || em.all_data_files_compacted;
        if self.destructed_self || self.all_data_files_compacted {
            // NOTE: the metakv files will be filtered out in
            // `SubcompactionCollector::add_new_file`. Therefore, the
            // self.segments and self.full_files can be clear here.
            self.full_files = Default::default();
            self.segments = Default::default();
            return;
        }
        self.full_files
            .extend(em.take_delete_physical_files().into_iter());
        for deletion in em.get_delete_logical_files() {
            if !self.segments.contains_key(deletion.get_path()) {
                self.segments
                    .insert(deletion.get_path().to_owned(), Default::default());
            }

            self.segments
                .get_mut(deletion.get_path())
                .unwrap()
                .extend(deletion.get_spans().iter().map(|span| span.offset));
        }
        for path in &self.full_files {
            self.segments.remove(path);
        }
        self.segments.shrink_to_fit();
    }

    fn should_retain(&self, file: &LogFileId) -> bool {
        if self.full_files.contains(file.name.deref()) {
            return false;
        }
        if self
            .segments
            .get(file.name.deref())
            .is_some_and(|map| map.contains(&file.offset))
        {
            return false;
        }

        true
    }
}

pub struct MigrationStorageWrapper<'a> {
    storage: &'a dyn ExternalStorage,
    migrations_prefix: &'a str,
}

impl<'a> MigrationStorageWrapper<'a> {
    pub fn new(storage: &'a dyn ExternalStorage) -> Self {
        Self {
            storage,
            migrations_prefix: MIGRATION_PREFIX,
        }
    }

    pub async fn load(&self) -> Result<Vec<Migration>> {
        self.storage
            .iter_prefix(self.migrations_prefix)
            .err_into()
            .and_then(|item| async move {
                let mut content = vec![];
                read_to_end(self.storage.read(&item.key), &mut content).await?;
                protobuf::parse_from_bytes(&content).adapt_err()
            })
            .try_collect()
            .await
    }

    pub async fn write(&self, migration: VersionedMigration) -> Result<()> {
        use protobuf::Message;

        let migration = migration.0;
        let id = self.largest_id().await?;
        // Note: perhaps we need to verify that there isn't concurrency writing in the
        // future.
        let name = name_of_migration(id + 1, &migration);
        let bytes = migration.write_to_bytes()?;
        retry_expr!(
            self.storage
                .write(
                    &format!("{}/{}", self.migrations_prefix, name),
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
            .iter_prefix(self.migrations_prefix)
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
    if file_name == "BASE" {
        return Some(0);
    }
    if file_name.len() < 8 {
        return None;
    }
    file_name[..8].parse::<u64>().ok()
}

pub fn hash_migration(m: &Migration) -> u64 {
    let mut crc64 = 0;
    for compaction in m.compactions.iter() {
        crc64 ^= compaction.artifacts_hash;
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use external_storage::ExternalStorage;
    use futures::stream::TryStreamExt;
    use kvproto::brpb::{DeleteSpansOfFile, MetaEdit, Migration, Span};
    use protobuf::Chars;

    use super::{LoadFromExt, MetaFile, StreamMetaStorage};
    use crate::{
        storage::{LogFileId, MetaEditFilters, MigrationStorageWrapper},
        test_util::{KvGen, LogFileBuilder, TmpStorage, gen_step},
    };

    async fn construct_storage(
        st: &TmpStorage,
        meta_path: impl Fn(i64) -> String,
        log_path: impl Fn(i64) -> String,
    ) -> Vec<MetaFile> {
        let gen_builder = |batch, num| {
            (num * batch..num * (batch + 1))
                .map(move |v| {
                    KvGen::new(gen_step(1, v + batch * num, num), |_| b"val".to_vec()).take(2)
                })
                .enumerate()
                .map(|(n, it)| {
                    let mut b = LogFileBuilder::new(|v| v.region_id = n as u64);
                    for kv in it {
                        b.add_encoded(&kv.key, &kv.value)
                    }
                    b
                })
        };
        let mut mfs = vec![];
        for i in 0..5 {
            let mf = st
                .build_flush(&log_path(i), &meta_path(i), gen_builder(i, 10))
                .await;
            mfs.push(mf);
        }
        mfs
    }

    #[tokio::test]
    async fn test_load_from_storage() {
        let st = TmpStorage::create();
        let mfs = construct_storage(
            &st,
            |i| format!("v1/backupmeta/the-meta-{}.bin", i),
            |i| format!("out/the-log-{}.bin", i),
        )
        .await;

        tracing_active_tree::init();

        let mfs = &mfs;
        let st = &st;
        let test_for_concurrency = |n| async move {
            let mut ext = LoadFromExt::default();
            ext.prefetch_running_count = n;
            let storage = st.storage().clone() as Arc<dyn ExternalStorage>;
            let sst = StreamMetaStorage::load_from_ext(&storage, ext)
                .await
                .unwrap();
            let mut result = sst.try_collect::<Vec<_>>().await.unwrap();
            result.sort_by(|a, b| a.name.cmp(&b.name));
            assert_eq!(&result, mfs);
        };

        test_for_concurrency(1).await;
        test_for_concurrency(2).await;
        test_for_concurrency(16).await;
    }

    #[tokio::test]
    async fn test_merge_meta_edits() {
        let meta_path = |i| format!("v1/backupmeta/00000000-{i}.meta");
        let file_path = |i| format!("v1/00000000/00000000-{i}.log");
        let span = |i, i1, i2, i3| {
            let mut s = DeleteSpansOfFile::new();
            s.set_path(file_path(i));
            let mut s1 = Span::new();
            s1.set_offset(i1);
            let mut s2 = Span::new();
            s2.set_offset(i2);
            let mut s3 = Span::new();
            s3.set_offset(i3);
            s.set_spans(vec![s1, s2, s3].into());
            s
        };
        let mut meta_edit1 = MetaEdit::new();
        meta_edit1.set_path(meta_path(1));
        meta_edit1.mut_delete_physical_files().push(file_path(1));
        meta_edit1.mut_delete_logical_files().push(span(2, 1, 2, 3));
        meta_edit1.mut_delete_logical_files().push(span(3, 1, 2, 3));
        let mut meta_edit2 = MetaEdit::new();
        meta_edit2.set_path(meta_path(1));
        meta_edit2.mut_delete_physical_files().push(file_path(2));
        meta_edit2.mut_delete_logical_files().push(span(3, 4, 5, 6));
        let mut meta_edit3 = MetaEdit::new();
        meta_edit3.set_path(meta_path(2));
        meta_edit3.mut_delete_physical_files().push(file_path(4));
        meta_edit3.mut_delete_logical_files().push(span(5, 1, 2, 3));
        let mut meta_edit4 = MetaEdit::new();
        meta_edit4.set_path(meta_path(2));
        meta_edit4.set_destruct_self(true);
        let mut mig1 = Migration::new();
        mig1.mut_edit_meta().push(meta_edit1);
        mig1.mut_edit_meta().push(meta_edit3);
        let mut mig2 = Migration::new();
        mig2.mut_edit_meta().push(meta_edit2);
        mig2.mut_edit_meta().push(meta_edit4);
        let migs = vec![mig1, mig2];
        let mefs = MetaEditFilters::from_migrations(migs);
        assert!(mefs.should_fully_skip(&meta_path(2)));
        assert!(!mefs.should_fully_skip(&meta_path(1)));
        let f1 = mefs.0.get(&meta_path(1)).unwrap();
        let log_file_id = |name: String, offset: u64| LogFileId {
            name: Chars::from(name),
            offset,
            length: offset + 1,
        };
        assert!(!f1.should_retain(&log_file_id(file_path(1), 234)));
        assert!(!f1.should_retain(&log_file_id(file_path(2), 435)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 1)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 2)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 3)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 4)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 5)));
        assert!(!f1.should_retain(&log_file_id(file_path(3), 6)));
        assert!(f1.should_retain(&log_file_id(file_path(3), 7)));
    }

    #[tokio::test]
    async fn test_different_prefix() {
        let st = TmpStorage::create();
        let mfs = construct_storage(
            &st,
            |i| format!("my-fantastic-meta-dir/{}.meta", i),
            |i| format!("{}.log", i),
        )
        .await;

        let mut ext = LoadFromExt::default();
        ext.meta_prefix = "my-fantastic-meta-dir";
        let storage = st.storage().clone() as Arc<dyn ExternalStorage>;
        let sst = StreamMetaStorage::load_from_ext(&storage, ext)
            .await
            .unwrap();
        let mut result = sst.try_collect::<Vec<_>>().await.unwrap();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(result, mfs);
    }

    #[tokio::test]
    async fn test_filter_out_by_migs() {
        let st = TmpStorage::create();
        let meta = |i| format!("v1/backupmeta/{}.meta", i);
        let log = |i| format!("{}.log", i);
        let mut mfs = construct_storage(&st, meta, log).await;
        let mig = {
            let mut mig = Migration::new();

            mig.mut_edit_meta().push({
                let mut me = MetaEdit::new();
                me.path = meta(1);
                me.destruct_self = true;
                me
            });
            mig.mut_edit_meta().push({
                let mut me = MetaEdit::new();
                me.path = meta(0);
                me.mut_delete_logical_files().push({
                    let mut ds = DeleteSpansOfFile::new();
                    ds.set_path(log(0));
                    ds.mut_spans().push({
                        let mut span = Span::new();
                        span.offset = 51;
                        span.length = 52;
                        span
                    });
                    ds.mut_spans().push({
                        let mut span = Span::new();
                        span.offset = 155;
                        span.length = 52;
                        span
                    });
                    ds
                });
                me
            });

            mig
        };
        let mig2 = {
            let mut mig = Migration::new();

            mig.mut_edit_meta().push({
                let mut me = MetaEdit::new();
                me.path = meta(2);
                me.mut_delete_physical_files().push(log(2));
                me
            });
            mig
        };

        let s = MigrationStorageWrapper::new(st.storage().as_ref());
        s.write(mig.into()).await.unwrap();
        s.write(mig2.into()).await.unwrap();
        let storage = st.storage().clone() as Arc<dyn ExternalStorage>;
        let mut sst = StreamMetaStorage::load_from_ext(&storage, Default::default())
            .await
            .unwrap();

        // Manually apply the meta edits...
        mfs[0].physical_files[0].files.remove(3);
        mfs[0].physical_files[0].files.remove(1);
        mfs[2].physical_files.clear();
        mfs.remove(1);

        let mut result = (&mut sst).try_collect::<Vec<_>>().await.unwrap();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(result, mfs);

        let stat = sst.take_statistic();
        assert_eq!(stat.log_filtered_out_by_migration, 12);
        assert_eq!(stat.meta_filtered_out_by_migration, 1);
    }
}
