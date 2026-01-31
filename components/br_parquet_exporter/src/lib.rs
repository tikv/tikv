// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    fs::{self, File},
    future::Future,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use collections::HashSet as FxHashSet;
use engine_rocks::RocksSstReader;
use engine_traits::{CF_DEFAULT, CF_WRITE, IterOptions, Iterator, RefIterable, SstReader};
use external_storage::{ExternalStorage, UnpinReader};
use futures::{
    AsyncRead, AsyncReadExt,
    executor::block_on,
    io::{AllowStdIo, Cursor},
    stream::TryStreamExt,
};
use hex::ToHex;
use keys::{self, DATA_PREFIX};
use kvproto::brpb::{BackupMeta, File as BackupFile, MetaFile, Schema as BackupSchema};
pub use parquet::basic::Compression;
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
use protobuf::Message;
use rayon::{ThreadPoolBuilder, prelude::*};
use serde::{Deserialize, Serialize};
use tempfile::{NamedTempFile, TempDir};
use thiserror::Error;
use tidb_query_datatype::{
    codec::{
        datum::{self, Datum, DatumDecoder},
        mysql::{Duration, Time},
        table,
    },
    def::{FieldTypeAccessor, FieldTypeTp},
    expr::EvalContext,
};
use tikv_util::{
    table_filter::TableFilter as NameFilter,
    time::{Instant, Limiter},
};
use tipb::ColumnInfo;
use txn_types::{Key, WriteRef, WriteType};

mod iceberg_table;
mod metrics;
mod schema;
mod writer;

use schema::ColumnKind;
pub use schema::{ColumnParquetType, ColumnSchema, TableSchema};
use writer::{CellValue, ParquetWriter};

pub use crate::metrics::ExporterMetricsSnapshot;
use crate::metrics::{
    BR_PARQUET_OUTPUT_BYTES, BR_PARQUET_ROWS, BR_PARQUET_SSTS, BR_PARQUET_STAGE_DURATION,
};

/// Returns a point-in-time snapshot of BR Parquet exporter metrics.
pub fn exporter_metrics_snapshot() -> ExporterMetricsSnapshot {
    metrics::snapshot()
}

/// Parses a Parquet compression codec name.
pub fn parse_parquet_compression(name: &str) -> std::result::Result<Compression, String> {
    match name.to_ascii_lowercase().as_str() {
        "snappy" => Ok(Compression::SNAPPY),
        "zstd" => Ok(Compression::ZSTD(ZstdLevel::default())),
        "gzip" => Ok(Compression::GZIP(GzipLevel::default())),
        "brotli" => Ok(Compression::BROTLI(BrotliLevel::default())),
        "lz4" | "lz4raw" => Ok(Compression::LZ4_RAW),
        "none" | "uncompressed" => Ok(Compression::UNCOMPRESSED),
        other => Err(format!("unsupported compression codec {}", other)),
    }
}

fn parquet_compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::SNAPPY => "snappy",
        Compression::ZSTD(_) => "zstd",
        Compression::GZIP(_) => "gzip",
        Compression::BROTLI(_) => "brotli",
        Compression::LZ4_RAW => "lz4",
        Compression::UNCOMPRESSED => "none",
        _ => "unknown",
    }
}

const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
const DEFAULT_OUTPUT_PREFIX: &str = "parquet";
const DEFAULT_BACKUP_META_PATH: &str = "backupmeta";
const CHECKPOINT_DIR: &str = "_checkpoint";
const CHECKPOINT_ENTRY_DIR: &str = "entries";
const CHECKPOINT_META_FILE: &str = "meta.json";
const CHECKPOINT_VERSION: u32 = 1;

/// Errors returned by the BR SST → Parquet exporter.
#[derive(Debug, Error)]
pub enum Error {
    #[error("I/O error {0}")]
    Io(#[from] std::io::Error),
    #[error("Parquet error {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("SST error {0}")]
    Sst(#[from] engine_traits::Error),
    #[error("Protobuf error {0}")]
    Protobuf(#[from] protobuf::error::ProtobufError),
    #[error("Codec error {0}")]
    Codec(#[from] tidb_query_datatype::codec::Error),
    #[error("Schema error {0}")]
    Schema(String),
    #[error("Invalid backup: {0}")]
    Invalid(String),
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Result type used by the BR SST → Parquet exporter.
pub type Result<T> = std::result::Result<T, Error>;

fn sha256_bytes(input: &[u8]) -> Result<Vec<u8>> {
    file_system::sha256(input).map_err(|err| Error::Invalid(format!("sha256 failed: {}", err)))
}

/// Export configuration for BR SST → Parquet conversion.
#[derive(Clone, Debug)]
pub struct ExportOptions {
    pub row_group_size: usize,
    pub compression: Compression,
    pub sst_concurrency: usize,
    pub use_checkpoint: bool,
    pub write_iceberg_table: bool,
    pub bloom_filter: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        let concurrency = std::thread::available_parallelism()
            .map(|threads| threads.get())
            .unwrap_or(1);
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compression: Compression::SNAPPY,
            sst_concurrency: concurrency,
            use_checkpoint: true,
            write_iceberg_table: false,
            bloom_filter: false,
        }
    }
}

/// Filter for selecting which TiDB tables to export.
#[derive(Clone, Debug, Default)]
pub struct TableFilter {
    table_ids: HashSet<i64>,
    rules: Option<NameFilter>,
    raw_rules: Vec<String>,
    raw_table_ids: Vec<i64>,
}

impl TableFilter {
    /// Builds a [`TableFilter`] from CLI arguments (table filters + table IDs).
    pub fn from_args(filters: &[String], table_ids: &[i64]) -> Result<Self> {
        let mut ids = HashSet::default();
        let mut raw_table_ids = Vec::new();
        for id in table_ids {
            ids.insert(*id);
            raw_table_ids.push(*id);
        }
        raw_table_ids.sort_unstable();
        let rules = if filters.is_empty() {
            None
        } else {
            let parsed =
                NameFilter::parse(filters).map_err(|err| Error::Invalid(err.to_string()))?;
            Some(
                parsed
                    .case_insensitive()
                    .map_err(|err| Error::Invalid(err.to_string()))?,
            )
        };
        Ok(TableFilter {
            table_ids: ids,
            rules,
            raw_rules: filters.to_vec(),
            raw_table_ids,
        })
    }

    fn is_active(&self) -> bool {
        !self.table_ids.is_empty() || self.rules.is_some()
    }

    fn matches(&self, table: &TableSchema) -> bool {
        if self.table_ids.contains(&table.table_id) {
            return true;
        }
        match &self.rules {
            None => self.table_ids.is_empty(),
            Some(rules) => {
                let schema = table.db_name.to_lowercase();
                let name = table.table_name.to_lowercase();
                rules.matches_table(&schema, &name)
            }
        }
    }
}

/// Export result produced by [`SstParquetExporter`].
#[derive(Clone, Debug, Default)]
pub struct ExportReport {
    pub files: Vec<ParquetFileInfo>,
    pub total_rows: u64,
    pub start_version: u64,
    pub end_version: u64,
}

/// Metadata for a single exported Parquet file.
#[derive(Clone, Debug)]
pub struct ParquetFileInfo {
    pub object_name: String,
    pub table_id: i64,
    pub db_name: String,
    pub table_name: String,
    pub row_count: u64,
    pub size: u64,
    pub sha256: Vec<u8>,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
}

impl ParquetFileInfo {
    /// Converts this file info into a BR `File` descriptor for Iceberg manifest
    /// generation.
    pub fn to_manifest_file(&self, start_version: u64, end_version: u64) -> BackupFile {
        let mut file = BackupFile::default();
        file.set_name(self.object_name.clone());
        file.set_sha256(self.sha256.clone());
        file.set_cf("parquet".to_string());
        file.set_start_key(self.start_key.clone());
        file.set_end_key(self.end_key.clone());
        file.set_total_kvs(self.row_count);
        file.set_total_bytes(self.size);
        file.set_size(self.size);
        file.set_start_version(start_version);
        file.set_end_version(end_version);
        file
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointMeta {
    version: u32,
    start_version: u64,
    end_version: u64,
    config_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckpointEntry {
    object_name: String,
    table_id: i64,
    db_name: String,
    table_name: String,
    row_count: u64,
    size: u64,
    sha256: String,
    start_key: String,
    end_key: String,
}

impl CheckpointEntry {
    fn from_parquet(info: &ParquetFileInfo) -> Self {
        Self {
            object_name: info.object_name.clone(),
            table_id: info.table_id,
            db_name: info.db_name.clone(),
            table_name: info.table_name.clone(),
            row_count: info.row_count,
            size: info.size,
            sha256: info.sha256.encode_hex(),
            start_key: info.start_key.encode_hex(),
            end_key: info.end_key.encode_hex(),
        }
    }

    fn into_parquet(self) -> Result<ParquetFileInfo> {
        Ok(ParquetFileInfo {
            object_name: self.object_name,
            table_id: self.table_id,
            db_name: self.db_name,
            table_name: self.table_name,
            row_count: self.row_count,
            size: self.size,
            sha256: hex::decode(self.sha256).map_err(|err| {
                Error::Invalid(format!("invalid checkpoint sha256 value: {}", err))
            })?,
            start_key: hex::decode(self.start_key).map_err(|err| {
                Error::Invalid(format!("invalid checkpoint start_key value: {}", err))
            })?,
            end_key: hex::decode(self.end_key).map_err(|err| {
                Error::Invalid(format!("invalid checkpoint end_key value: {}", err))
            })?,
        })
    }
}

struct CheckpointState {
    entry_prefix: String,
    existing: HashMap<String, ParquetFileInfo>,
    recorded: Mutex<HashSet<String>>,
}

impl CheckpointState {
    fn new(entry_prefix: String, existing: HashMap<String, ParquetFileInfo>) -> Self {
        let recorded = existing.keys().cloned().collect();
        Self {
            entry_prefix,
            existing,
            recorded: Mutex::new(recorded),
        }
    }

    fn lookup(&self, object_name: &str) -> Option<ParquetFileInfo> {
        self.existing.get(object_name).cloned()
    }

    fn mark_recorded(&self, object_name: &str) -> bool {
        let mut recorded = self.recorded.lock().unwrap();
        recorded.insert(object_name.to_string())
    }

    fn unmark_recorded(&self, object_name: &str) {
        let mut recorded = self.recorded.lock().unwrap();
        recorded.remove(object_name);
    }

    fn entry_path(&self, object_name: &str) -> Result<String> {
        let digest = sha256_bytes(object_name.as_bytes())?;
        Ok(format!(
            "{}/{}.json",
            self.entry_prefix,
            hex::encode(digest)
        ))
    }
}

struct LoadedBackupMeta {
    meta: BackupMeta,
    digest: Vec<u8>,
}

/// Exports BR backup SST files into per-table Parquet files.
pub struct SstParquetExporter<'a> {
    input: &'a dyn ExternalStorage,
    output: &'a dyn ExternalStorage,
    tmp: TempDir,
    options: ExportOptions,
    runtime: Option<tokio::runtime::Handle>,
}

struct FileTask {
    file: BackupFile,
    tables: Vec<Arc<TableSchema>>,
}

struct FileTaskReport {
    files: Vec<ParquetFileInfo>,
    total_rows: u64,
}

impl<'a> SstParquetExporter<'a> {
    /// Creates a new exporter from `input` to `output` using the given
    /// `options`.
    pub fn new(
        input: &'a dyn ExternalStorage,
        output: &'a dyn ExternalStorage,
        options: ExportOptions,
    ) -> Result<Self> {
        Ok(Self {
            input,
            output,
            tmp: TempDir::new()?,
            options,
            runtime: tokio::runtime::Handle::try_current().ok(),
        })
    }

    fn block_on<F>(&self, fut: F) -> F::Output
    where
        F: Future,
    {
        if let Some(handle) = &self.runtime {
            handle.block_on(fut)
        } else {
            block_on(fut)
        }
    }

    /// Loads the default backup meta (`backupmeta`) and exports all matched
    /// tables.
    pub fn export_backup_meta(&mut self, output_prefix: &str) -> Result<ExportReport> {
        self.export_backup_meta_with_filter(output_prefix, &TableFilter::default())
    }

    /// Exports BR backup SST files using the provided [`TableFilter`].
    pub fn export_backup_meta_with_filter(
        &mut self,
        output_prefix: &str,
        filter: &TableFilter,
    ) -> Result<ExportReport> {
        let loaded = self.load_backup_meta(DEFAULT_BACKUP_META_PATH)?;
        let meta = self.expand_backup_meta(loaded.meta)?;
        self.export_from_meta(meta, output_prefix, filter, &loaded.digest)
    }

    fn load_backup_meta(&self, meta_path: &str) -> Result<LoadedBackupMeta> {
        let mut reader = self.input.read(meta_path);
        let mut buf = Vec::new();
        block_on(reader.read_to_end(&mut buf))?;
        let digest = sha256_bytes(&buf)?;
        let mut meta = BackupMeta::default();
        meta.merge_from_bytes(&buf)?;
        Ok(LoadedBackupMeta { meta, digest })
    }

    fn expand_backup_meta(&self, mut meta: BackupMeta) -> Result<BackupMeta> {
        if meta.get_is_raw_kv() {
            return Ok(meta);
        }
        let schema_index = meta.get_schema_index();
        let file_index = meta.get_file_index();
        let has_schema_index =
            !schema_index.get_meta_files().is_empty() || !schema_index.get_schemas().is_empty();
        let has_file_index =
            !file_index.get_meta_files().is_empty() || !file_index.get_data_files().is_empty();
        let schemas = if has_schema_index {
            self.collect_schemas_from_index(schema_index)?
        } else {
            meta.get_schemas().to_vec()
        };
        let files = if has_file_index {
            self.collect_files_from_index(file_index)?
        } else {
            meta.get_files().to_vec()
        };
        if schemas.is_empty() {
            return Err(Error::Invalid(
                "backup meta does not contain table schemas".into(),
            ));
        }
        meta.mut_schemas().clear();
        for schema in schemas {
            meta.mut_schemas().push(schema);
        }
        meta.mut_files().clear();
        for file in files {
            meta.mut_files().push(file);
        }
        Ok(meta)
    }

    fn collect_schemas_from_index(&self, index: &MetaFile) -> Result<Vec<BackupSchema>> {
        let mut leaves = Vec::new();
        self.walk_meta_index(index, &mut leaves)?;
        let mut schemas = Vec::new();
        for leaf in leaves {
            schemas.extend(leaf.get_schemas().iter().cloned());
        }
        Ok(schemas)
    }

    fn collect_files_from_index(&self, index: &MetaFile) -> Result<Vec<BackupFile>> {
        let mut leaves = Vec::new();
        self.walk_meta_index(index, &mut leaves)?;
        let mut files = Vec::new();
        for leaf in leaves {
            files.extend(leaf.get_data_files().iter().cloned());
        }
        Ok(files)
    }

    fn walk_meta_index(&self, index: &MetaFile, leaves: &mut Vec<MetaFile>) -> Result<()> {
        if index.get_meta_files().is_empty() {
            leaves.push(index.clone());
            return Ok(());
        }
        for file in index.get_meta_files() {
            let child = self.load_meta_file(file)?;
            self.walk_meta_index(&child, leaves)?;
        }
        Ok(())
    }

    fn load_meta_file(&self, file: &BackupFile) -> Result<MetaFile> {
        if !file.get_cipher_iv().is_empty() {
            return Err(Error::Invalid(format!(
                "encrypted backupmeta index {} is not supported",
                file.get_name()
            )));
        }
        let mut reader = self.input.read(file.get_name());
        let mut buf = Vec::new();
        block_on(reader.read_to_end(&mut buf))?;
        if !file.get_sha256().is_empty() {
            let digest = sha256_bytes(&buf)?;
            if digest.as_slice() != file.get_sha256() {
                return Err(Error::Invalid(format!(
                    "backupmeta index checksum mismatch for {}",
                    file.get_name()
                )));
            }
        }
        let mut meta = MetaFile::default();
        meta.merge_from_bytes(&buf)?;
        Ok(meta)
    }

    fn load_checkpoint(
        &self,
        output_prefix: &str,
        start_version: u64,
        end_version: u64,
        filter: &TableFilter,
        backup_meta_digest: &[u8],
    ) -> Result<CheckpointState> {
        let checkpoint_prefix = format!("{}/{}", output_prefix, CHECKPOINT_DIR);
        let entry_prefix = format!("{}/{}", checkpoint_prefix, CHECKPOINT_ENTRY_DIR);
        let meta_path = format!("{}/{}", checkpoint_prefix, CHECKPOINT_META_FILE);
        let config_hash =
            self.checkpoint_config_hash(start_version, end_version, filter, backup_meta_digest)?;
        let config_hash_hex = hex::encode(&config_hash);
        let mut existing = HashMap::new();
        let meta = self.load_checkpoint_meta(&meta_path)?;
        if let Some(ref meta) = meta {
            if meta.config_hash != config_hash_hex {
                return Err(Error::Invalid(format!(
                    "checkpoint config mismatch for {}, remove the checkpoint directory or use a new output prefix",
                    checkpoint_prefix
                )));
            }
            existing = self.load_checkpoint_entries(&entry_prefix)?;
        }
        if meta.is_none() {
            let meta = CheckpointMeta {
                version: CHECKPOINT_VERSION,
                start_version,
                end_version,
                config_hash: config_hash_hex,
            };
            self.write_checkpoint_meta(&meta_path, &meta)?;
        }
        tikv_util::info!(
            "br parquet checkpoint loaded";
            "prefix" => %checkpoint_prefix,
            "reuse_entries" => existing.len(),
            "start_version" => start_version,
            "end_version" => end_version
        );
        Ok(CheckpointState::new(entry_prefix, existing))
    }

    fn checkpoint_config_hash(
        &self,
        start_version: u64,
        end_version: u64,
        filter: &TableFilter,
        backup_meta_digest: &[u8],
    ) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"br-parquet-checkpoint");
        buf.extend_from_slice(&CHECKPOINT_VERSION.to_le_bytes());
        buf.extend_from_slice(&start_version.to_le_bytes());
        buf.extend_from_slice(&end_version.to_le_bytes());
        buf.extend_from_slice(backup_meta_digest);
        buf.extend_from_slice(&(self.options.row_group_size as u64).to_le_bytes());
        let compression = format!("{:?}", self.options.compression);
        buf.extend_from_slice(compression.as_bytes());
        for rule in &filter.raw_rules {
            buf.extend_from_slice(rule.as_bytes());
            buf.push(0);
        }
        for id in &filter.raw_table_ids {
            buf.extend_from_slice(&id.to_le_bytes());
        }
        sha256_bytes(&buf)
    }

    fn load_checkpoint_meta(&self, meta_path: &str) -> Result<Option<CheckpointMeta>> {
        let mut reader = self.output.read(meta_path);
        let mut buf = Vec::new();
        match self.block_on(reader.read_to_end(&mut buf)) {
            Ok(_) => {
                let meta: CheckpointMeta = serde_json::from_slice(&buf)
                    .map_err(|err| Error::Invalid(format!("invalid checkpoint meta: {}", err)))?;
                Ok(Some(meta))
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(Error::Io(err))
                }
            }
        }
    }

    fn write_checkpoint_meta(&self, meta_path: &str, meta: &CheckpointMeta) -> Result<()> {
        let data = serde_json::to_vec(meta)
            .map_err(|err| Error::Invalid(format!("failed to encode checkpoint meta: {}", err)))?;
        let len = data.len() as u64;
        let reader = UnpinReader(Box::new(Cursor::new(data)));
        self.block_on(self.output.write(meta_path, reader, len))?;
        Ok(())
    }

    fn load_checkpoint_entries(
        &self,
        entry_prefix: &str,
    ) -> Result<HashMap<String, ParquetFileInfo>> {
        let fut = async {
            let mut entries = HashMap::new();
            let mut stream = self.output.iter_prefix(entry_prefix);
            while let Some(item) = stream.try_next().await? {
                let mut reader = self.output.read(&item.key);
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await?;
                let entry: CheckpointEntry = serde_json::from_slice(&buf)
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
                let info = entry
                    .into_parquet()
                    .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
                entries.insert(info.object_name.clone(), info);
            }
            Ok(entries)
        };
        self.block_on(fut).map_err(Error::Io)
    }

    fn persist_checkpoint(
        &self,
        checkpoint: &CheckpointState,
        info: &ParquetFileInfo,
    ) -> Result<()> {
        if !checkpoint.mark_recorded(&info.object_name) {
            return Ok(());
        }
        let entry = CheckpointEntry::from_parquet(info);
        let data = serde_json::to_vec(&entry)
            .map_err(|err| Error::Invalid(format!("failed to encode checkpoint entry: {}", err)))?;
        let len = data.len() as u64;
        let reader = UnpinReader(Box::new(Cursor::new(data)));
        let path = checkpoint.entry_path(&info.object_name)?;
        if let Err(err) = self.block_on(self.output.write(&path, reader, len)) {
            checkpoint.unmark_recorded(&info.object_name);
            return Err(Error::Io(err));
        }
        Ok(())
    }

    fn export_from_meta(
        &self,
        meta: BackupMeta,
        output_prefix: &str,
        filter: &TableFilter,
        backup_meta_digest: &[u8],
    ) -> Result<ExportReport> {
        if meta.get_is_raw_kv() {
            return Err(Error::Invalid(
                "raw kv backups are not supported by the parquet exporter".into(),
            ));
        }
        let start_version = meta.get_start_version();
        let end_version = meta.get_end_version();
        let prefix = if output_prefix.is_empty() {
            DEFAULT_OUTPUT_PREFIX.to_string()
        } else {
            output_prefix.trim_matches('/').to_string()
        };
        let mut tables = Vec::new();
        for schema in meta.get_schemas() {
            if schema.get_table().is_empty() || schema.get_db().is_empty() {
                continue;
            }
            tables.push(Arc::new(TableSchema::from_backup_schema(schema)?));
        }
        if tables.is_empty() {
            return Err(Error::Invalid(
                "backup meta does not contain table schemas".into(),
            ));
        }
        if filter.is_active() {
            tables.retain(|table| filter.matches(table));
            if tables.is_empty() {
                return Err(Error::Invalid("no tables match the export filters".into()));
            }
        }
        let table_map: HashMap<i64, Arc<TableSchema>> = tables
            .iter()
            .map(|table| (table.table_id, Arc::clone(table)))
            .collect();
        let tasks = build_file_tasks(meta.get_files(), &table_map);
        let checkpoint = if self.options.use_checkpoint {
            Some(self.load_checkpoint(
                &prefix,
                start_version,
                end_version,
                filter,
                backup_meta_digest,
            )?)
        } else {
            None
        };
        let mut report = ExportReport {
            start_version,
            end_version,
            ..Default::default()
        };
        if tasks.is_empty() {
            return Ok(report);
        }
        let max_workers = self.options.sst_concurrency.max(1);
        let worker_count = max_workers.min(tasks.len());
        if worker_count == 1 {
            for task in tasks.iter() {
                let task_report = self.export_file_task(task, &prefix, checkpoint.as_ref())?;
                report.total_rows += task_report.total_rows;
                report.files.extend(task_report.files);
            }
        } else {
            let pool = ThreadPoolBuilder::new()
                .num_threads(worker_count)
                .build()
                .map_err(|err| {
                    Error::Invalid(format!("failed to build export thread pool: {}", err))
                })?;
            let task_reports: Result<Vec<FileTaskReport>> = pool.install(|| {
                tasks
                    .par_iter()
                    .map(|task| self.export_file_task(task, &prefix, checkpoint.as_ref()))
                    .collect()
            });
            for task_report in task_reports? {
                report.total_rows += task_report.total_rows;
                report.files.extend(task_report.files);
            }
        }
        report
            .files
            .sort_by(|left, right| left.object_name.cmp(&right.object_name));
        if self.options.write_iceberg_table {
            self.write_iceberg_tables(&prefix, &tables, &report)?;
        }
        Ok(report)
    }

    fn write_iceberg_tables(
        &self,
        output_prefix: &str,
        tables: &[Arc<TableSchema>],
        report: &ExportReport,
    ) -> Result<()> {
        let mut base_uri = self.output.url()?.to_string();
        if let Some(stripped) = base_uri.strip_prefix("local://") {
            base_uri = format!("file://{}", stripped);
        }
        if !base_uri.ends_with('/') {
            base_uri.push('/');
        }
        let parquet_compression = parquet_compression_name(self.options.compression);

        let mut file_map: HashMap<i64, Vec<&ParquetFileInfo>> = HashMap::default();
        for file in &report.files {
            file_map.entry(file.table_id).or_default().push(file);
        }

        for table in tables {
            let Some(files) = file_map.get(&table.table_id) else {
                continue;
            };
            if files.is_empty() {
                continue;
            }
            let table_root = format!(
                "{}/{}/{}",
                output_prefix,
                sanitize_name(&table.db_name),
                sanitize_name(&table.table_name)
            );
            let artifacts = iceberg_table::build_iceberg_table_artifacts(
                &base_uri,
                &table_root,
                table,
                files,
                report.start_version,
                report.end_version,
                parquet_compression,
            )?;
            self.write_bytes(&artifacts.manifest_object, artifacts.manifest_avro)?;
            self.write_bytes(
                &artifacts.manifest_list_object,
                artifacts.manifest_list_avro,
            )?;
            self.write_bytes(&artifacts.metadata_object, artifacts.metadata_json)?;
            self.write_bytes(&artifacts.version_hint_object, artifacts.version_hint)?;
            tikv_util::info!(
                "br parquet wrote iceberg table";
                "db" => %table.db_name,
                "table" => %table.table_name,
                "location" => %table_root
            );
        }
        Ok(())
    }

    fn write_bytes(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        let len = bytes.len() as u64;
        let reader = UnpinReader(Box::new(Cursor::new(bytes)));
        self.block_on(self.output.write(object, reader, len))?;
        Ok(())
    }

    fn export_file_task(
        &self,
        task: &FileTask,
        output_prefix: &str,
        checkpoint: Option<&CheckpointState>,
    ) -> Result<FileTaskReport> {
        let mut files = Vec::new();
        let mut total_rows = 0;
        let mut pending = Vec::new();
        for table in task.tables.iter() {
            let object_name = self.build_object_name(output_prefix, table, &task.file);
            if let Some(checkpoint) = checkpoint {
                if let Some(info) = checkpoint.lookup(&object_name) {
                    total_rows += info.row_count;
                    files.push(info);
                    continue;
                }
            }
            pending.push((Arc::clone(table), object_name));
        }
        if pending.is_empty() {
            return Ok(FileTaskReport { files, total_rows });
        }
        let local = self.download(&task.file)?;
        for (table, object_name) in pending {
            let info = self.export_table_file(&table, &task.file, &local, &object_name)?;
            total_rows += info.row_count;
            if let Some(checkpoint) = checkpoint {
                self.persist_checkpoint(checkpoint, &info)?;
            }
            files.push(info);
        }
        if let Err(err) = fs::remove_file(&local) {
            tikv_util::debug!(
                "br parquet failed to remove temp sst";
                "sst" => task.file.get_name(),
                "error" => %err
            );
        }
        Ok(FileTaskReport { files, total_rows })
    }

    fn download(&self, file: &BackupFile) -> Result<PathBuf> {
        let timer = Instant::now();
        let tmp_path = self.tmp.path().join(&file.name);
        if let Some(parent) = tmp_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut reader = self.input.read(&file.name);
        let mut writer = File::create(&tmp_path)?;
        let limiter = Limiter::new(f64::INFINITY);
        self.block_on(copy_stream_internal(
            &mut reader,
            &mut writer,
            &limiter,
            file.get_size(),
        ))?;
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["download"])
            .observe(timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS.with_label_values(&["downloaded"]).inc();
        tikv_util::info!(
            "br parquet downloaded sst";
            "sst" => file.get_name(),
            "bytes" => file.get_size(),
            "takes" => ?timer.saturating_elapsed()
        );
        Ok(tmp_path)
    }

    fn build_object_name(
        &self,
        output_prefix: &str,
        table: &TableSchema,
        file_meta: &BackupFile,
    ) -> String {
        format!(
            "{}/{}/{}/{}.parquet",
            output_prefix,
            sanitize_name(&table.db_name),
            sanitize_name(&table.table_name),
            sanitize_name(
                Path::new(&file_meta.name)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("part")
            )
        )
    }

    fn export_table_file(
        &self,
        table: &TableSchema,
        file_meta: &BackupFile,
        local_path: &Path,
        object_name: &str,
    ) -> Result<ParquetFileInfo> {
        let cf = file_meta.get_cf();
        let file_timer = Instant::now();
        tikv_util::info!(
            "br parquet exporting sst";
            "table_id" => table.table_id,
            "db" => %table.db_name,
            "table" => %table.table_name,
            "sst" => file_meta.get_name(),
            "cf" => cf
        );
        let convert_timer = Instant::now();
        let tmp = NamedTempFile::new_in(self.tmp.path())?;
        let sink = tmp.reopen()?;
        let mut writer = ParquetWriter::try_new(
            table,
            sink,
            self.options.compression,
            self.options.row_group_size,
            self.options.bloom_filter,
        )?;
        let reader = RocksSstReader::open(
            local_path
                .to_str()
                .ok_or_else(|| Error::Invalid("invalid sst path".into()))?,
            None,
        )?;
        let mut iter = reader.iter(IterOptions::default())?;
        iter.seek_to_first()?;
        let mut ctx = EvalContext::default();
        let column_ids: FxHashSet<i64> = table.column_map.keys().copied().collect();
        let column_infos: Arc<[ColumnInfo]> =
            Arc::from(table.column_map.values().cloned().collect::<Vec<_>>());
        let mut scanned_keys = 0u64;
        let mut matched_keys = 0u64;
        let mut skipped_non_record = 0u64;
        let mut skipped_write = 0u64;
        let mut mismatch_logged = false;
        while iter.valid()? {
            let raw_key = iter.key();
            if raw_key.is_empty() {
                iter.next()?;
                continue;
            }
            scanned_keys += 1;
            let user_key = match decode_user_key(raw_key) {
                Ok(key) => key,
                Err(err) => {
                    tikv_util::warn!(
                        "br parquet skipping key with decode error";
                        "error" => %err,
                        "key" => raw_key.encode_hex::<String>()
                    );
                    iter.next()?;
                    continue;
                }
            };
            let table_id = table::decode_table_id(user_key.as_ref())?;
            if table_id != table.table_id {
                if !mismatch_logged {
                    mismatch_logged = true;
                    tikv_util::debug!(
                        "br parquet skipping key with mismatched table id";
                        "expected_table_id" => table.table_id,
                        "decoded_table_id" => table_id,
                        "key" => user_key.as_ref().encode_hex::<String>()
                    );
                }
                iter.next()?;
                continue;
            }
            if table::check_record_key(user_key.as_ref()).is_err() {
                skipped_non_record += 1;
                iter.next()?;
                continue;
            }
            let row_value = match cf {
                CF_DEFAULT => Some(iter.value()),
                CF_WRITE => {
                    let write = match WriteRef::parse(iter.value()) {
                        Ok(write) => write,
                        Err(err) => {
                            skipped_write += 1;
                            tikv_util::debug!(
                                "br parquet skipping write record with parse error";
                                "error" => %err,
                                "key" => user_key.as_ref().encode_hex::<String>()
                            );
                            iter.next()?;
                            continue;
                        }
                    };
                    if write.write_type != WriteType::Put {
                        skipped_write += 1;
                        iter.next()?;
                        continue;
                    }
                    match write.short_value {
                        Some(value) => Some(value),
                        None => {
                            skipped_write += 1;
                            iter.next()?;
                            continue;
                        }
                    }
                }
                _ => {
                    skipped_write += 1;
                    iter.next()?;
                    continue;
                }
            };
            matched_keys += 1;
            let handle = decode_handle(table, user_key.as_ref())?;
            let mut row = decode_row_for_table(
                row_value.unwrap_or_default(),
                &mut ctx,
                table,
                &column_ids,
                &column_infos,
            )?;
            fill_handle_columns(&mut row, table, &handle, &mut ctx)?;
            let projected = build_row_projection(table, &handle, &row)?;
            writer.write_row(projected)?;
            iter.next()?;
        }
        let total_rows = writer.total_rows;
        writer.close()?;
        let convert_elapsed = convert_timer.saturating_elapsed();
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["convert"])
            .observe(convert_timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS.with_label_values(&["converted"]).inc();
        BR_PARQUET_ROWS.inc_by(total_rows);

        let size = tmp.as_file().metadata()?.len();
        let (mut checksum_reader, checksum_hasher) = file_system::Sha256Reader::new(tmp.reopen()?)
            .map_err(|err| Error::Invalid(format!("failed to create sha256 reader: {}", err)))?;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = checksum_reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
        }
        let checksum = checksum_hasher
            .lock()
            .unwrap()
            .finish()
            .map_err(|err| Error::Invalid(format!("sha256 finish failed: {}", err)))?
            .to_vec();

        let upload = tmp.reopen()?;
        let reader = AllowStdIo::new(upload);
        let upload_timer = Instant::now();
        self.block_on(
            self.output
                .write(object_name, UnpinReader(Box::new(reader)), size),
        )?;
        let upload_elapsed = upload_timer.saturating_elapsed();
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["upload"])
            .observe(upload_timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS.with_label_values(&["uploaded"]).inc();
        BR_PARQUET_OUTPUT_BYTES.inc_by(size);
        let total_elapsed = file_timer.saturating_elapsed();
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["total"])
            .observe(file_timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS.with_label_values(&["completed"]).inc();
        tikv_util::info!(
            "br parquet export finished";
            "table_id" => table.table_id,
            "db" => %table.db_name,
            "table" => %table.table_name,
            "sst" => file_meta.get_name(),
            "rows" => total_rows,
            "bytes" => size,
            "scanned_keys" => scanned_keys,
            "matched_keys" => matched_keys,
            "skipped_non_record" => skipped_non_record,
            "skipped_write" => skipped_write,
            "convert_ms" => convert_elapsed.as_millis(),
            "upload_ms" => upload_elapsed.as_millis(),
            "total_ms" => total_elapsed.as_millis()
        );

        Ok(ParquetFileInfo {
            object_name: object_name.to_string(),
            table_id: table.table_id,
            db_name: table.db_name.clone(),
            table_name: table.table_name.clone(),
            row_count: total_rows,
            size,
            sha256: checksum,
            start_key: file_meta.get_start_key().to_vec(),
            end_key: file_meta.get_end_key().to_vec(),
        })
    }
}

fn build_row_projection(
    table: &TableSchema,
    handle: &HandleValue,
    row: &HashMap<i64, Datum>,
) -> Result<Vec<Option<CellValue>>> {
    table
        .columns
        .iter()
        .map(|column| match &column.kind {
            ColumnKind::TableId => Ok(Some(CellValue::Int64(table.table_id))),
            ColumnKind::Handle => Ok(Some(CellValue::Bytes(handle.to_bytes()))),
            ColumnKind::Physical(id) => {
                if let Some(datum) = row.get(id) {
                    if matches!(datum, Datum::Null) {
                        Ok(None)
                    } else {
                        datum_to_cell(column, datum).map(Some)
                    }
                } else {
                    Ok(None)
                }
            }
        })
        .collect()
}

fn datum_to_cell(column: &ColumnSchema, datum: &Datum) -> Result<CellValue> {
    match column.parquet_type {
        ColumnParquetType::Int64 => match datum {
            Datum::I64(v) => Ok(CellValue::Int64(*v)),
            Datum::U64(v) => {
                let signed = i64::try_from(*v).map_err(|_| {
                    Error::Schema(format!(
                        "column {} expects signed 64-bit integer, but value {} overflows i64",
                        column.name, v
                    ))
                })?;
                Ok(CellValue::Int64(signed))
            }
            _ => Err(Error::Schema(format!(
                "column {} expects integer, got {}",
                column.name, datum
            ))),
        },
        ColumnParquetType::Double => match datum {
            Datum::F64(v) => Ok(CellValue::Double(*v)),
            Datum::I64(v) => Ok(CellValue::Double(*v as f64)),
            Datum::U64(v) => Ok(CellValue::Double(*v as f64)),
            _ => Err(Error::Schema(format!(
                "column {} expects floating point, got {}",
                column.name, datum
            ))),
        },
        ColumnParquetType::Utf8 => Ok(CellValue::Bytes(datum_to_string(datum)?.into_bytes())),
        ColumnParquetType::Binary => Ok(CellValue::Bytes(match datum {
            Datum::Bytes(b) => b.clone(),
            other => format!("{}", other).into_bytes(),
        })),
    }
}

fn datum_to_string(datum: &Datum) -> Result<String> {
    match datum {
        Datum::Bytes(b) => Ok(String::from_utf8_lossy(b).into_owned()),
        Datum::Json(j) => Ok(j.to_string()),
        Datum::Time(t) => Ok(t.to_string()),
        Datum::Dur(d) => Ok(d.to_string()),
        Datum::Dec(d) => Ok(d.to_string()),
        Datum::I64(v) => Ok(v.to_string()),
        Datum::U64(v) => Ok(v.to_string()),
        Datum::Null => Err(Error::Schema("unexpected NULL datum".into())),
        other => Err(Error::Schema(format!("unsupported datum {}", other))),
    }
}

fn build_file_tasks(
    files: &[BackupFile],
    table_map: &HashMap<i64, Arc<TableSchema>>,
) -> Vec<FileTask> {
    let mut tasks = Vec::new();
    for file in files {
        if file.get_cf() != CF_DEFAULT && file.get_cf() != CF_WRITE {
            continue;
        }
        let table_ids = if file.get_table_metas().is_empty() {
            decode_table_ids_from_key(file)
        } else {
            file.get_table_metas()
                .iter()
                .map(|m| m.get_physical_id())
                .collect()
        };
        let mut tables = Vec::new();
        let mut seen = HashSet::new();
        for table_id in table_ids {
            if !seen.insert(table_id) {
                continue;
            }
            if let Some(table) = table_map.get(&table_id) {
                tables.push(Arc::clone(table));
            }
        }
        if tables.is_empty() {
            continue;
        }
        tasks.push(FileTask {
            file: file.clone(),
            tables,
        });
    }
    tasks
}

fn decode_table_ids_from_key(file: &BackupFile) -> Vec<i64> {
    if let Ok(table_id) = table::decode_table_id(file.get_start_key()) {
        vec![table_id]
    } else {
        Vec::new()
    }
}

fn decode_user_key(raw_key: &[u8]) -> Result<Cow<'_, [u8]>> {
    if raw_key.is_empty() {
        return Err(Error::Invalid("empty key".into()));
    }
    let origin_key = if raw_key[0] == DATA_PREFIX {
        keys::origin_key(raw_key)
    } else {
        raw_key
    };
    if origin_key.is_empty() {
        return Err(Error::Invalid("empty key".into()));
    }
    // Fast path for already-raw table keys.
    if table::check_record_key(origin_key).is_ok() || table::check_index_key(origin_key).is_ok() {
        return Ok(Cow::Borrowed(origin_key));
    }
    // Default CF keys are MVCC-encoded (memcomparable + commit ts).
    if let Ok(truncated) = Key::truncate_ts_for(origin_key) {
        if let Ok(raw) = Key::from_encoded_slice(truncated).into_raw() {
            return Ok(Cow::Owned(raw));
        }
    }
    if let Ok(raw) = Key::from_encoded_slice(origin_key).into_raw() {
        return Ok(Cow::Owned(raw));
    }
    Err(Error::Invalid("failed to decode mvcc user key".into()))
}

#[derive(Debug)]
enum HandleValue {
    Int(i64),
    Common(Vec<u8>),
}

impl HandleValue {
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            HandleValue::Int(v) => v.to_string().into_bytes(),
            HandleValue::Common(b) => format!("0x{}", b.encode_hex::<String>()).into_bytes(),
        }
    }
}

fn decode_handle(table: &TableSchema, key: &[u8]) -> Result<HandleValue> {
    if table.is_common_handle {
        let raw = table::decode_common_handle(key)?;
        Ok(HandleValue::Common(raw.to_vec()))
    } else {
        let handle = table::decode_int_handle(key)?;
        Ok(HandleValue::Int(handle))
    }
}

fn unflatten_for_export(
    ctx: &mut EvalContext,
    datum: Datum,
    field_type: &dyn FieldTypeAccessor,
) -> Result<Datum> {
    if matches!(datum, Datum::Null) {
        return Ok(datum);
    }
    let tp = field_type.tp();
    match tp {
        FieldTypeTp::Float => Ok(Datum::F64(f64::from(datum.f64() as f32))),
        FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
            let fsp = field_type.decimal() as i8;
            let t = Time::from_packed_u64(ctx, datum.u64(), tp.try_into()?, fsp)?;
            Ok(Datum::Time(t))
        }
        FieldTypeTp::Duration => {
            let dur = Duration::from_nanos(datum.i64(), field_type.decimal() as i8)?;
            Ok(Datum::Dur(dur))
        }
        FieldTypeTp::Enum | FieldTypeTp::Set | FieldTypeTp::Bit => Ok(datum),
        _ => Ok(datum),
    }
}

fn decode_row_v1_for_export(
    data: &[u8],
    ctx: &mut EvalContext,
    table: &TableSchema,
) -> Result<HashMap<i64, Datum>> {
    let mut slice = data;
    let mut values = datum::decode(&mut slice)?;
    if values.first().is_none_or(|d| *d == Datum::Null) {
        return Ok(HashMap::default());
    }
    if values.len() & 1 == 1 {
        return Err(Error::Invalid(
            "decoded row values' length should be even".into(),
        ));
    }
    let mut row = HashMap::with_capacity(table.column_map.len());
    let mut drain = values.drain(..);
    loop {
        let id = match drain.next() {
            None => return Ok(row),
            Some(id) => id.i64(),
        };
        let v = drain.next().unwrap();
        if let Some(ci) = table.column_map.get(&id) {
            let v = unflatten_for_export(ctx, v, ci)?;
            row.insert(id, v);
        }
    }
}

fn decode_col_value_for_export(
    data: &mut &[u8],
    ctx: &mut EvalContext,
    col: &ColumnInfo,
) -> Result<Datum> {
    let datum = data.read_datum()?;
    unflatten_for_export(ctx, datum, col)
}

fn decode_row_for_table(
    data: &[u8],
    ctx: &mut EvalContext,
    table: &TableSchema,
    column_ids: &FxHashSet<i64>,
    column_infos: &Arc<[ColumnInfo]>,
) -> Result<HashMap<i64, Datum>> {
    if data.is_empty() || (data.len() == 1 && data[0] == datum::NIL_FLAG) {
        return Ok(HashMap::default());
    }
    if data[0] != tidb_query_datatype::codec::row::v2::CODEC_VERSION {
        return decode_row_v1_for_export(data, ctx, table);
    }

    let row = table::cut_row(data.to_vec(), column_ids, Arc::clone(column_infos))?;
    if row.is_empty() {
        return Ok(HashMap::default());
    }
    let mut decoded = HashMap::with_capacity(table.column_map.len());
    for (id, info) in &table.column_map {
        if let Some(cell) = row.get(*id) {
            let mut cell_slice = cell;
            let datum = decode_col_value_for_export(&mut cell_slice, ctx, info)?;
            decoded.insert(*id, datum);
        }
    }
    Ok(decoded)
}

fn fill_handle_columns(
    row: &mut HashMap<i64, Datum>,
    table: &TableSchema,
    handle: &HandleValue,
    ctx: &mut EvalContext,
) -> Result<()> {
    if table.is_common_handle {
        let HandleValue::Common(handle_bytes) = handle else {
            return Ok(());
        };
        if table.primary_key_ids.is_empty() {
            return Ok(());
        }
        let mut buf = handle_bytes.as_slice();
        for pk_id in &table.primary_key_ids {
            if row.contains_key(pk_id) {
                continue;
            }
            if let Some(info) = table.column_map.get(pk_id) {
                if buf.is_empty() {
                    break;
                }
                let datum = decode_col_value_for_export(&mut buf, ctx, info)?;
                row.insert(*pk_id, datum);
            }
        }
        return Ok(());
    }

    if !table.pk_is_handle {
        return Ok(());
    }
    let pk_id = match table.primary_key_ids.as_slice() {
        [id] => *id,
        _ => return Ok(()),
    };
    if row.contains_key(&pk_id) {
        return Ok(());
    }
    let HandleValue::Int(handle_val) = handle else {
        return Ok(());
    };
    let datum = if let Some(info) = table.column_map.get(&pk_id) {
        if info.as_accessor().is_unsigned() {
            Datum::U64(*handle_val as u64)
        } else {
            Datum::I64(*handle_val)
        }
    } else {
        Datum::I64(*handle_val)
    };
    row.insert(pk_id, datum);
    Ok(())
}

fn sanitize_name(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut changed = false;
    for c in input.chars() {
        if c.is_ascii_alphanumeric() || c == '_' {
            output.push(c);
        } else {
            output.push('_');
            changed = true;
        }
    }
    if changed {
        let digest = sha256_bytes(input.as_bytes()).unwrap_or_else(|_| vec![0; 32]);
        output.push_str("__");
        output.push_str(&hex::encode(&digest[..8]));
    }
    output
}

async fn copy_stream_internal(
    reader: &mut (dyn AsyncRead + Unpin + Send),
    writer: &mut File,
    limiter: &Limiter,
    expected_len: u64,
) -> std::io::Result<()> {
    let mut written: u64 = 0;
    let mut buf = vec![0; 64 * 1024];
    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        limiter.consume(n);
        writer.write_all(&buf[..n])?;
        written += n as u64;
    }
    writer.flush()?;
    if expected_len > 0 && written != expected_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "downloaded length mismatch",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use backup::IcebergCatalog;
    use engine_test::kv::{self, KvTestEngine};
    use engine_traits::{
        CF_DEFAULT, CF_WRITE, ExternalSstFileInfo, SstExt, SstWriter, SstWriterBuilder,
    };
    use external_storage::local::LocalStorage;
    use kvproto::brpb::{BackupMeta, File as BackupFile, MetaFile, Schema};
    use lazy_static::lazy_static;
    use parquet::{
        file::reader::{FileReader, SerializedFileReader},
        record::{Field, RowAccessor},
    };
    use tempfile::{NamedTempFile, TempDir};
    use tidb_query_datatype::{codec::Datum, expr::EvalContext};
    use tikv::config::BackupIcebergConfig;
    use tokio::runtime::Runtime;
    use txn_types::{TimeStamp, Write as TxnWrite, WriteType};

    use super::*;

    lazy_static! {
        static ref EXPORT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    }

    #[test]
    fn exports_simple_table_to_parquet() {
        let _lock = EXPORT_TEST_LOCK.lock().unwrap();
        let runtime = Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();
        let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let sst_path = input_dir.path().join("test.sst");
        let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(CF_DEFAULT)
            .build(sst_path.to_str().unwrap())
            .unwrap();
        let mut ctx = EvalContext::default();
        let value =
            table::encode_row(&mut ctx, vec![Datum::I64(42), Datum::Null], &[1, 2]).unwrap();
        let raw_key = table::encode_row_key(1, 1);
        let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(1));
        let key = keys::data_key(encoded_key.as_encoded());
        sst_writer.put(&key, &value).unwrap();
        let sst_info = sst_writer.finish().unwrap();
        fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

        let mut file = BackupFile::default();
        file.set_name("data.sst".into());
        file.set_cf(CF_DEFAULT.to_string());
        file.set_start_key(raw_key.clone());
        file.set_end_key(raw_key);
        file.set_start_version(1);
        file.set_end_version(2);

        let mut schema = Schema::default();
        schema.set_db(r#"{"name":{"O":"test","L":"test"}}"#.as_bytes().to_vec());
        schema.set_table(
            r#"{"id":1,"name":{"O":"t","L":"t"},"cols":[{"id":1,"name":{"O":"c","L":"c"},"tp":3,"flag":0},{"id":2,"name":{"O":"c2","L":"c2"},"tp":3,"flag":0}]}"#
                .as_bytes()
                .to_vec(),
        );

        let mut meta = BackupMeta::default();
        meta.mut_files().push(file);
        meta.mut_schemas().push(schema);
        meta.set_start_version(1);
        meta.set_end_version(2);

        let mut meta_bytes = Vec::new();
        meta.write_to_writer(&mut meta_bytes).unwrap();
        fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

        let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
        let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
        let mut exporter =
            SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default())
                .unwrap();
        let report = exporter.export_backup_meta("parquet").unwrap();
        assert_eq!(report.total_rows, 1);
        assert_eq!(report.files.len(), 1);

        let parquet_path = collect_parquet_file(output_dir.path());
        let reader = SerializedFileReader::new(File::open(parquet_path).unwrap()).expect("parquet");
        let mut row_iter = reader.get_row_iter(None).unwrap();
        let row = row_iter.next().unwrap().unwrap();
        assert_eq!(row.get_long(0).unwrap(), 1);
        assert_eq!(row.get_long(2).unwrap(), 42);
        let mut null_found = false;
        for (name, field) in row.get_column_iter() {
            if name == "c2" {
                assert!(matches!(field, Field::Null));
                null_found = true;
            }
        }
        assert!(null_found);

        let cfg = BackupIcebergConfig {
            enable: true,
            warehouse: "warehouse".into(),
            namespace: "analytics".into(),
            table: "t".into(),
            manifest_prefix: "manifest".into(),
        };
        let catalog = IcebergCatalog::from_config(cfg).unwrap();
        let manifest_files: Vec<_> = report
            .files
            .iter()
            .map(|info| info.to_manifest_file(report.start_version, report.end_version))
            .collect();
        runtime
            .block_on(catalog.write_manifest(
                output.as_ref(),
                &manifest_files,
                TimeStamp::new(report.start_version),
                TimeStamp::new(report.end_version),
            ))
            .unwrap();

        let manifest_path = output_dir.path().join(format!(
            "warehouse/analytics/t/metadata/manifest_{}_0.json",
            report.end_version
        ));
        let raw = fs::read_to_string(manifest_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(json["files"][0]["file_name"], report.files[0].object_name);
        assert_eq!(json["files"][0]["cf"], "parquet");
        assert_eq!(
            json["files"][0]["total_kvs"].as_u64().unwrap(),
            report.files[0].row_count
        );
        assert_eq!(
            json["snapshot_end_ts"].as_u64().unwrap(),
            report.end_version
        );
    }

    #[test]
    fn exports_short_value_from_write_cf() {
        let _lock = EXPORT_TEST_LOCK.lock().unwrap();
        let runtime = Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();
        let engine =
            kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT, CF_WRITE]).unwrap();
        let sst_path = input_dir.path().join("test.sst");
        let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(CF_WRITE)
            .build(sst_path.to_str().unwrap())
            .unwrap();
        let mut ctx = EvalContext::default();
        let value = table::encode_row(&mut ctx, vec![Datum::I64(99)], &[1]).unwrap();
        let raw_key = table::encode_row_key(1, 1);
        let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(2));
        let key = keys::data_key(encoded_key.as_encoded());
        let write = TxnWrite::new(WriteType::Put, TimeStamp::new(1), Some(value));
        sst_writer.put(&key, &write.as_ref().to_bytes()).unwrap();
        let sst_info = sst_writer.finish().unwrap();
        fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

        let mut file = BackupFile::default();
        file.set_name("data.sst".into());
        file.set_cf(CF_WRITE.to_string());
        file.set_start_key(raw_key.clone());
        file.set_end_key(raw_key);
        file.set_start_version(1);
        file.set_end_version(2);

        let mut schema = Schema::default();
        schema.set_db(r#"{"name":{"O":"test","L":"test"}}"#.as_bytes().to_vec());
        schema.set_table(
            r#"{"id":1,"name":{"O":"t","L":"t"},"cols":[{"id":1,"name":{"O":"c","L":"c"},"tp":3,"flag":0}]}"#
                .as_bytes()
                .to_vec(),
        );

        let mut meta = BackupMeta::default();
        meta.mut_files().push(file);
        meta.mut_schemas().push(schema);
        meta.set_start_version(1);
        meta.set_end_version(2);

        let mut meta_bytes = Vec::new();
        meta.write_to_writer(&mut meta_bytes).unwrap();
        fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

        let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
        let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
        let mut exporter =
            SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default())
                .unwrap();
        let report = exporter.export_backup_meta("parquet").unwrap();
        assert_eq!(report.total_rows, 1);
        assert_eq!(report.files.len(), 1);
    }

    #[test]
    fn exports_multiple_ssts_in_parallel() {
        let _lock = EXPORT_TEST_LOCK.lock().unwrap();
        let runtime = Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();
        let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

        let mut ctx = EvalContext::default();
        for (idx, handle) in [(1, 1_i64), (2, 2_i64)] {
            let sst_path = input_dir.path().join(format!("test{}.sst", idx));
            let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
                .set_db(&engine)
                .set_cf(CF_DEFAULT)
                .build(sst_path.to_str().unwrap())
                .unwrap();
            let value = table::encode_row(&mut ctx, vec![Datum::I64(handle)], &[1]).unwrap();
            let raw_key = table::encode_row_key(1, handle);
            let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(idx as u64));
            let key = keys::data_key(encoded_key.as_encoded());
            sst_writer.put(&key, &value).unwrap();
            let sst_info = sst_writer.finish().unwrap();
            let name = format!("data{}.sst", idx);
            fs::copy(sst_info.file_path(), input_dir.path().join(&name)).unwrap();
        }

        let mut file1 = BackupFile::default();
        file1.set_name("data1.sst".into());
        file1.set_cf(CF_DEFAULT.to_string());
        file1.set_start_key(table::encode_row_key(1, 1));
        file1.set_end_key(table::encode_row_key(1, 1));
        file1.set_start_version(1);
        file1.set_end_version(3);

        let mut file2 = BackupFile::default();
        file2.set_name("data2.sst".into());
        file2.set_cf(CF_DEFAULT.to_string());
        file2.set_start_key(table::encode_row_key(1, 2));
        file2.set_end_key(table::encode_row_key(1, 2));
        file2.set_start_version(1);
        file2.set_end_version(3);

        let mut schema = Schema::default();
        schema.set_db(r#"{"name":{"O":"test","L":"test"}}"#.as_bytes().to_vec());
        schema.set_table(
            r#"{"id":1,"name":{"O":"t","L":"t"},"cols":[{"id":1,"name":{"O":"c","L":"c"},"tp":3,"flag":0}]}"#
                .as_bytes()
                .to_vec(),
        );

        let mut meta = BackupMeta::default();
        meta.mut_files().push(file1);
        meta.mut_files().push(file2);
        meta.mut_schemas().push(schema);
        meta.set_start_version(1);
        meta.set_end_version(3);

        let mut meta_bytes = Vec::new();
        meta.write_to_writer(&mut meta_bytes).unwrap();
        fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

        let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
        let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
        let mut options = ExportOptions::default();
        options.sst_concurrency = 2;
        let mut exporter =
            SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
        let report = exporter.export_backup_meta("parquet").unwrap();
        assert_eq!(report.total_rows, 2);
        assert_eq!(report.files.len(), 2);
        assert!(report.files[0].object_name.ends_with("data1.parquet"));
        assert!(report.files[1].object_name.ends_with("data2.parquet"));
    }

    #[test]
    fn exports_backupmeta_v2_index() {
        let _lock = EXPORT_TEST_LOCK.lock().unwrap();
        let runtime = Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();
        let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let sst_path = input_dir.path().join("test.sst");
        let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(CF_DEFAULT)
            .build(sst_path.to_str().unwrap())
            .unwrap();
        let mut ctx = EvalContext::default();
        let value = table::encode_row(&mut ctx, vec![Datum::I64(7)], &[1]).unwrap();
        let raw_key = table::encode_row_key(1, 1);
        let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(1));
        let key = keys::data_key(encoded_key.as_encoded());
        sst_writer.put(&key, &value).unwrap();
        let sst_info = sst_writer.finish().unwrap();
        fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

        let mut file = BackupFile::default();
        file.set_name("data.sst".into());
        file.set_cf(CF_DEFAULT.to_string());
        file.set_start_key(raw_key.clone());
        file.set_end_key(raw_key);
        file.set_start_version(1);
        file.set_end_version(2);

        let mut schema = Schema::default();
        schema.set_db(r#"{"name":{"O":"test","L":"test"}}"#.as_bytes().to_vec());
        schema.set_table(
            r#"{"id":1,"name":{"O":"t","L":"t"},"cols":[{"id":1,"name":{"O":"c","L":"c"},"tp":3,"flag":0}]}"#
                .as_bytes()
                .to_vec(),
        );

        let mut schema_meta = MetaFile::default();
        schema_meta.mut_schemas().push(schema);
        let mut schema_bytes = Vec::new();
        schema_meta.write_to_writer(&mut schema_bytes).unwrap();
        let schema_name = "backupmeta.schema.000000001";
        fs::write(input_dir.path().join(schema_name), &schema_bytes).unwrap();
        let schema_sha = file_system::sha256(&schema_bytes).unwrap();
        let mut schema_ref = BackupFile::default();
        schema_ref.set_name(schema_name.into());
        schema_ref.set_sha256(schema_sha);
        schema_ref.set_size(schema_bytes.len() as u64);

        let mut file_meta = MetaFile::default();
        file_meta.mut_data_files().push(file);
        let mut file_bytes = Vec::new();
        file_meta.write_to_writer(&mut file_bytes).unwrap();
        let file_name = "backupmeta.file.000000001";
        fs::write(input_dir.path().join(file_name), &file_bytes).unwrap();
        let file_sha = file_system::sha256(&file_bytes).unwrap();
        let mut file_ref = BackupFile::default();
        file_ref.set_name(file_name.into());
        file_ref.set_sha256(file_sha);
        file_ref.set_size(file_bytes.len() as u64);

        let mut schema_index = MetaFile::default();
        schema_index.mut_meta_files().push(schema_ref);
        let mut file_index = MetaFile::default();
        file_index.mut_meta_files().push(file_ref);

        let mut meta = BackupMeta::default();
        meta.set_version(1);
        *meta.mut_schema_index() = schema_index;
        *meta.mut_file_index() = file_index;
        meta.set_start_version(1);
        meta.set_end_version(2);

        let mut meta_bytes = Vec::new();
        meta.write_to_writer(&mut meta_bytes).unwrap();
        fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

        let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
        let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
        let mut exporter =
            SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default())
                .unwrap();
        let report = exporter.export_backup_meta("parquet").unwrap();
        assert_eq!(report.total_rows, 1);
        assert_eq!(report.files.len(), 1);
    }

    fn make_table_schema(db: &str, table: &str, id: i64) -> TableSchema {
        let mut schema = Schema::default();
        let db_lower = db.to_lowercase();
        let table_lower = table.to_lowercase();
        schema.set_db(
            format!(r#"{{"name":{{"O":"{}","L":"{}"}}}}"#, db, db_lower)
                .as_bytes()
                .to_vec(),
        );
        schema.set_table(
            format!(
                r#"{{"id":{},"name":{{"O":"{}","L":"{}"}},"cols":[{{"id":1,"name":{{"O":"c","L":"c"}},"tp":3,"flag":0}}]}}"#,
                id, table, table_lower
            )
            .as_bytes()
            .to_vec(),
        );
        TableSchema::from_backup_schema(&schema).unwrap()
    }

    #[test]
    fn table_filter_matches_br_syntax() {
        let table = make_table_schema("Test", "T", 42);

        let filter =
            TableFilter::from_args(&["*.*".to_string(), "!test.t".to_string()], &[]).unwrap();
        assert!(!filter.matches(&table));

        let filter =
            TableFilter::from_args(&["test.*".to_string(), "!test.x".to_string()], &[]).unwrap();
        assert!(filter.matches(&table));

        let filter = TableFilter::from_args(&["/^te.*/./^t$/".to_string()], &[]).unwrap();
        assert!(filter.matches(&table));
    }

    #[test]
    fn table_filter_import_file() {
        let table = make_table_schema("db1", "tbl1", 7);
        let cwd = std::env::current_dir().unwrap();
        let mut file = NamedTempFile::new_in(&cwd).unwrap();
        writeln!(file, "db?.tbl?").unwrap();
        writeln!(file, "!db1.tbl1").unwrap();
        let rel_path = file.path().strip_prefix(&cwd).unwrap();
        let rule = format!("@{}", rel_path.display());
        let filter = TableFilter::from_args(&[rule], &[]).unwrap();
        assert!(!filter.matches(&table));
    }

    #[test]
    fn sanitize_name_appends_hash_on_change() {
        let dashed = sanitize_name("table-name");
        let underscored = sanitize_name("table_name");
        assert!(dashed.starts_with("table_name__"));
        assert_ne!(dashed, "table_name");
        assert_eq!(underscored, "table_name");
        assert_ne!(dashed, underscored);
    }

    #[test]
    fn table_filter_respects_table_ids() {
        let table = make_table_schema("Test", "T", 42);
        let other = make_table_schema("Test", "Other", 43);

        let filter = TableFilter::from_args(&["# comment".to_string()], &[]).unwrap();
        assert!(!filter.matches(&table));

        let filter = TableFilter::from_args(&["# comment".to_string()], &[42]).unwrap();
        assert!(filter.matches(&table));
        assert!(!filter.matches(&other));
    }

    fn collect_parquet_file(root: &Path) -> PathBuf {
        let mut stack = vec![root.to_path_buf()];
        while let Some(path) = stack.pop() {
            for entry in fs::read_dir(path).unwrap() {
                let entry = entry.unwrap();
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        return path;
                    }
                }
            }
        }
        panic!("parquet file not found")
    }
}
