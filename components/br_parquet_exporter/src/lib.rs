// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use engine_rocks::RocksSstReader;
use engine_traits::{CF_DEFAULT, IterOptions, Iterator, RefIterable, SstReader};
use external_storage::{ExternalStorage, UnpinReader};
use futures::{AsyncRead, AsyncReadExt, executor::block_on, io::AllowStdIo};
use hex::ToHex;
use keys::{self, DATA_PREFIX};
use kvproto::brpb::{BackupMeta, File as BackupFile, MetaFile, Schema as BackupSchema};
use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
pub use parquet::basic::Compression;
use protobuf::Message;
use sha2::{Digest, Sha256};
use tempfile::{NamedTempFile, TempDir};
use thiserror::Error;
use tidb_query_datatype::{
    codec::{datum::Datum, table},
    expr::EvalContext,
};
use tikv_util::table_filter::TableFilter as NameFilter;
use tikv_util::time::{Instant, Limiter};

mod metrics;
mod schema;
mod writer;

pub use crate::metrics::ExporterMetricsSnapshot;
use crate::metrics::{
    BR_PARQUET_OUTPUT_BYTES, BR_PARQUET_ROWS, BR_PARQUET_STAGE_DURATION, BR_PARQUET_SSTS,
};
use schema::ColumnKind;
pub use schema::{ColumnParquetType, ColumnSchema, TableSchema};
use writer::{CellValue, ParquetWriter};

pub fn exporter_metrics_snapshot() -> ExporterMetricsSnapshot {
    metrics::snapshot()
}

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

const DEFAULT_ROW_GROUP_SIZE: usize = 8192;
const DEFAULT_OUTPUT_PREFIX: &str = "parquet";
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

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ExportOptions {
    pub row_group_size: usize,
    pub compression: Compression,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compression: Compression::SNAPPY,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TableFilter {
    table_ids: HashSet<i64>,
    rules: Option<NameFilter>,
}

impl TableFilter {
    pub fn from_args(filters: &[String], table_ids: &[i64]) -> Result<Self> {
        let mut ids = HashSet::default();
        for id in table_ids {
            ids.insert(*id);
        }
        let rules = if filters.is_empty() {
            None
        } else {
            let parsed = NameFilter::parse(filters)
                .map_err(|err| Error::Invalid(err.to_string()))?;
            Some(
                parsed
                    .case_insensitive()
                    .map_err(|err| Error::Invalid(err.to_string()))?,
            )
        };
        Ok(TableFilter {
            table_ids: ids,
            rules,
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

#[derive(Clone, Debug)]
pub struct ExportReport {
    pub files: Vec<ParquetFileInfo>,
    pub total_rows: u64,
    pub start_version: u64,
    pub end_version: u64,
}

impl Default for ExportReport {
    fn default() -> Self {
        Self {
            files: Vec::new(),
            total_rows: 0,
            start_version: 0,
            end_version: 0,
        }
    }
}

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

pub struct SstParquetExporter<'a> {
    input: &'a dyn ExternalStorage,
    output: &'a dyn ExternalStorage,
    tmp: TempDir,
    options: ExportOptions,
}

impl<'a> SstParquetExporter<'a> {
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
        })
    }

    pub fn export_backup_meta(
        &mut self,
        meta_path: &str,
        output_prefix: &str,
    ) -> Result<ExportReport> {
        self.export_backup_meta_with_filter(meta_path, output_prefix, &TableFilter::default())
    }

    pub fn export_backup_meta_with_filter(
        &mut self,
        meta_path: &str,
        output_prefix: &str,
        filter: &TableFilter,
    ) -> Result<ExportReport> {
        let meta = self.load_backup_meta(meta_path)?;
        let meta = self.expand_backup_meta(meta)?;
        self.export_from_meta(meta, output_prefix, filter)
    }

    fn load_backup_meta(&self, meta_path: &str) -> Result<BackupMeta> {
        let mut reader = self.input.read(meta_path);
        let mut buf = Vec::new();
        block_on(reader.read_to_end(&mut buf))?;
        let mut meta = BackupMeta::default();
        meta.merge_from_bytes(&buf)?;
        Ok(meta)
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
            let mut hasher = Sha256::new();
            hasher.update(&buf);
            let digest = hasher.finalize();
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

    fn export_from_meta(
        &mut self,
        meta: BackupMeta,
        output_prefix: &str,
        filter: &TableFilter,
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
            tables.push(TableSchema::from_backup_schema(schema)?);
        }
        if tables.is_empty() {
            return Err(Error::Invalid(
                "backup meta does not contain table schemas".into(),
            ));
        }
        if filter.is_active() {
            tables.retain(|table| filter.matches(table));
            if tables.is_empty() {
                return Err(Error::Invalid(
                    "no tables match the export filters".into(),
                ));
            }
        }
        let file_map = group_files_by_table(meta.get_files());
        let mut report = ExportReport {
            start_version,
            end_version,
            ..Default::default()
        };
        for table in tables.iter() {
            if let Some(files) = file_map.get(&table.table_id) {
                for file in files {
                    let local = self.download(file)?;
                    let info = self.export_table_file(table, file, &local, &prefix)?;
                    report.total_rows += info.row_count;
                    report.files.push(info);
                }
            }
        }
        Ok(report)
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
        copy_stream(&mut reader, &mut writer, &limiter, file.get_size())?;
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["download"])
            .observe(timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS
            .with_label_values(&["downloaded"])
            .inc();
        tikv_util::info!(
            "br parquet downloaded sst";
            "sst" => file.get_name(),
            "bytes" => file.get_size(),
            "takes" => ?timer.saturating_elapsed()
        );
        Ok(tmp_path)
    }

    fn export_table_file(
        &mut self,
        table: &TableSchema,
        file_meta: &BackupFile,
        local_path: &Path,
        output_prefix: &str,
    ) -> Result<ParquetFileInfo> {
        let file_timer = Instant::now();
        tikv_util::info!(
            "br parquet exporting sst";
            "table_id" => table.table_id,
            "db" => %table.db_name,
            "table" => %table.table_name,
            "sst" => file_meta.get_name()
        );
        let convert_timer = Instant::now();
        let tmp = NamedTempFile::new_in(self.tmp.path())?;
        let sink = tmp.reopen()?;
        let mut writer = ParquetWriter::try_new(
            table,
            sink,
            self.options.compression,
            self.options.row_group_size,
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
        while iter.valid()? {
            let raw_key = iter.key();
            if raw_key.is_empty() || raw_key[0] != DATA_PREFIX {
                iter.next()?;
                continue;
            }
            let origin_key = keys::origin_key(raw_key);
            let table_id = table::decode_table_id(origin_key)?;
            if table_id != table.table_id {
                iter.next()?;
                continue;
            }
            let handle = decode_handle(table, origin_key)?;
            let mut row_slice = iter.value();
            let fx_row = table::decode_row(&mut row_slice, &mut ctx, &table.column_map)?;
            let row: HashMap<_, _> = fx_row.into_iter().collect();
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
        BR_PARQUET_SSTS
            .with_label_values(&["converted"])
            .inc();
        BR_PARQUET_ROWS.inc_by(total_rows);

        let object_name = format!(
            "{}/{}/{}/{}.parquet",
            output_prefix,
            sanitize_name(&table.db_name),
            sanitize_name(&table.table_name),
            sanitize_name(
                &Path::new(&file_meta.name)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("part")
            )
        );
        let mut upload = tmp.reopen()?;
        let mut hasher = Sha256::new();
        let mut size = 0u64;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = upload.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
            size += n as u64;
        }
        upload.seek(SeekFrom::Start(0))?;
        let checksum = hasher.finalize().to_vec();
        let reader = AllowStdIo::new(upload);
        let upload_timer = Instant::now();
        block_on(
            self.output
                .write(&object_name, UnpinReader(Box::new(reader)), size),
        )?;
        let upload_elapsed = upload_timer.saturating_elapsed();
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["upload"])
            .observe(upload_timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS
            .with_label_values(&["uploaded"])
            .inc();
        BR_PARQUET_OUTPUT_BYTES.inc_by(size);
        let total_elapsed = file_timer.saturating_elapsed();
        BR_PARQUET_STAGE_DURATION
            .with_label_values(&["total"])
            .observe(file_timer.saturating_elapsed_secs());
        BR_PARQUET_SSTS
            .with_label_values(&["completed"])
            .inc();
        tikv_util::info!(
            "br parquet export finished";
            "table_id" => table.table_id,
            "db" => %table.db_name,
            "table" => %table.table_name,
            "sst" => file_meta.get_name(),
            "rows" => total_rows,
            "bytes" => size,
            "convert_ms" => convert_elapsed.as_millis(),
            "upload_ms" => upload_elapsed.as_millis(),
            "total_ms" => total_elapsed.as_millis()
        );

        Ok(ParquetFileInfo {
            object_name,
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

fn group_files_by_table(files: &[BackupFile]) -> HashMap<i64, Vec<BackupFile>> {
    let mut map: HashMap<i64, Vec<BackupFile>> = HashMap::new();
    for file in files {
        if file.get_cf() != CF_DEFAULT {
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
        for table_id in table_ids {
            map.entry(table_id).or_default().push(file.clone());
        }
    }
    map
}

fn decode_table_ids_from_key(file: &BackupFile) -> Vec<i64> {
    if let Ok(table_id) = table::decode_table_id(file.get_start_key()) {
        vec![table_id]
    } else {
        Vec::new()
    }
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

fn sanitize_name(input: &str) -> String {
    input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
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

fn copy_stream(
    reader: &mut (dyn AsyncRead + Unpin + Send),
    writer: &mut File,
    limiter: &Limiter,
    expected_len: u64,
) -> std::io::Result<()> {
    block_on(copy_stream_internal(reader, writer, limiter, expected_len))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::Write,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use engine_test::kv::{self, KvTestEngine};
    use engine_traits::{
        CF_DEFAULT, ExternalSstFileInfo, SstExt, SstWriter, SstWriterBuilder,
    };
    use external_storage::local::LocalStorage;
    use kvproto::brpb::{BackupMeta, File as BackupFile, MetaFile, Schema};
    use parquet::{
        file::reader::{FileReader, SerializedFileReader},
        record::{Field, RowAccessor},
    };
    use lazy_static::lazy_static;
    use tempfile::{NamedTempFile, TempDir};
    use tidb_query_datatype::{codec::Datum, expr::EvalContext};
    use tokio::runtime::Runtime;

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
        let engine =
            kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(CF_DEFAULT)
            .build("test")
            .unwrap();
        let mut ctx = EvalContext::default();
        let value = table::encode_row(&mut ctx, vec![Datum::I64(42), Datum::Null], &[1, 2])
            .unwrap();
        let key = keys::data_key(&table::encode_row_key(1, 1));
        sst_writer.put(&key, &value).unwrap();
        let sst_info = sst_writer.finish().unwrap();
        fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

        let mut file = BackupFile::default();
        file.set_name("data.sst".into());
        file.set_cf(CF_DEFAULT.to_string());
        file.set_start_key(table::encode_row_key(1, 1));
        file.set_end_key(table::encode_row_key(1, 1));
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
        let report = exporter
            .export_backup_meta("backupmeta", "parquet")
            .unwrap();
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
    }

    #[test]
    fn exports_backupmeta_v2_index() {
        let _lock = EXPORT_TEST_LOCK.lock().unwrap();
        let runtime = Runtime::new().unwrap();
        let _runtime_guard = runtime.enter();
        let input_dir = TempDir::new().unwrap();
        let output_dir = TempDir::new().unwrap();
        let engine =
            kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();
        let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
            .set_db(&engine)
            .set_cf(CF_DEFAULT)
            .build("test")
            .unwrap();
        let mut ctx = EvalContext::default();
        let value = table::encode_row(&mut ctx, vec![Datum::I64(7)], &[1]).unwrap();
        let key = keys::data_key(&table::encode_row_key(1, 1));
        sst_writer.put(&key, &value).unwrap();
        let sst_info = sst_writer.finish().unwrap();
        fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

        let mut file = BackupFile::default();
        file.set_name("data.sst".into());
        file.set_cf(CF_DEFAULT.to_string());
        file.set_start_key(table::encode_row_key(1, 1));
        file.set_end_key(table::encode_row_key(1, 1));
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
        let schema_sha = Sha256::digest(&schema_bytes).to_vec();
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
        let file_sha = Sha256::digest(&file_bytes).to_vec();
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
        let report = exporter
            .export_backup_meta("backupmeta", "parquet")
            .unwrap();
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

        let filter =
            TableFilter::from_args(&["/^te.*/./^t$/".to_string()], &[]).unwrap();
        assert!(filter.matches(&table));
    }

    #[test]
    fn table_filter_import_file() {
        let table = make_table_schema("db1", "tbl1", 7);
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "db?.tbl?").unwrap();
        writeln!(file, "!db1.tbl1").unwrap();
        let rule = format!("@{}", file.path().display());
        let filter = TableFilter::from_args(&[rule], &[]).unwrap();
        assert!(!filter.matches(&table));
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
