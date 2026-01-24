// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::OsStr,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};

use br_parquet_exporter::{ExportOptions, SstParquetExporter, TableFilter};
use engine_test::kv::{self, KvTestEngine};
use engine_traits::{CF_DEFAULT, ExternalSstFileInfo, SstExt, SstWriter, SstWriterBuilder};
use external_storage::local::LocalStorage;
use kvproto::brpb::{BackupMeta, File as BackupFile, Schema, TableMeta};
use lazy_static::lazy_static;
use parquet::file::reader::{FileReader, SerializedFileReader};
use protobuf::Message;
use tempfile::TempDir;
use tidb_query_datatype::{
    codec::{datum::Datum, table},
    expr::EvalContext,
};
use txn_types::{Key, TimeStamp};
use tokio::runtime::Runtime;

lazy_static! {
    static ref EXPORT_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}

#[test]
fn integration_row_group_size() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine =
        kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build("row_group")
        .unwrap();
    let mut ctx = EvalContext::default();
    let mut raw_keys = Vec::new();
    for (ts, handle) in [(1_u64, 1_i64), (2_u64, 2_i64)] {
        let value = table::encode_row(&mut ctx, vec![Datum::I64(handle)], &[1]).unwrap();
        let raw_key = table::encode_row_key(1, handle);
        raw_keys.push(raw_key.clone());
        let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(ts));
        let key = keys::data_key(encoded_key.as_encoded());
        sst_writer.put(&key, &value).unwrap();
    }
    let sst_info = sst_writer.finish().unwrap();
    fs::copy(sst_info.file_path(), input_dir.path().join("data.sst")).unwrap();

    let mut file = BackupFile::default();
    file.set_name("data.sst".into());
    file.set_cf(CF_DEFAULT.to_string());
    file.set_start_key(raw_keys[0].clone());
    file.set_end_key(raw_keys[1].clone());
    file.set_start_version(1);
    file.set_end_version(3);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(1);
    meta.set_end_version(3);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut options = ExportOptions::default();
    options.row_group_size = 1;
    let mut exporter =
        SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
    let report = exporter
        .export_backup_meta("backupmeta", "parquet")
        .unwrap();
    assert_eq!(report.total_rows, 2);
    assert_eq!(report.files.len(), 1);

    let parquet_files = collect_parquet_files(output_dir.path());
    assert_eq!(parquet_files.len(), 1);
    let reader =
        SerializedFileReader::new(File::open(&parquet_files[0]).unwrap()).unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 2);
    for idx in 0..metadata.num_row_groups() {
        assert_eq!(metadata.row_group(idx).num_rows(), 1);
    }
}

#[test]
fn integration_table_filter() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine =
        kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build("multi_table")
        .unwrap();
    let mut ctx = EvalContext::default();
    let raw_key_1 = table::encode_row_key(1, 1);
    let value_1 = table::encode_row(&mut ctx, vec![Datum::I64(10)], &[1]).unwrap();
    let encoded_key_1 = Key::from_raw(&raw_key_1).append_ts(TimeStamp::new(1));
    let key_1 = keys::data_key(encoded_key_1.as_encoded());
    sst_writer.put(&key_1, &value_1).unwrap();

    let raw_key_2 = table::encode_row_key(2, 1);
    let value_2 = table::encode_row(&mut ctx, vec![Datum::I64(20)], &[1]).unwrap();
    let encoded_key_2 = Key::from_raw(&raw_key_2).append_ts(TimeStamp::new(1));
    let key_2 = keys::data_key(encoded_key_2.as_encoded());
    sst_writer.put(&key_2, &value_2).unwrap();
    let sst_info = sst_writer.finish().unwrap();
    fs::copy(sst_info.file_path(), input_dir.path().join("multi.sst")).unwrap();

    let mut file = BackupFile::default();
    file.set_name("multi.sst".into());
    file.set_cf(CF_DEFAULT.to_string());
    file.set_start_key(raw_key_1.clone());
    file.set_end_key(raw_key_2.clone());
    file.set_start_version(1);
    file.set_end_version(2);
    let mut table_meta_1 = TableMeta::default();
    table_meta_1.set_physical_id(1);
    let mut table_meta_2 = TableMeta::default();
    table_meta_2.set_physical_id(2);
    file.mut_table_metas().push(table_meta_1);
    file.mut_table_metas().push(table_meta_2);

    let schema_1 = make_schema("test", "t1", 1);
    let schema_2 = make_schema("test", "t2", 2);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema_1);
    meta.mut_schemas().push(schema_2);
    meta.set_start_version(1);
    meta.set_end_version(2);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let filter = TableFilter::from_args(&["test.t1".to_string()], &[]).unwrap();
    let mut exporter =
        SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default())
            .unwrap();
    let report = exporter
        .export_backup_meta_with_filter("backupmeta", "parquet", &filter)
        .unwrap();
    assert_eq!(report.total_rows, 1);
    assert_eq!(report.files.len(), 1);
    assert!(report.files[0].object_name.contains("/test/t1/"));

    let parquet_files = collect_parquet_files(output_dir.path());
    assert_eq!(parquet_files.len(), 1);
    let path = parquet_files[0].to_string_lossy();
    assert!(path.contains("/test/t1/"));
    assert!(!path.contains("/test/t2/"));
}

fn make_schema(db: &str, table: &str, id: i64) -> Schema {
    let mut schema = Schema::default();
    let db_lower = db.to_ascii_lowercase();
    let table_lower = table.to_ascii_lowercase();
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
    schema
}

fn collect_parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension() == Some(OsStr::new("parquet")) {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}
