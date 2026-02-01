// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use apache_avro::{Reader as AvroReader, types::Value as AvroValue};
use br_parquet_exporter::{ExportOptions, SstParquetExporter, TableFilter};
use engine_test::kv::{self, KvTestEngine};
use engine_traits::{CF_DEFAULT, ExternalSstFileInfo, SstExt, SstWriter, SstWriterBuilder};
use external_storage::local::LocalStorage;
use kvproto::brpb::{BackupMeta, File as BackupFile, Schema, TableMeta};
use lazy_static::lazy_static;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use protobuf::Message;
use tempfile::TempDir;
use tidb_query_datatype::{
    codec::{datum::Datum, table},
    expr::EvalContext,
};
use tokio::runtime::Runtime;
use txn_types::{Key, TimeStamp};

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
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("row_group.sst");
    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path.to_str().unwrap())
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
    file.set_start_version(0);
    file.set_end_version(3);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(0);
    meta.set_end_version(3);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut options = ExportOptions::default();
    options.row_group_size = 1;
    let mut exporter = SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
    let report = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(report.total_rows, 2);
    assert_eq!(report.files.len(), 1);

    let parquet_files = collect_parquet_files(output_dir.path());
    assert_eq!(parquet_files.len(), 1);
    let reader = SerializedFileReader::new(File::open(&parquet_files[0]).unwrap()).unwrap();
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 2);
    for idx in 0..metadata.num_row_groups() {
        assert_eq!(metadata.row_group(idx).num_rows(), 1);
    }
}

#[test]
fn integration_bloom_filter() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("bloom.sst");
    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path.to_str().unwrap())
        .unwrap();
    let mut ctx = EvalContext::default();
    let mut raw_keys = Vec::new();
    for handle in [1_i64, 2_i64] {
        let value = table::encode_row(&mut ctx, vec![Datum::I64(handle)], &[1]).unwrap();
        let raw_key = table::encode_row_key(1, handle);
        raw_keys.push(raw_key.clone());
        let encoded_key = Key::from_raw(&raw_key).append_ts(TimeStamp::new(1));
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
    file.set_start_version(0);
    file.set_end_version(2);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(0);
    meta.set_end_version(2);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut options = ExportOptions::default();
    options.bloom_filter = true;
    let mut exporter = SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
    let report = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(report.total_rows, 2);
    assert_eq!(report.files.len(), 1);

    let parquet_files = collect_parquet_files(output_dir.path());
    assert_eq!(parquet_files.len(), 1);
    let reader = SerializedFileReader::new(File::open(&parquet_files[0]).unwrap()).unwrap();
    let metadata = reader.metadata();
    let row_group = metadata.row_group(0);
    assert!(row_group.column(0).bloom_filter_offset().is_none());
    assert!(row_group.column(1).bloom_filter_offset().is_some());
    assert!(row_group.column(2).bloom_filter_offset().is_some());
}

#[test]
fn integration_table_filter() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("multi_table.sst");
    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path.to_str().unwrap())
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
    file.set_start_version(0);
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
    meta.set_start_version(0);
    meta.set_end_version(2);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let filter = TableFilter::from_args(&["test.t1".to_string()], &[]).unwrap();
    let mut exporter =
        SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default()).unwrap();
    let report = exporter
        .export_backup_meta_with_filter("parquet", &filter)
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

#[test]
fn integration_checkpoint_resume() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("checkpoint.sst");
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
    file.set_start_version(0);
    file.set_end_version(2);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(0);
    meta.set_end_version(2);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut exporter =
        SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default()).unwrap();
    let first = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(first.total_rows, 1);
    assert_eq!(first.files.len(), 1);

    fs::remove_file(input_dir.path().join("data.sst")).unwrap();
    let mut exporter =
        SstParquetExporter::new(input.as_ref(), output.as_ref(), ExportOptions::default()).unwrap();
    let second = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(second.total_rows, 1);
    assert_eq!(second.files.len(), 1);
    assert_eq!(second.files[0].object_name, first.files[0].object_name);
}

#[test]
fn integration_iceberg_table_output() {
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("iceberg.sst");
    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path.to_str().unwrap())
        .unwrap();
    let mut ctx = EvalContext::default();
    let raw_key = table::encode_row_key(1, 1);
    let value = table::encode_row(&mut ctx, vec![Datum::I64(7)], &[1]).unwrap();
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
    file.set_start_version(0);
    file.set_end_version(3);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(0);
    meta.set_end_version(3);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut options = ExportOptions::default();
    options.write_iceberg_table = true;
    let mut exporter = SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
    let report = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(report.total_rows, 1);
    assert_eq!(report.files.len(), 1);

    let metadata_dir = output_dir.path().join("parquet/test/t/metadata");
    assert!(metadata_dir.join("version-hint.text").exists());
    assert!(metadata_dir.join("v1.metadata.json").exists());
    assert!(
        metadata_dir
            .join(format!("manifest-{}.avro", report.end_version))
            .exists()
    );
    assert!(
        metadata_dir
            .join(format!("snap-{}.avro", report.end_version))
            .exists()
    );

    let raw = fs::read(metadata_dir.join("v1.metadata.json")).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(json["format-version"], 2);
    assert_eq!(json["current-snapshot-id"], report.end_version);
    let expected_location = format!(
        "file://{}",
        output_dir.path().join("parquet/test/t").display()
    );
    assert_eq!(json["location"], expected_location);

    let snapshots = json["snapshots"].as_array().unwrap();
    let snapshot = snapshots
        .iter()
        .find(|snap| snap["snapshot-id"] == report.end_version)
        .unwrap();
    let manifest_list_uri = snapshot["manifest-list"].as_str().unwrap();
    let manifest_list_path = file_uri_to_path(manifest_list_uri);
    let manifest_list_entry = read_first_avro_record(&manifest_list_path);
    let manifest_uri = avro_get_path(&manifest_list_entry, &["manifest_path"])
        .and_then(avro_as_string)
        .unwrap();

    let manifest_path = file_uri_to_path(manifest_uri);
    let manifest_entry = read_first_avro_record(&manifest_path);
    let parquet_file_uri = avro_get_path(&manifest_entry, &["data_file", "file_path"])
        .and_then(avro_as_string)
        .unwrap();
    let record_count = avro_get_path(&manifest_entry, &["data_file", "record_count"])
        .and_then(avro_as_long)
        .unwrap();
    assert_eq!(record_count, 1);

    let value_counts = avro_get_path(&manifest_entry, &["data_file", "value_counts"])
        .and_then(avro_optional_map)
        .unwrap();
    assert_eq!(value_counts.get("1").and_then(avro_as_long).unwrap(), 1);
    assert_eq!(value_counts.get("2").and_then(avro_as_long).unwrap(), 1);
    assert_eq!(value_counts.get("1001").and_then(avro_as_long).unwrap(), 1);

    let null_value_counts = avro_get_path(&manifest_entry, &["data_file", "null_value_counts"])
        .and_then(avro_optional_map)
        .unwrap();
    assert_eq!(
        null_value_counts.get("1").and_then(avro_as_long).unwrap(),
        0
    );
    assert_eq!(
        null_value_counts.get("2").and_then(avro_as_long).unwrap(),
        0
    );
    assert_eq!(
        null_value_counts
            .get("1001")
            .and_then(avro_as_long)
            .unwrap(),
        0
    );

    let lower_bounds = avro_get_path(&manifest_entry, &["data_file", "lower_bounds"])
        .and_then(avro_optional_map)
        .unwrap();
    assert_eq!(
        lower_bounds.get("1").and_then(avro_as_bytes).unwrap(),
        1i64.to_be_bytes().as_slice()
    );
    assert_eq!(lower_bounds.get("2").and_then(avro_as_bytes).unwrap(), b"1");
    assert_eq!(
        lower_bounds.get("1001").and_then(avro_as_bytes).unwrap(),
        7i64.to_be_bytes().as_slice()
    );

    let upper_bounds = avro_get_path(&manifest_entry, &["data_file", "upper_bounds"])
        .and_then(avro_optional_map)
        .unwrap();
    assert_eq!(
        upper_bounds.get("1").and_then(avro_as_bytes).unwrap(),
        1i64.to_be_bytes().as_slice()
    );
    assert_eq!(upper_bounds.get("2").and_then(avro_as_bytes).unwrap(), b"1");
    assert_eq!(
        upper_bounds.get("1001").and_then(avro_as_bytes).unwrap(),
        7i64.to_be_bytes().as_slice()
    );

    let parquet_path = file_uri_to_path(parquet_file_uri);
    assert!(parquet_path.exists());
    assert!(parquet_file_uri.ends_with(&report.files[0].object_name));
    let reader = SerializedFileReader::new(File::open(parquet_path).unwrap()).expect("parquet");
    let mut row_iter = reader.get_row_iter(None).unwrap();
    let row = row_iter.next().unwrap().unwrap();
    assert_eq!(row.get_long(0).unwrap(), 1);
    assert_eq!(row.get_string(1).unwrap(), "1");
    assert_eq!(row.get_long(2).unwrap(), 7);
}

#[test]
#[ignore]
fn integration_spark_sql_readback() {
    // Spark + Iceberg readback (manual / CI opt-in):
    //
    // - Provide `spark-sql` via `SPARK_SQL=/path/to/spark-sql` or
    //   `SPARK_HOME=/path/to/spark`.
    // - Provide Iceberg Spark runtime via:
    //   - `ICEBERG_SPARK_RUNTIME_JAR=/path/to/
    //     iceberg-spark-runtime-<spark>_<scala>-<ver>.jar` (preferred), or
    //   - `ICEBERG_SPARK_PACKAGES=org.apache.iceberg:
    //     iceberg-spark-runtime-<spark>_<scala>:<ver>`
    //
    // Example:
    //   SPARK_SQL=$(command -v spark-sql) \\
    //   ICEBERG_SPARK_RUNTIME_JAR=/path/to/iceberg-spark-runtime.jar \\
    //   cargo test -p br_parquet_exporter --test export -- --ignored
    // integration_spark_sql_readback --nocapture
    let _lock = EXPORT_TEST_LOCK.lock().unwrap();
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();
    let input_dir = TempDir::new().unwrap();
    let output_dir = TempDir::new().unwrap();
    let engine = kv::new_engine(input_dir.path().to_str().unwrap(), &[CF_DEFAULT]).unwrap();

    let sst_path = input_dir.path().join("spark_readback.sst");
    let mut sst_writer = <KvTestEngine as SstExt>::SstWriterBuilder::new()
        .set_db(&engine)
        .set_cf(CF_DEFAULT)
        .build(sst_path.to_str().unwrap())
        .unwrap();
    let mut ctx = EvalContext::default();
    let raw_key = table::encode_row_key(1, 1);
    let value = table::encode_row(&mut ctx, vec![Datum::I64(7)], &[1]).unwrap();
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
    file.set_start_version(0);
    file.set_end_version(2);

    let schema = make_schema("test", "t", 1);
    let mut meta = BackupMeta::default();
    meta.mut_files().push(file);
    meta.mut_schemas().push(schema);
    meta.set_start_version(0);
    meta.set_end_version(2);

    let mut meta_bytes = Vec::new();
    meta.write_to_writer(&mut meta_bytes).unwrap();
    fs::write(input_dir.path().join("backupmeta"), meta_bytes).unwrap();

    let input = Arc::new(LocalStorage::new(input_dir.path()).unwrap());
    let output = Arc::new(LocalStorage::new(output_dir.path()).unwrap());
    let mut options = ExportOptions::default();
    options.write_iceberg_table = true;
    let mut exporter = SstParquetExporter::new(input.as_ref(), output.as_ref(), options).unwrap();
    let report = exporter.export_backup_meta("parquet").unwrap();
    assert_eq!(report.total_rows, 1);
    assert_eq!(report.files.len(), 1);

    let spark_sql = env::var("SPARK_SQL").ok().or_else(|| {
        env::var("SPARK_HOME")
            .ok()
            .map(|home| format!("{}/bin/spark-sql", home))
    });
    let Some(spark_sql) = spark_sql else {
        eprintln!("skipping spark-sql readback: set SPARK_SQL or SPARK_HOME");
        return;
    };

    let iceberg_pkg = env::var("ICEBERG_SPARK_PACKAGES").ok();
    let iceberg_jar = env::var("ICEBERG_SPARK_RUNTIME_JAR").ok();
    if iceberg_pkg.is_none() && iceberg_jar.is_none() {
        eprintln!(
            "skipping spark-sql readback: set ICEBERG_SPARK_RUNTIME_JAR (preferred) or ICEBERG_SPARK_PACKAGES"
        );
        return;
    }

    let warehouse = output_dir.path().join("parquet");
    let warehouse_uri = format!("file://{}", warehouse.display());
    let query =
        "SELECT concat('ROW=', _tidb_handle, ':', cast(c as string)) AS out FROM local.test.t";

    let mut cmd = Command::new(&spark_sql);
    if let Some(jar) = iceberg_jar {
        cmd.arg("--jars").arg(jar);
    } else if let Some(packages) = iceberg_pkg {
        cmd.arg("--packages").arg(packages);
    }
    cmd.current_dir(output_dir.path());
    let output = cmd
        .arg("--conf")
        .arg("spark.ui.enabled=false")
        .arg("--conf")
        .arg("spark.sql.shuffle.partitions=1")
        .arg("--conf")
        .arg("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .arg("--conf")
        .arg("spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog")
        .arg("--conf")
        .arg("spark.sql.catalog.local.type=hadoop")
        .arg("--conf")
        .arg(format!("spark.sql.catalog.local.warehouse={}", warehouse_uri))
        .arg("-e")
        .arg(query)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "spark-sql failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let combined = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("ROW=1:7"),
        "spark-sql output missing expected row\n{}",
        combined
    );
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

fn file_uri_to_path(uri: &str) -> PathBuf {
    if let Some(stripped) = uri.strip_prefix("file://") {
        PathBuf::from(stripped)
    } else {
        PathBuf::from(uri)
    }
}

fn read_first_avro_record(path: &Path) -> AvroValue {
    let mut reader = AvroReader::new(File::open(path).unwrap()).unwrap();
    reader.next().unwrap().unwrap()
}

fn avro_get_path<'a>(value: &'a AvroValue, path: &[&str]) -> Option<&'a AvroValue> {
    let mut cur = value;
    for segment in path {
        cur = match cur {
            AvroValue::Record(fields) => fields
                .iter()
                .find_map(|(name, val)| (name == segment).then_some(val))?,
            _ => return None,
        };
    }
    Some(cur)
}

fn avro_as_string(value: &AvroValue) -> Option<&str> {
    match value {
        AvroValue::String(s) => Some(s.as_str()),
        _ => None,
    }
}

fn avro_as_long(value: &AvroValue) -> Option<i64> {
    match value {
        AvroValue::Long(v) => Some(*v),
        _ => None,
    }
}

fn avro_as_bytes(value: &AvroValue) -> Option<&[u8]> {
    match value {
        AvroValue::Bytes(v) => Some(v.as_slice()),
        _ => None,
    }
}

fn avro_unwrap_union(value: &AvroValue) -> &AvroValue {
    match value {
        AvroValue::Union(_, inner) => inner.as_ref(),
        _ => value,
    }
}

fn avro_optional_map(value: &AvroValue) -> Option<&HashMap<String, AvroValue>> {
    match avro_unwrap_union(value) {
        AvroValue::Map(map) => Some(map),
        _ => None,
    }
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
