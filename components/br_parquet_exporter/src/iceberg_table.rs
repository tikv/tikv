// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use apache_avro::{Schema as AvroSchema, Writer as AvroWriter, to_value};
use serde::Serialize;
use uuid::Uuid;

use crate::{ColumnParquetType, Error, ParquetFileInfo, Result, TableSchema};

const ICEBERG_FORMAT_VERSION: u8 = 2;
const ICEBERG_SCHEMA_ID: i32 = 0;
const ICEBERG_PARTITION_SPEC_ID: i32 = 0;
const ICEBERG_SORT_ORDER_ID: i64 = 0;
const ICEBERG_LAST_PARTITION_ID_UNPARTITIONED: i32 = 999;
const ICEBERG_SEQUENCE_NUMBER: i64 = 1;
const ICEBERG_MANIFEST_MAX_DATA_FILES: usize = 10_000;

const MANIFEST_STATUS_ADDED: i32 = 1;
const MANIFEST_CONTENT_DATA: i32 = 0;
const DATA_FILE_CONTENT_DATA: i32 = 0;

// Iceberg manifest list Avro schema (v2) for unpartitioned tables.
// This is intentionally minimal: Iceberg readers use a richer reader schema
// and Avro defaulting fills any omitted optional fields.
const MANIFEST_LIST_SCHEMA_V2: &str = r#"
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    { "name": "manifest_path", "type": "string", "field-id": 500 },
    { "name": "manifest_length", "type": "long", "field-id": 501 },
    { "name": "partition_spec_id", "type": "int", "field-id": 502 },
    { "name": "content", "type": "int", "field-id": 517, "default": 0 },
    { "name": "sequence_number", "type": "long", "field-id": 515 },
    { "name": "min_sequence_number", "type": "long", "field-id": 516 },
    { "name": "added_snapshot_id", "type": "long", "field-id": 503 }
  ]
}
"#;

// Iceberg manifest entry Avro schema (v2) for unpartitioned tables.
// This is intentionally minimal: Iceberg readers use a richer reader schema
// and Avro defaulting fills any omitted optional fields.
const MANIFEST_ENTRY_SCHEMA_V2: &str = r#"
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    { "name": "status", "type": "int", "field-id": 0 },
    { "name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1 },
    { "name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3 },
    { "name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4 },
    {
      "name": "data_file",
      "field-id": 2,
      "type": {
        "type": "record",
        "name": "r2",
        "fields": [
          { "name": "content", "type": "int", "field-id": 134, "default": 0 },
          { "name": "file_path", "type": "string", "field-id": 100 },
          { "name": "file_format", "type": "string", "field-id": 101 },
          {
            "name": "partition",
            "field-id": 102,
            "type": { "type": "record", "name": "r102", "fields": [] }
          },
          { "name": "record_count", "type": "long", "field-id": 103 },
          { "name": "file_size_in_bytes", "type": "long", "field-id": 104 },
          {
            "name": "value_counts",
            "type": [
              "null",
              { "type": "map", "values": "long", "key-id": 119, "value-id": 120 }
            ],
            "default": null,
            "field-id": 109
          },
          {
            "name": "null_value_counts",
            "type": [
              "null",
              { "type": "map", "values": "long", "key-id": 121, "value-id": 122 }
            ],
            "default": null,
            "field-id": 110
          },
          {
            "name": "lower_bounds",
            "type": [
              "null",
              { "type": "map", "values": "bytes", "key-id": 126, "value-id": 127 }
            ],
            "default": null,
            "field-id": 125
          },
          {
            "name": "upper_bounds",
            "type": [
              "null",
              { "type": "map", "values": "bytes", "key-id": 129, "value-id": 130 }
            ],
            "default": null,
            "field-id": 128
          }
        ]
      }
    }
  ]
}
"#;

#[derive(Clone, Debug)]
pub(crate) struct IcebergTableArtifacts {
    pub(crate) version_hint_object: String,
    pub(crate) metadata_object: String,
    pub(crate) manifest_objects: Vec<String>,
    pub(crate) manifest_list_object: String,
    pub(crate) version_hint: Vec<u8>,
    pub(crate) metadata_json: Vec<u8>,
    pub(crate) manifest_avros: Vec<Vec<u8>>,
    pub(crate) manifest_list_avro: Vec<u8>,
}

pub(crate) fn build_iceberg_table_artifacts(
    base_uri: &str,
    table_root: &str,
    table: &TableSchema,
    parquet_files: &[&ParquetFileInfo],
    start_version: u64,
    end_version: u64,
    parquet_compression: &str,
) -> Result<IcebergTableArtifacts> {
    let snapshot_id = i64::try_from(end_version)
        .map_err(|_| Error::Invalid("snapshot id overflows i64".into()))?;
    let table_location = format!("{}{}", base_uri, table_root);
    let metadata_prefix = format!("{}/metadata", table_root);

    let version_hint_object = format!("{}/version-hint.text", metadata_prefix);
    let metadata_object = format!("{}/v1.metadata.json", metadata_prefix);
    let manifest_list_object = format!("{}/snap-{}.avro", metadata_prefix, snapshot_id);

    let manifest_list_path = format!("{}{}", base_uri, manifest_list_object);

    let schema = build_schema(table);
    let schema_json = serde_json::to_vec(&schema)
        .map_err(|e| Error::Invalid(format!("encode iceberg schema: {}", e)))?;
    let partition_spec_json = serde_json::to_vec(&Vec::<IcebergPartitionField>::new())
        .map_err(|e| Error::Invalid(format!("encode iceberg partition spec: {}", e)))?;

    let timestamp_ms = now_ms();
    let (total_rows, total_bytes) = parquet_files.iter().fold((0u64, 0u64), |acc, file| {
        (
            acc.0.saturating_add(file.row_count),
            acc.1.saturating_add(file.size),
        )
    });
    let mut properties = HashMap::new();
    properties.insert("write.format.default".to_string(), "parquet".to_string());
    properties.insert(
        "write.parquet.compression-codec".to_string(),
        parquet_compression.to_string(),
    );
    properties.insert(
        "tidb.snapshot.start-version".to_string(),
        start_version.to_string(),
    );
    properties.insert(
        "tidb.snapshot.end-version".to_string(),
        end_version.to_string(),
    );
    properties.insert("tidb.table.id".to_string(), table.table_id.to_string());

    let chunk_count = parquet_files
        .len()
        .div_ceil(ICEBERG_MANIFEST_MAX_DATA_FILES.max(1));
    let mut manifest_objects = Vec::with_capacity(chunk_count);
    let mut manifest_avros = Vec::with_capacity(chunk_count);
    let mut manifest_list_entries = Vec::with_capacity(chunk_count);
    for (idx, chunk) in parquet_files
        .chunks(ICEBERG_MANIFEST_MAX_DATA_FILES.max(1))
        .enumerate()
    {
        let manifest_object = if chunk_count == 1 {
            format!("{}/manifest-{}.avro", metadata_prefix, snapshot_id)
        } else {
            format!(
                "{}/manifest-{}-{:05}.avro",
                metadata_prefix, snapshot_id, idx
            )
        };
        let manifest_path = format!("{}{}", base_uri, manifest_object);
        let manifest_avro = build_manifest_avro(
            &schema_json,
            &partition_spec_json,
            base_uri,
            chunk,
            snapshot_id,
        )?;
        let manifest_length = i64::try_from(manifest_avro.len()).unwrap_or(i64::MAX);
        manifest_objects.push(manifest_object);
        manifest_avros.push(manifest_avro);
        manifest_list_entries.push(ManifestListEntryV2 {
            manifest_path,
            manifest_length,
            partition_spec_id: ICEBERG_PARTITION_SPEC_ID,
            content: MANIFEST_CONTENT_DATA,
            sequence_number: ICEBERG_SEQUENCE_NUMBER,
            min_sequence_number: ICEBERG_SEQUENCE_NUMBER,
            added_snapshot_id: snapshot_id,
        });
    }
    let manifest_list_avro = build_manifest_list_avro(&manifest_list_entries, snapshot_id)?;
    let metadata_json = build_table_metadata_json(
        &table_location,
        schema,
        &manifest_list_path,
        snapshot_id,
        timestamp_ms,
        parquet_files.len(),
        total_rows,
        total_bytes,
        properties,
    )?;

    Ok(IcebergTableArtifacts {
        version_hint_object,
        metadata_object,
        manifest_list_object,
        manifest_objects,
        version_hint: b"1".to_vec(),
        metadata_json,
        manifest_avros,
        manifest_list_avro,
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

#[derive(Serialize, Clone)]
#[serde(untagged)]
enum IcebergType {
    Primitive(&'static str),
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
struct IcebergField {
    id: i32,
    name: String,
    required: bool,
    #[serde(rename = "type")]
    field_type: IcebergType,
    #[serde(skip_serializing_if = "Option::is_none")]
    doc: Option<String>,
}

#[derive(Serialize, Clone)]
struct IcebergStructType {
    #[serde(rename = "type")]
    kind: &'static str,
    fields: Vec<IcebergField>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
struct IcebergSchemaV2 {
    schema_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    identifier_field_ids: Option<Vec<i32>>,
    #[serde(flatten)]
    fields: IcebergStructType,
}

fn build_schema(table: &TableSchema) -> IcebergSchemaV2 {
    let fields = table
        .columns
        .iter()
        .map(|col| IcebergField {
            id: col.field_id,
            name: col.name.clone(),
            required: !col.nullable,
            field_type: IcebergType::Primitive(match col.parquet_type {
                ColumnParquetType::Int64 => "long",
                ColumnParquetType::Double => "double",
                ColumnParquetType::Utf8 => "string",
                ColumnParquetType::Binary => "binary",
            }),
            doc: None,
        })
        .collect();
    IcebergSchemaV2 {
        schema_id: ICEBERG_SCHEMA_ID,
        identifier_field_ids: Some(vec![2]),
        fields: IcebergStructType {
            kind: "struct",
            fields,
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionSpec {
    spec_id: i32,
    fields: Vec<IcebergPartitionField>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergPartitionField {
    source_id: i32,
    field_id: i32,
    name: String,
    transform: String,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSortOrder {
    order_id: i64,
    fields: Vec<IcebergSortField>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSortField {
    source_id: i32,
    transform: String,
    direction: String,
    null_order: String,
}

#[derive(Serialize)]
struct IcebergSummary {
    operation: &'static str,
    #[serde(flatten)]
    additional: HashMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSnapshotV2 {
    snapshot_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_snapshot_id: Option<i64>,
    sequence_number: i64,
    timestamp_ms: i64,
    manifest_list: String,
    summary: IcebergSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_id: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSnapshotLog {
    timestamp_ms: i64,
    snapshot_id: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergSnapshotReference {
    snapshot_id: i64,
    #[serde(flatten)]
    retention: IcebergSnapshotRetention,
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase", tag = "type")]
enum IcebergSnapshotRetention {
    #[serde(rename_all = "kebab-case")]
    Branch {
        #[serde(skip_serializing_if = "Option::is_none")]
        min_snapshots_to_keep: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_snapshot_age_ms: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_ref_age_ms: Option<i64>,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergTableMetadataV2 {
    format_version: u8,
    table_uuid: String,
    location: String,
    last_sequence_number: i64,
    last_updated_ms: i64,
    last_column_id: i32,
    schemas: Vec<IcebergSchemaV2>,
    current_schema_id: i32,
    partition_specs: Vec<IcebergPartitionSpec>,
    default_spec_id: i32,
    last_partition_id: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots: Option<Vec<IcebergSnapshotV2>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_log: Option<Vec<IcebergSnapshotLog>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata_log: Option<Vec<IcebergMetadataLog>>,
    sort_orders: Vec<IcebergSortOrder>,
    default_sort_order_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    refs: Option<HashMap<String, IcebergSnapshotReference>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    statistics: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    partition_statistics: Vec<serde_json::Value>,
}

#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct IcebergMetadataLog {
    metadata_file: String,
    timestamp_ms: i64,
}

fn build_table_metadata_json(
    table_location: &str,
    schema: IcebergSchemaV2,
    manifest_list_path: &str,
    snapshot_id: i64,
    timestamp_ms: i64,
    added_data_files: usize,
    total_rows: u64,
    total_bytes: u64,
    properties: HashMap<String, String>,
) -> Result<Vec<u8>> {
    let last_column_id = schema
        .fields
        .fields
        .iter()
        .map(|field| field.id)
        .max()
        .unwrap_or(0);
    let mut summary_props = HashMap::new();
    summary_props.insert("added-data-files".to_string(), added_data_files.to_string());
    summary_props.insert("added-records".to_string(), total_rows.to_string());
    summary_props.insert("added-files-size".to_string(), total_bytes.to_string());
    summary_props.insert("tidb.snapshot-id".to_string(), snapshot_id.to_string());

    let snapshot = IcebergSnapshotV2 {
        snapshot_id,
        parent_snapshot_id: None,
        sequence_number: ICEBERG_SEQUENCE_NUMBER,
        timestamp_ms,
        manifest_list: manifest_list_path.to_string(),
        summary: IcebergSummary {
            operation: "append",
            additional: summary_props,
        },
        schema_id: Some(ICEBERG_SCHEMA_ID),
    };

    let mut refs = HashMap::new();
    refs.insert(
        "main".to_string(),
        IcebergSnapshotReference {
            snapshot_id,
            retention: IcebergSnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        },
    );

    let metadata = IcebergTableMetadataV2 {
        format_version: ICEBERG_FORMAT_VERSION,
        table_uuid: Uuid::new_v4().to_string(),
        location: table_location.to_string(),
        last_sequence_number: ICEBERG_SEQUENCE_NUMBER,
        last_updated_ms: timestamp_ms,
        last_column_id,
        schemas: vec![schema],
        current_schema_id: ICEBERG_SCHEMA_ID,
        partition_specs: vec![IcebergPartitionSpec {
            spec_id: ICEBERG_PARTITION_SPEC_ID,
            fields: Vec::new(),
        }],
        default_spec_id: ICEBERG_PARTITION_SPEC_ID,
        last_partition_id: ICEBERG_LAST_PARTITION_ID_UNPARTITIONED,
        properties: Some(properties),
        current_snapshot_id: Some(snapshot_id),
        snapshots: Some(vec![snapshot]),
        snapshot_log: Some(vec![IcebergSnapshotLog {
            timestamp_ms,
            snapshot_id,
        }]),
        metadata_log: None,
        sort_orders: vec![IcebergSortOrder {
            order_id: ICEBERG_SORT_ORDER_ID,
            fields: Vec::new(),
        }],
        default_sort_order_id: ICEBERG_SORT_ORDER_ID,
        refs: Some(refs),
        statistics: Vec::new(),
        partition_statistics: Vec::new(),
    };
    serde_json::to_vec_pretty(&metadata)
        .map_err(|e| Error::Invalid(format!("encode iceberg table metadata: {}", e)))
}

#[derive(Serialize)]
struct EmptyPartition {}

#[derive(Clone, Debug)]
struct AvroBytes(Vec<u8>);

impl Serialize for AvroBytes {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

#[derive(Serialize)]
struct DataFileV2 {
    #[serde(default)]
    content: i32,
    file_path: String,
    file_format: String,
    partition: EmptyPartition,
    record_count: i64,
    file_size_in_bytes: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_counts: Option<HashMap<String, i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    null_value_counts: Option<HashMap<String, i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lower_bounds: Option<HashMap<String, AvroBytes>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    upper_bounds: Option<HashMap<String, AvroBytes>>,
}

fn iceberg_i64_metrics_map(metrics: &HashMap<i32, u64>) -> HashMap<String, i64> {
    metrics
        .iter()
        .map(|(field_id, value)| {
            (
                field_id.to_string(),
                i64::try_from(*value).unwrap_or(i64::MAX),
            )
        })
        .collect()
}

fn iceberg_bytes_metrics_map(metrics: &HashMap<i32, Vec<u8>>) -> HashMap<String, AvroBytes> {
    metrics
        .iter()
        .map(|(field_id, value)| (field_id.to_string(), AvroBytes(value.clone())))
        .collect()
}

#[derive(Serialize)]
struct ManifestEntryV2 {
    status: i32,
    snapshot_id: Option<i64>,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: DataFileV2,
}

#[derive(Serialize)]
struct ManifestListEntryV2 {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
}

fn build_manifest_avro(
    schema_json: &[u8],
    partition_spec_json: &[u8],
    base_uri: &str,
    parquet_files: &[&ParquetFileInfo],
    snapshot_id: i64,
) -> Result<Vec<u8>> {
    let avro_schema = AvroSchema::parse_str(MANIFEST_ENTRY_SCHEMA_V2)
        .map_err(|e| Error::Invalid(format!("parse iceberg manifest schema: {}", e)))?;
    let mut writer = AvroWriter::new(&avro_schema, Vec::new());
    writer
        .add_user_metadata("schema".to_string(), schema_json)
        .map_err(|e| Error::Invalid(format!("set iceberg manifest schema metadata: {}", e)))?;
    writer
        .add_user_metadata("schema-id".to_string(), ICEBERG_SCHEMA_ID.to_string())
        .map_err(|e| Error::Invalid(format!("set iceberg manifest schema-id metadata: {}", e)))?;
    writer
        .add_user_metadata("partition-spec".to_string(), partition_spec_json)
        .map_err(|e| {
            Error::Invalid(format!(
                "set iceberg manifest partition-spec metadata: {}",
                e
            ))
        })?;
    writer
        .add_user_metadata(
            "partition-spec-id".to_string(),
            ICEBERG_PARTITION_SPEC_ID.to_string(),
        )
        .map_err(|e| {
            Error::Invalid(format!(
                "set iceberg manifest partition-spec-id metadata: {}",
                e
            ))
        })?;
    writer
        .add_user_metadata(
            "format-version".to_string(),
            ICEBERG_FORMAT_VERSION.to_string(),
        )
        .map_err(|e| {
            Error::Invalid(format!(
                "set iceberg manifest format-version metadata: {}",
                e
            ))
        })?;
    writer
        .add_user_metadata("content".to_string(), "data")
        .map_err(|e| Error::Invalid(format!("set iceberg manifest content metadata: {}", e)))?;

    for file in parquet_files {
        let value_counts = file
            .iceberg_metrics
            .as_ref()
            .map(|metrics| iceberg_i64_metrics_map(&metrics.value_counts));
        let null_value_counts = file
            .iceberg_metrics
            .as_ref()
            .map(|metrics| iceberg_i64_metrics_map(&metrics.null_value_counts));
        let lower_bounds = file
            .iceberg_metrics
            .as_ref()
            .map(|metrics| iceberg_bytes_metrics_map(&metrics.lower_bounds));
        let upper_bounds = file
            .iceberg_metrics
            .as_ref()
            .map(|metrics| iceberg_bytes_metrics_map(&metrics.upper_bounds));
        let entry = ManifestEntryV2 {
            status: MANIFEST_STATUS_ADDED,
            snapshot_id: Some(snapshot_id),
            sequence_number: Some(ICEBERG_SEQUENCE_NUMBER),
            file_sequence_number: None,
            data_file: DataFileV2 {
                content: DATA_FILE_CONTENT_DATA,
                file_path: format!("{}{}", base_uri, file.object_name),
                file_format: "PARQUET".to_string(),
                partition: EmptyPartition {},
                record_count: i64::try_from(file.row_count).unwrap_or(i64::MAX),
                file_size_in_bytes: i64::try_from(file.size).unwrap_or(i64::MAX),
                value_counts,
                null_value_counts,
                lower_bounds,
                upper_bounds,
            },
        };
        let mut value =
            to_value(entry).map_err(|e| Error::Invalid(format!("encode manifest entry: {}", e)))?;
        value = value
            .resolve(&avro_schema)
            .map_err(|e| Error::Invalid(format!("resolve manifest entry: {}", e)))?;
        writer
            .append(value)
            .map_err(|e| Error::Invalid(format!("append manifest entry: {}", e)))?;
    }
    let bytes = writer
        .into_inner()
        .map_err(|e| Error::Invalid(format!("finalize iceberg manifest: {}", e)))?;

    Ok(bytes)
}

fn build_manifest_list_avro(entries: &[ManifestListEntryV2], snapshot_id: i64) -> Result<Vec<u8>> {
    let avro_schema = AvroSchema::parse_str(MANIFEST_LIST_SCHEMA_V2)
        .map_err(|e| Error::Invalid(format!("parse iceberg manifest-list schema: {}", e)))?;
    let mut writer = AvroWriter::new(&avro_schema, Vec::new());
    writer
        .add_user_metadata("snapshot-id".to_string(), snapshot_id.to_string())
        .map_err(|e| Error::Invalid(format!("set manifest-list snapshot-id metadata: {}", e)))?;
    writer
        .add_user_metadata(
            "sequence-number".to_string(),
            ICEBERG_SEQUENCE_NUMBER.to_string(),
        )
        .map_err(|e| {
            Error::Invalid(format!("set manifest-list sequence-number metadata: {}", e))
        })?;
    writer
        .add_user_metadata(
            "format-version".to_string(),
            ICEBERG_FORMAT_VERSION.to_string(),
        )
        .map_err(|e| Error::Invalid(format!("set manifest-list format-version metadata: {}", e)))?;
    writer
        .add_user_metadata("parent-snapshot-id".to_string(), "null")
        .map_err(|e| {
            Error::Invalid(format!(
                "set manifest-list parent-snapshot-id metadata: {}",
                e
            ))
        })?;

    for entry in entries {
        let mut value = to_value(entry)
            .map_err(|e| Error::Invalid(format!("encode manifest-list entry: {}", e)))?;
        value = value
            .resolve(&avro_schema)
            .map_err(|e| Error::Invalid(format!("resolve manifest-list entry: {}", e)))?;
        writer
            .append(value)
            .map_err(|e| Error::Invalid(format!("append manifest-list entry: {}", e)))?;
    }
    let bytes = writer
        .into_inner()
        .map_err(|e| Error::Invalid(format!("finalize iceberg manifest-list: {}", e)))?;

    Ok(bytes)
}
