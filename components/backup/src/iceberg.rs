// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use external_storage::{ExternalStorage, UnpinReader};
use futures::io::Cursor;
use kvproto::brpb::File;
use serde::Serialize;
use tikv::config::BackupIcebergConfig;
use tikv_util::{box_err, info};
use txn_types::TimeStamp;

use crate::{Error, Result};

/// A minimal Iceberg manifest writer for BR backup output.
///
/// This is used by `tikv-ctl br-parquet-export` to emit JSON manifests that can
/// be consumed by downstream tools, without changing the existing BR backup
/// format.
pub struct IcebergCatalog {
    cfg: BackupIcebergConfig,
    sequence: AtomicU64,
}

impl IcebergCatalog {
    /// Creates an [`IcebergCatalog`] only when `cfg.enable` is `true`.
    pub fn from_config(cfg: BackupIcebergConfig) -> Option<Self> {
        if cfg.enable {
            Some(Self {
                cfg,
                sequence: AtomicU64::new(0),
            })
        } else {
            None
        }
    }

    /// Writes an Iceberg-compatible manifest file describing `files`.
    ///
    /// The manifest is written under
    /// `<warehouse>/<namespace>/<table>/metadata/` and includes BR metadata
    /// such as key ranges and version bounds.
    pub async fn write_manifest(
        &self,
        storage: &dyn ExternalStorage,
        files: &[File],
        start_version: TimeStamp,
        end_version: TimeStamp,
    ) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let generated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or_default();
        let snapshot_start = start_version.into_inner();
        let snapshot_end = end_version.into_inner();
        let entries = files.iter().map(IcebergDataFile::from).collect();
        let manifest = IcebergManifest {
            format_version: 1,
            warehouse: normalize(&self.cfg.warehouse),
            namespace: normalize_namespace(&self.cfg.namespace),
            table: normalize(&self.cfg.table),
            generated_at_ms: generated_at,
            snapshot_start_ts: snapshot_start,
            snapshot_end_ts: snapshot_end,
            files: entries,
        };
        let payload = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| Error::Other(box_err!("encode iceberg manifest failed: {}", e)))?;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let object = self.manifest_path(seq, snapshot_end);
        let len = payload.len() as u64;
        let reader = Cursor::new(payload);
        storage
            .write(&object, UnpinReader(Box::new(reader)), len)
            .await
            .map_err(Error::Io)?;
        info!(
            "wrote iceberg manifest";
            "object" => %object,
            "snapshot_start" => snapshot_start,
            "snapshot_end" => snapshot_end,
        );
        Ok(())
    }

    fn manifest_path(&self, seq: u64, snapshot_end: u64) -> String {
        let mut parts = Vec::new();
        let warehouse = normalize(&self.cfg.warehouse);
        if !warehouse.is_empty() {
            parts.push(warehouse);
        }
        let namespace = normalize_namespace(&self.cfg.namespace);
        if !namespace.is_empty() {
            parts.push(namespace);
        }
        let table = normalize(&self.cfg.table);
        if !table.is_empty() {
            parts.push(table);
        }
        parts.push("metadata".to_string());
        let prefix = normalize(&self.cfg.manifest_prefix);
        let file = format!("{}_{}_{}.json", prefix, snapshot_end, seq);
        parts.push(file);
        parts.join("/")
    }
}

#[derive(Serialize)]
struct IcebergManifest {
    format_version: u32,
    warehouse: String,
    namespace: String,
    table: String,
    generated_at_ms: u64,
    snapshot_start_ts: u64,
    snapshot_end_ts: u64,
    files: Vec<IcebergDataFile>,
}

#[derive(Serialize)]
struct IcebergDataFile {
    file_name: String,
    cf: String,
    sha256: Option<String>,
    start_version: u64,
    end_version: u64,
    total_kvs: u64,
    total_bytes: u64,
    size: u64,
    start_key: String,
    end_key: String,
}

impl From<&File> for IcebergDataFile {
    fn from(file: &File) -> Self {
        let sha256 = if file.get_sha256().is_empty() {
            None
        } else {
            Some(hex::encode(file.get_sha256()))
        };
        Self {
            file_name: file.get_name().to_owned(),
            cf: file.get_cf().to_owned(),
            sha256,
            start_version: file.get_start_version(),
            end_version: file.get_end_version(),
            total_kvs: file.get_total_kvs(),
            total_bytes: file.get_total_bytes(),
            size: file.get_size(),
            start_key: encode_bytes(file.get_start_key()),
            end_key: encode_bytes(file.get_end_key()),
        }
    }
}

fn encode_bytes(input: &[u8]) -> String {
    if input.is_empty() {
        String::new()
    } else {
        base64::encode(input)
    }
}

fn normalize(input: &str) -> String {
    let trimmed = input.trim_matches('/');
    trimmed.to_owned()
}

fn normalize_namespace(input: &str) -> String {
    let trimmed = input.trim_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else {
        trimmed.replace('.', "/")
    }
}

#[cfg(test)]
mod tests {
    use external_storage::{BackendConfig, create_storage, make_local_backend};
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn writes_manifest_into_local_storage() {
        let tmp = TempDir::new().unwrap();
        let backend = make_local_backend(tmp.path());
        let storage = create_storage(&backend, BackendConfig::default()).unwrap();
        let cfg = BackupIcebergConfig {
            enable: true,
            warehouse: "warehouse".into(),
            namespace: "analytics".into(),
            table: "orders".into(),
            manifest_prefix: "manifest".into(),
        };
        let catalog = IcebergCatalog::from_config(cfg).unwrap();
        let mut file = File::default();
        file.set_name("0000_default.sst".into());
        file.set_cf("default".into());
        file.set_start_key(b"a".to_vec());
        file.set_end_key(b"b".to_vec());
        file.set_total_kvs(1);
        file.set_total_bytes(2);
        file.set_size(10);
        file.set_start_version(5);
        file.set_end_version(6);
        file.set_sha256(vec![1, 2, 3, 4]);
        catalog
            .write_manifest(
                storage.as_ref(),
                &[file],
                TimeStamp::new(5),
                TimeStamp::new(10),
            )
            .await
            .unwrap();

        let manifest_path = tmp
            .path()
            .join("warehouse/analytics/orders/metadata/manifest_10_0.json");
        let raw = std::fs::read_to_string(manifest_path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(json["files"][0]["file_name"], "0000_default.sst");
        assert_eq!(json["files"][0]["start_key"], base64::encode("a"));
    }

    #[test]
    fn disabled_config_does_not_create_catalog() {
        assert!(IcebergCatalog::from_config(BackupIcebergConfig::default()).is_none());
    }

    #[test]
    fn manifest_path_normalizes_namespace_and_prefix() {
        let cfg = BackupIcebergConfig {
            enable: true,
            warehouse: "warehouse".into(),
            namespace: "analytics.core".into(),
            table: "orders".into(),
            manifest_prefix: "manifest".into(),
        };
        let catalog = IcebergCatalog::from_config(cfg).unwrap();
        let path = catalog.manifest_path(7, 42);
        assert_eq!(
            path,
            "warehouse/analytics/core/orders/metadata/manifest_42_7.json"
        );
    }
}
