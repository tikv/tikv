// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use api_version::match_template_api_version;
use api_version::KvFormat;
use api_version::RawValue;
use engine_traits::raw_ttl::ttl_to_expire_ts;
use kvproto::import_sstpb::*;
use kvproto::kvrpcpb::ApiVersion;
use std::sync::Arc;

use encryption::DataKeyManager;
use engine_traits::{KvEngine, SstWriter};
use tikv_util::time::Instant;
use txn_types::{is_short_value, Key, TimeStamp, Write as KvWrite, WriteType};

use crate::import_file::ImportPath;
use crate::metrics::*;
use crate::Result;

pub struct TxnSstWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_bytes: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    write: E::SstWriter,
    write_entries: u64,
    write_bytes: u64,
    write_path: ImportPath,
    write_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
}

impl<E: KvEngine> TxnSstWriter<E> {
    pub fn new(
        default: E::SstWriter,
        write: E::SstWriter,
        default_path: ImportPath,
        write_path: ImportPath,
        default_meta: SstMeta,
        write_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        TxnSstWriter {
            default,
            default_path,
            default_entries: 0,
            default_bytes: 0,
            default_meta,
            write,
            write_path,
            write_entries: 0,
            write_bytes: 0,
            write_meta,
            key_manager,
        }
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let start = Instant::now_coarse();

        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            let k = Key::from_raw(m.get_key()).append_ts(commit_ts);
            self.put(k.as_encoded(), m.get_value(), m.get_op())?;
        }

        IMPORT_LOCAL_WRITE_CHUNK_DURATION_VEC
            .with_label_values(&["txn"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(())
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        let (_, commit_ts) = Key::split_on_ts_for(key)?;
        let w = match (op, is_short_value(value)) {
            (PairOp::Delete, _) => KvWrite::new(WriteType::Delete, commit_ts, None),
            (PairOp::Put, true) => KvWrite::new(WriteType::Put, commit_ts, Some(value.to_vec())),
            (PairOp::Put, false) => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                self.default_bytes += (k.len() + value.len()) as u64;
                KvWrite::new(WriteType::Put, commit_ts, None)
            }
        };
        let write = w.as_ref().to_bytes();
        self.write.put(&k, &write)?;
        self.write_entries += 1;
        self.write_bytes += (k.len() + write.len()) as u64;
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        let default_meta = self.default_meta.clone();
        let write_meta = self.write_meta.clone();
        let mut metas = Vec::with_capacity(2);
        let (default_entries, write_entries) = (self.default_entries, self.write_entries);
        let (default_bytes, write_bytes) = (self.default_bytes, self.write_bytes);
        let (p1, p2) = (self.default_path.clone(), self.write_path.clone());
        let (w1, w2, key_manager) = (self.default, self.write, self.key_manager);

        if default_entries > 0 {
            w1.finish()?;
            p1.save(key_manager.as_deref())?;
            metas.push(default_meta);
        }
        if write_entries > 0 {
            w2.finish()?;
            p2.save(key_manager.as_deref())?;
            metas.push(write_meta);
        }

        info!("finish write to sst";
            "default entries" => default_entries,
            "default bytes" => default_bytes,
            "write entries" => write_entries,
            "write bytes" => write_bytes,
        );
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["txn_default_cf"])
            .inc_by(default_entries);
        IMPORT_LOCAL_WRITE_BYTES_VEC
            .with_label_values(&["txn_default_cf"])
            .inc_by(default_bytes);
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["txn_write_cf"])
            .inc_by(write_entries);
        IMPORT_LOCAL_WRITE_BYTES_VEC
            .with_label_values(&["txn_write_cf"])
            .inc_by(write_bytes);

        Ok(metas)
    }
}

pub struct RawSstWriter<E: KvEngine> {
    default: E::SstWriter,
    default_entries: u64,
    default_deletes: u64,
    default_bytes: u64,
    default_path: ImportPath,
    default_meta: SstMeta,
    key_manager: Option<Arc<DataKeyManager>>,
    api_version: ApiVersion,
}

impl<E: KvEngine> RawSstWriter<E> {
    pub fn new(
        default: E::SstWriter,
        default_path: ImportPath,
        default_meta: SstMeta,
        key_manager: Option<Arc<DataKeyManager>>,
        api_version: ApiVersion,
    ) -> Self {
        RawSstWriter {
            default,
            default_path,
            default_entries: 0,
            default_bytes: 0,
            default_deletes: 0,
            default_meta,
            key_manager,
            api_version,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8], op: PairOp) -> Result<()> {
        let k = keys::data_key(key);
        match op {
            PairOp::Delete => {
                self.default.delete(&k)?;
                self.default_deletes += 1;
                self.default_bytes += k.len() as u64;
            }
            PairOp::Put => {
                self.default.put(&k, value)?;
                self.default_entries += 1;
                self.default_bytes += (k.len() + value.len()) as u64;
            }
        }
        Ok(())
    }

    pub fn write(&mut self, mut batch: RawWriteBatch) -> Result<()> {
        let start = Instant::now_coarse();

        match_template_api_version!(
            API,
            match self.api_version {
                ApiVersion::API => {
                    let expire_ts = if batch.get_ttl() == 0 {
                        None
                    } else if API::IS_TTL_ENABLED {
                        ttl_to_expire_ts(batch.get_ttl())
                    } else {
                        return Err(crate::Error::TtlNotEnabled);
                    };

                    for m in batch.take_pairs().into_iter() {
                        match m.get_op() {
                            PairOp::Put => {
                                let value = RawValue {
                                    user_value: m.get_value(),
                                    expire_ts,
                                    is_delete: false,
                                };
                                self.put(m.get_key(), &API::encode_raw_value(value), PairOp::Put)?;
                            }
                            PairOp::Delete => {
                                self.put(m.get_key(), &[], PairOp::Delete)?;
                            }
                        };
                    }
                }
            }
        );

        IMPORT_LOCAL_WRITE_CHUNK_DURATION_VEC
            .with_label_values(&["raw"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<SstMeta>> {
        if self.default_entries == 0 {
            return Ok(vec![]);
        }

        self.default.finish()?;
        self.default_path.save(self.key_manager.as_deref())?;

        info!(
            "finish raw write to sst";
            "default entries" => self.default_entries,
            "default bytes" => self.default_deletes,
            "default bytes" => self.default_bytes
        );
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["raw_default_cf"])
            .inc_by(self.default_entries);
        IMPORT_LOCAL_WRITE_KEYS_VEC
            .with_label_values(&["raw_default_cf_delete"])
            .inc_by(self.default_deletes);
        IMPORT_LOCAL_WRITE_BYTES_VEC
            .with_label_values(&["raw_default_cf"])
            .inc_by(self.default_bytes);

        Ok(vec![self.default_meta])
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::DATA_CFS;
    use test_sst_importer::*;
    use uuid::Uuid;

    use super::*;
    use crate::{Config, SstImporter};

    #[test]
    fn test_write_txn_sst() {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_txn_writer::<TestEngine>(&db, meta).unwrap();
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in wirte cf
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // put big value kv in default cf
        let big_value = vec![42; 256];
        let mut pair = Pair::default();
        pair.set_key(b"k2".to_vec());
        pair.set_value(big_value);
        pairs.push(pair);

        // put delete type key in write cf
        let mut pair = Pair::default();
        pair.set_key(b"k3".to_vec());
        pair.set_op(PairOp::Delete);
        pairs.push(pair);

        // generate two cf metas
        batch.set_commit_ts(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.write_entries, 3);
        assert_eq!(w.default_entries, 1);

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 2);
    }

    #[test]
    fn test_raw_write_sst_ttl() {
        test_raw_write_sst_ttl_impl(ApiVersion::V1ttl);
        test_raw_write_sst_ttl_impl(ApiVersion::V2);
    }

    fn test_raw_write_sst_ttl_impl(api_version: ApiVersion) {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_raw_writer::<TestEngine>(&db, meta).unwrap();
        let mut batch = RawWriteBatch::default();
        let mut pairs = vec![];

        // put value
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // delete value
        let mut pair = Pair::default();
        pair.set_key(b"k2".to_vec());
        pair.set_op(PairOp::Delete);
        pairs.push(pair);

        // generate meta
        batch.set_ttl(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.default_entries, 1);
        assert_eq!(w.default_deletes, 1);
        match api_version {
            ApiVersion::V1ttl => {
                // ttl takes 8 more bytes
                assert_eq!(
                    w.default_bytes as usize,
                    b"zk1".len() + b"short_value".len() + 8 + b"zk2".len()
                );
            }
            ApiVersion::V2 => {
                // ttl takes 8 more bytes and meta take 1 more byte
                assert_eq!(
                    w.default_bytes as usize,
                    b"zk1".len() + b"short_value".len() + b"zk2".len() + 9
                );
            }
            _ => unreachable!(),
        }

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 1);
    }

    #[test]
    fn test_raw_write_ttl_not_enabled() {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, ApiVersion::V1).unwrap();
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);

        let mut w = importer.new_raw_writer::<TestEngine>(&db, meta).unwrap();
        let mut batch = RawWriteBatch::default();
        batch.set_ttl(10);
        assert!(w.write(batch).is_err());
    }
}
