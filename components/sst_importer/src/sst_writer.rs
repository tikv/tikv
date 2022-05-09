// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use api_version::{dispatch_api_version, match_template_api_version, KeyMode, KvFormat, RawValue};
use encryption::DataKeyManager;
use engine_traits::{raw_ttl::ttl_to_expire_ts, KvEngine, SstWriter};
use kvproto::{import_sstpb::*, kvrpcpb::ApiVersion};
use tikv_util::time::Instant;
use txn_types::{is_short_value, Key, TimeStamp, Write as KvWrite, WriteType};

use crate::{import_file::ImportPath, metrics::*, Error, Result};

#[derive(Debug)]
pub enum SstWriterType {
    Txn,
    Raw,
}

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
    api_version: ApiVersion,
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
        api_version: ApiVersion,
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
            api_version,
        }
    }

    fn check_api_version<K: KvFormat>(&self, key: &[u8]) -> Result<()> {
        let mode = K::parse_key_mode(key);
        if self.api_version == ApiVersion::V2 && mode != KeyMode::Txn && mode != KeyMode::TiDB {
            return Err(Error::invalid_key_mode(
                SstWriterType::Txn,
                self.api_version,
                key,
            ));
        }
        Ok(())
    }

    pub fn write(&mut self, batch: WriteBatch) -> Result<()> {
        let start = Instant::now_coarse();

        let commit_ts = TimeStamp::new(batch.get_commit_ts());
        for m in batch.get_pairs().iter() {
            dispatch_api_version!(self.api_version, {
                self.check_api_version::<API>(m.get_key())?;
            });
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

    // TODO: move this check to mod api_version
    fn check_api_version<K: KvFormat>(&self, key: &[u8]) -> Result<()> {
        let mode = K::parse_key_mode(key);
        if self.api_version == ApiVersion::V2 && mode != KeyMode::Raw {
            return Err(Error::invalid_key_mode(
                SstWriterType::Raw,
                self.api_version,
                key,
            ));
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

                    for mut m in batch.take_pairs().into_iter() {
                        self.check_api_version::<API>(m.get_key())?;
                        let key = API::encode_raw_key_owned(
                            m.take_key(),
                            Some(TimeStamp::new(batch.get_ts())),
                        );
                        match m.get_op() {
                            PairOp::Put => {
                                let value = RawValue {
                                    user_value: m.take_value(),
                                    expire_ts,
                                    is_delete: false,
                                };
                                self.put(
                                    key.as_encoded(),
                                    &API::encode_raw_value_owned(value),
                                    PairOp::Put,
                                )?;
                            }
                            PairOp::Delete => self.put(key.as_encoded(), &[], PairOp::Delete)?,
                        }
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
    use api_version::{ApiV1Ttl, ApiV2};
    use engine_rocks::RocksEngine;
    use engine_traits::{DATA_CFS, DATA_KEY_PREFIX_LEN};
    use tempfile::TempDir;
    use test_sst_importer::*;
    use uuid::Uuid;

    use super::*;
    use crate::{Config, SstImporter};

    // Return the temp dir path to avoid it drop out of the scope.
    fn new_writer<W, F: Fn(&SstImporter, &RocksEngine, SstMeta) -> Result<W>>(
        f: F,
        api_version: ApiVersion,
    ) -> (W, TempDir) {
        let mut meta = SstMeta::default();
        meta.set_uuid(Uuid::new_v4().as_bytes().to_vec());

        let importer_dir = tempfile::tempdir().unwrap();
        let cfg = Config::default();
        let importer = SstImporter::new(&cfg, &importer_dir, None, api_version).unwrap();
        let db_path = importer_dir.path().join("db");
        let db = new_test_engine(db_path.to_str().unwrap(), DATA_CFS);
        (f(&importer, &db, meta).unwrap(), importer_dir)
    }

    #[test]
    fn test_write_txn_sst() {
        let (mut w, _handle) = new_writer(SstImporter::new_txn_writer, ApiVersion::V1);
        let mut batch = WriteBatch::default();
        let mut pairs = vec![];

        // put short value kv in write cf
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
        let (mut w, _handle) = new_writer(SstImporter::new_raw_writer, api_version);

        let mut batch = RawWriteBatch::default();
        batch.set_ts(1);
        let mut pairs = vec![];
        let key1: &[u8] = if api_version == ApiVersion::V2 {
            b"rk1"
        } else {
            b"k1"
        };
        let key2: &[u8] = if api_version == ApiVersion::V2 {
            b"rk2"
        } else {
            b"k2"
        };

        // put value
        let mut pair = Pair::default();
        pair.set_op(PairOp::Put);
        pair.set_key(key1.to_vec());
        pair.set_value(b"short_value".to_vec());
        pairs.push(pair);

        // delete value
        let mut pair = Pair::default();
        pair.set_op(PairOp::Delete);
        pair.set_key(key2.to_vec());
        pairs.push(pair);

        // generate meta
        batch.set_ttl(10);
        batch.set_pairs(pairs.into());
        w.write(batch).unwrap();
        assert_eq!(w.default_entries, 1);
        assert_eq!(w.default_deletes, 1);

        match api_version {
            ApiVersion::V1ttl => {
                let write_size = DATA_KEY_PREFIX_LEN
                    + ApiV1Ttl::encode_raw_key(b"k1", None).len()
                    + ApiV1Ttl::encode_raw_value_owned(RawValue {
                        user_value: b"short_value".to_vec(),
                        expire_ts: Some(10),
                        is_delete: false,
                    })
                    .len()
                    + DATA_KEY_PREFIX_LEN
                    + ApiV1Ttl::encode_raw_key(b"k2", None).len();
                assert_eq!(write_size, w.default_bytes as usize);
            }
            ApiVersion::V2 => {
                let write_size = DATA_KEY_PREFIX_LEN
                    + ApiV2::encode_raw_key(b"rk1", Some(TimeStamp::new(1))).len()
                    + ApiV2::encode_raw_value_owned(RawValue {
                        user_value: b"short_value".to_vec(),
                        expire_ts: Some(10),
                        is_delete: false,
                    })
                    .len()
                    + DATA_KEY_PREFIX_LEN
                    + ApiV2::encode_raw_key(b"rk2", Some(TimeStamp::new(1))).len();
                assert_eq!(write_size, w.default_bytes as usize);
            }
            _ => unreachable!(),
        }

        let metas = w.finish().unwrap();
        assert_eq!(metas.len(), 1);
    }

    #[test]
    fn test_raw_write_ttl_not_enabled() {
        let (mut w, _handle) = new_writer(SstImporter::new_raw_writer, ApiVersion::V1);
        let mut batch = RawWriteBatch::default();
        batch.set_ttl(10);
        assert!(w.write(batch).is_err());
    }

    #[test]
    fn test_raw_write_v1() {
        let (mut w, _handle) = new_writer(SstImporter::new_raw_writer, ApiVersion::V1);
        let mut batch = RawWriteBatch::default();

        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"v1".to_vec());
        batch.set_pairs(vec![pair].into());
        w.write(batch).unwrap();
    }

    #[test]
    fn test_raw_write_invalid_key_mode() {
        let (mut w, _handle) = new_writer(SstImporter::new_raw_writer, ApiVersion::V2);
        let mut batch = RawWriteBatch::default();
        batch.set_ts(1);

        // put an invalid key
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        let pairs = vec![pair];
        batch.set_pairs(pairs.into());

        assert!(w.write(batch).is_err());
    }

    #[test]
    fn test_txn_write_v2() {
        let (mut w, _handle) = new_writer(SstImporter::new_txn_writer, ApiVersion::V2);
        let mut batch = WriteBatch::default();
        batch.set_commit_ts(1);

        // put an invalid key
        let mut pair = Pair::default();
        pair.set_key(b"k1".to_vec());
        pair.set_value(b"short_value".to_vec());
        let pairs = vec![pair];
        batch.set_pairs(pairs.into());

        assert!(w.write(batch.clone()).is_err());

        // put a valid key
        let mut pair = Pair::default();
        pair.set_key(b"xk1".to_vec());
        pair.set_value(b"short_value".to_vec());
        let pairs = vec![pair];
        batch.set_pairs(pairs.into());

        w.write(batch).unwrap();
    }
}
