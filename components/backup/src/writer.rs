// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Display;
use std::io::Read;
use std::sync::Arc;

use encryption::{EncrypterReader, Iv};
use engine_rocks::raw::DB;
use engine_rocks::{RocksEngine, RocksSstWriter, RocksSstWriterBuilder};
use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use engine_traits::{ExternalSstFileInfo, SstCompressionType, SstWriter, SstWriterBuilder};
use external_storage_export::{ExternalStorage, UnpinReader};
use file_system::Sha256Reader;
use futures_util::io::AllowStdIo;
use kvproto::brpb::{CipherInfo, File};
use kvproto::metapb::Region;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::storage::txn::TxnEntry;
use tikv_util::{
    self, box_err, error,
    time::{Instant, Limiter},
};
use txn_types::KvPair;

use crate::metrics::*;
use crate::{backup_file_name, utils::KeyValueCodec, Error, Result};

#[derive(Debug, Clone, Copy)]
/// CfNameWrap wraps the CfName type.
/// For removing the 'static lifetime bound in the async function,
/// which doesn't compile due to 'captures lifetime that does not appear in bounds' :(.
/// see https://github.com/rust-lang/rust/issues/63033
/// FIXME: remove this.
pub struct CfNameWrap(pub &'static str);

impl Display for CfNameWrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<CfName> for CfNameWrap {
    fn from(f: CfName) -> Self {
        Self(f)
    }
}

impl From<CfNameWrap> for CfName {
    fn from(w: CfNameWrap) -> CfName {
        w.0
    }
}

struct Writer {
    writer: RocksSstWriter,
    total_kvs: u64,
    total_bytes: u64,
    checksum: u64,
    digest: crc64fast::Digest,
}

impl Writer {
    fn new(writer: RocksSstWriter) -> Self {
        Writer {
            writer,
            total_kvs: 0,
            total_bytes: 0,
            checksum: 0,
            digest: crc64fast::Digest::new(),
        }
    }

    fn write(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // HACK: The actual key stored in TiKV is called
        // data_key and always prefix a `z`. But iterator strips
        // it, we need to add the prefix manually.
        let data_key_write = keys::data_key(key);
        self.writer.put(&data_key_write, value)?;
        Ok(())
    }

    fn update_with(&mut self, entry: TxnEntry, need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        if need_checksum {
            let (k, v) = entry
                .into_kvpair()
                .map_err(|err| Error::Other(box_err!("Decode error: {:?}", err)))?;
            self.total_bytes += (k.len() + v.len()) as u64;
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), &k, &v);
        }
        Ok(())
    }

    fn update_raw_with(&mut self, key: &[u8], value: &[u8], need_checksum: bool) -> Result<()> {
        self.total_kvs += 1;
        self.total_bytes += (key.len() + value.len()) as u64;
        if need_checksum {
            self.checksum = checksum_crc64_xor(self.checksum, self.digest.clone(), key, value);
        }
        Ok(())
    }

    // FIXME: we cannot get sst_info in [save_and_build_file], which may cause the !Send type
    // [RocksEnternalSstFileInfo] sent between threads.
    fn finish_read(writer: RocksSstWriter) -> Result<(u64, impl Read)> {
        let (sst_info, sst_reader) = writer.finish_read()?;
        Ok((sst_info.file_size(), sst_reader))
    }

    async fn save_and_build_file(
        self,
        name: &str,
        cf: CfNameWrap,
        limiter: Limiter,
        storage: &dyn ExternalStorage,
        cipher: &CipherInfo,
    ) -> Result<File> {
        let (size, sst_reader) = Self::finish_read(self.writer)?;
        BACKUP_RANGE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[cf.into()])
            .observe(size as f64);
        BACKUP_SCAN_KV_SIZE
            .with_label_values(&[cf.into()])
            .inc_by(self.total_bytes);
        BACKUP_SCAN_KV_COUNT
            .with_label_values(&[cf.into()])
            .inc_by(self.total_kvs);
        let file_name = format!("{}_{}.sst", name, cf);
        let iv = Iv::new_ctr();
        let encrypter_reader =
            EncrypterReader::new(sst_reader, cipher.cipher_type, &cipher.cipher_key, iv)
                .map_err(|e| Error::Other(box_err!("new EncrypterReader error: {:?}", e)))?;

        let (reader, hasher) = Sha256Reader::new(encrypter_reader)
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
        storage
            .write(
                &file_name,
                // AllowStdIo here only introduces the Sha256 reader and an in-memory sst reader.
                UnpinReader(Box::new(limiter.limit(AllowStdIo::new(reader)))),
                size,
            )
            .await?;
        let sha256 = hasher
            .lock()
            .unwrap()
            .finish()
            .map(|digest| digest.to_vec())
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;

        let mut file = File::default();
        file.set_name(file_name);
        file.set_sha256(sha256);
        file.set_crc64xor(self.checksum);
        file.set_total_kvs(self.total_kvs);
        file.set_total_bytes(self.total_bytes);
        file.set_cf(cf.0.to_owned());
        file.set_size(size);
        file.set_cipher_iv(iv.as_slice().to_vec());
        Ok(file)
    }

    fn is_empty(&self) -> bool {
        self.total_kvs == 0
    }
}

pub struct BackupWriterBuilder {
    store_id: u64,
    limiter: Limiter,
    region: Region,
    db: Arc<DB>,
    compression_type: Option<SstCompressionType>,
    compression_level: i32,
    sst_max_size: u64,
    cipher: CipherInfo,
}

impl BackupWriterBuilder {
    pub fn new(
        store_id: u64,
        limiter: Limiter,
        region: Region,
        db: Arc<DB>,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
        sst_max_size: u64,
        cipher: CipherInfo,
    ) -> BackupWriterBuilder {
        Self {
            store_id,
            limiter,
            region,
            db,
            compression_type,
            compression_level,
            sst_max_size,
            cipher,
        }
    }

    pub fn build(&self, start_key: Vec<u8>) -> Result<BackupWriter> {
        let key = file_system::sha256(&start_key).ok().map(hex::encode);
        let store_id = self.store_id;
        let name = backup_file_name(store_id, &self.region, key);
        BackupWriter::new(
            self.db.clone(),
            &name,
            self.compression_type,
            self.compression_level,
            self.limiter.clone(),
            self.sst_max_size,
            self.cipher.clone(),
        )
    }
}

/// A writer writes txn entries into SST files.
pub struct BackupWriter {
    name: String,
    default: Writer,
    write: Writer,
    limiter: Limiter,
    sst_max_size: u64,
    cipher: CipherInfo,
}

impl BackupWriter {
    /// Create a new BackupWriter.
    pub fn new(
        db: Arc<DB>,
        name: &str,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
        limiter: Limiter,
        sst_max_size: u64,
        cipher: CipherInfo,
    ) -> Result<BackupWriter> {
        let default = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(RocksEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        let write = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_WRITE)
            .set_db(RocksEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        let name = name.to_owned();
        Ok(BackupWriter {
            name,
            default: Writer::new(default),
            write: Writer::new(write),
            limiter,
            sst_max_size,
            cipher,
        })
    }

    /// Write entries to buffered SST files.
    pub fn write<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for e in entries {
            let mut value_in_default = false;
            match &e {
                TxnEntry::Commit { default, write, .. } => {
                    // Default may be empty if value is small.
                    if !default.0.is_empty() {
                        self.default.write(&default.0, &default.1)?;
                        value_in_default = true;
                    }
                    assert!(!write.0.is_empty());
                    self.write.write(&write.0, &write.1)?;
                }
                TxnEntry::Prewrite { .. } => {
                    return Err(Error::Other("prewrite is not supported".into()));
                }
            }
            if value_in_default {
                self.default.update_with(e, need_checksum)?;
            } else {
                self.write.update_with(e, need_checksum)?;
            }
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub async fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(2);
        let write_written = !self.write.is_empty() || !self.default.is_empty();
        if !self.default.is_empty() {
            // Save default cf contents.
            let default = self
                .default
                .save_and_build_file(
                    &self.name,
                    CF_DEFAULT.into(),
                    self.limiter.clone(),
                    storage,
                    &self.cipher,
                )
                .await?;
            files.push(default);
        }
        if write_written {
            // Save write cf contents.
            let write = self
                .write
                .save_and_build_file(
                    &self.name,
                    CF_WRITE.into(),
                    self.limiter.clone(),
                    storage,
                    &self.cipher,
                )
                .await?;
            files.push(write);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(files)
    }

    pub fn need_split_keys(&self) -> bool {
        self.default.total_bytes + self.write.total_bytes >= self.sst_max_size
    }

    pub fn need_flush_keys(&self) -> bool {
        self.default.total_bytes + self.write.total_bytes > 0
    }
}

/// A writer writes Raw kv into SST files.
pub struct BackupRawKvWriter {
    name: String,
    cf: CfName,
    writer: Writer,
    limiter: Limiter,
    cipher: CipherInfo,
    codec: KeyValueCodec,
}

impl BackupRawKvWriter {
    /// Create a new BackupRawKvWriter.
    pub fn new(
        db: Arc<DB>,
        name: &str,
        cf: CfNameWrap,
        limiter: Limiter,
        compression_type: Option<SstCompressionType>,
        compression_level: i32,
        cipher: CipherInfo,
        codec: KeyValueCodec,
    ) -> Result<BackupRawKvWriter> {
        let writer = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(cf.into())
            .set_db(RocksEngine::from_ref(&db))
            .set_compression_type(compression_type)
            .set_compression_level(compression_level)
            .build(name)?;
        Ok(BackupRawKvWriter {
            name: name.to_owned(),
            cf: cf.into(),
            writer: Writer::new(writer),
            limiter,
            cipher,
            codec,
        })
    }

    /// Write Kv_pair to buffered SST files.
    pub fn write<I>(&mut self, kv_pairs: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = Result<KvPair>>,
    {
        for kv_pair in kv_pairs {
            let (k, v) = match kv_pair {
                Ok(s) => s,
                Err(e) => {
                    error!("write raw kv"; "error" => ?e);
                    return Err(Error::Other("occur an error when written raw kv".into()));
                }
            };

            self.writer.write(&k, &v)?;
            self.writer.update_raw_with(
                &self.codec.decode_dst_encoded_key(&k)?,
                self.codec.decode_dst_encoded_value(&v)?,
                need_checksum,
            )?;
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub async fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(1);
        if !self.writer.is_empty() {
            let file = self
                .writer
                .save_and_build_file(
                    &self.name,
                    self.cf.into(),
                    self.limiter.clone(),
                    storage,
                    &self.cipher,
                )
                .await?;
            files.push(file);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save_raw"])
            .observe(start.saturating_elapsed().as_secs_f64());
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_traits::Iterable;
    use kvproto::encryptionpb;
    use raftstore::store::util::new_peer;
    use std::collections::BTreeMap;
    use std::path::Path;
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;
    use txn_types::OldValue;

    type CfKvs<'a> = (engine_traits::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(engine_traits::CfName, &Path)], kvs: &[CfKvs<'_>]) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine_traits::CF_DEFAULT, engine_traits::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();

        let opt = engine_rocks::raw::IngestExternalFileOptions::new();
        for (cf, sst) in ssts {
            let handle = db.as_inner().cf_handle(cf).unwrap();
            db.as_inner()
                .ingest_external_file_cf(handle, &opt, &[sst.to_str().unwrap()])
                .unwrap();
        }
        for (cf, kv) in kvs {
            let mut map = BTreeMap::new();
            db.scan_cf(
                cf,
                keys::DATA_MIN_KEY,
                keys::DATA_MAX_KEY,
                false,
                |key, value| {
                    map.insert(key.to_owned(), value.to_owned());
                    Ok(true)
                },
            )
            .unwrap();
            assert_eq!(map.len(), kv.len(), "{} {:?} {:?}", cf, map, kv);
            for (k, v) in *kv {
                assert_eq!(&v.to_vec(), map.get(&k.to_vec()).unwrap());
            }
        }
    }

    #[tokio::test]
    async fn test_writer() {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[
                engine_traits::CF_DEFAULT,
                engine_traits::CF_LOCK,
                engine_traits::CF_WRITE,
            ])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();
        let backend = external_storage_export::make_local_backend(temp.path());
        let storage =
            external_storage_export::create_storage(&backend, Default::default()).unwrap();

        // Test empty file.
        let mut r = kvproto::metapb::Region::default();
        r.set_id(1);
        r.mut_peers().push(new_peer(1, 1));
        let mut writer = BackupWriter::new(
            db.get_sync_db(),
            "foo",
            None,
            0,
            Limiter::new(f64::INFINITY),
            144 * 1024 * 1024,
            {
                let mut ci = CipherInfo::default();
                ci.set_cipher_type(encryptionpb::EncryptionMethod::Plaintext);
                ci
            },
        )
        .unwrap();
        writer.write(vec![].into_iter(), false).unwrap();
        assert!(writer.save(&storage).await.unwrap().is_empty());

        // Test write only txn.
        let mut writer = BackupWriter::new(
            db.get_sync_db(),
            "foo1",
            None,
            0,
            Limiter::new(f64::INFINITY),
            144 * 1024 * 1024,
            {
                let mut ci = CipherInfo::default();
                ci.set_cipher_type(encryptionpb::EncryptionMethod::Plaintext);
                ci
            },
        )
        .unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![], vec![]),
                    write: (vec![b'a'], vec![b'a']),
                    old_value: OldValue::None,
                }]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).await.unwrap();
        assert_eq!(files.len(), 1);
        check_sst(
            &[(
                engine_traits::CF_WRITE,
                &temp.path().join(files[0].get_name()),
            )],
            &[(
                engine_traits::CF_WRITE,
                &[(&keys::data_key(&[b'a']), &[b'a'])],
            )],
        );

        // Test write and default.
        let mut writer = BackupWriter::new(
            db.get_sync_db(),
            "foo2",
            None,
            0,
            Limiter::new(f64::INFINITY),
            144 * 1024 * 1024,
            {
                let mut ci = CipherInfo::default();
                ci.set_cipher_type(encryptionpb::EncryptionMethod::Plaintext);
                ci
            },
        )
        .unwrap();
        writer
            .write(
                vec![
                    TxnEntry::Commit {
                        default: (vec![b'a'], vec![b'a']),
                        write: (vec![b'a'], vec![b'a']),
                        old_value: OldValue::None,
                    },
                    TxnEntry::Commit {
                        default: (vec![], vec![]),
                        write: (vec![b'b'], vec![]),
                        old_value: OldValue::None,
                    },
                ]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).await.unwrap();
        assert_eq!(files.len(), 2);
        check_sst(
            &[
                (
                    engine_traits::CF_DEFAULT,
                    &temp.path().join(files[0].get_name()),
                ),
                (
                    engine_traits::CF_WRITE,
                    &temp.path().join(files[1].get_name()),
                ),
            ],
            &[
                (
                    engine_traits::CF_DEFAULT,
                    &[(&keys::data_key(&[b'a']), &[b'a'])],
                ),
                (
                    engine_traits::CF_WRITE,
                    &[
                        (&keys::data_key(&[b'a']), &[b'a']),
                        (&keys::data_key(&[b'b']), &[]),
                    ],
                ),
            ],
        );
    }
}
