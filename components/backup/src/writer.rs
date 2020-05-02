// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine::DB;
use engine_rocks::{RocksEngine, RocksSstWriter, RocksSstWriterBuilder};
use engine_traits::{CfName, CF_DEFAULT, CF_WRITE};
use engine_traits::{ExternalSstFileInfo, SstWriter, SstWriterBuilder};
use external_storage::ExternalStorage;
use futures_util::io::AllowStdIo;
use kvproto::backup::File;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::storage::txn::TxnEntry;
use tikv_util::{self, box_err, file::Sha256Reader, time::Limiter};
use txn_types::KvPair;

use crate::metrics::*;
use crate::{Error, Result};

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

    fn save_and_build_file(
        self,
        name: &str,
        cf: &'static str,
        limiter: Limiter,
        storage: &dyn ExternalStorage,
    ) -> Result<File> {
        let (sst_info, sst_reader) = self.writer.finish_read()?;
        BACKUP_RANGE_SIZE_HISTOGRAM_VEC
            .with_label_values(&[cf])
            .observe(sst_info.file_size() as f64);
        let file_name = format!("{}_{}.sst", name, cf);

        let (reader, hasher) = Sha256Reader::new(sst_reader)
            .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
        storage.write(
            &file_name,
            Box::new(limiter.limit(AllowStdIo::new(reader))),
            sst_info.file_size(),
        )?;
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
        file.set_cf(cf.to_owned());
        file.set_size(sst_info.file_size());
        Ok(file)
    }

    fn is_empty(&self) -> bool {
        self.total_kvs == 0
    }
}

/// A writer writes txn entries into SST files.
pub struct BackupWriter {
    name: String,
    default: Writer,
    write: Writer,
    limiter: Limiter,
}

impl BackupWriter {
    /// Create a new BackupWriter.
    pub fn new(db: Arc<DB>, name: &str, limiter: Limiter) -> Result<BackupWriter> {
        let default = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(RocksEngine::from_ref(&db))
            .build(name)?;
        let write = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_WRITE)
            .set_db(RocksEngine::from_ref(&db))
            .build(name)?;
        let name = name.to_owned();
        Ok(BackupWriter {
            name,
            default: Writer::new(default),
            write: Writer::new(write),
            limiter,
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
                TxnEntry::Commit { default, write } => {
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
    pub fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(2);
        let write_written = !self.write.is_empty() || !self.default.is_empty();
        if !self.default.is_empty() {
            // Save default cf contents.
            let default = self.default.save_and_build_file(
                &self.name,
                CF_DEFAULT,
                self.limiter.clone(),
                storage,
            )?;
            files.push(default);
        }
        if write_written {
            // Save write cf contents.
            let write = self.write.save_and_build_file(
                &self.name,
                CF_WRITE,
                self.limiter.clone(),
                storage,
            )?;
            files.push(write);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save"])
            .observe(start.elapsed().as_secs_f64());
        Ok(files)
    }
}

/// A writer writes Raw kv into SST files.
pub struct BackupRawKVWriter {
    name: String,
    cf: CfName,
    writer: Writer,
    limiter: Limiter,
}

impl BackupRawKVWriter {
    /// Create a new BackupRawKVWriter.
    pub fn new(db: Arc<DB>, name: &str, cf: CfName, limiter: Limiter) -> Result<BackupRawKVWriter> {
        let writer = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(cf)
            .set_db(RocksEngine::from_ref(&db))
            .build(name)?;
        Ok(BackupRawKVWriter {
            name: name.to_owned(),
            cf,
            writer: Writer::new(writer),
            limiter,
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

            assert!(!k.is_empty());
            self.writer.write(&k, &v)?;
            self.writer.update_raw_with(&k, &v, need_checksum)?;
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub fn save(self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let start = Instant::now();
        let mut files = Vec::with_capacity(1);
        if !self.writer.is_empty() {
            let file = self.writer.save_and_build_file(
                &self.name,
                self.cf,
                self.limiter.clone(),
                storage,
            )?;
            files.push(file);
        }
        BACKUP_RANGE_HISTOGRAM_VEC
            .with_label_values(&["save_raw"])
            .observe(start.elapsed().as_secs_f64());
        Ok(files)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_rocks::Compat;
    use engine_traits::Iterable;
    use std::collections::BTreeMap;
    use std::f64::INFINITY;
    use std::path::Path;
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;

    type CfKvs<'a> = (engine_traits::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(engine_traits::CfName, &Path)], kvs: &[CfKvs]) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine_traits::CF_DEFAULT, engine_traits::CF_WRITE])
            .build()
            .unwrap();
        let db = rocks.get_rocksdb();

        let opt = engine::rocks::IngestExternalFileOptions::new();
        for (cf, sst) in ssts {
            let handle = db.cf_handle(cf).unwrap();
            db.ingest_external_file_cf(handle, &opt, &[sst.to_str().unwrap()])
                .unwrap();
        }
        for (cf, kv) in kvs {
            let mut map = BTreeMap::new();
            db.c()
                .scan_cf(
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

    #[test]
    fn test_writer() {
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
        let backend = external_storage::make_local_backend(temp.path());
        let storage = external_storage::create_storage(&backend).unwrap();

        // Test empty file.
        let mut writer = BackupWriter::new(db.clone(), "foo", Limiter::new(INFINITY)).unwrap();
        writer.write(vec![].into_iter(), false).unwrap();
        assert!(writer.save(&storage).unwrap().is_empty());

        // Test write only txn.
        let mut writer = BackupWriter::new(db.clone(), "foo1", Limiter::new(INFINITY)).unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![], vec![]),
                    write: (vec![b'a'], vec![b'a']),
                }]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).unwrap();
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
        let mut writer = BackupWriter::new(db, "foo2", Limiter::new(INFINITY)).unwrap();
        writer
            .write(
                vec![
                    TxnEntry::Commit {
                        default: (vec![b'a'], vec![b'a']),
                        write: (vec![b'a'], vec![b'a']),
                    },
                    TxnEntry::Commit {
                        default: (vec![], vec![]),
                        write: (vec![b'b'], vec![]),
                    },
                ]
                .into_iter(),
                false,
            )
            .unwrap();
        let files = writer.save(&storage).unwrap();
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
