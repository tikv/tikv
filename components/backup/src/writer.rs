// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine::rocks::{SstWriter, SstWriterBuilder};
use engine::{CF_DEFAULT, CF_WRITE, DB};
use external_storage::ExternalStorage;
use futures_util::io::AllowStdIo;
use kvproto::backup::File;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::raftstore::store::keys;
use tikv::storage::txn::TxnEntry;
use tikv_util::{self, box_err, file::Sha256Reader, time::Limiter};

use crate::metrics::*;
use crate::{Error, Result};

struct Writer {
    writer: SstWriter,
    total_kvs: u64,
    total_bytes: u64,
    checksum: u64,
    digest: crc64fast::Digest,
}

impl Writer {
    fn new(writer: SstWriter) -> Self {
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

        let mut file = File::new();
        file.set_name(file_name);
        file.set_sha256(sha256);
        file.set_crc64xor(self.checksum);
        file.set_total_kvs(self.total_kvs);
        file.set_total_bytes(self.total_bytes);
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
        let default = SstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(db.clone())
            .build(name)?;
        let write = SstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_WRITE)
            .set_db(db.clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use engine::Iterable;
    use std::collections::BTreeMap;
    use std::f64::INFINITY;
    use std::path::Path;
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;

    type CfKvs<'a> = (engine::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(engine::CfName, &Path)], kvs: &[CfKvs<'_>]) {
        let temp = TempDir::new().unwrap();
        let rocks = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(&[engine::CF_DEFAULT, engine::CF_WRITE])
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
            assert_eq!(map.len(), kv.len(), "{:?} {:?}", map, kv);
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
            .cfs(&[engine::CF_DEFAULT, engine::CF_LOCK, engine::CF_WRITE])
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
            &[(engine::CF_WRITE, &temp.path().join(files[0].get_name()))],
            &[(engine::CF_WRITE, &[(&keys::data_key(&[b'a']), &[b'a'])])],
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
                (engine::CF_DEFAULT, &temp.path().join(files[0].get_name())),
                (engine::CF_WRITE, &temp.path().join(files[1].get_name())),
            ],
            &[
                (
                    engine::CF_DEFAULT,
                    &[
                        (&keys::data_key(&[b'a']), &[b'a']),
                        (&keys::data_key(&[]), &[]),
                    ],
                ),
                (
                    engine::CF_WRITE,
                    &[
                        (&keys::data_key(&[b'a']), &[b'a']),
                        (&keys::data_key(&[b'b']), &[]),
                    ],
                ),
            ],
        );
    }
}
