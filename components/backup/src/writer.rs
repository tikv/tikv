// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine::rocks::util::io_limiter::{IOLimiter, LimitReader};
use engine::rocks::{SstWriter, SstWriterBuilder};
use engine::{CF_DEFAULT, CF_WRITE, DB};
use external_storage::ExternalStorage;
use kvproto::backup::File;
use tikv::coprocessor::checksum_crc64_xor;
use tikv::raftstore::store::keys;
use tikv::storage::txn::TxnEntry;
use tikv_util::{self, box_err};

use crate::metrics::*;
use crate::{Error, Result};

/// A writer writes txn entries into SST files.
pub struct BackupWriter {
    name: String,
    default: SstWriter,
    default_kvs: u64,
    default_bytes: u64,
    default_checksum: u64,
    write: SstWriter,
    write_kvs: u64,
    write_bytes: u64,
    write_checksum: u64,

    limiter: Option<Arc<IOLimiter>>,
}

impl BackupWriter {
    /// Create a new BackupWriter.
    pub fn new(db: Arc<DB>, name: &str, limiter: Option<Arc<IOLimiter>>) -> Result<BackupWriter> {
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
            default,
            default_kvs: 0,
            default_bytes: 0,
            default_checksum: 0,
            write,
            write_kvs: 0,
            write_bytes: 0,
            write_checksum: 0,
            limiter,
        })
    }

    /// Wrtie entries to buffered SST files.
    pub fn write<I>(&mut self, entries: I, need_checksum: bool) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for e in entries {
            let mut value_in_default = Some(false);
            match &e {
                TxnEntry::Commit { default, write } => {
                    // Default may be empty if value is small.
                    if !default.0.is_empty() {
                        // HACK: The actual key stored in TiKV is called
                        // data_key and always prefix a `z`. But iterator strips
                        // it, we need to add the prefix manually.
                        let data_key_default = keys::data_key(&default.0);
                        self.default.put(&data_key_default, &default.1)?;
                        value_in_default.replace(true);
                    }
                    assert!(!write.0.is_empty());
                    let data_key_write = keys::data_key(&write.0);
                    self.write.put(&data_key_write, &write.1)?;
                }
                TxnEntry::Prewrite { .. } => {
                    value_in_default.take();
                    return Err(Error::Other("prewrite is not supported".into()));
                }
            }
            match value_in_default {
                Some(true) => {
                    self.default_kvs += 1;
                    if !need_checksum {
                        continue;
                    }
                    let (k, v) = e
                        .into_kvpair()
                        .map_err(|err| Error::Other(box_err!("Decode error: {:?}", err)))?;
                    self.default_bytes += (k.len() + v.len()) as u64;
                    self.default_checksum = checksum_crc64_xor(self.default_checksum, &k, &v);
                }
                Some(false) => {
                    self.write_kvs += 1;
                    if !need_checksum {
                        continue;
                    }
                    let (k, v) = e
                        .into_kvpair()
                        .map_err(|err| Error::Other(box_err!("Decode error: {:?}", err)))?;
                    self.write_bytes += (k.len() + v.len()) as u64;
                    self.write_checksum = checksum_crc64_xor(self.write_checksum, &k, &v);
                }
                None => {}
            }
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub fn save(mut self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let name = self.name;
        let save_and_build_file =
            |cf, mut contents: &[u8], limiter: Option<Arc<IOLimiter>>| -> Result<File> {
                BACKUP_RANGE_SIZE_HISTOGRAM_VEC
                    .with_label_values(&[cf])
                    .observe(contents.len() as _);
                let name = format!("{}_{}.sst", name, cf);
                let checksum = tikv_util::file::sha256(&contents)
                    .map_err(|e| Error::Other(box_err!("Sha256 error: {:?}", e)))?;
                let mut limit_reader = LimitReader::new(limiter, &mut contents);
                storage.write(&name, &mut limit_reader)?;
                let mut file = File::new();
                file.set_sha256(checksum);
                file.set_name(name);
                Ok(file)
            };
        let start = Instant::now();
        let mut files = Vec::with_capacity(2);
        let mut buf = Vec::new();
        if self.default_kvs != 0 {
            // Save default cf contents.
            buf.reserve(self.default.file_size() as _);
            self.default.finish_into(&mut buf)?;
            let mut default = save_and_build_file(CF_DEFAULT, &mut buf, self.limiter.clone())?;
            default.set_crc64xor(self.default_checksum);
            default.set_total_kvs(self.default_kvs);
            default.set_total_bytes(self.default_bytes);
            files.push(default);
            buf.clear();
        }
        if self.write_kvs != 0 || self.default_kvs != 0 {
            // Save write cf contents.
            buf.reserve(self.write.file_size() as _);
            self.write.finish_into(&mut buf)?;
            let mut write = save_and_build_file(CF_WRITE, &mut buf, self.limiter)?;
            write.set_crc64xor(self.write_checksum);
            write.set_total_kvs(self.write_kvs);
            write.set_total_bytes(self.write_bytes);
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
    use std::path::Path;
    use tempfile::TempDir;
    use tikv::storage::TestEngineBuilder;

    type CfKvs<'a> = (engine::CfName, &'a [(&'a [u8], &'a [u8])]);

    fn check_sst(ssts: &[(engine::CfName, &Path)], kvs: &[CfKvs]) {
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
        let storage =
            external_storage::create_storage(&format!("local://{}", temp.path().display()))
                .unwrap();

        // Test empty file.
        let mut writer = BackupWriter::new(db.clone(), "foo", None).unwrap();
        writer.write(vec![].into_iter(), false).unwrap();
        assert!(writer.save(&storage).unwrap().is_empty());

        // Test write only txn.
        let mut writer = BackupWriter::new(db.clone(), "foo1", None).unwrap();
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
        let mut writer = BackupWriter::new(db.clone(), "foo2", None).unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![b'a'], vec![b'a']),
                    write: (vec![b'a'], vec![b'a']),
                }]
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
                (engine::CF_DEFAULT, &[(&keys::data_key(&[b'a']), &[b'a'])]),
                (engine::CF_WRITE, &[(&keys::data_key(&[b'a']), &[b'a'])]),
            ],
        );
    }
}
