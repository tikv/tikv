// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::Instant;

use engine::rocks::{SstWriter, SstWriterBuilder};
use engine::{CF_DEFAULT, CF_WRITE, DB};
use external_storage::ExternalStorage;
use kvproto::backup::File;
use tikv::raftstore::store::keys;
use tikv::storage::txn::TxnEntry;
use tikv_util;

use crate::metrics::*;
use crate::{Error, Result};

/// A writer writes txn entries into SST files.
pub struct BackupWriter {
    name: String,
    default: SstWriter,
    default_written: bool,
    write: SstWriter,
    write_written: bool,
}

impl BackupWriter {
    /// Create a new BackupWriter.
    pub fn new(db: Arc<DB>, name: &str) -> Result<BackupWriter> {
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
            default_written: false,
            write,
            write_written: false,
        })
    }

    /// Wrtie entries to buffered SST files.
    pub fn write<I>(&mut self, entries: I) -> Result<()>
    where
        I: Iterator<Item = TxnEntry>,
    {
        for e in entries {
            match e {
                TxnEntry::Commit { default, write } => {
                    // Default may be empty if value is small.
                    if !default.0.is_empty() {
                        // HACK: The actual key stored in TiKV is called
                        // data_key and always prefix a `z`. But iterator strips
                        // it, we need to add the prefix manually.
                        let data_key_default = keys::data_key(&default.0);
                        self.default.put(&data_key_default, &default.1)?;
                        self.default_written = true;
                    }
                    assert!(!write.0.is_empty());
                    let data_key_write = keys::data_key(&write.0);
                    self.write.put(&data_key_write, &write.1)?;
                    self.write_written = true;
                }
                TxnEntry::Prewrite { .. } => {
                    return Err(Error::Other("prewrite is not supported".into()))
                }
            }
        }
        Ok(())
    }

    /// Save buffered SST files to the given external storage.
    pub fn save(mut self, storage: &dyn ExternalStorage) -> Result<Vec<File>> {
        let name = self.name;
        let save_and_build_file = |cf, mut contents: &[u8]| -> Result<File> {
            BACKUP_RANGE_SIZE_HISTOGRAM_VEC
                .with_label_values(&[cf])
                .observe(contents.len() as _);
            let name = format!("{}_{}.sst", name, cf);
            let checksum = tikv_util::file::calc_crc32_bytes(contents);
            storage.write(&name, &mut contents as &mut dyn std::io::Read)?;
            let mut file = File::new();
            file.set_crc32(checksum);
            file.set_name(name);
            Ok(file)
        };
        let start = Instant::now();
        let mut files = Vec::with_capacity(2);
        let mut buf = Vec::new();
        if self.default_written {
            // Save default cf contents.
            buf.reserve(self.default.file_size() as _);
            self.default.finish_into(&mut buf)?;
            let default = save_and_build_file(CF_DEFAULT, &mut buf)?;
            files.push(default);
            buf.clear();
        }
        if self.write_written {
            // Save write cf contents.
            buf.reserve(self.write.file_size() as _);
            self.write.finish_into(&mut buf)?;
            let write = save_and_build_file(CF_WRITE, &mut buf)?;
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
        let mut writer = BackupWriter::new(db.clone(), "foo").unwrap();
        writer.write(vec![].into_iter()).unwrap();
        assert!(writer.save(&storage).unwrap().is_empty());

        // Test write only txn.
        let mut writer = BackupWriter::new(db.clone(), "foo1").unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![], vec![]),
                    write: (vec![b'a'], vec![b'a']),
                }]
                .into_iter(),
            )
            .unwrap();
        let files = writer.save(&storage).unwrap();
        assert_eq!(files.len(), 1);
        check_sst(
            &[(engine::CF_WRITE, &temp.path().join(files[0].get_name()))],
            &[(engine::CF_WRITE, &[(&keys::data_key(&[b'a']), &[b'a'])])],
        );

        // Test write and default.
        let mut writer = BackupWriter::new(db.clone(), "foo2").unwrap();
        writer
            .write(
                vec![TxnEntry::Commit {
                    default: (vec![b'a'], vec![b'a']),
                    write: (vec![b'a'], vec![b'a']),
                }]
                .into_iter(),
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
