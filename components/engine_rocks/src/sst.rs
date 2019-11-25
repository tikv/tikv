// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::RocksEngine;
use crate::options::RocksReadOptions;
use engine_traits::Error;
use engine_traits::IterOptions;
use engine_traits::{CfName, CF_DEFAULT};
use engine_traits::{ExternalSstFileInfo, SstWriter, SstWriterBuilder};
use engine_traits::{Iterable, Result, SstExt, SstReader};
use engine_traits::{Iterator, SeekKey};
use rocksdb::DBCompressionType;
use rocksdb::DBIterator;
use rocksdb::ExternalSstFileInfo as RawExternalSstFileInfo;
use rocksdb::DB;
use rocksdb::{ColumnFamilyOptions, SstFileReader};
use rocksdb::{Env, EnvOptions, SstFileWriter};
use std::rc::Rc;
use std::sync::Arc;
// FIXME: Move RocksSeekKey into a common module since
// it's shared between multiple iterators
use crate::engine_iterator::RocksSeekKey;
use engine::rocks::util::get_fastest_supported_compression_type;
use std::path::PathBuf;

impl SstExt for RocksEngine {
    type SstReader = RocksSstReader;
    type SstWriter = RocksSstWriter;
    type SstWriterBuilder = RocksSstWriterBuilder;
}

// FIXME: like in RocksEngineIterator and elsewhere, here we are using
// Rc to avoid putting references in an associated type, which
// requires generic associated types.
pub struct RocksSstReader {
    inner: Rc<SstFileReader>,
}

impl SstReader for RocksSstReader {
    fn open(path: &str) -> Result<Self> {
        let mut reader = SstFileReader::new(ColumnFamilyOptions::new());
        reader.open(path)?;
        let inner = Rc::new(reader);
        Ok(RocksSstReader { inner })
    }
    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iterator {
        RocksSstIterator(SstFileReader::iter_rc(self.inner.clone()))
    }
}

impl Iterable for RocksSstReader {
    type Iterator = RocksSstIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(RocksSstIterator(SstFileReader::iter_opt_rc(
            self.inner.clone(),
            opt,
        )))
    }

    fn iterator_cf_opt(&self, _cf: &str, _opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!() // FIXME: What should happen here?
    }
}

// FIXME: See comment on RocksSstReader for why this contains Rc
pub struct RocksSstIterator(DBIterator<Rc<SstFileReader>>);

impl Iterator for RocksSstIterator {
    fn seek(&mut self, key: SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek(k.into_raw())
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw())
    }

    fn prev(&mut self) -> bool {
        self.0.prev()
    }

    fn next(&mut self) -> bool {
        self.0.next()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn status(&self) -> Result<()> {
        self.0.status().map_err(Error::Engine)
    }
}

pub struct RocksSstWriterBuilder {
    cf: Option<CfName>,
    db: Option<Arc<DB>>,
    in_memory: bool,
}

impl SstWriterBuilder for RocksSstWriterBuilder {
    type KvEngine = RocksEngine;
    type SstWriter = RocksSstWriter;

    fn new() -> Self {
        RocksSstWriterBuilder {
            cf: None,
            in_memory: false,
            db: None,
        }
    }

    fn set_db(mut self, db: &Self::KvEngine) -> Self {
        self.db = Some(db.as_inner().clone());
        self
    }

    fn set_cf(mut self, cf: CfName) -> Self {
        self.cf = Some(cf);
        self
    }

    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    fn build(self, path: &str) -> Result<Self::SstWriter> {
        let mut env = None;
        let mut io_options = if let Some(db) = self.db.as_ref() {
            env = db.env();
            let handle = db
                .cf_handle(self.cf.unwrap_or(CF_DEFAULT))
                .ok_or_else(|| format!("CF {:?} is not found", self.cf))?;
            db.get_options_cf(handle).clone()
        } else {
            ColumnFamilyOptions::new()
        };
        if self.in_memory {
            // Set memenv.
            let mem_env = Arc::new(Env::new_mem());
            io_options.set_env(mem_env.clone());
            env = Some(mem_env);
        } else if let Some(env) = env.as_ref() {
            io_options.set_env(env.clone());
        }
        io_options.compression(get_fastest_supported_compression_type());
        // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        writer.open(path)?;
        Ok(RocksSstWriter { writer, env })
    }
}

pub struct RocksSstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

impl SstWriter for RocksSstWriter {
    type ExternalSstFileInfo = RocksExternalSstFileInfo;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.writer.put(key, val)?)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.writer.delete(key)?)
    }

    fn file_size(&mut self) -> u64 {
        self.writer.file_size()
    }

    fn finish(mut self) -> Result<Self::ExternalSstFileInfo> {
        Ok(RocksExternalSstFileInfo(self.writer.finish()?))
    }

    fn finish_into(mut self, buf: &mut Vec<u8>) -> Result<Self::ExternalSstFileInfo> {
        use std::io::Read;
        if let Some(env) = self.env.take() {
            let sst_info = self.writer.finish()?;
            let p = sst_info.file_path();
            let path = p.as_os_str().to_str().ok_or_else(|| {
                Error::Engine(format!(
                    "failed to sequential file bad path {}",
                    sst_info.file_path().display()
                ))
            })?;
            let mut seq_file = env.new_sequential_file(path, EnvOptions::new())?;
            let len = seq_file
                .read_to_end(buf)
                .map_err(|e| Error::Engine(format!("failed to read sequential file {:?}", e)))?;
            if len as u64 != sst_info.file_size() {
                Err(Error::Engine(format!(
                    "failed to read sequential file inconsistent length {} != {}",
                    len,
                    sst_info.file_size()
                )))
            } else {
                Ok(RocksExternalSstFileInfo(sst_info))
            }
        } else {
            Err(Error::Engine(
                "failed to read sequential file no env provided".to_owned(),
            ))
        }
    }
}

pub struct RocksExternalSstFileInfo(RawExternalSstFileInfo);

impl ExternalSstFileInfo for RocksExternalSstFileInfo {
    fn new() -> Self {
        RocksExternalSstFileInfo(RawExternalSstFileInfo::new())
    }

    fn file_path(&self) -> PathBuf {
        self.0.file_path()
    }

    fn smallest_key(&self) -> &[u8] {
        self.0.smallest_key()
    }

    fn largest_key(&self) -> &[u8] {
        self.0.largest_key()
    }

    fn sequence_number(&self) -> u64 {
        self.0.sequence_number()
    }

    fn file_size(&self) -> u64 {
        self.0.file_size()
    }

    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::new_default_engine;
    use tempfile::Builder;

    #[test]
    fn test_smoke() {
        let path = Builder::new().tempdir().unwrap();
        let engine = new_default_engine(path.path().to_str().unwrap()).unwrap();
        let (k, v) = (b"foo", b"bar");

        let p = path.path().join("sst");
        let mut writer = RocksSstWriterBuilder::new()
            .set_cf(CF_DEFAULT)
            .set_db(&engine)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let sst_file = writer.finish().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        // There must be a file in disk.
        std::fs::metadata(p).unwrap();

        // Test in-memory sst writer.
        let p = path.path().join("inmem.sst");
        let mut writer = RocksSstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(&engine)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let sst_file = writer.finish_into(&mut buf).unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        assert_eq!(buf.len() as u64, sst_file.file_size());
        // There must not be a file in disk.
        std::fs::metadata(p).unwrap_err();
    }
}
