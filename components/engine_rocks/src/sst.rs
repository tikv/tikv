// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, sync::Arc};

use ::encryption::DataKeyManager;
use engine_traits::{
    Error, ExternalSstFileInfo, ExternalSstFileReader, IterOptions, Iterator, RefIterable, Result,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder, CF_DEFAULT,
};
use fail::fail_point;
use file_system::get_io_rate_limiter;
use rocksdb::{
    rocksdb::supported_compression, ColumnFamilyOptions, DBCompressionType, DBIterator, Env,
    EnvOptions, ExternalSstFileInfo as RawExternalSstFileInfo, SequentialFile, SstFileReader,
    SstFileWriter, DB,
};

use crate::{engine::RocksEngine, get_env, options::RocksReadOptions, r2e};

impl SstExt for RocksEngine {
    type SstReader = RocksSstReader;
    type SstWriter = RocksSstWriter;
    type SstWriterBuilder = RocksSstWriterBuilder;
}

pub struct RocksSstReader {
    inner: SstFileReader,
}

impl RocksSstReader {
    pub fn open_with_env(path: &str, env: Option<Arc<Env>>) -> Result<Self> {
        let mut cf_options = ColumnFamilyOptions::new();
        if let Some(env) = env {
            cf_options.set_env(env);
        }
        let mut reader = SstFileReader::new(cf_options);
        reader.open(path).map_err(r2e)?;
        Ok(RocksSstReader { inner: reader })
    }

    pub fn compression_name(&self) -> String {
        let mut result = String::new();
        self.inner.read_table_properties(|p| {
            result = p.compression_name().to_owned();
        });
        result
    }
}

impl SstReader for RocksSstReader {
    fn open(path: &str, mgr: Option<Arc<DataKeyManager>>) -> Result<Self> {
        let env = get_env(mgr, get_io_rate_limiter())?;
        Self::open_with_env(path, Some(env))
    }

    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum().map_err(r2e)
    }

    fn kv_count_and_size(&self) -> (u64, u64) {
        let mut count = 0;
        let mut bytes = 0;
        self.inner.read_table_properties(|p| {
            count = p.num_entries();
            bytes = p.raw_key_size() + p.raw_value_size();
        });
        (count, bytes)
    }
}

impl RefIterable for RocksSstReader {
    type Iterator<'a> = RocksSstIterator<'a>;

    #[inline]
    fn iter(&self, opts: IterOptions) -> Result<Self::Iterator<'_>> {
        let opt: RocksReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(RocksSstIterator(SstFileReader::iter_opt(&self.inner, opt)))
    }
}

pub struct RocksSstIterator<'a>(DBIterator<&'a SstFileReader>);

// It's OK to send the iterator around.
// TODO: remove this when using tirocks.
unsafe impl Send for RocksSstIterator<'_> {}

impl Iterator for RocksSstIterator<'_> {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Key(key)).map_err(r2e)
    }

    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        self.0
            .seek_for_prev(rocksdb::SeekKey::Key(key))
            .map_err(r2e)
    }

    /// Seek to the first key in the database.
    fn seek_to_first(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::Start).map_err(r2e)
    }

    /// Seek to the last key in the database.
    fn seek_to_last(&mut self) -> Result<bool> {
        self.0.seek(rocksdb::SeekKey::End).map_err(r2e)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.prev().map_err(r2e)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(r2e("Iterator invalid"));
        }
        self.0.next().map_err(r2e)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(r2e)
    }
}

pub struct RocksSstWriterBuilder {
    cf: Option<String>,
    db: Option<Arc<DB>>,
    in_memory: bool,
    compression_type: Option<DBCompressionType>,
    compression_level: i32,
}

impl SstWriterBuilder<RocksEngine> for RocksSstWriterBuilder {
    fn new() -> Self {
        RocksSstWriterBuilder {
            cf: None,
            in_memory: false,
            db: None,
            compression_type: None,
            compression_level: 0,
        }
    }

    fn set_db(mut self, db: &RocksEngine) -> Self {
        self.db = Some(db.as_inner().clone());
        self
    }

    fn set_cf(mut self, cf: &str) -> Self {
        self.cf = Some(cf.to_string());
        self
    }

    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.compression_type = compression.map(to_rocks_compression_type);
        self
    }

    fn set_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    fn build(self, path: &str) -> Result<RocksSstWriter> {
        let mut env = None;
        let mut io_options = if let Some(db) = self.db.as_ref() {
            env = db.env();
            let handle = db
                .cf_handle(self.cf.as_deref().unwrap_or(CF_DEFAULT))
                .ok_or_else(|| r2e(format!("CF {:?} is not found", self.cf)))?;
            db.get_options_cf(handle)
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
        let compress_type = if let Some(ct) = self.compression_type {
            let all_supported_compression = supported_compression();
            if !all_supported_compression.contains(&ct) {
                return Err(Error::Other(
                    format!(
                        "compression type '{}' is not supported by rocksdb",
                        fmt_db_compression_type(ct)
                    )
                    .into(),
                ));
            }
            ct
        } else {
            get_fastest_supported_compression_type()
        };
        // TODO: 0 is a valid value for compression_level
        if self.compression_level != 0 {
            // other 4 fields are default value.
            io_options.set_compression_options(
                -14,
                self.compression_level,
                0, // strategy
                0, // max_dict_bytes
                0, // zstd_max_train_bytes
                1, // parallel_threads
            );
        }
        io_options.compression(compress_type);
        // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        fail_point!("on_open_sst_writer");
        writer.open(path).map_err(r2e)?;
        Ok(RocksSstWriter { writer, env })
    }
}

pub struct RocksSstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

pub struct ResetSeqFile {
    env: Arc<Env>,
    path: String,
    state: SequentialFile,
}

impl std::io::Read for ResetSeqFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.state.read(buf)
    }
}

impl ExternalSstFileReader for ResetSeqFile {
    fn reset(&mut self) -> Result<()> {
        self.state = self
            .env
            .new_sequential_file(&self.path, EnvOptions::new())
            .map_err(r2e)?;
        Ok(())
    }
}

impl SstWriter for RocksSstWriter {
    type ExternalSstFileInfo = RocksExternalSstFileInfo;
    type ExternalSstFileReader = ResetSeqFile;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.writer.put(key, val).map_err(r2e)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.writer.delete(key).map_err(r2e)
    }

    fn file_size(&mut self) -> u64 {
        self.writer.file_size()
    }

    fn finish(mut self) -> Result<Self::ExternalSstFileInfo> {
        Ok(RocksExternalSstFileInfo(self.writer.finish().map_err(r2e)?))
    }

    fn finish_read(mut self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        let env = self
            .env
            .take()
            .ok_or_else(|| r2e("failed to read sequential file no env provided"))?;
        let sst_info = self.writer.finish().map_err(r2e)?;
        let p = sst_info.file_path();
        let path = p.as_os_str().to_str().ok_or_else(|| {
            r2e(format!(
                "failed to sequential file bad path {}",
                p.display()
            ))
        })?;
        let seq_file = env
            .new_sequential_file(path, EnvOptions::new())
            .map_err(r2e)?;
        let reset_file = ResetSeqFile {
            env,
            path: path.to_owned(),
            state: seq_file,
        };
        Ok((RocksExternalSstFileInfo(sst_info), reset_file))
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

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

fn fmt_db_compression_type(ct: DBCompressionType) -> &'static str {
    match ct {
        DBCompressionType::Lz4 => "lz4",
        DBCompressionType::Snappy => "snappy",
        DBCompressionType::Zstd => "zstd",
        _ => unreachable!(),
    }
}

fn to_rocks_compression_type(ct: SstCompressionType) -> DBCompressionType {
    match ct {
        SstCompressionType::Lz4 => DBCompressionType::Lz4,
        SstCompressionType::Snappy => DBCompressionType::Snappy,
        SstCompressionType::Zstd => DBCompressionType::Zstd,
    }
}

pub fn from_rocks_compression_type(ct: DBCompressionType) -> Option<SstCompressionType> {
    match ct {
        DBCompressionType::Lz4 => Some(SstCompressionType::Lz4),
        DBCompressionType::Snappy => Some(SstCompressionType::Snappy),
        DBCompressionType::Zstd => Some(SstCompressionType::Zstd),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::Builder;

    use super::*;
    use crate::util::new_default_engine;

    #[test]
    fn test_smoke() {
        let path = Builder::new().tempdir().unwrap();
        let engine = new_default_engine(path.path().to_str().unwrap()).unwrap();
        let (k, v) = (b"foo", b"bar");

        let p = path.path().join("sst");
        let mut writer = RocksSstWriterBuilder::new()
            .set_cf(CF_DEFAULT)
            .set_db(&engine)
            .build(p.to_str().unwrap())
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
            .build(p.to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let (sst_file, mut reader) = writer.finish_read().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len() as u64, sst_file.file_size());
        // There must not be a file in disk.
        std::fs::metadata(p).unwrap_err();
    }
}
