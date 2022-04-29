// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::iterable::Iterable;
use kvproto::import_sstpb::SstMeta;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct SstMetaInfo {
    pub total_bytes: u64,
    pub total_kvs: u64,
    pub meta: SstMeta,
}

pub trait SstExt: Sized {
    type SstReader: SstReader;
    type SstWriter: SstWriter;
    type SstWriterBuilder: SstWriterBuilder<Self>;
}

/// SstReader is used to read an SST file.
pub trait SstReader: Iterable + Sized {
    fn open(path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    // FIXME: Shouldn't this me a method on Iterable?
    fn iter(&self) -> Self::Iterator;
}

/// SstWriter is used to create sst files that can be added to database later.
pub trait SstWriter: Send {
    type ExternalSstFileInfo: ExternalSstFileInfo;
    type ExternalSstFileReader: std::io::Read;

    /// Add key, value to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Add a deletion key to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Return the current file size.
    fn file_size(&mut self) -> u64;

    /// Finalize writing to sst file and close file.
    fn finish(self) -> Result<Self::ExternalSstFileInfo>;

    /// Finalize writing to sst file and read the contents into the buffer.
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)>;
}

// compression type used for write sst file
#[derive(Copy, Clone)]
pub enum SstCompressionType {
    Lz4,
    Snappy,
    Zstd,
}

/// A builder builds a SstWriter.
pub trait SstWriterBuilder<E>
where
    E: SstExt,
{
    /// Create a new SstWriterBuilder.
    fn new() -> Self;

    /// Set DB for the builder. The builder may need some config from the DB.
    #[must_use]
    fn set_db(self, db: &E) -> Self;

    /// Set CF for the builder. The builder may need some config from the CF.
    #[must_use]
    fn set_cf(self, cf: &str) -> Self;

    /// Set it to true, the builder builds a in-memory SST builder.
    #[must_use]
    fn set_in_memory(self, in_memory: bool) -> Self;

    /// set other config specified by writer
    #[must_use]
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self;

    #[must_use]
    fn set_compression_level(self, level: i32) -> Self;

    /// Builder a SstWriter.
    fn build(self, path: &str) -> Result<E::SstWriter>;
}

pub trait ExternalSstFileInfo {
    fn new() -> Self;
    fn file_path(&self) -> PathBuf;
    fn smallest_key(&self) -> &[u8];
    fn largest_key(&self) -> &[u8];
    fn sequence_number(&self) -> u64;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}
