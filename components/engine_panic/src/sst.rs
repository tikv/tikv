// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{marker::PhantomData, path::PathBuf};

use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, RefIterable, Result,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
};

use crate::engine::PanicEngine;

impl SstExt for PanicEngine {
    type SstReader = PanicSstReader;
    type SstWriter = PanicSstWriter;
    type SstWriterBuilder = PanicSstWriterBuilder;
}

pub struct PanicSstReader;

impl SstReader for PanicSstReader {
    fn open(path: &str) -> Result<Self> {
        panic!()
    }
    fn verify_checksum(&self) -> Result<()> {
        panic!()
    }
}

impl RefIterable for PanicSstReader {
    type Iterator<'a> = PanicSstReaderIterator<'a>;

    fn iter(&self, opts: IterOptions) -> Result<Self::Iterator<'_>> {
        panic!()
    }
}

pub struct PanicSstReaderIterator<'a> {
    _phantom: PhantomData<&'a ()>,
}

impl Iterator for PanicSstReaderIterator<'_> {
    fn seek(&mut self, key: &[u8]) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: &[u8]) -> Result<bool> {
        panic!()
    }

    fn seek_to_first(&mut self) -> Result<bool> {
        panic!()
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}

pub struct PanicSstWriter;

impl SstWriter for PanicSstWriter {
    type ExternalSstFileInfo = PanicExternalSstFileInfo;
    type ExternalSstFileReader = PanicExternalSstFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::ExternalSstFileInfo> {
        panic!()
    }
    fn finish_read(self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        panic!()
    }
}

pub struct PanicSstWriterBuilder;

impl SstWriterBuilder<PanicEngine> for PanicSstWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, db: &PanicEngine) -> Self {
        panic!()
    }
    fn set_cf(self, cf: &str) -> Self {
        panic!()
    }
    fn set_in_memory(self, in_memory: bool) -> Self {
        panic!()
    }
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        panic!()
    }
    fn set_compression_level(self, level: i32) -> Self {
        panic!()
    }

    fn build(self, path: &str) -> Result<PanicSstWriter> {
        panic!()
    }
}

pub struct PanicExternalSstFileInfo;

impl ExternalSstFileInfo for PanicExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        panic!()
    }
    fn smallest_key(&self) -> &[u8] {
        panic!()
    }
    fn largest_key(&self) -> &[u8] {
        panic!()
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        panic!()
    }
    fn num_entries(&self) -> u64 {
        panic!()
    }
}

pub struct PanicExternalSstFileReader;

impl std::io::Read for PanicExternalSstFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
