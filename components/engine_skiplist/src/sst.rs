// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_rocks::{
    RocksExternalSstFileInfo, RocksSstReader, RocksSstWriter, RocksSstWriterBuilder,
};
use engine_traits::{
    CfName, ExternalSstFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    SstCompressionType, SstExt, SstReader, SstWriter, SstWriterBuilder,
};
use std::path::PathBuf;

impl SstExt for SkiplistEngine {
    type SstReader = RocksSstReader;
    type SstWriter = RocksSstWriter;
    type SstWriterBuilder = SkiplistSstWriterBuilder;
}

pub struct SkiplistSstWriterBuilder {
    cf: Option<CfName>,
}

impl SstWriterBuilder<SkiplistEngine> for SkiplistSstWriterBuilder {
    fn new() -> Self {
        SkiplistSstWriterBuilder { cf: None }
    }
    fn set_db(self, db: &SkiplistEngine) -> Self {
        self
    }
    fn set_cf(mut self, cf: CfName) -> Self {
        self.cf = Some(cf);
        self
    }
    fn set_in_memory(self, in_memory: bool) -> Self {
        self
    }
    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        self
    }

    fn build(self, path: &str) -> Result<RocksSstWriter> {
        if let Some(cf) = self.cf {
            RocksSstWriterBuilder::new().set_cf(cf).build(path)
        } else {
            RocksSstWriterBuilder::new().build(path)
        }
    }
    fn set_compression_level(self, level: i32) -> Self {
        self
    }
}
