// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, RangeCacheEngine, Result, SstCompressionType, SstExt, SstWriterBuilder,
};

use crate::engine::HybridEngine;

pub struct HybridEngineSstWriteBuilder<EK: SstExt>(EK::SstWriterBuilder);

impl<EK, EC> SstExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type SstReader = EK::SstReader;
    type SstWriter = EK::SstWriter;
    type SstWriterBuilder = HybridEngineSstWriteBuilder<EK>;
}

impl<EK, EC> SstWriterBuilder<HybridEngine<EK, EC>> for HybridEngineSstWriteBuilder<EK>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn new() -> Self {
        HybridEngineSstWriteBuilder(EK::SstWriterBuilder::new())
    }

    fn set_db(self, db: &HybridEngine<EK, EC>) -> Self {
        HybridEngineSstWriteBuilder(self.0.set_db(db.disk_engine()))
    }

    fn set_cf(self, cf: &str) -> Self {
        HybridEngineSstWriteBuilder(self.0.set_cf(cf))
    }

    fn set_in_memory(self, in_memory: bool) -> Self {
        HybridEngineSstWriteBuilder(self.0.set_in_memory(in_memory))
    }

    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        HybridEngineSstWriteBuilder(self.0.set_compression_type(compression))
    }

    fn set_compression_level(self, level: i32) -> Self {
        HybridEngineSstWriteBuilder(self.0.set_compression_level(level))
    }

    fn build(self, path: &str) -> Result<<HybridEngine<EK, EC> as SstExt>::SstWriter> {
        self.0.build(path)
    }
}
