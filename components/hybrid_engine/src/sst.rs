// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, RangeCacheEngine, Result, SstCompressionType, SstExt, SstWriterBuilder,
};

use crate::engine::HybridEngine;

pub struct HybridEngineSstWriteBuilder {}

impl<EK, EC> SstExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    type SstReader = EK::SstReader;
    type SstWriter = EK::SstWriter;
    type SstWriterBuilder = HybridEngineSstWriteBuilder;
}

impl<EK, EC> SstWriterBuilder<HybridEngine<EK, EC>> for HybridEngineSstWriteBuilder
where
    EK: KvEngine,
    EC: RangeCacheEngine,
{
    fn new() -> Self {
        unimplemented!()
    }

    fn set_db(self, _db: &HybridEngine<EK, EC>) -> Self {
        unimplemented!()
    }

    fn set_cf(self, _cf: &str) -> Self {
        unimplemented!()
    }

    fn set_in_memory(self, _in_memory: bool) -> Self {
        unimplemented!()
    }

    fn set_compression_type(self, _compression: Option<SstCompressionType>) -> Self {
        unimplemented!()
    }

    fn set_compression_level(self, level: i32) -> Self {
        unimplemented!()
    }

    fn build(self, _path: &str) -> Result<<HybridEngine<EK, EC> as SstExt>::SstWriter> {
        unimplemented!()
    }
}
