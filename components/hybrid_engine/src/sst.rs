// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{KvEngine, MemoryEngine, Result, SstCompressionType, SstExt, SstWriterBuilder};

use crate::engine::HybridEngine;

pub struct HybridSstWriteBuilder {}

impl<EK, EM> SstExt for HybridEngine<EK, EM>
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    type SstReader = EK::SstReader;
    type SstWriter = EK::SstWriter;
    type SstWriterBuilder = HybridSstWriteBuilder;
}

impl<EK, EM> SstWriterBuilder<HybridEngine<EK, EM>> for HybridSstWriteBuilder
where
    EK: KvEngine,
    EM: MemoryEngine,
{
    fn new() -> Self {
        unimplemented!()
    }

    fn set_db(self, _db: &HybridEngine<EK, EM>) -> Self {
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

    fn build(self, _path: &str) -> Result<<HybridEngine<EK, EM> as SstExt>::SstWriter> {
        unimplemented!()
    }
}
