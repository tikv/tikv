// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{
    KvEngine, RegionCacheEngine, Result, SstCompressionType, SstExt, SstWriterBuilder,
};

use crate::engine::HybridEngine;

pub struct HybridEngineSstWriteBuilder<EK: SstExt> {
    disk_sst_writer: EK::SstWriterBuilder,
}

impl<EK, EC> SstExt for HybridEngine<EK, EC>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    type SstReader = EK::SstReader;
    type SstWriter = EK::SstWriter;
    type SstWriterBuilder = HybridEngineSstWriteBuilder<EK>;
}

impl<EK, EC> SstWriterBuilder<HybridEngine<EK, EC>> for HybridEngineSstWriteBuilder<EK>
where
    EK: KvEngine,
    EC: RegionCacheEngine,
{
    fn new() -> Self {
        Self {
            disk_sst_writer: EK::SstWriterBuilder::new(),
        }
    }

    fn set_db(self, db: &HybridEngine<EK, EC>) -> Self {
        Self {
            disk_sst_writer: self.disk_sst_writer.set_db(db.disk_engine()),
        }
    }

    fn set_cf(self, cf: &str) -> Self {
        Self {
            disk_sst_writer: self.disk_sst_writer.set_cf(cf),
        }
    }

    fn set_in_memory(self, in_memory: bool) -> Self {
        Self {
            disk_sst_writer: self.disk_sst_writer.set_in_memory(in_memory),
        }
    }

    fn set_compression_type(self, compression: Option<SstCompressionType>) -> Self {
        Self {
            disk_sst_writer: self.disk_sst_writer.set_compression_type(compression),
        }
    }

    fn set_compression_level(self, level: i32) -> Self {
        Self {
            disk_sst_writer: self.disk_sst_writer.set_compression_level(level),
        }
    }

    fn build(self, path: &str) -> Result<<HybridEngine<EK, EC> as SstExt>::SstWriter> {
        self.disk_sst_writer.build(path)
    }
}
