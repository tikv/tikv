// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::RocksEngine;

use std::sync::Arc;

use engine_traits::{CFOptionsExt, ColumnFamilyOptions, MiscExt, CF_DEFAULT, CF_LOCK, CF_WRITE};
use file_system::{IORateLimiter, IOType};
use tikv_util::math::MovingMaxU32;

pub struct RocksEngineResourceInfo {
    engine: RocksEngine,
    // Synchronize latest info with limiter
    limiter: Arc<IORateLimiter>,
    normalized_num_mem_tables: MovingMaxU32,
    normalized_level_zero_num_files: MovingMaxU32,
    normalized_pending_bytes: MovingMaxU32,
}

impl RocksEngineResourceInfo {
    pub fn new(
        engine: RocksEngine,
        limiter: Arc<IORateLimiter>,
        max_samples_to_preserve: usize,
    ) -> Self {
        RocksEngineResourceInfo {
            engine,
            limiter,
            normalized_num_mem_tables: MovingMaxU32::new(max_samples_to_preserve),
            normalized_level_zero_num_files: MovingMaxU32::new(max_samples_to_preserve),
            normalized_pending_bytes: MovingMaxU32::new(max_samples_to_preserve),
        }
    }
    pub fn update(&self) {
        let (mut level_zero_num_files, mut num_mem_tables, mut pending_bytes) = (0, 0, 0);
        for cf in &[CF_DEFAULT, CF_WRITE, CF_LOCK] {
            if let Ok(cf_opts) = self.engine.get_options_cf(cf) {
                if let Ok(Some(n)) = self.engine.get_cf_num_immutable_mem_table(cf) {
                    num_mem_tables = std::cmp::max(
                        num_mem_tables,
                        n as u32 * 100 / cf_opts.get_max_write_buffer_number(),
                    );
                }
                if let Ok(Some(n)) = self.engine.get_cf_num_files_at_level(cf, 0) {
                    level_zero_num_files = std::cmp::max(
                        level_zero_num_files,
                        n as u32 * 100 / cf_opts.get_level_zero_slowdown_writes_trigger(),
                    );
                }
                if let Ok(Some(b)) = self.engine.get_cf_compaction_pending_bytes(cf) {
                    pending_bytes = std::cmp::max(
                        pending_bytes,
                        b * 100 / cf_opts.get_soft_pending_compaction_bytes_limit(),
                    );
                }
            }
        }
        let num_mem_tables = self.normalized_num_mem_tables.add(num_mem_tables);
        let level_zero_num_files = self
            .normalized_level_zero_num_files
            .add(level_zero_num_files);
        let pending_bytes = self.normalized_pending_bytes.add(pending_bytes as u32);
        self.limiter
            .update_backlog(IOType::Flush, num_mem_tables as f32 / 100.0);
        self.limiter.update_backlog(
            IOType::Compaction,
            std::cmp::max(level_zero_num_files, pending_bytes) as f32 / 100.0,
        );
    }

    pub fn normalized_num_mem_tables(&self) -> f32 {
        self.normalized_num_mem_tables.fetch() as f32 / 100.0
    }

    pub fn normalized_level_zero_num_files(&self) -> f32 {
        self.normalized_level_zero_num_files.fetch() as f32 / 100.0
    }

    pub fn normalized_pending_bytes(&self) -> f32 {
        self.normalized_pending_bytes.fetch() as f32 / 100.0
    }
}
