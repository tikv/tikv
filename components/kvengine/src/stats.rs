// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{load_bool, load_u64, CF_LEVELS, NUM_CFS};
use bytes::Bytes;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Instant;

#[derive(Default, Debug)]
pub struct EngineStats {
    pub num_shards: usize,
    pub num_splitting_shards: usize,
    pub num_active_shards: usize,
    pub num_compacting_shards: usize,
    pub num_mem_tables: usize,
    pub total_mem_tables_size: usize,
    pub total_splitting_mem_tables_size: usize,
    pub num_l0_tables: usize,
    pub total_l0_tables_size: usize,
    pub cfs_num_files: Vec<usize>,
    pub cf_total_sizes: Vec<usize>,
    pub level_num_files: Vec<usize>,
    pub level_total_sizes: Vec<usize>,
}

impl EngineStats {
    pub fn new() -> Self {
        let mut stats = EngineStats::default();
        stats.cfs_num_files = vec![0; 3];
        stats.cf_total_sizes = vec![0; 3];
        stats.level_num_files = vec![0; 3];
        stats.level_total_sizes = vec![0; 3];
        stats
    }
}

impl super::Engine {
    pub fn get_shard_stats(&self) -> Vec<ShardStats> {
        let mut shard_stats = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            let stats = shard.get_stats();
            shard_stats.push(stats)
        }
        shard_stats
    }

    pub fn get_engine_stats(&self) -> EngineStats {
        let shard_stats = self.get_shard_stats();
        let mut engine_stats = EngineStats::new();
        engine_stats.num_shards = shard_stats.len();
        for shard in &shard_stats {
            if shard.splitting_mem_tbls.len() > 0 {
                engine_stats.num_splitting_shards += 1;
            }
            if shard.active {
                engine_stats.num_active_shards += 1;
            }
            if shard.compacting {
                engine_stats.num_compacting_shards += 1;
            }
            for size in &shard.splitting_mem_tbls {
                engine_stats.total_splitting_mem_tables_size += *size;
            }
            engine_stats.num_mem_tables + shard.mem_tbls.len();
            for size in &shard.mem_tbls {
                engine_stats.total_mem_tables_size += *size;
            }
            engine_stats.num_l0_tables += shard.l0_tbls.len();
            for (_, l0_size) in &shard.l0_tbls {
                engine_stats.total_l0_tables_size += *l0_size;
            }
            for cf in 0..NUM_CFS {
                let shard_cf_stat = &shard.cfs[cf];
                for (i, level_stat) in shard_cf_stat.levels.iter().enumerate() {
                    engine_stats.level_num_files[i] += level_stat.tables.len();
                    engine_stats.cf_total_sizes[cf] += level_stat.tables.len();
                    for (_, tbl_size) in &level_stat.tables {
                        engine_stats.level_total_sizes[i] += *tbl_size;
                        engine_stats.cf_total_sizes[cf] += *tbl_size;
                    }
                }
            }
        }
        engine_stats
    }
}

pub struct ShardStats {
    pub id: u64,
    pub ver: u64,
    pub start: Bytes,
    pub end: Bytes,
    pub split_stage: kvenginepb::SplitStage,
    pub split_keys: Vec<Bytes>,
    pub active: bool,
    pub compacting: bool,
    pub flushed: bool,
    pub mem_tbls: Vec<usize>,
    pub last_switch_time: u64,
    pub splitting_mem_tbls: Vec<usize>,
    pub l0_tbls: Vec<(u64, usize)>,
    pub cfs: Vec<CFStats>,
    pub base_version: u64,
    pub max_mem_table_size: u64,
    pub meta_sequence: u64,
    pub write_sequence: u64,
}

pub struct CFStats {
    pub levels: Vec<LevelStats>,
}

pub struct LevelStats {
    pub tables: Vec<(u64, usize)>,
}

impl super::Shard {
    pub fn get_stats(&self) -> ShardStats {
        let splitting_ctx = self.get_split_ctx();
        let mut splitting_mem_tbls = vec![];
        let split_keys = splitting_ctx.split_keys.clone();
        for splitting_mem_tbl in &splitting_ctx.mem_tbls {
            splitting_mem_tbls.push(splitting_mem_tbl.size())
        }
        let shard_mem_tbls = self.get_mem_tbls();
        let mut mem_tbls = vec![];
        for mem_tbl in shard_mem_tbls.tbls.as_ref() {
            mem_tbls.push(mem_tbl.size());
        }
        let shard_l0_tbls = self.get_l0_tbls();
        let mut l0_tbls = vec![];
        for l0_tbl in shard_l0_tbls.tbls.as_ref() {
            l0_tbls.push((l0_tbl.id(), l0_tbl.size() as usize));
        }
        let mut cfs = vec![];
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            let mut cf_stat = CFStats { levels: vec![] };
            for l in scf.levels.as_ref() {
                let mut level_stats = LevelStats { tables: vec![] };
                for t in &l.tables {
                    level_stats.tables.push((t.id(), t.size() as usize))
                }
                cf_stat.levels.push(level_stats);
            }
            cfs.push(cf_stat);
        }
        ShardStats {
            id: self.id,
            ver: self.ver,
            start: self.start.clone(),
            end: self.end.clone(),
            split_stage: self.get_split_stage(),
            split_keys,
            active: self.is_active(),
            compacting: load_bool(&self.compacting),
            flushed: self.get_initial_flushed(),
            mem_tbls,
            last_switch_time: load_u64(&self.last_switch_time),
            splitting_mem_tbls,
            l0_tbls,
            cfs,
            base_version: self.base_version,
            max_mem_table_size: self.get_max_mem_table_size(),
            meta_sequence: self.get_meta_sequence(),
            write_sequence: self.get_write_sequence(),
        }
    }
}
