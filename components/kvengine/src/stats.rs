// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{load_bool, NUM_CFS};
use bytes::Buf;

#[derive(Default, Debug, Serialize, Deserialize)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EngineStats {
    pub num_shards: usize,
    pub num_initial_flushed_shard: usize,
    pub num_active_shards: usize,
    pub num_compacting_shards: usize,
    pub num_mem_tables: usize,
    pub total_mem_tables_size: usize,
    pub num_l0_tables: usize,
    pub num_partial_l0_tables: usize,
    pub num_partial_tables: usize,
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

    pub fn get_engine_stats(shard_stats: Vec<ShardStats>) -> EngineStats {
        let mut engine_stats = EngineStats::new();
        engine_stats.num_shards = shard_stats.len();
        for shard in &shard_stats {
            if shard.active {
                engine_stats.num_active_shards += 1;
            }
            if shard.compacting {
                engine_stats.num_compacting_shards += 1;
            }
            if shard.flushed {
                engine_stats.num_initial_flushed_shard += 1;
            }
            engine_stats.num_mem_tables += shard.mem_tbls.len();
            for size in &shard.mem_tbls {
                engine_stats.total_mem_tables_size += *size;
            }
            engine_stats.num_l0_tables += shard.l0_tbls.len();
            for (_, l0_size) in &shard.l0_tbls {
                engine_stats.total_l0_tables_size += *l0_size;
            }
            engine_stats.num_partial_l0_tables += shard.partial_l0s;
            engine_stats.num_partial_tables += shard.partial_tbls;
            for cf in 0..NUM_CFS {
                let shard_cf_stat = &shard.cfs[cf];
                for (i, level_stat) in shard_cf_stat.levels.iter().enumerate() {
                    engine_stats.level_num_files[i] += level_stat.tables.len();
                    engine_stats.cfs_num_files[cf] += level_stat.tables.len();
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

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ShardStats {
    pub id: u64,
    pub ver: u64,
    pub start: String,
    pub end: String,
    pub active: bool,
    pub compacting: bool,
    pub flushed: bool,
    pub mem_tbls: Vec<usize>,
    pub l0_tbls: Vec<(u64, usize)>,
    pub cfs: Vec<CFStats>,
    pub base_version: u64,
    pub max_mem_table_size: u64,
    pub meta_sequence: u64,
    pub write_sequence: u64,
    pub total_size: u64,
    pub partial_l0s: usize,
    pub partial_tbls: usize,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CFStats {
    pub levels: Vec<LevelStats>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LevelStats {
    pub tables: Vec<(u64, usize)>,
}

impl super::Shard {
    pub fn get_stats(&self) -> ShardStats {
        let mut total_size = 0;
        let shard_mem_tbls = self.get_mem_tbls();
        let mut mem_tbls = vec![];
        for mem_tbl in shard_mem_tbls.tbls.as_ref() {
            total_size += mem_tbl.size() as u64;
            mem_tbls.push(mem_tbl.size());
        }
        let mut partial_l0s = 0;
        let shard_l0_tbls = self.get_l0_tbls();
        let mut l0_tbls = vec![];
        for l0_tbl in shard_l0_tbls.tbls.as_ref() {
            if self.cover_full_table(l0_tbl.smallest(), l0_tbl.biggest()) {
                total_size += l0_tbl.size();
            } else {
                total_size += l0_tbl.size() / 2;
                partial_l0s += 1;
            }
            l0_tbls.push((l0_tbl.id(), l0_tbl.size() as usize));
        }
        let mut partial_tbls = 0;
        let mut cfs = vec![];
        for cf in 0..NUM_CFS {
            let scf = self.get_cf(cf);
            let mut cf_stat = CFStats { levels: vec![] };
            for l in scf.levels.as_ref() {
                let mut level_stats = LevelStats { tables: vec![] };
                for t in &l.tables {
                    if self.cover_full_table(t.smallest(), t.biggest()) {
                        total_size += t.size();
                    } else {
                        total_size += t.size() / 2;
                        partial_tbls += 1;
                    }
                    level_stats.tables.push((t.id(), t.size() as usize))
                }
                cf_stat.levels.push(level_stats);
            }
            cfs.push(cf_stat);
        }
        ShardStats {
            id: self.id,
            ver: self.ver,
            start: format!("{:?}", self.start.chunk()),
            end: format!("{:?}", self.end.chunk()),
            active: self.is_active(),
            compacting: load_bool(&self.compacting),
            flushed: self.get_initial_flushed(),
            mem_tbls,
            l0_tbls,
            cfs,
            base_version: self.base_version,
            max_mem_table_size: self.get_max_mem_table_size(),
            meta_sequence: self.get_meta_sequence(),
            write_sequence: self.get_write_sequence(),
            total_size,
            partial_l0s,
            partial_tbls,
        }
    }
}
