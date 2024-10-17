// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

pub use engine_traits::SstCompressionType;

use crate::{
    compaction::SubcompactionResult,
    statistic::{
        CollectSubcompactionStatistic, LoadMetaStatistic, LoadStatistic, SubcompactStatistic,
    },
};

pub mod checkpoint;
pub mod consistency;
pub mod observability;
pub mod save_meta;

#[derive(Default)]
pub struct CollectStatistic {
    load_stat: LoadStatistic,
    compact_stat: SubcompactStatistic,
    load_meta_stat: LoadMetaStatistic,
    collect_stat: CollectSubcompactionStatistic,
}

impl CollectStatistic {
    fn update_subcompaction(&mut self, res: &SubcompactionResult) {
        self.load_stat += res.load_stat.clone();
        self.compact_stat += res.compact_stat.clone();
    }

    fn update_collect_compaction_stat(&mut self, stat: &CollectSubcompactionStatistic) {
        self.collect_stat += stat.clone()
    }

    fn update_load_meta_stat(&mut self, stat: &LoadMetaStatistic) {
        self.load_meta_stat += stat.clone()
    }
}
