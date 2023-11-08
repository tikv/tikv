// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    mem,
    ops::{Deref, DerefMut},
};

use tirocks::{
    option::{RawCfOptions, TitanCfOptions},
    CfOptions,
};

enum Options {
    Rocks(CfOptions),
    Titan(TitanCfOptions),
    // Only used for replace.
    None,
}

pub struct RocksCfOptions(Options);

impl RocksCfOptions {
    #[inline]
    pub fn is_titan(&self) -> bool {
        matches!(self.0, Options::Titan(_))
    }

    #[inline]
    pub fn default_titan() -> Self {
        RocksCfOptions(Options::Titan(Default::default()))
    }

    #[inline]
    pub(crate) fn into_rocks(self) -> CfOptions {
        match self.0 {
            Options::Rocks(opt) => opt,
            _ => panic!("it's a titan cf option"),
        }
    }

    #[inline]
    pub(crate) fn into_titan(self) -> TitanCfOptions {
        match self.0 {
            Options::Titan(opt) => opt,
            _ => panic!("it's not a titan cf option"),
        }
    }
}

impl Default for RocksCfOptions {
    #[inline]
    fn default() -> Self {
        RocksCfOptions(Options::Rocks(Default::default()))
    }
}

impl Deref for RocksCfOptions {
    type Target = RawCfOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Options::Rocks(opt) => opt,
            Options::Titan(opt) => opt,
            Options::None => unreachable!(),
        }
    }
}

impl DerefMut for RocksCfOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            Options::Rocks(opt) => opt,
            Options::Titan(opt) => opt,
            Options::None => unreachable!(),
        }
    }
}

impl engine_traits::TitanCfOptions for RocksCfOptions {
    fn new() -> Self {
        // TODO: should use accessor of CfOptions instead.
        panic!()
    }

    fn set_min_blob_size(&mut self, size: u64) {
        if let Options::Titan(opt) = &mut self.0 {
            opt.set_min_blob_size(size);
            return;
        }
        if let Options::Rocks(r) = mem::replace(&mut self.0, Options::None) {
            let mut opt: TitanCfOptions = r.into();
            opt.set_min_blob_size(size);
            self.0 = Options::Titan(opt);
            return;
        }
        unreachable!()
    }
}

impl engine_traits::CfOptions for RocksCfOptions {
    type TitanCfOptions = Self;

    #[inline]
    fn new() -> Self {
        Self::default()
    }

    #[inline]
    fn get_max_write_buffer_number(&self) -> u32 {
        self.max_write_buffer_number() as u32
    }

    fn get_level_zero_slowdown_writes_trigger(&self) -> i32 {
        self.level0_slowdown_writes_trigger()
    }

    fn get_level_zero_stop_writes_trigger(&self) -> i32 {
        self.level0_stop_writes_trigger()
    }

    fn set_level_zero_file_num_compaction_trigger(&mut self, v: i32) {
        self.set_level0_file_num_compaction_trigger(v);
    }

    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        self.soft_pending_compaction_bytes_limit()
    }

    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        self.hard_pending_compaction_bytes_limit()
    }

    fn get_block_cache_capacity(&self) -> u64 {
        // TODO: block cache should be managed by global shared resource.
        panic!()
    }

    fn set_block_cache_capacity(&self, _: u64) -> engine_traits::Result<()> {
        // TODO: block cache should be managed by global shared resource.
        panic!()
    }

    fn set_titan_cf_options(&mut self, _: &Self::TitanCfOptions) {
        // TODO: change to use mut accessor instead of setter.
        panic!()
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.target_file_size_base()
    }

    fn set_disable_auto_compactions(&mut self, v: bool) {
        (**self).set_disable_auto_compactions(v);
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.disable_auto_compactions()
    }

    fn get_disable_write_stall(&self) -> bool {
        self.disable_write_stall()
    }

    fn set_sst_partitioner_factory<F: engine_traits::SstPartitionerFactory>(&mut self, _: F) {
        // TODO: It should be shared.
        panic!()
    }
}
