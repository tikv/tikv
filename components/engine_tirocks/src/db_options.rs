// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tirocks::{
    env::Env,
    option::{RawDbOptions, TitanDbOptions},
    DbOptions,
};

pub enum RocksDbOptions {
    Rocks(DbOptions),
    Titan(TitanDbOptions),
}

impl RocksDbOptions {
    #[inline]
    pub fn env(&self) -> Option<&Arc<Env>> {
        match self {
            RocksDbOptions::Rocks(opt) => opt.env(),
            RocksDbOptions::Titan(opt) => opt.env(),
        }
    }

    #[inline]
    pub fn is_titan(&self) -> bool {
        matches!(self, RocksDbOptions::Titan(_))
    }

    #[inline]
    pub(crate) fn into_rocks(self) -> DbOptions {
        match self {
            RocksDbOptions::Rocks(opt) => opt,
            _ => panic!("it's a titan option"),
        }
    }

    #[inline]
    pub(crate) fn into_rocks_titan(self) -> TitanDbOptions {
        match self {
            RocksDbOptions::Titan(opt) => opt,
            _ => panic!("it's not a titan option"),
        }
    }
}

impl Default for RocksDbOptions {
    #[inline]
    fn default() -> Self {
        Self::Rocks(Default::default())
    }
}

impl Deref for RocksDbOptions {
    type Target = RawDbOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            RocksDbOptions::Rocks(opt) => opt,
            RocksDbOptions::Titan(opt) => opt,
        }
    }
}

impl DerefMut for RocksDbOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            RocksDbOptions::Rocks(opt) => opt,
            RocksDbOptions::Titan(opt) => opt,
        }
    }
}
