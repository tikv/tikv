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

enum Options {
    Rocks(DbOptions),
    Titan(TitanDbOptions),
}

pub struct RocksDbOptions(Options);

impl RocksDbOptions {
    #[inline]
    pub fn env(&self) -> Option<&Arc<Env>> {
        match &self.0 {
            Options::Rocks(opt) => opt.env(),
            Options::Titan(opt) => opt.env(),
        }
    }

    #[inline]
    pub fn is_titan(&self) -> bool {
        matches!(self.0, Options::Titan(_))
    }

    #[inline]
    pub(crate) fn into_rocks(self) -> DbOptions {
        match self.0 {
            Options::Rocks(opt) => opt,
            _ => panic!("it's a titan option"),
        }
    }

    #[inline]
    pub(crate) fn into_titan(self) -> TitanDbOptions {
        match self.0 {
            Options::Titan(opt) => opt,
            _ => panic!("it's not a titan option"),
        }
    }
}

impl Default for RocksDbOptions {
    #[inline]
    fn default() -> Self {
        RocksDbOptions(Options::Rocks(Default::default()))
    }
}

impl Deref for RocksDbOptions {
    type Target = RawDbOptions;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match &self.0 {
            Options::Rocks(opt) => opt,
            Options::Titan(opt) => opt,
        }
    }
}

impl DerefMut for RocksDbOptions {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.0 {
            Options::Rocks(opt) => opt,
            Options::Titan(opt) => opt,
        }
    }
}
