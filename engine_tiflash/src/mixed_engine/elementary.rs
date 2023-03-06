// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;

use engine_rocks::RocksEngineIterator;
use engine_traits::{IterOptions, ReadOptions, Result, WriteOptions};

use super::MixedDbVector;
use crate::mixed_engine::write_batch::RocksWriteBatchVec;

pub trait ElementaryWriteBatch: Send {
    fn as_any(&self) -> &dyn std::any::Any;

    fn use_default(&self) -> bool;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()>;

    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64>;

    fn data_size(&self) -> usize;

    fn count(&self) -> usize;

    fn is_empty(&self) -> bool;

    fn clear(&mut self);

    fn merge(&mut self, other: RocksWriteBatchVec) -> Result<()>;
}
pub trait ElementaryEngine: Debug {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()>;

    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<MixedDbVector>>;

    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<MixedDbVector>>;

    #[allow(clippy::type_complexity)]
    fn scan(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: &mut dyn FnMut(&[u8], &[u8]) -> Result<bool>,
    ) -> Result<()>;

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<RocksEngineIterator>;

    fn element_wb(&self) -> Box<dyn ElementaryWriteBatch>;
}
