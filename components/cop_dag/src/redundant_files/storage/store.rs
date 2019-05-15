// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::types::Key;
use crate::storage::Statistics;
use crate::storage::{KvPair, Value};

pub trait Store: Send {
    type Error: Into<crate::Error>;
    type Scanner: Scanner<Error = Self::Error>;

    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>, Self::Error>;

    fn batch_get(
        &self,
        keys: &[Key],
        statistics: &mut Statistics,
    ) -> Vec<Result<Option<Value>, Self::Error>>;

    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner, Self::Error>;
}

pub trait Scanner: Send {
    type Error: Into<crate::Error>;

    fn next(&mut self) -> Result<Option<(Key, Value)>, Self::Error>;

    fn scan(&mut self, limit: usize) -> Result<Vec<Result<KvPair, Self::Error>>, Self::Error>; // TODO: implement Scan in storage

    fn take_statistics(&mut self) -> Statistics;
}
