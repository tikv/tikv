#![feature(once_cell)]

use std::{borrow::Cow, sync::LazyLock};

use fxhash::FxBuildHasher;
use moka::sync::{Cache, CacheBuilder};
use txn_types::{Key, TimeStamp};

pub static ROW_CACHE: LazyLock<RowCache> = LazyLock::new(|| RowCache::default());

pub struct RowCache {
    rows: Cache<Key, Row, FxBuildHasher>,
}

impl Default for RowCache {
    fn default() -> Self {
        Self {
            rows: CacheBuilder::new(1 << 30)
                .weigher(|k: &Key, v: &Row| {
                    (k.len() + v.value.len()).try_into().unwrap_or(u32::MAX)
                })
                .build_with_hasher(FxBuildHasher::default()),
        }
    }
}

impl RowCache {
    pub fn get(&self, key: &Key) -> Option<Row> {
        self.rows.get(key)
    }

    pub fn insert(&self, key: Key, commit_ts: TimeStamp, value: Cow<[u8]>) {
        self.rows.get_with_if(
            key,
            || Row {
                commit_ts,
                value: value.into_owned(),
            },
            |row| commit_ts > row.commit_ts,
        );
    }
}

#[derive(Clone)]
pub struct Row {
    pub commit_ts: TimeStamp,
    pub value: Vec<u8>,
}
