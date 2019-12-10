// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use engine_traits::{self, Error, Result};
use rocksdb::{DBIterator, SeekKey as RawSeekKey, DB};

// FIXME: Would prefer using &DB instead of Arc<DB>.  As elsewhere in
// this crate, it would require generic associated types.
pub struct RocksEngineIterator(DBIterator<Arc<DB>>);

impl RocksEngineIterator {
    pub fn from_raw(iter: DBIterator<Arc<DB>>) -> RocksEngineIterator {
        RocksEngineIterator(iter)
    }
}

impl engine_traits::Iterator for RocksEngineIterator {
    fn seek(&mut self, key: engine_traits::SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek(k.into_raw())
    }

    fn seek_for_prev(&mut self, key: engine_traits::SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw())
    }

    fn prev(&mut self) -> bool {
        self.0.prev()
    }

    fn next(&mut self) -> bool {
        self.0.next()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn status(&self) -> Result<()> {
        self.0.status().map_err(Error::Engine)
    }
}

pub struct RocksSeekKey<'a>(RawSeekKey<'a>);

impl<'a> RocksSeekKey<'a> {
    pub fn into_raw(self) -> RawSeekKey<'a> {
        self.0
    }
}

impl<'a> From<engine_traits::SeekKey<'a>> for RocksSeekKey<'a> {
    fn from(key: engine_traits::SeekKey<'a>) -> Self {
        let k = match key {
            engine_traits::SeekKey::Start => RawSeekKey::Start,
            engine_traits::SeekKey::End => RawSeekKey::End,
            engine_traits::SeekKey::Key(k) => RawSeekKey::Key(k),
        };
        RocksSeekKey(k)
    }
}
