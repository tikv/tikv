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

    pub fn sequence(&self) -> Option<u64> {
        self.0.sequence()
    }
}

impl engine_traits::Iterator for RocksEngineIterator {
    fn seek(&mut self, key: engine_traits::SeekKey) -> Result<bool> {
        let k: RocksSeekKey = key.into();
        self.0.seek(k.into_raw()).map_err(Error::Engine)
    }

    fn seek_for_prev(&mut self, key: engine_traits::SeekKey) -> Result<bool> {
        let k: RocksSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::Engine)
    }

    fn prev(&mut self) -> Result<bool> {
        assert!(cfg!(feature = "nortcheck") || self.valid()?);
        self.0.prev().map_err(Error::Engine)
    }

    fn next(&mut self) -> Result<bool> {
        assert!(cfg!(feature = "nortcheck") || self.valid()?);
        self.0.next().map_err(Error::Engine)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::Engine)
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
