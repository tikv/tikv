// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{DBIterator, RocksSeekKey, DB};
use crate::{Error, Iterator, Result, SeekKey};
use std::sync::Arc;

// TODO: use &DB
pub type RocksIterator = DBIterator<Arc<DB>>;

impl Iterator for RocksIterator {
    fn seek(&mut self, key: SeekKey) -> bool {
        DBIterator::seek(self, key.into())
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> bool {
        DBIterator::seek_for_prev(self, key.into())
    }

    fn prev(&mut self) -> bool {
        DBIterator::prev(self)
    }

    fn next(&mut self) -> bool {
        DBIterator::next(self)
    }

    fn key(&self) -> Result<&[u8]> {
        Ok(DBIterator::key(self))
    }

    fn value(&self) -> Result<&[u8]> {
        Ok(DBIterator::value(self))
    }

    fn valid(&self) -> bool {
        DBIterator::valid(self)
    }

    fn status(&self) -> Result<()> {
        DBIterator::status(self).map_err(Error::Engine)
    }
}

impl<'a> From<SeekKey<'a>> for RocksSeekKey<'a> {
    fn from(key: SeekKey<'a>) -> Self {
        match key {
            SeekKey::Start => RocksSeekKey::Start,
            SeekKey::End => RocksSeekKey::End,
            SeekKey::Key(k) => RocksSeekKey::Key(k),
        }
    }
}

pub struct IterOptionss {}
