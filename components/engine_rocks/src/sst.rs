// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Result, Iterable, SstExt, SstReader};
use engine_traits::{SeekMode, SeekKey, Iterator};
use engine_traits::IterOptions;
use engine_traits::Error;
use crate::engine::RocksEngine;
use rocksdb::{SstFileReader, ColumnFamilyOptions};
use rocksdb::DBIterator;
use std::rc::Rc;
use crate::options::{RocksReadOptions, RocksWriteOptions};
// FIXME: Move RocksSeekKey into a common module since
// it's shared between multiple iterators
use crate::engine_iterator::{RocksSeekKey};

impl SstExt for RocksEngine {
    type SstReader = RocksSstReader;
}

// FIXME: like in RocksEngineIterator and elsewhere, here we are using
// Rc to avoid putting references in an associated type, which
// requires generic associated types.
pub struct RocksSstReader {
    inner: Rc<SstFileReader>,
}

impl SstReader for RocksSstReader {
    fn open(path: &str) -> Result<Self> {
        let mut reader = SstFileReader::new(ColumnFamilyOptions::new());
        reader.open(path)?;
        let inner = Rc::new(reader);
        Ok(RocksSstReader { inner })
    }
    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iterator {
        RocksSstIterator(SstFileReader::iter_rc(self.inner.clone()))
    }
}

impl Iterable for RocksSstReader {
    type Iterator = RocksSstIterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iterator> {
        let opt: RocksReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(RocksSstIterator(SstFileReader::iter_opt_rc(self.inner.clone(), opt)))
    }

    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iterator> {
        unimplemented!() // FIXME: What should happen here?
    }
}

// FIXME: See comment on RocksSstReader for why this contains Rc
pub struct RocksSstIterator(DBIterator<Rc<SstFileReader>>);

impl Iterator for RocksSstIterator {
    fn seek(&mut self, key: SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek(k.into_raw())
    }

    fn seek_for_prev(&mut self, key: SeekKey) -> bool {
        let k: RocksSeekKey = key.into();
        self.0.seek_for_prev(k.into_raw())
    }

    fn prev(&mut self) -> bool {
        self.0.prev()
    }

    fn next(&mut self) -> bool {
        self.0.next()
    }

    fn key(&self) -> Result<&[u8]> {
        Ok(self.0.key())
    }

    fn value(&self) -> Result<&[u8]> {
        Ok(self.0.value())
    }

    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn status(&self) -> Result<()> {
        self.0.status().map_err(Error::Engine)
    }
}
