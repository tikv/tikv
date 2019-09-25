// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::keybuilder::KeyBuilder;

use crate::*;

#[derive(Clone, PartialEq)]
pub enum SeekMode {
    TotalOrder,
    Prefix,
}

pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

pub trait Iterator {
    fn seek(&mut self, key: SeekKey) -> bool;
    fn seek_for_prev(&mut self, key: SeekKey) -> bool;

    fn prev(&mut self) -> bool;
    fn next(&mut self) -> bool;

    fn key(&self) -> Result<&[u8]>;
    fn value(&self) -> Result<&[u8]>;

    fn valid(&self) -> bool;
    fn status(&self) -> Result<()>;
}

pub trait Iterable {
    type Iter: Iterator;

    fn iterator_opt(&self, opts: &IterOptions) -> Result<Self::Iter>;
    fn iterator_cf_opt(&self, opts: &IterOptions, cf: &str) -> Result<Self::Iter>;

    fn iterator(&self) -> Result<Self::Iter> {
        self.iterator_opt(&IterOptions::default())
    }

    fn iterator_cf(&self, cf: &str) -> Result<Self::Iter> {
        self.iterator_cf_opt(&IterOptions::default(), cf)
    }

    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN, 0);
        let end = KeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        scan_impl(self.iterator_opt(&iter_opt)?, start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN, 0);
        let end = KeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        scan_impl(self.iterator_cf_opt(&iter_opt, cf)?, start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator()?;
        iter.seek(SeekKey::Key(key));
        if iter.valid() {
            Ok(Some((iter.key()?.to_vec(), iter.value()?.to_vec())))
        } else {
            Ok(None)
        }
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator_cf(cf)?;
        iter.seek(SeekKey::Key(key));
        if iter.valid() {
            Ok(Some((iter.key()?.to_vec(), iter.value()?.to_vec())))
        } else {
            Ok(None)
        }
    }
}

fn scan_impl<Iter, F>(mut it: Iter, start_key: &[u8], mut f: F) -> Result<()>
where
    Iter: Iterator,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    it.seek(SeekKey::Key(start_key));
    while it.valid() {
        let r = f(it.key()?, it.value()?)?;
        if !r || !it.next() {
            break;
        }
    }

    it.status().map_err(From::from)
}
