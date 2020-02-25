// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::keybuilder::KeyBuilder;

use crate::*;

pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

pub trait Iterator {
    fn seek(&mut self, key: SeekKey) -> Result<bool>;
    fn seek_for_prev(&mut self, key: SeekKey) -> Result<bool>;

    fn seek_to_first(&mut self) -> Result<bool> {
        self.seek(SeekKey::Start)
    }

    fn seek_to_last(&mut self) -> Result<bool> {
        self.seek(SeekKey::End)
    }

    fn prev(&mut self) -> Result<bool>;
    fn next(&mut self) -> Result<bool>;

    /// Only be called when `self.valid() == Ok(true)`.
    fn key(&self) -> &[u8];
    /// Only be called when `self.valid() == Ok(true)`.
    fn value(&self) -> &[u8];

    fn valid(&self) -> Result<bool>;
}

pub trait Iterable {
    type Iterator: Iterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator>;
    fn iterator_cf_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator>;

    fn iterator(&self) -> Result<Self::Iterator> {
        self.iterator_opt(IterOptions::default())
    }

    fn iterator_cf(&self, cf: &str) -> Result<Self::Iterator> {
        self.iterator_cf_opt(cf, IterOptions::default())
    }

    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], fill_cache: bool, f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start = KeyBuilder::from_slice(start_key, DATA_KEY_PREFIX_LEN, 0);
        let end = KeyBuilder::from_slice(end_key, DATA_KEY_PREFIX_LEN, 0);
        let iter_opt = IterOptions::new(Some(start), Some(end), fill_cache);
        scan_impl(self.iterator_opt(iter_opt)?, start_key, f)
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
        scan_impl(self.iterator_cf_opt(cf, iter_opt)?, start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator()?;
        if iter.seek(SeekKey::Key(key))? {
            let (k, v) = (iter.key().to_vec(), iter.value().to_vec());
            return Ok(Some((k, v)));
        }
        Ok(None)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator_cf(cf)?;
        if iter.seek(SeekKey::Key(key))? {
            return Ok(Some((iter.key().to_vec(), iter.value().to_vec())));
        }
        Ok(None)
    }
}

fn scan_impl<Iter, F>(mut it: Iter, start_key: &[u8], mut f: F) -> Result<()>
where
    Iter: Iterator,
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    let mut remained = it.seek(SeekKey::Key(start_key))?;
    while remained {
        remained = f(it.key(), it.value())? && it.next()?;
    }
    Ok(())
}

impl<'a> From<&'a [u8]> for SeekKey<'a> {
    fn from(bs: &'a [u8]) -> SeekKey {
        SeekKey::Key(bs)
    }
}

/// Collect all items of `it` into a vector, generally used for tests.
///
/// # Panics
///
/// If any errors occur during iterator.
pub fn collect<I: Iterator>(mut it: I) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut v = Vec::new();
    let mut it_valid = it.valid().unwrap();
    while it_valid {
        let kv = (it.key().to_vec(), it.value().to_vec());
        v.push(kv);
        it_valid = it.next().unwrap();
    }
    v
}
