// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::keybuilder::KeyBuilder;

use crate::*;

pub enum SeekKey<'a> {
    Start,
    End,
    Key(&'a [u8]),
}

pub trait Iterator {
    fn seek(&mut self, key: SeekKey) -> bool;
    fn seek_for_prev(&mut self, key: SeekKey) -> bool;

    fn seek_to_first(&mut self) -> bool {
        self.seek(SeekKey::Start)
    }

    fn seek_to_last(&mut self) -> bool {
        self.seek(SeekKey::End)
    }

    fn prev(&mut self) -> bool;
    fn next(&mut self) -> bool;

    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];

    fn kv(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.valid() {
            let k = self.key();
            let v = self.value();
            Some((k.to_vec(), v.to_vec()))
        } else {
            None
        }
    }

    fn valid(&self) -> bool;
    fn status(&self) -> Result<()>;

    fn as_std(&mut self) -> StdIterator<Self>
    where
        Self: Sized,
    {
        StdIterator(self)
    }
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
        iter.seek(SeekKey::Key(key));
        if iter.valid() {
            Ok(Some((iter.key().to_vec(), iter.value().to_vec())))
        } else {
            Ok(None)
        }
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.iterator_cf(cf)?;
        iter.seek(SeekKey::Key(key));
        if iter.valid() {
            Ok(Some((iter.key().to_vec(), iter.value().to_vec())))
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
        let r = f(it.key(), it.value())?;
        if !r || !it.next() {
            break;
        }
    }

    it.status().map_err(From::from)
}

pub struct StdIterator<'a, I: Iterator>(&'a mut I);

pub type Kv = (Vec<u8>, Vec<u8>);

impl<'a, I: Iterator> std::iter::Iterator for StdIterator<'a, I> {
    type Item = Kv;

    fn next(&mut self) -> Option<Kv> {
        let kv = self.0.kv();
        if kv.is_some() {
            I::next(&mut self.0);
        }
        kv
    }
}

impl<'a> From<&'a [u8]> for SeekKey<'a> {
    fn from(bs: &'a [u8]) -> SeekKey {
        SeekKey::Key(bs)
    }
}
