// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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
}
