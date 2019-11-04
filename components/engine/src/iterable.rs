// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::rocks::{DBIterator, ReadOptions, DB};
use crate::Result;
use keys::{BasicPhysicalKey, PhysicalKey, PhysicalKeySlice, RaftPhysicalKey, ToPhysicalKeySlice};

#[derive(Clone, PartialEq)]
enum SeekMode {
    TotalOrder,
    Prefix,
}

pub struct IterOption<Key: PhysicalKey> {
    lower_bound: Option<Key>,
    upper_bound: Option<Key>,
    prefix_same_as_start: bool,
    fill_cache: bool,
    // only supported when Titan enabled, otherwise it doesn't take effect.
    titan_key_only: bool,
    seek_mode: SeekMode,
}

impl<Key: PhysicalKey> IterOption<Key> {
    pub fn new(lower_bound: Option<Key>, upper_bound: Option<Key>, fill_cache: bool) -> Self {
        Self {
            lower_bound,
            upper_bound,
            prefix_same_as_start: false,
            fill_cache,
            titan_key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    #[inline]
    pub fn set_prefix_seek(&mut self) -> &mut Self {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    #[inline]
    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    #[inline]
    pub fn set_fill_cache(&mut self, v: bool) -> &mut Self {
        self.fill_cache = v;
        self
    }

    #[inline]
    pub fn set_titan_key_only(&mut self, v: bool) -> &mut Self {
        self.titan_key_only = v;
        self
    }

    #[inline]
    pub fn lower_bound(&self) -> Option<&Key> {
        self.lower_bound.as_ref()
    }

    #[inline]
    pub fn mut_lower_bound(&mut self) -> Option<&mut Key> {
        self.lower_bound.as_mut()
    }

    #[inline]
    pub fn set_lower_bound(&mut self, bound: Key) -> &mut Self {
        self.lower_bound = Some(bound);
        self
    }

    #[inline]
    pub fn upper_bound(&self) -> Option<&Key> {
        self.upper_bound.as_ref()
    }

    #[inline]
    pub fn mut_upper_bound(&mut self) -> Option<&mut Key> {
        self.upper_bound.as_mut()
    }

    #[inline]
    pub fn set_upper_bound(&mut self, bound: Key) -> &mut Self {
        self.upper_bound = Some(bound);
        self
    }

    #[inline]
    pub fn set_prefix_same_as_start(&mut self, enable: bool) -> &mut Self {
        self.prefix_same_as_start = enable;
        self
    }

    pub fn build_read_opts(self) -> ReadOptions {
        let mut opts = ReadOptions::new();
        opts.fill_cache(self.fill_cache);
        if self.titan_key_only {
            opts.set_titan_key_only(true);
        }
        if self.total_order_seek_used() {
            opts.set_total_order_seek(true);
        } else if self.prefix_same_as_start {
            opts.set_prefix_same_as_start(true);
        }
        if let Some(key) = self.lower_bound {
            opts.set_iterate_lower_bound(key.into_physical_vec());
        }
        if let Some(key) = self.upper_bound {
            opts.set_iterate_upper_bound(key.into_physical_vec());
        }
        opts
    }
}

impl<Key: PhysicalKey> Default for IterOption<Key> {
    fn default() -> Self {
        Self {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_start: false,
            fill_cache: true,
            titan_key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}

// TODO: Not suitable to put it in this module.
impl From<IterOption<RaftPhysicalKey>> for IterOption<BasicPhysicalKey> {
    fn from(iter: IterOption<RaftPhysicalKey>) -> Self {
        IterOption {
            lower_bound: iter.lower_bound.map(|v| v.into()),
            upper_bound: iter.upper_bound.map(|v| v.into()),
            prefix_same_as_start: iter.prefix_same_as_start,
            fill_cache: iter.fill_cache,
            titan_key_only: iter.titan_key_only,
            seek_mode: iter.seek_mode,
        }
    }
}

// TODO: refactor this trait into rocksdb trait.
pub trait Iterable {
    type Key: PhysicalKey;

    fn new_iterator(&self, iter_opt: IterOption<Self::Key>) -> DBIterator<&DB>;
    fn new_iterator_cf(&self, _: &str, iter_opt: IterOption<Self::Key>) -> Result<DBIterator<&DB>>;
    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    fn scan<F>(
        &self,
        start_key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
        end_key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start_key = start_key.to_physical_slice_container();
        let end_key = end_key.to_physical_slice_container();
        // TODO: Can we avoid the allocation?
        let iter_opt = IterOption::new(
            Some(start_key.alloc_to_physical_key()),
            Some(end_key.alloc_to_physical_key()),
            fill_cache,
        );
        scan_impl(self.new_iterator(iter_opt), &*start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_cf<F>(
        &self,
        cf: &str,
        start_key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
        end_key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let start_key = start_key.to_physical_slice_container();
        let end_key = end_key.to_physical_slice_container();
        let iter_opt = IterOption::new(
            Some(start_key.alloc_to_physical_key()),
            Some(end_key.alloc_to_physical_key()),
            fill_cache,
        );
        scan_impl(self.new_iterator_cf(cf, iter_opt)?, &*start_key, f)
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek(
        &self,
        key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(IterOption::default());
        iter.seek(
            key.to_physical_slice_container()
                .as_physical_std_slice()
                .into(),
        );
        Ok(iter.kv())
    }

    // Seek the first key >= given key, if not found, return None.
    fn seek_cf(
        &self,
        cf: &str,
        key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator_cf(cf, IterOption::default())?;
        iter.seek(
            key.to_physical_slice_container()
                .as_physical_std_slice()
                .into(),
        );
        Ok(iter.kv())
    }
}

fn scan_impl<F, K: PhysicalKeySlice + ?Sized>(
    mut it: DBIterator<&DB>,
    start_key: impl ToPhysicalKeySlice<K>,
    mut f: F,
) -> Result<()>
where
    F: FnMut(&[u8], &[u8]) -> Result<bool>,
{
    it.seek(
        start_key
            .to_physical_slice_container()
            .as_physical_std_slice()
            .into(),
    );
    while it.valid() {
        let r = f(it.key(), it.value())?;
        if !r || !it.next() {
            break;
        }
    }

    it.status().map_err(From::from)
}
