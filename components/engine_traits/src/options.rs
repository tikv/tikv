// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use tikv_util::keybuilder::KeyBuilder;

#[derive(Clone)]
pub struct ReadOptions {}

impl ReadOptions {
    pub fn new() -> ReadOptions {
        ReadOptions {}
    }
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        ReadOptions {}
    }
}

#[derive(Clone)]
pub struct WriteOptions {
    sync: bool,
}

impl WriteOptions {
    pub fn new() -> WriteOptions {
        WriteOptions { sync: false }
    }

    pub fn set_sync(&mut self, sync: bool) {
        self.sync = sync;
    }

    pub fn sync(&self) -> bool {
        self.sync
    }
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        WriteOptions { sync: false }
    }
}

#[derive(Clone, PartialEq)]
pub enum SeekMode {
    TotalOrder,
    Prefix,
}

pub struct IterOptions {
    lower_bound: Option<KeyBuilder>,
    upper_bound: Option<KeyBuilder>,
    prefix_same_as_start: bool,
    fill_cache: bool,
    // only supported when Titan enabled, otherwise it doesn't take effect.
    key_only: bool,
    seek_mode: SeekMode,
}

impl IterOptions {
    pub fn new(
        lower_bound: Option<KeyBuilder>,
        upper_bound: Option<KeyBuilder>,
        fill_cache: bool,
    ) -> IterOptions {
        IterOptions {
            lower_bound,
            upper_bound,
            prefix_same_as_start: false,
            fill_cache,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    #[inline]
    pub fn use_prefix_seek(mut self) -> IterOptions {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    #[inline]
    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    #[inline]
    pub fn fill_cache(&self) -> bool {
        self.fill_cache
    }

    #[inline]
    pub fn set_fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    #[inline]
    pub fn key_only(&self) -> bool {
        self.key_only
    }

    #[inline]
    pub fn set_key_only(&mut self, v: bool) {
        self.key_only = v;
    }

    #[inline]
    pub fn lower_bound(&self) -> Option<&[u8]> {
        self.lower_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_lower_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = KeyBuilder::from_slice(bound, reserved_prefix_len, 0);
        self.lower_bound = Some(builder);
    }

    pub fn set_vec_lower_bound(&mut self, bound: Vec<u8>) {
        self.lower_bound = Some(KeyBuilder::from_vec(bound, 0, 0));
    }

    pub fn set_lower_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.lower_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn upper_bound(&self) -> Option<&[u8]> {
        self.upper_bound.as_ref().map(|v| v.as_slice())
    }

    #[inline]
    pub fn set_upper_bound(&mut self, bound: &[u8], reserved_prefix_len: usize) {
        let builder = KeyBuilder::from_slice(bound, reserved_prefix_len, 0);
        self.upper_bound = Some(builder);
    }

    pub fn set_vec_upper_bound(&mut self, bound: Vec<u8>) {
        self.upper_bound = Some(KeyBuilder::from_vec(bound, 0, 0));
    }

    pub fn set_upper_bound_prefix(&mut self, prefix: &[u8]) {
        if let Some(ref mut builder) = self.upper_bound {
            builder.set_prefix(prefix);
        }
    }

    #[inline]
    pub fn build_bounds(self) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let lower = self.lower_bound.map(KeyBuilder::build);
        let upper = self.upper_bound.map(KeyBuilder::build);
        (lower, upper)
    }

    #[inline]
    pub fn prefix_same_as_start(&self) -> bool {
        self.prefix_same_as_start
    }

    #[inline]
    pub fn set_prefix_same_as_start(mut self, enable: bool) -> IterOptions {
        self.prefix_same_as_start = enable;
        self
    }
}

impl Default for IterOptions {
    fn default() -> IterOptions {
        IterOptions {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_start: false,
            fill_cache: true,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}
