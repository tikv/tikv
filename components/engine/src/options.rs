// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use tikv_util::keybuilder::KeyBuilder;

use crate::SeekMode;

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
    pub sync: bool,
}

impl WriteOptions {
    pub fn new() -> WriteOptions {
        WriteOptions { sync: false }
    }

    pub fn set_sync(&mut self, sync: bool) {
        self.sync = sync;
    }
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        WriteOptions { sync: false }
    }
}

#[derive(Clone)]
pub struct CFOptions {}

impl CFOptions {
    pub fn new() -> CFOptions {
        CFOptions {}
    }
}

impl Default for CFOptions {
    fn default() -> CFOptions {
        CFOptions {}
    }
}

#[derive(Clone)]
pub struct IterOptionss {
    pub lower_bound: Option<KeyBuilder>,
    pub upper_bound: Option<KeyBuilder>,
    pub prefix_same_as_start: bool,
    pub fill_cache: bool,
    pub key_only: bool,
    pub seek_mode: SeekMode,
}

impl IterOptionss {
    pub fn new(
        lower_bound: Option<KeyBuilder>,
        upper_bound: Option<KeyBuilder>,
        fill_cache: bool,
    ) -> IterOptionss {
        IterOptionss {
            lower_bound,
            upper_bound,
            prefix_same_as_start: false,
            fill_cache,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }

    #[inline]
    pub fn use_prefix_seek(mut self) -> IterOptionss {
        self.seek_mode = SeekMode::Prefix;
        self
    }

    #[inline]
    pub fn total_order_seek_used(&self) -> bool {
        self.seek_mode == SeekMode::TotalOrder
    }

    #[inline]
    pub fn fill_cache(&mut self, v: bool) {
        self.fill_cache = v;
    }

    #[inline]
    pub fn key_only(&mut self, v: bool) {
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
    pub fn set_prefix_same_as_start(mut self, enable: bool) -> IterOptionss {
        self.prefix_same_as_start = enable;
        self
    }
}

impl Default for IterOptionss {
    fn default() -> IterOptionss {
        IterOptionss {
            lower_bound: None,
            upper_bound: None,
            prefix_same_as_start: false,
            fill_cache: false,
            key_only: false,
            seek_mode: SeekMode::TotalOrder,
        }
    }
}

pub struct DeleteRangeOptions {
    pub use_delete_files: bool,
    pub use_delete_range: bool,
}

impl DeleteRangeOptions {
    pub fn new() -> DeleteRangeOptions {
        DeleteRangeOptions {
            use_delete_files: false,
            use_delete_range: false,
        }
    }

    pub fn use_delete_files(&mut self, v: bool) {
        self.use_delete_files = v;
    }

    pub fn use_delete_range(&mut self, v: bool) {
        self.use_delete_range = v;
    }
}

impl Default for DeleteRangeOptions {
    fn default() -> DeleteRangeOptions {
        DeleteRangeOptions {
            use_delete_files: false,
            use_delete_range: false,
        }
    }
}

pub struct IngestExternalFileOptions {
    pub move_files: bool,
}

impl IngestExternalFileOptions {
    pub fn new() -> IngestExternalFileOptions {
        IngestExternalFileOptions { move_files: false }
    }

    pub fn move_files(&mut self, v: bool) {
        self.move_files = v;
    }
}

impl Default for IngestExternalFileOptions {
    fn default() -> IngestExternalFileOptions {
        IngestExternalFileOptions { move_files: false }
    }
}
