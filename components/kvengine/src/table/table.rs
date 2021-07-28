// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use std::{ptr, slice};

pub trait Table {
    fn id(&self) -> u64;
    fn size(&self) -> i64;
    fn smallest(&self) -> &[u8];
    fn biggest(&self) -> &[u8];
    fn new_iterator(&self) -> Box<dyn Iterator>;
    fn get(&self, key: &[u8], version: u64, key_hash: u64) -> Result<Value, Error>;
    fn has_overlap(&self, start: &[u8], end: &[u8], include_end: bool) -> bool;
}

pub trait Iterator {
    // next returns the next entry with different key on the latest version.
    // If old version is needed, call next_version.
    fn next(&mut self);

    // next_version set the current entry to an older version.
    // The iterator must be valid to call this method.
    // It returns true if there is an older version, returns false if there is no older version.
    // The iterator is still valid and on the same key.
    fn next_version(&mut self) -> bool;

    fn rewind(&mut self);

    fn seek(&mut self, key: &[u8]);

    fn key(&self) -> &[u8];

    fn value(&self) -> Value;

    fn valid(&self) -> bool;

    fn close(&self);
}

pub fn seek_to_version(it: &mut dyn Iterator, version: u64) -> bool {
    if version >= it.value().version {
        return true;
    }
    while it.next_version() {
        if version >= it.value().version {
            return true;
        }
    }
    return false;
}

pub fn next_all_version(it: &mut dyn Iterator) {
    if !it.next_version() {
        it.next()
    }
}

const BIT_DELETE: u8 = 1;

pub fn is_deleted(meta: u8) -> bool {
    meta & BIT_DELETE > 0
}

const VALUE_PTR_OFF: usize = 10;

#[derive(Debug, Clone, Copy)]
pub struct Value {
    pub meta: u8,
    user_meta_len: u8,
    val_len: u32,
    pub version: u64,
    um_ptr: *const u8,
    val_ptr: *const u8,
}

impl Value {
    pub fn encode(&self, buf: &mut [u8]) {
        buf[0] = self.meta;
        buf[1] = self.user_meta_len;
        LittleEndian::write_u64(&mut buf[2..10], self.version);
        unsafe {
            if self.user_meta_len > 0 {
                ptr::copy(
                    self.um_ptr,
                    buf[VALUE_PTR_OFF..].as_mut_ptr(),
                    self.user_meta_len as usize,
                )
            }
            let off = VALUE_PTR_OFF + self.user_meta_len as usize;
            ptr::copy(self.val_ptr, buf[off..].as_mut_ptr(), self.val_len as usize)
        }
    }

    pub fn decode(bin: &[u8]) -> Self {
        let meta = bin[0];
        let user_meta_len = bin[1];
        let version = LittleEndian::read_u64(&bin[2..10]);
        let val_len = (bin.len() - VALUE_PTR_OFF - user_meta_len as usize) as u32;
        Self {
            meta,
            user_meta_len,
            val_len,
            version,
            um_ptr: unsafe { bin.as_ptr().add(VALUE_PTR_OFF) },
            val_ptr: unsafe { bin.as_ptr().add(VALUE_PTR_OFF + user_meta_len as usize) },
        }
    }

    pub fn new(meta: u8, user_meta: &[u8], version: u64, val: &[u8]) -> Self {
        Self {
            meta,
            user_meta_len: 0,
            val_len: val.len() as u32,
            version,
            um_ptr: user_meta.as_ptr(),
            val_ptr: val.as_ptr(),
        }
    }

    pub fn empty() -> Self {
        Self {
            meta: 0,
            user_meta_len: 0,
            val_len: 0,
            version: 0,
            um_ptr: ptr::null(),
            val_ptr: ptr::null(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.meta == 0 && self.val_ptr == ptr::null()
    }

    pub fn user_meta(&self) -> &[u8] {
        unsafe { slice::from_raw_parts::<u8>(self.val_ptr, self.user_meta_len as usize) }
    }

    pub fn get_value(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts::<u8>(
                self.val_ptr.add(self.user_meta_len as usize),
                self.val_len as usize,
            )
        }
    }

    pub fn encoded_size(&self) -> usize {
        self.user_meta_len as usize + self.val_len as usize + VALUE_PTR_OFF
    }
}

pub enum Error {
    NotFound,
}

/// simple rewrite of golang sort.Search
pub fn search<F>(n: usize, mut f: F) -> usize
where
    F: FnMut(usize) -> bool,
{
    let mut i = 0;
    let mut j = n;
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}
