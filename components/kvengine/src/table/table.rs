// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, ptr, result, slice};

use byteorder::{ByteOrder, LittleEndian};
use thiserror::Error;

use crate::dfs;

pub trait Iterator: Send {
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

    fn seek_to_version(&mut self, version: u64) -> bool {
        if version >= self.value().version {
            return true;
        }
        while self.next_version() {
            if version >= self.value().version {
                return true;
            }
        }
        false
    }

    fn next_all_version(&mut self) {
        if !self.next_version() {
            self.next()
        }
    }
}

pub const BIT_DELETE: u8 = 1;

pub fn is_deleted(meta: u8) -> bool {
    meta & BIT_DELETE > 0
}

const VALUE_PTR_OFF: usize = 10;

// Value is a short life struct used to pass value across iterators.
// It is valid until iterator call next or next_version.
// As long as the value is never escaped, there will be no dangling pointer.
#[derive(Debug, Clone, Copy)]
pub struct Value {
    ptr: *const u8,
    pub meta: u8,
    user_meta_len: u8,
    val_len: u32,
    pub version: u64,
}

unsafe impl Send for Value {}

impl Value {
    pub(crate) fn new() -> Self {
        Self {
            ptr: ptr::null(),
            meta: 0,
            user_meta_len: 0,
            val_len: 0,
            version: 0,
        }
    }

    pub(crate) fn encode(&self, buf: &mut [u8]) {
        buf[0] = self.meta;
        buf[1] = self.user_meta_len;
        LittleEndian::write_u64(&mut buf[2..10], self.version);
        unsafe {
            if self.user_meta_len > 0 {
                ptr::copy(
                    self.ptr,
                    buf[VALUE_PTR_OFF..].as_mut_ptr(),
                    self.user_meta_len as usize,
                )
            }
            let off = VALUE_PTR_OFF + self.user_meta_len as usize;
            ptr::copy(
                self.ptr.add(self.user_meta_len as usize),
                buf[off..].as_mut_ptr(),
                self.val_len as usize,
            )
        }
    }

    pub fn encode_buf(meta: u8, user_meta: &[u8], version: u64, val: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(VALUE_PTR_OFF + user_meta.len() + val.len());
        buf.resize(buf.capacity(), 0);
        let m_buf = buf.as_mut_slice();
        m_buf[0] = meta;
        m_buf[1] = user_meta.len() as u8;
        LittleEndian::write_u64(&mut m_buf[2..], version);
        let off = 10usize;
        m_buf[off..off + user_meta.len()].copy_from_slice(user_meta);
        let off = 10 + user_meta.len();
        m_buf[off..off + val.len()].copy_from_slice(val);
        buf
    }

    pub fn decode(bin: &[u8]) -> Self {
        let meta = bin[0];
        let user_meta_len = bin[1];
        let version = LittleEndian::read_u64(&bin[2..10]);
        let val_len = (bin.len() - VALUE_PTR_OFF - user_meta_len as usize) as u32;
        Self {
            ptr: bin[VALUE_PTR_OFF..].as_ptr(),
            meta,
            user_meta_len,
            val_len,
            version,
        }
    }

    pub(crate) fn new_with_meta_version(
        meta: u8,
        version: u64,
        user_meta_len: u8,
        bin: &[u8],
    ) -> Self {
        Self {
            ptr: bin.as_ptr(),
            meta,
            user_meta_len,
            val_len: bin.len() as u32 - user_meta_len as u32,
            version,
        }
    }

    pub(crate) fn new_tombstone(version: u64) -> Self {
        Self {
            ptr: ptr::null(),
            meta: BIT_DELETE,
            user_meta_len: 0,
            val_len: 0,
            version,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.meta == 0 && self.ptr.is_null()
    }

    pub(crate) fn is_valid(&self) -> bool {
        !self.is_empty()
    }

    #[inline(always)]
    pub(crate) fn is_deleted(&self) -> bool {
        is_deleted(self.meta)
    }

    pub fn value_len(&self) -> usize {
        self.val_len as usize
    }

    pub fn user_meta_len(&self) -> usize {
        self.user_meta_len as usize
    }

    #[inline(always)]
    pub fn user_meta(&self) -> &[u8] {
        unsafe { slice::from_raw_parts::<u8>(self.ptr, self.user_meta_len as usize) }
    }

    #[inline(always)]
    pub fn get_value(&self) -> &[u8] {
        unsafe {
            slice::from_raw_parts::<u8>(
                self.ptr.add(self.user_meta_len as usize),
                self.val_len as usize,
            )
        }
    }

    pub fn encoded_size(&self) -> usize {
        self.user_meta_len as usize + self.val_len as usize + VALUE_PTR_OFF
    }
}
pub struct EmptyIterator;

impl Iterator for EmptyIterator {
    fn next(&mut self) {}

    fn next_version(&mut self) -> bool {
        false
    }

    fn rewind(&mut self) {}

    fn seek(&mut self, _: &[u8]) {}

    fn key(&self) -> &[u8] {
        &[]
    }

    fn value(&self) -> Value {
        Value::new()
    }

    fn valid(&self) -> bool {
        false
    }
}

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("Key not found")]
    NotFound,
    #[error("Invalid checksum {0}")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Invalid file size")]
    InvalidFileSize,
    #[error("Invalid magic number")]
    InvalidMagicNumber,
    #[error("IO error: {0}")]
    Io(String),
    #[error("EOF")]
    EOF,
    #[error("{0}")]
    Other(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

impl From<dfs::Error> for Error {
    #[inline]
    fn from(e: dfs::Error) -> Error {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;

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

#[derive(Clone, Copy, Default)]
pub struct LocalAddr {
    pub start: usize,
    pub end: usize,
}

impl LocalAddr {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn get(self, buf: &[u8]) -> &[u8] {
        &buf[self.start..self.end]
    }

    pub fn len(self) -> usize {
        (self.end - self.start) as usize
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

pub fn new_merge_iterator<'a>(
    mut iters: Vec<Box<dyn Iterator + 'a>>,
    reverse: bool,
) -> Box<dyn Iterator + 'a> {
    match iters.len() {
        0 => Box::new(EmptyIterator {}),
        1 => iters.pop().unwrap(),
        2 => {
            let second_iter: Box<dyn Iterator + 'a> = iters.pop().unwrap();
            let first_iter: Box<dyn Iterator + 'a> = iters.pop().unwrap();
            let first: Box<super::MergeIteratorChild<'a>> =
                Box::new(super::MergeIteratorChild::new(true, first_iter));
            let second = Box::new(super::MergeIteratorChild::new(false, second_iter));
            let merge_iter = super::MergeIterator::new(first, second, reverse);
            Box::new(merge_iter)
        }
        _ => {
            let mid = iters.len() / 2;
            let mut second = vec![];
            for _ in 0..mid {
                second.push(iters.pop().unwrap())
            }
            second.reverse();
            let first_it = new_merge_iterator(iters, reverse);
            let second_it = new_merge_iterator(second, reverse);
            new_merge_iterator(vec![first_it, second_it], reverse)
        }
    }
}
