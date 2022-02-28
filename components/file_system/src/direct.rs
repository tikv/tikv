// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{IOOp, IORateLimiter, IOType};
use std::fs::{File, OpenOptions};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::{io, mem};

const WRITE_BATCH_SIZE: usize = 256 * 1024;
const ALIGN_SIZE: usize = 4096;
const ALIGN_MASK: u64 = 0xffff_f000;

pub struct DirectWriter {
    buf: Vec<u8>,
    rate_limiter: Arc<IORateLimiter>,
    io_type: IOType,
    reserved_cap: usize,
}

impl Deref for DirectWriter {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for DirectWriter {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl DirectWriter {
    pub fn new(rate_limiter: Arc<IORateLimiter>, io_type: IOType) -> Self {
        Self {
            buf: vec![],
            rate_limiter,
            io_type,
            reserved_cap: 0,
        }
    }

    pub fn write_to_file(&mut self, filename: PathBuf) -> io::Result<()> {
        assert!(
            self.buf.len() < self.reserved_cap,
            "call reserve before write to buffer"
        );
        let file = open_direct_file(filename, false)?;
        let origin_buf_len = self.buf.len();
        self.resize(aligned_len(origin_buf_len), 0);
        let mut cursor = 0usize;
        while cursor < self.buf.len() {
            let mut end = cursor + WRITE_BATCH_SIZE;
            if end > self.buf.len() {
                end = self.buf.len();
            }
            self.rate_limiter
                .request(self.io_type, IOOp::Write, WRITE_BATCH_SIZE);
            file.write_all_at(&self.buf[cursor..end], cursor as u64)?;
            cursor = end;
        }
        file.set_len(origin_buf_len as u64)?;
        file.sync_all()?;
        Ok(())
    }

    /// Make sure the buffer has sufficient capacity.
    pub fn reserve(&mut self, additional: usize) {
        if self.buf.len() + additional > self.buf.capacity() {
            let mut new_buf = alloc_aligned(self.buf.len() + additional + self.buf.len() / 2);
            self.reserved_cap = new_buf.capacity();
            new_buf.extend_from_slice(self.buf.as_slice());
            self.buf = new_buf;
        }
    }
}

pub fn open_direct_file(filename: PathBuf, sync: bool) -> io::Result<File> {
    let mut flag = o_direct_flag();
    if sync {
        flag |= libc::O_DSYNC;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(flag)
        .open(filename)?;
    Ok(file)
}

fn o_direct_flag() -> i32 {
    if std::env::consts::OS != "linux" {
        return 0;
    }
    if std::env::consts::ARCH == "aarch64" {
        0x10000
    } else {
        0x4000
    }
}

pub fn aligned_len(origin_len: usize) -> usize {
    ((origin_len + ALIGN_SIZE - 1) as u64 & ALIGN_MASK) as usize
}

#[repr(C, align(4096))]
struct AlignTo4K([u8; ALIGN_SIZE]);

fn alloc_aligned(n_bytes: usize) -> Vec<u8> {
    let n_units = (n_bytes + ALIGN_SIZE - 1) / ALIGN_SIZE;

    let mut aligned: Vec<AlignTo4K> = Vec::with_capacity(n_units);

    let ptr = aligned.as_mut_ptr();
    let cap_units = aligned.capacity();

    mem::forget(aligned);
    let result =
        unsafe { Vec::from_raw_parts(ptr as *mut u8, 0, cap_units * mem::size_of::<AlignTo4K>()) };
    result
}
