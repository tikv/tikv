// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{File, OpenOptions},
    io, mem,
    os::unix::fs::{FileExt, OpenOptionsExt},
    path::PathBuf,
    sync::Arc,
};

use crate::{IOOp, IORateLimiter, IOType};

const WRITE_BATCH_SIZE: usize = 256 * 1024;
const ALIGN_SIZE: usize = 4096;
const ALIGN_MASK: u64 = 0xffff_f000;

pub struct DirectWriter {
    aligned_buf: Vec<u8>,
    rate_limiter: Arc<IORateLimiter>,
    io_type: IOType,
}

impl DirectWriter {
    pub fn new(rate_limiter: Arc<IORateLimiter>, io_type: IOType) -> Self {
        Self {
            aligned_buf: alloc_aligned(WRITE_BATCH_SIZE),
            rate_limiter,
            io_type,
        }
    }

    pub fn write_to_file(&mut self, buf: &[u8], filename: &PathBuf) -> io::Result<()> {
        let file = open_direct_file(filename, false)?;
        let origin_buf_len = buf.len();
        let mut cursor = 0usize;
        while cursor < buf.len() {
            let mut end = cursor + WRITE_BATCH_SIZE;
            if end > buf.len() {
                end = buf.len();
            }
            self.rate_limiter
                .request(self.io_type, IOOp::Write, WRITE_BATCH_SIZE);
            self.aligned_buf.truncate(0);
            self.aligned_buf.extend_from_slice(&buf[cursor..end]);
            let aligned_batch_len = aligned_len(self.aligned_buf.len());
            self.aligned_buf.resize(aligned_batch_len, 0);
            file.write_all_at(&self.aligned_buf, cursor as u64)?;
            cursor = end;
        }
        file.set_len(origin_buf_len as u64)?;
        file.sync_all()?;
        Ok(())
    }
}

pub fn open_direct_file(filename: &PathBuf, sync: bool) -> io::Result<File> {
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

    unsafe { Vec::from_raw_parts(ptr as *mut u8, 0, cap_units * mem::size_of::<AlignTo4K>()) }
}
