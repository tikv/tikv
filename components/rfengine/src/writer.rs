// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use std::{
    fs::File,
    fs::{self, OpenOptions},
    mem,
    os::unix::fs::OpenOptionsExt,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

use bytes::{Buf, BufMut};

pub const ENTRY_HEADER_SIZE: usize = 8;
pub const BATCH_HEADER_SIZE: usize = 12;
pub(crate) const ALIGN_SIZE: usize = 4096;
pub(crate) const ALIGN_MASK: u64 = 0xffff_f000;
pub(crate) const INITIAL_BUF_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const RECYCLE_DIR: &str = "recycle";

const O_DIRECT: i32 = 0o0040000;

#[repr(C, align(64))]
struct AlignTo4K([u8; ALIGN_SIZE]);

pub fn alloc_aligned(n_bytes: usize) -> Vec<u8> {
    let n_units = (n_bytes + ALIGN_SIZE - 1) / ALIGN_SIZE;

    let mut aligned: Vec<AlignTo4K> = Vec::with_capacity(n_units);

    let ptr = aligned.as_mut_ptr();
    let cap_units = aligned.capacity();

    mem::forget(aligned);
    let result =
        unsafe { Vec::from_raw_parts(ptr as *mut u8, 0, cap_units * mem::size_of::<AlignTo4K>()) };
    result
}

pub(crate) struct WALWriter {
    dir: PathBuf,
    pub(crate) epoch_id: u32,
    pub(crate) wal_size: usize,
    fd: File,
    buf: Vec<u8>,
    // file_off is always aligned.
    file_off: u64,
}

impl WALWriter {
    pub(crate) fn new(dir: &Path, epoch_id: u32, wal_size: usize) -> Result<Self> {
        let wal_size = (wal_size + ALIGN_SIZE - 1) & ALIGN_MASK as usize;
        let fd = open_direct_file(dir, epoch_id, wal_size)?;
        let mut buf = alloc_aligned(INITIAL_BUF_SIZE);
        buf.resize(BATCH_HEADER_SIZE, 0);
        Ok(Self {
            dir: dir.to_path_buf(),
            epoch_id,
            wal_size,
            fd,
            buf,
            file_off: 0,
        })
    }

    pub(crate) fn seek(&mut self, file_offset: u64) {
        assert_eq!(file_offset & ALIGN_MASK, 0);
        self.file_off = file_offset;
    }

    pub(crate) fn offset(&self) -> u64 {
        self.buf.len() as u64 + self.file_off
    }

    pub(crate) fn reallocate(&mut self) {
        let new_cap = self.buf.capacity() * 2;
        let mut new_buf = alloc_aligned(new_cap);
        new_buf.truncate(0);
        new_buf.extend_from_slice(self.buf.as_slice());
        let _ = mem::replace(&mut self.buf, new_buf);
    }

    pub(crate) fn aligned_buf_len(&self) -> usize {
        ((self.buf.len() + ALIGN_SIZE - 1) as u64 & ALIGN_MASK) as usize
    }

    pub(crate) fn append_header(&mut self, tp: u32, length: usize) {
        self.buf.put_u32_le(tp);
        self.buf.put_u32_le(length as u32);
    }

    pub(crate) fn append_raft_log(&mut self, op: RaftLogOp) {
        let size = raft_log_size(&op);
        if self.buf.len() + size > self.buf.capacity() {
            self.reallocate();
        }
        self.append_header(TYPE_RAFT_LOG, size - ENTRY_HEADER_SIZE);
        self.buf.put_u64_le(op.region_id);
        self.buf.put_u64_le(op.index);
        self.buf.put_u32_le(op.term);
        self.buf.put_i32_le(op.e_type);
        self.buf.extend_from_slice(op.data.chunk());
    }

    pub(crate) fn append_state(&mut self, region_id: u64, key: &[u8], val: &[u8]) {
        let size = state_size(key, val);
        if self.buf.len() + size > self.buf.capacity() {
            self.reallocate();
        }
        self.append_header(TYPE_STATE, size - ENTRY_HEADER_SIZE);
        self.buf.put_u64_le(region_id);
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.extend_from_slice(val);
    }

    pub(crate) fn append_truncate(&mut self, region_id: u64, index: u64) {
        let size = ENTRY_HEADER_SIZE + 16;
        if self.buf.len() + size > self.buf.capacity() {
            self.reallocate();
        }
        self.append_header(TYPE_TRUNCATE, size - ENTRY_HEADER_SIZE);
        self.buf.put_u64_le(region_id);
        self.buf.put_u64_le(index);
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        let batch = &mut self.buf[..];
        let (mut batch_header, batch_payload) = batch.split_at_mut(BATCH_HEADER_SIZE);
        batch_header.put_u32_le(self.epoch_id);
        let checksum = crc32c::crc32c(batch_payload);
        batch_header.put_u32_le(checksum);
        batch_header.put_u32_le(batch_payload.len() as u32);
        self.buf.resize(self.aligned_buf_len(), 0);
        self.fd.write_all_at(&self.buf[..], self.file_off)?;
        self.file_off += self.aligned_buf_len() as u64;
        self.reset_batch();
        Ok(())
    }

    pub(crate) fn reset_batch(&mut self) {
        self.buf.truncate(0);
        self.buf.extend_from_slice(&[0; BATCH_HEADER_SIZE]);
    }

    pub(crate) fn rotate(&mut self) -> Result<()> {
        self.epoch_id += 1;
        self.open_file()
    }

    pub(crate) fn open_file(&mut self) -> Result<()> {
        let file = open_direct_file(&self.dir, self.epoch_id, self.wal_size)?;
        self.fd = file;
        self.reset_batch();
        self.file_off = 0;
        Ok(())
    }
}

pub(crate) fn open_direct_file(dir: &Path, epoch_id: u32, wal_size: usize) -> Result<File> {
    let filename = wal_file_name(dir, epoch_id);
    if !filename.exists() {
        if let Ok(Some(recycle_filename)) = find_recycled_file(dir) {
            fs::rename(recycle_filename, filename.clone())?;
        }
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(O_DIRECT)
        .custom_flags(libc::O_DSYNC)
        .open(filename)?;
    file.set_len(wal_size as u64)?;
    Ok(file)
}

pub(crate) fn find_recycled_file(dir: &Path) -> Result<Option<PathBuf>> {
    let recycle_dir = dir.join(RECYCLE_DIR);
    let read_dir = recycle_dir.read_dir()?;
    let mut recycle_file = None;
    for x in read_dir {
        let dir_entry = x?;
        if dir_entry.path().is_file() {
            recycle_file = Some(dir_entry.path())
        }
    }
    Ok(recycle_file)
}

pub(crate) fn raft_log_size(op: &RaftLogOp) -> usize {
    ENTRY_HEADER_SIZE + 16 + 8 + op.data.len()
}

pub(crate) fn state_size(key: &[u8], val: &[u8]) -> usize {
    ENTRY_HEADER_SIZE + 8 + 2 + key.len() + val.len()
}
