// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fs,
    fs::File,
    mem,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
};

use bytes::BufMut;
use file_system::open_direct_file;
use tikv_util::time::Instant;

use crate::{write_batch::RegionBatch, *};

pub const BATCH_HEADER_SIZE: usize = 12;
pub(crate) const ALIGN_SIZE: usize = 4096;
pub(crate) const ALIGN_MASK: u64 = 0xffff_f000;
pub(crate) const INITIAL_BUF_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const RECYCLE_DIR: &str = "recycle";

pub(crate) struct DmaBuffer(Vec<u8>);

impl DmaBuffer {
    const DMA_ALIGN: usize = 4096;

    fn new(cap: usize) -> Self {
        Self(Self::allocate(cap))
    }

    fn allocate(cap: usize) -> Vec<u8> {
        #[repr(align(4096))]
        struct AlignTo4K([u8; DmaBuffer::DMA_ALIGN]);

        let aligned_len = Self::aligned_len(cap);
        let mut aligned: Vec<AlignTo4K> = Vec::with_capacity(aligned_len / DmaBuffer::DMA_ALIGN);
        let ptr = aligned.as_mut_ptr();

        mem::forget(aligned);

        unsafe { Vec::from_raw_parts(ptr as *mut u8, 0, aligned_len) }
    }

    fn deallocate(mut buf: Vec<u8>) {
        unsafe {
            std::alloc::dealloc(
                buf.as_mut_ptr(),
                std::alloc::Layout::from_size_align_unchecked(buf.capacity(), DmaBuffer::DMA_ALIGN),
            )
        }
        mem::forget(buf);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn ensure_space(&mut self, size: usize) {
        if self.0.capacity() - self.0.len() >= size {
            return;
        }

        let mut new = Self::allocate(std::cmp::max(self.0.capacity() * 2, self.0.len() + size));
        new.extend_from_slice(self.0.as_slice());
        let old = mem::replace(&mut self.0, new);
        Self::deallocate(old);
    }

    unsafe fn as_vec(&mut self) -> &mut Vec<u8> {
        &mut self.0
    }

    fn pad_to_align(&mut self) {
        let aligned_len = Self::aligned_len(self.0.len());
        assert!(aligned_len <= self.0.capacity());
        unsafe {
            self.0.set_len(aligned_len);
        }
    }

    pub(crate) fn aligned_len(len: usize) -> usize {
        len.wrapping_add(Self::DMA_ALIGN - 1) & !(Self::DMA_ALIGN - 1)
    }
}

impl Drop for DmaBuffer {
    fn drop(&mut self) {
        Self::deallocate(mem::take(&mut self.0))
    }
}

pub(crate) struct WALWriter {
    dir: PathBuf,
    pub(crate) epoch_id: u32,
    pub(crate) wal_size: usize,
    fd: File,
    buf: DmaBuffer,
    // file_off is always aligned.
    file_off: u64,
}

impl WALWriter {
    pub(crate) fn new(dir: &Path, epoch_id: u32, wal_size: usize) -> Result<Self> {
        let wal_size = (wal_size + ALIGN_SIZE - 1) & ALIGN_MASK as usize;
        let file_path = get_wal_file_path(dir, epoch_id)?;
        let fd = open_direct_file(&file_path, true)?;
        let mut buf = DmaBuffer::new(INITIAL_BUF_SIZE);
        unsafe {
            buf.as_vec().resize(BATCH_HEADER_SIZE, 0);
        }
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
        assert_eq!(file_offset & ALIGN_MASK.reverse_bits(), 0);
        self.file_off = file_offset;
    }

    pub(crate) fn append_region_data(&mut self, region_batch: &RegionBatch) {
        self.buf.ensure_space(region_batch.encoded_len());
        region_batch.encode_to(unsafe { self.buf.as_vec() });
    }

    pub(crate) fn flush(&mut self) -> Result<bool> {
        let mut rotated = false;
        if DmaBuffer::aligned_len(self.buf.len()) + self.file_off as usize > self.wal_size {
            self.rotate()?;
            rotated = true;
        }
        let batch = unsafe { self.buf.as_vec() };
        let (mut batch_header, batch_payload) = batch.split_at_mut(BATCH_HEADER_SIZE);
        let checksum = crc32fast::hash(batch_payload);
        batch_header.put_u32_le(self.epoch_id);
        batch_header.put_u32_le(checksum);
        batch_header.put_u32_le(batch_payload.len() as u32);
        self.buf.pad_to_align();
        let timer = Instant::now_coarse();
        self.fd
            .write_all_at(unsafe { self.buf.as_vec() }, self.file_off)?;
        ENGINE_WAL_WRITE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        self.file_off += self.buf.len() as u64;
        self.reset_batch();
        Ok(rotated)
    }

    pub(crate) fn reset_batch(&mut self) {
        unsafe {
            self.buf.as_vec().truncate(BATCH_HEADER_SIZE);
        }
    }

    fn rotate(&mut self) -> Result<()> {
        let timer = Instant::now_coarse();
        self.epoch_id += 1;
        let res = self.open_file();
        ENGINE_ROTATE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        res
    }

    pub(crate) fn open_file(&mut self) -> Result<()> {
        let filename = get_wal_file_path(&self.dir, self.epoch_id)?;
        let file = open_direct_file(&filename, true)?;
        file.set_len(self.wal_size as u64)?;
        self.fd = file;
        self.file_off = 0;
        Ok(())
    }

    pub(crate) fn buf_size(&mut self) -> usize {
        self.buf.len()
    }
}

pub(crate) fn get_wal_file_path(dir: &Path, epoch_id: u32) -> Result<PathBuf> {
    let filename = wal_file_name(dir, epoch_id);
    if !filename.exists() {
        if let Ok(Some(recycle_filename)) = find_recycled_file(dir) {
            fs::rename(recycle_filename, filename.clone())?;
        }
    }
    Ok(filename)
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
