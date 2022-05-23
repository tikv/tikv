// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{self, Layout},
    cmp, fs,
    fs::File,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
    ptr::NonNull,
};

use bytes::BufMut;
use file_system::open_direct_file;
use tikv_util::time::Instant;

use crate::{write_batch::RegionBatch, *};

pub const BATCH_HEADER_SIZE: usize = 4 /* epoch_id */ + 4 /* checksum */ + 4 /* batch_len */;
pub(crate) const INITIAL_BUF_SIZE: usize = 8 * 1024 * 1024;
pub(crate) const RECYCLE_DIR: &str = "recycle";

/// `DmaBuffer` is a buffer used for direct I/O that follows the alignment restrictions
/// on the length and address of user-space buffers.
///
/// The typical usage is:
///
/// ```ignore
/// let mut buf = DmaBuffer::new(16*1024);
/// let data = b"data";
/// buf.ensure_space(data.len());
/// let chunk = unsafe { buf.chunk_mut() };
/// chunk.copy_from_slice(data);
/// unsafe { buf.advance_mut(data.len()) };
/// buf.pad_to_align();
/// write(buf.as_ref());
/// ```
pub(crate) struct DmaBuffer {
    data: NonNull<u8>,
    layout: Layout,
    len: usize,
}

unsafe impl Send for DmaBuffer {}

impl DmaBuffer {
    const DMA_ALIGN: usize = 4096;

    fn new(cap: usize) -> Self {
        debug_assert!(0 < cap && cap <= isize::MAX as usize);
        let layout = Layout::from_size_align(cap, Self::DMA_ALIGN)
            .unwrap()
            .pad_to_align();
        let data = unsafe { alloc::alloc(layout) };
        let data = NonNull::new(data).expect("memory allocation success");
        Self {
            data,
            layout,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn capacity(&self) -> usize {
        self.layout.size()
    }

    /// Shortens the vector, keeping the first `len` elements and dropping
    /// the rest.
    ///
    /// If `len` is greater than the vector's current length, this has no
    /// effect.
    fn truncate(&mut self, len: usize) {
        self.len = cmp::min(self.len, len);
    }

    /// Ensures enough space for `size`.
    fn ensure_space(&mut self, size: usize) {
        if self.capacity() - self.len >= size {
            return;
        }
        let require_cap = self
            .len
            .checked_add(size)
            .expect("capacity shouldn't overflow");
        let new_cap = cmp::max(self.layout.size() * 2, require_cap);
        let new_layout = Layout::from_size_align(new_cap, Self::DMA_ALIGN)
            .unwrap()
            .pad_to_align();
        let data = unsafe { alloc::realloc(self.data.as_ptr(), self.layout, new_layout.size()) };
        self.data = NonNull::new(data).expect("memory allocation success");
        self.layout = new_layout;
    }

    /// Pads the length of buf to the alignment. It doesn't pad zeros.
    fn pad_to_align(&mut self) {
        self.len = Self::aligned_len(self.len);
        assert!(self.len <= self.capacity());
    }

    /// Returns a mutable slice starting at the current position.
    ///
    /// This function is unsafe because the returned byte slice may represent uninitialized memory.
    unsafe fn chunk_mut(&mut self) -> &mut [u8] {
        &mut std::slice::from_raw_parts_mut(self.data.as_ptr(), self.capacity())[self.len..]
    }

    /// Advances the internal cursor of the BufMut
    ///
    /// The next call to `chunk_mut` will return a slice starting `cnt` bytes
    /// further into the underlying buf.
    ///
    /// This function is unsafe because there is no guarantee that the bytes
    /// being advanced past have been initialized.
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.len += cnt;
        assert!(self.len <= self.capacity());
    }

    pub(crate) fn aligned_len(len: usize) -> usize {
        len.wrapping_add(Self::DMA_ALIGN - 1) & !(Self::DMA_ALIGN - 1)
    }
}

impl Drop for DmaBuffer {
    fn drop(&mut self) {
        unsafe {
            alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl AsRef<[u8]> for DmaBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.len) }
    }
}

impl AsMut<[u8]> for DmaBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.len) }
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
        let wal_size = DmaBuffer::aligned_len(wal_size);
        let file_path = get_wal_file_path(dir, epoch_id)?;
        let fd = open_direct_file(&file_path, true)?;
        let mut buf = DmaBuffer::new(INITIAL_BUF_SIZE);
        buf.ensure_space(BATCH_HEADER_SIZE);
        // Safety: ensured enough space and `flush` will init the header.
        unsafe {
            buf.advance_mut(BATCH_HEADER_SIZE);
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
        assert_eq!(file_offset & (DmaBuffer::DMA_ALIGN - 1) as u64, 0);
        self.file_off = file_offset;
    }

    pub(crate) fn append_region_data(&mut self, region_batch: &RegionBatch) {
        let data_len = region_batch.encoded_len();
        self.buf.ensure_space(data_len);
        // Safety: `data_len` is the length of data encoded by `encode_to` and
        // `ensure_space` ensures enough space.
        unsafe {
            region_batch.encode_to(&mut self.buf.chunk_mut());
            self.buf.advance_mut(data_len);
        }
    }

    pub(crate) fn flush(&mut self) -> Result<(usize, bool)> {
        let data_len = self.buf.len();
        let batch = self.buf.as_mut();
        let (mut batch_header, batch_payload) = batch.split_at_mut(BATCH_HEADER_SIZE);
        let checksum = crc32fast::hash(batch_payload);
        batch_header.put_u32_le(self.epoch_id);
        batch_header.put_u32_le(checksum);
        batch_header.put_u32_le(batch_payload.len() as u32);
        self.buf.pad_to_align();

        let timer = Instant::now_coarse();
        self.fd.write_all_at(self.buf.as_ref(), self.file_off)?;
        ENGINE_WAL_WRITE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        self.file_off += self.buf.len() as u64;
        self.buf.truncate(BATCH_HEADER_SIZE);

        let mut rotated = false;
        if self.file_off as usize > self.wal_size {
            self.rotate()?;
            rotated = true;
        }

        Ok((data_len, rotated))
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

#[cfg(test)]
mod tests {
    use super::DmaBuffer;

    #[test]
    fn test_dma_buffer() {
        let mut buf = DmaBuffer::new(4095);
        assert_eq!(buf.layout.size(), DmaBuffer::aligned_len(4095));
        let addr = buf.data.as_ptr() as usize;
        assert_eq!(addr, DmaBuffer::aligned_len(addr));

        let data = b"data";
        for i in 1..=1025 {
            buf.ensure_space(data.len());
            let chunk = unsafe { buf.chunk_mut() };
            chunk[..data.len()].copy_from_slice(data);
            unsafe { buf.advance_mut(data.len()) };
            assert_eq!(buf.len(), data.len() * i);
            assert_eq!(buf.as_ref(), data.repeat(i));
            assert_eq!(buf.as_mut(), data.repeat(i));
        }
        assert_eq!(buf.layout.size(), 8192);
        let addr = buf.data.as_ptr() as usize;
        assert_eq!(addr, DmaBuffer::aligned_len(addr));
    }
}
