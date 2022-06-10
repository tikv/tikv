// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    alloc::{self, Layout},
    cmp, fs,
    fs::File,
    os::unix::prelude::FileExt,
    path::{Path, PathBuf},
    ptr::NonNull,
};

use bytes::{Buf, BufMut};
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

    /// Shortens the buffer, keeping the first `len` elements and dropping
    /// the rest.
    ///
    /// If `len` is greater than the buffer's current length, this has no
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

    /// Advances the internal cursor of the Buffer.
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

/// Magic Number of the WAL file. It's picked by running
///    echo rfengine.wal | sha1sum
/// and taking the leading 64 bits.
const WAL_MAGIC_NUMBER: u64 = 0xf126b8135c90588e;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u64)]
enum Version {
    V1 = 1,
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct WalHeader {
    version: Version,
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            version: Version::V1,
        }
    }
}

impl WalHeader {
    pub(crate) const fn len() -> usize {
        DmaBuffer::DMA_ALIGN
    }

    fn encode_to(&self, mut buf: &mut [u8]) {
        assert!(buf.len() >= Self::len());
        buf.put_u64_le(WAL_MAGIC_NUMBER);
        buf.put_u64_le(self.version as u64)
    }

    pub(crate) fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption("WAL header mismatch".to_owned()));
        }
        let magic_number = buf.get_u64_le();
        if magic_number != WAL_MAGIC_NUMBER {
            return Err(Error::Corruption("WAL magic number mismatch".to_owned()));
        }
        let version = buf.get_u64_le();
        if version != Version::V1 as u64 {
            return Err(Error::Corruption("WAL version mismatch".to_owned()));
        }
        Ok(Self {
            version: Version::V1,
        })
    }
}

pub(crate) struct WalWriter {
    dir: PathBuf,
    pub(crate) epoch_id: u32,
    pub(crate) wal_size: usize,
    header: WalHeader,
    fd: Option<File>,
    buf: DmaBuffer,
    // file_off is always aligned.
    file_off: u64,
}

impl WalWriter {
    pub(crate) fn new(dir: &Path, epoch_id: u32, wal_size: usize) -> Result<Self> {
        let mut buf = DmaBuffer::new(INITIAL_BUF_SIZE);
        buf.ensure_space(BATCH_HEADER_SIZE);
        // Safety: ensured enough space and `flush` will init the header.
        unsafe {
            buf.advance_mut(BATCH_HEADER_SIZE);
        }
        let mut writer = Self {
            dir: dir.to_path_buf(),
            epoch_id,
            wal_size: DmaBuffer::aligned_len(wal_size),
            header: WalHeader::default(),
            fd: None,
            buf,
            file_off: 0,
        };
        writer.open_file()?;
        Ok(writer)
    }

    fn file(&self) -> &File {
        self.fd.as_ref().unwrap()
    }

    pub(crate) fn seek(&mut self, file_offset: u64) {
        assert_eq!(
            file_offset as usize,
            DmaBuffer::aligned_len(file_offset as usize)
        );
        self.file_off = cmp::max(self.file_off, file_offset);
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
        let mut rotated = false;
        if DmaBuffer::aligned_len(self.buf.len()) + self.file_off as usize > self.wal_size {
            self.rotate()?;
            rotated = true;
        }

        let data_len = self.buf.len();
        let batch = self.buf.as_mut();
        let (mut batch_header, batch_payload) = batch.split_at_mut(BATCH_HEADER_SIZE);
        let checksum = crc32fast::hash(batch_payload);
        batch_header.put_u32_le(self.epoch_id);
        batch_header.put_u32_le(checksum);
        batch_header.put_u32_le(batch_payload.len() as u32);
        self.buf.pad_to_align();
        let aligned_len = self.buf.len();
        if aligned_len + self.file_off as usize != self.wal_size {
            // An empty batch header is added after each new batch to differentiate the old record.
            self.buf.ensure_space(BATCH_HEADER_SIZE);
            unsafe {
                let mut buf = self.buf.chunk_mut();
                buf.put_u32_le(0);
                buf.put_u32_le(0);
                buf.put_u32_le(0);
                self.buf.advance_mut(BATCH_HEADER_SIZE);
            }
            self.buf.pad_to_align();
        }

        let timer = Instant::now_coarse();
        self.file().write_all_at(self.buf.as_ref(), self.file_off)?;
        ENGINE_WAL_WRITE_DURATION_HISTOGRAM.observe(timer.saturating_elapsed_secs());
        self.file_off += aligned_len as u64;
        self.buf.truncate(BATCH_HEADER_SIZE);

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
        file_system::sync_dir(&self.dir)?;
        file.set_len(self.wal_size as u64)?;
        self.fd = Some(file);
        self.file_off = 0;
        self.write_header()?;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        let mut buf = DmaBuffer::new(WalHeader::len());
        unsafe {
            self.header.encode_to(buf.chunk_mut());
            buf.advance_mut(WalHeader::len());
        }
        buf.pad_to_align();
        self.file().write_all_at(buf.as_ref(), 0)?;
        self.file_off = buf.len() as u64;
        Ok(())
    }
}

pub(crate) fn get_wal_file_path(dir: &Path, epoch_id: u32) -> Result<PathBuf> {
    let filename = wal_file_name(dir, epoch_id);
    if !filename.exists() {
        if let Ok(Some(recycle_filename)) = find_recycled_file(dir) {
            // Before using the recycle file, empty the old wal header and the first batch header.
            let recycle_file = open_direct_file(&recycle_filename, true)?;
            let overwrite_len = WalHeader::len() + DmaBuffer::DMA_ALIGN;
            let mut buf = DmaBuffer::new(overwrite_len);
            unsafe {
                buf.advance_mut(overwrite_len);
            }
            buf.pad_to_align();
            recycle_file.write_all_at(buf.as_ref(), 0)?;
            fs::rename(recycle_filename, filename.clone())?;
            file_system::sync_dir(dir.join(RECYCLE_DIR))?;
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
    use crate::WalHeader;

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

    #[test]
    fn test_wal_header() {
        let wal_header = WalHeader::default();
        let mut buf = [0_u8; WalHeader::len()];
        wal_header.encode_to(buf.as_mut_slice());
        assert_eq!(WalHeader::decode(buf.as_slice()).unwrap(), wal_header);
    }
}
