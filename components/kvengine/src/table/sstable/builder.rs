// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{convert::TryFrom, mem, slice};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use farmhash;
use xorf::BinaryFuse8;

use super::super::table::Value;

pub const CRC32_IEEE: u8 = 1;
pub const PROP_KEY_SMALLEST: &str = "smallest";
pub const PROP_KEY_BIGGEST: &str = "biggest";
pub const PROP_KEY_MAX_TS: &str = "max_ts";
pub const PROP_KEY_ENTRIES: &str = "entries";
pub const PROP_KEY_OLD_ENTRIES: &str = "old_entries";
pub const PROP_KEY_TOMBS: &str = "tombs";
pub const AUX_INDEX_BINARY_FUSE8: u32 = 1;
pub const INDEX_FORMAT_V1: u32 = 1;
pub const BLOCK_FORMAT_V1: u32 = 1;
pub const NO_COMPRESSION: u8 = 0;
pub const LZ4_COMPRESSION: u8 = 1;
pub const ZSTD_COMPRESSION: u8 = 2;
pub const TABLE_FORMAT_V1: u16 = 1;
pub const MAGIC_NUMBER: u32 = 2940551257;
pub const META_HAS_OLD: u8 = 1 << 1;
pub const BLOCK_ADDR_SIZE: usize = mem::size_of::<BlockAddress>();

#[derive(Clone, Copy)]
pub struct TableBuilderOptions {
    pub block_size: usize,
    pub max_table_size: usize,
    pub compression_tps: [u8; 3],
}

impl Default for TableBuilderOptions {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024,
            max_table_size: 16 * 1024 * 1024,
            compression_tps: [LZ4_COMPRESSION, LZ4_COMPRESSION, ZSTD_COMPRESSION],
        }
    }
}

#[derive(Default)]
struct EntrySlice {
    buf: Vec<u8>,
    end_offs: Vec<u32>,
}

#[allow(dead_code)]
impl EntrySlice {
    fn new() -> Self {
        Self {
            buf: Vec::new(),
            end_offs: Vec::new(),
        }
    }

    fn append(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
        self.end_offs.push(self.buf.len() as u32);
    }

    fn append_value(&mut self, val: Value) {
        let old_len = self.buf.len();
        let new_len = old_len + val.encoded_size();
        self.buf.resize(new_len, 0);
        let slice = self.buf.as_mut_slice();
        val.encode(&mut slice[old_len..]);
        self.end_offs.push(new_len as u32);
    }

    fn length(&self) -> usize {
        self.end_offs.len()
    }

    fn get_last(&self) -> &[u8] {
        self.get_entry(self.length() - 1)
    }

    fn get_entry(&self, i: usize) -> &[u8] {
        let start_off = if i > 0 {
            self.end_offs[i - 1] as usize
        } else {
            0
        };
        let slice = self.buf.as_slice();
        &slice[start_off..self.end_offs[i] as usize]
    }

    fn size(&self) -> usize {
        self.buf.len() + self.end_offs.len() * 4
    }

    fn reset(&mut self) {
        self.buf.truncate(0);
        self.end_offs.truncate(0);
    }
}

#[allow(dead_code)]
#[derive(Default)]
pub struct Builder {
    fid: u64,

    block_builder: BlockBuilder,
    old_builder: BlockBuilder,
    block_size: usize,
    checksum_tp: u8,
    key_hashes: Vec<u64>,
    smallest: Vec<u8>,
    biggest: Vec<u8>,
    max_ts: u64,
    old_entries: u32,
    tombs: u32,
}

impl Builder {
    pub fn new(fid: u64, block_size: usize, compression_tp: u8) -> Self {
        let mut x = Self::default();
        x.fid = fid;
        x.checksum_tp = CRC32_IEEE;
        x.block_size = block_size;
        x.block_builder.compression_tp = compression_tp;
        x.old_builder.compression_tp = compression_tp;
        x
    }

    pub fn reset(&mut self, fid: u64) {
        self.fid = fid;
        self.block_builder.reset_all();
        self.old_builder.reset_all();
        self.key_hashes.truncate(0);
        self.smallest.truncate(0);
        self.biggest.truncate(0);
        self.max_ts = 0;
        self.tombs = 0;
    }

    fn add_property(buf: &mut BytesMut, key: &[u8], val: &[u8]) {
        buf.put_u16_le(key.len() as u16);
        buf.put_slice(key);
        buf.put_u32_le(val.len() as u32);
        buf.put_slice(val);
    }

    pub fn add(&mut self, key: &[u8], val: Value) {
        if self.block_builder.same_last_key(key) {
            self.block_builder
                .set_last_entry_old_ver_if_zero(val.version);
            self.old_builder.add_entry(key, val);
            self.old_entries += 1;
        } else {
            // Only try to finish block when the key is different than last.
            if self.block_builder.block.size > self.block_size {
                self.block_builder.finish_block(self.fid, self.checksum_tp);
            }
            if self.old_builder.block.size > self.block_size {
                self.old_builder.finish_block(self.fid, self.checksum_tp);
            }
            self.block_builder.add_entry(key, val);
            self.key_hashes.push(farmhash::fingerprint64(key));
            if self.smallest.is_empty() {
                self.smallest.extend_from_slice(key);
            }
            if self.max_ts < val.version {
                self.max_ts = val.version;
            }
        }
        if val.value_len() == 0 {
            self.tombs += 1;
        }
    }

    pub fn estimated_size(&self) -> usize {
        let mut size = self.block_builder.buf.len()
            + self.old_builder.buf.len()
            + self.block_builder.block.size
            + self.old_builder.block.size;
        size += size / 32; // reserve extra capacity to avoid reallocate.
        size
    }

    pub fn finish(&mut self, base_off: u32, data_buf: &mut BytesMut) -> BuildResult {
        if self.block_builder.block.size > 0 {
            let last_key = self.block_builder.block.tmp_keys.get_last();
            self.biggest.extend_from_slice(last_key);
            self.block_builder.finish_block(self.fid, self.checksum_tp);
        }
        if self.old_builder.block.size > 0 {
            self.old_builder.finish_block(self.fid, self.checksum_tp);
        }
        assert_eq!(self.block_builder.block_keys.length() > 0, true);
        data_buf.extend_from_slice(self.block_builder.buf.as_slice());
        let data_section_size = self.block_builder.buf.len() as u32;
        data_buf.extend_from_slice(self.old_builder.buf.as_slice());
        let old_data_section_size = self.old_builder.buf.len() as u32;

        self.block_builder.build_index(base_off, self.checksum_tp);
        data_buf.extend_from_slice(self.block_builder.buf.as_slice());
        let index_section_size = self.block_builder.buf.len() as u32;
        self.old_builder
            .build_index(base_off + data_section_size, self.checksum_tp);
        data_buf.extend_from_slice(self.old_builder.buf.as_slice());
        let old_index_section_size = self.old_builder.buf.len() as u32;
        let aux_index_section_size = if let Ok(filter) = BinaryFuse8::try_from(&self.key_hashes) {
            let bin = bincode::serialize(&filter).unwrap();
            let origin_len = data_buf.len();
            self.build_aux_index(data_buf, &bin);
            (data_buf.len() - origin_len) as u32
        } else {
            warn!("failed to build binary fuse 8 filter");
            0
        };
        self.build_properties(data_buf);

        let mut footer = Footer::default();
        footer.old_data_offset = data_section_size;
        footer.index_offset = footer.old_data_offset + old_data_section_size;
        footer.old_index_offset = footer.index_offset + index_section_size;
        footer.aux_index_offset = footer.old_index_offset + old_index_section_size;
        footer.properties_offset = footer.aux_index_offset + aux_index_section_size;
        footer.compression_type = self.block_builder.compression_tp;
        footer.checksum_type = self.checksum_tp;
        footer.table_format_version = TABLE_FORMAT_V1;
        footer.magic = MAGIC_NUMBER;
        data_buf.extend_from_slice(footer.marshal());
        BuildResult {
            id: self.fid,
            smallest: self.smallest.clone(),
            biggest: self.biggest.clone(),
        }
    }

    fn build_aux_index(&self, buf: &mut BytesMut, fuse8: &[u8]) {
        let origin_len = buf.len();
        buf.put_u32_le(0);
        buf.put_u32_le(AUX_INDEX_BINARY_FUSE8);
        buf.put_u32_le(fuse8.len() as u32);
        buf.extend_from_slice(fuse8);
        if self.checksum_tp == CRC32_IEEE {
            let checksum = crc32fast::hash(&buf[(origin_len + 4)..]);
            LittleEndian::write_u32(&mut buf[origin_len..], checksum);
        }
    }

    fn build_properties(&self, buf: &mut BytesMut) {
        let origin_len = buf.len();
        buf.put_u32_le(0);
        Builder::add_property(buf, PROP_KEY_SMALLEST.as_bytes(), self.smallest.as_slice());
        Builder::add_property(buf, PROP_KEY_BIGGEST.as_bytes(), self.biggest.as_slice());
        Builder::add_property(buf, PROP_KEY_MAX_TS.as_bytes(), &self.max_ts.to_le_bytes());
        let entries = self.key_hashes.len() as u32;
        Builder::add_property(buf, PROP_KEY_ENTRIES.as_bytes(), &entries.to_le_bytes());
        Builder::add_property(
            buf,
            PROP_KEY_OLD_ENTRIES.as_bytes(),
            &self.old_entries.to_le_bytes(),
        );
        Builder::add_property(buf, PROP_KEY_TOMBS.as_bytes(), &self.tombs.to_le_bytes());
        if self.checksum_tp == CRC32_IEEE {
            let checksum = crc32fast::hash(&buf[(origin_len + 4)..]);
            LittleEndian::write_u32(&mut buf[origin_len..], checksum);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.smallest.len() == 0
    }

    pub fn get_smallest(&self) -> &[u8] {
        self.smallest.as_slice()
    }

    pub fn get_biggest(&self) -> &[u8] {
        self.biggest.as_slice()
    }
}

pub const FOOTER_SIZE: usize = mem::size_of::<Footer>();

#[derive(Default, Clone, Copy)]
pub struct Footer {
    pub old_data_offset: u32,
    pub index_offset: u32,
    pub old_index_offset: u32,
    pub aux_index_offset: u32,
    pub properties_offset: u32,
    pub compression_type: u8,
    pub checksum_type: u8,
    pub table_format_version: u16,
    pub magic: u32,
}

impl Footer {
    pub fn data_len(&self) -> usize {
        self.old_data_offset as usize
    }

    pub fn old_data_len(&self) -> usize {
        (self.index_offset - self.old_data_offset) as usize
    }

    pub fn index_len(&self) -> usize {
        (self.old_index_offset - self.index_offset) as usize
    }

    pub fn old_index_len(&self) -> usize {
        (self.aux_index_offset - self.old_index_offset) as usize
    }

    pub fn aux_index_len(&self) -> usize {
        (self.properties_offset - self.aux_index_offset) as usize
    }

    pub fn properties_len(&self, table_size: usize) -> usize {
        table_size - self.properties_offset as usize - FOOTER_SIZE
    }

    pub fn unmarshal(&mut self, data: &[u8]) {
        let footer_ptr = data.as_ptr() as *const Footer;
        *self = unsafe { *footer_ptr };
    }

    pub fn marshal(&self) -> &[u8] {
        let footer_ptr = self as *const Footer as *const u8;
        unsafe { slice::from_raw_parts(footer_ptr, FOOTER_SIZE) }
    }
}

#[derive(Default)]
struct BlockBuffer {
    tmp_keys: EntrySlice,
    tmp_vals: EntrySlice,
    old_vers: Vec<u64>,
    entry_sizes: Vec<u32>,
    size: usize,
}

impl BlockBuffer {
    fn reset(&mut self) {
        self.tmp_keys.reset();
        self.tmp_vals.reset();
        self.old_vers.truncate(0);
        self.entry_sizes.truncate(0);
        self.size = 0;
    }

    fn build_entry(&self, buf: &mut Vec<u8>, i: usize, common_prefix_len: usize) {
        let key = self.tmp_keys.get_entry(i);
        let key_suffix = &key[common_prefix_len..];
        buf.put_u16_le(key_suffix.len() as u16);
        buf.extend_from_slice(key_suffix);
        let val_bin = self.tmp_vals.get_entry(i);
        let v = Value::decode(val_bin);
        let mut meta = v.meta;
        let old_ver = self.old_vers[i];
        if old_ver != 0 {
            meta |= META_HAS_OLD;
        } else {
            // The val meta from the old table may have `metaHasOld` flag, need to unset it.
            meta &= !META_HAS_OLD;
        }
        buf.push(meta);
        buf.put_u64_le(v.version);
        if old_ver != 0 {
            buf.put_u64_le(old_ver);
        }
        buf.push(v.user_meta().len() as u8);
        buf.extend_from_slice(v.user_meta());
        buf.extend_from_slice(v.get_value());
    }
}

#[derive(Default)]
struct BlockBuilder {
    buf: Vec<u8>,
    block: BlockBuffer,
    block_keys: EntrySlice,
    block_addrs: Vec<BlockAddress>,
    compression_tp: u8,
    compression_buf: Vec<u8>,
}

impl BlockBuilder {
    fn same_last_key(&self, key: &[u8]) -> bool {
        if self.block.tmp_keys.length() > 0 {
            let last = self.block.tmp_keys.get_last();
            return last.eq(key);
        }
        false
    }

    fn set_last_entry_old_ver_if_zero(&mut self, ver: u64) {
        let last_old_ver_idx = self.block.old_vers.len() - 1;
        if self.block.old_vers[last_old_ver_idx] == 0 {
            self.block.old_vers[last_old_ver_idx] = ver;
            let last_entry_size_idx = self.block.entry_sizes.len() - 1;
            self.block.entry_sizes[last_entry_size_idx] += 8;
        }
    }

    fn add_entry(&mut self, key: &[u8], val: Value) {
        self.block.tmp_keys.append(key);
        self.block.tmp_vals.append_value(val);
        self.block.old_vers.push(0);
        let entry_size = 2 + key.len() + val.encoded_size();
        self.block.entry_sizes.push(entry_size as u32);
        self.block.size += entry_size;
    }

    fn finish_block(&mut self, fid: u64, checksum_tp: u8) {
        self.block_keys.append(self.block.tmp_keys.get_entry(0));
        self.block_addrs
            .push(BlockAddress::new(fid, self.buf.len() as u32));
        self.buf.put_u32_le(0); // checksum place holder.
        let begin_off = self.buf.len();
        let common_prefix_len = self.get_block_common_prefix_len();
        let buf = if self.compression_tp == NO_COMPRESSION {
            &mut self.buf
        } else {
            self.compression_buf.truncate(0);
            &mut self.compression_buf
        };
        let num_entries = self.block.tmp_keys.length();
        buf.put_u32_le(BLOCK_FORMAT_V1);
        buf.put_u32_le(num_entries as u32);
        let mut offset = 0u32;
        for i in 0..num_entries {
            buf.put_u32_le(offset);
            // The entry size calculated in the first pass use full key size, we need to subtract common prefix size.
            offset += self.block.entry_sizes[i] - common_prefix_len as u32;
        }
        buf.put_u16_le(common_prefix_len as u16);
        let common_prefix = &self.block.tmp_keys.get_entry(0)[..common_prefix_len];
        buf.extend_from_slice(common_prefix);
        for i in 0..num_entries {
            self.block.build_entry(buf, i, common_prefix_len);
        }
        if self.compression_tp != NO_COMPRESSION {
            self.compress();
        }
        let mut checksum = 0u32;
        if checksum_tp == CRC32_IEEE {
            checksum = crc32fast::hash(&self.buf[begin_off..]);
        }
        let slice = self.buf.as_mut_slice();
        LittleEndian::write_u32(&mut slice[(begin_off - 4)..], checksum);
        self.block.reset()
    }

    fn get_block_common_prefix_len(&self) -> usize {
        let first_key = self.block.tmp_keys.get_entry(0);
        let last_key = self.block.tmp_keys.get_last();
        key_diff_idx(first_key, last_key)
    }

    fn get_index_common_prefix_len(&self) -> usize {
        let first_key = self.block_keys.get_entry(0);
        let last_key = self.block_keys.get_last();
        key_diff_idx(first_key, last_key)
    }

    fn reset_all(&mut self) {
        self.block.reset();
        self.buf.truncate(0);
        self.block_keys.reset();
        self.block_addrs.truncate(0);
    }

    fn build_index(&mut self, base_off: u32, checksum_tp: u8) {
        self.buf.truncate(0);
        let num_blocks = self.block_addrs.len();
        // checksum place holder.
        self.buf.put_u32_le(0);
        self.buf.put_u32_le(INDEX_FORMAT_V1);
        self.buf.put_u32_le(num_blocks as u32);
        let mut common_prefix_len = 0;
        if num_blocks > 0 {
            common_prefix_len = self.get_index_common_prefix_len();
        }
        let mut key_offset = 0u32;
        for i in 0..num_blocks {
            self.buf.put_u32_le(key_offset);
            let block_key = self.block_keys.get_entry(i);
            key_offset += block_key.len() as u32 - common_prefix_len as u32;
        }
        for i in 0..num_blocks {
            let block_addr = &self.block_addrs[i];
            self.buf.put_u64_le(block_addr.origin_fid);
            self.buf.put_u32_le(block_addr.origin_off + base_off);
            self.buf.put_u32_le(block_addr.curr_off + base_off);
        }
        self.buf.put_u16_le(common_prefix_len as u16);
        if common_prefix_len > 0 {
            let common_prefix = &self.block_keys.get_entry(0)[..common_prefix_len];
            self.buf.extend_from_slice(common_prefix);
        }
        let block_keys_len = self.block_keys.buf.len() - num_blocks * common_prefix_len;
        self.buf.put_u32_le(block_keys_len as u32);
        for i in 0..num_blocks {
            let block_key = self.block_keys.get_entry(i);
            self.buf.extend_from_slice(&block_key[common_prefix_len..]);
        }
        if checksum_tp == CRC32_IEEE {
            let slice = self.buf.as_mut_slice();
            LittleEndian::write_u32(slice, crc32fast::hash(&slice[4..]))
        }
    }

    fn compress(&mut self) {
        unsafe {
            let buf_len = self.buf.len();
            let compress_bound = if self.compression_tp == LZ4_COMPRESSION {
                lz4::liblz4::LZ4_compressBound(self.compression_buf.len() as i32) as usize + 4
            } else {
                zstd_sys::ZSTD_compressBound(self.compression_buf.len())
            };
            self.buf.reserve(compress_bound);
            // self.buf.set_len(buf_len + compress_bound);
            let src = &self.compression_buf;
            let dst = &mut self.buf[buf_len..];
            let size = if self.compression_tp == LZ4_COMPRESSION {
                lz4::liblz4::LZ4_compress_default(
                    src.as_ptr() as *const libc::c_char,
                    dst.as_mut_ptr() as *mut libc::c_char,
                    src.len() as i32,
                    compress_bound as i32,
                ) as usize
            } else if self.compression_tp == ZSTD_COMPRESSION {
                zstd_sys::ZSTD_compress(
                    dst.as_mut_ptr() as *mut libc::c_void,
                    compress_bound,
                    src.as_ptr() as *const libc::c_void,
                    src.len(),
                    zstd_sys::ZSTD_defaultCLevel(),
                )
            } else {
                panic!("unexpected compression type {}", self.compression_tp);
            };
            self.buf.set_len(buf_len + size);
        }
    }
}

#[derive(Default, Clone, Copy, Debug)]
pub struct BlockAddress {
    pub origin_fid: u64,
    pub origin_off: u32,
    pub curr_off: u32,
}

impl BlockAddress {
    fn new(fid: u64, offset: u32) -> Self {
        Self {
            origin_fid: fid,
            origin_off: offset,
            curr_off: offset,
        }
    }
}

pub struct BuildResult {
    pub id: u64,
    pub smallest: Vec<u8>,
    pub biggest: Vec<u8>,
}

fn key_diff_idx(k1: &[u8], k2: &[u8]) -> usize {
    let mut i: usize = 0;
    while i < k1.len() && i < k2.len() {
        if k1[i] != k2[i] {
            break;
        }
        i += 1;
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_entry_slice() {
        let mut es = EntrySlice::new();
        es.append("abc".as_bytes());
        let val_buf = Value::encode_buf(1, &[1], 1, "abc".as_bytes());
        let val = Value::decode(&val_buf);
        es.append_value(val);
        // dbg!(es.buf);
        // dbg!(es.end_offs);
    }
}
