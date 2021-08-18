use std::sync::Arc;

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use moka::sync::SegmentedCache;

use crate::{NUM_CFS, dfs, table::Value};

use super::*;
use crate::table::table::Result;


const L0_FOOTER_SIZE: usize = std::mem::size_of::<L0Footer>();

#[derive(Default, Clone)]
struct L0Footer {
    commit_ts: u64,
    num_cfs: u32,
    magic: u32,
}

impl L0Footer {
    fn marshal(&self) -> [u8; L0_FOOTER_SIZE] {
        let mut bin = [0u8; L0_FOOTER_SIZE];
        LittleEndian::write_u64(&mut bin, self.commit_ts);
        LittleEndian::write_u32(&mut bin[8..], self.num_cfs);
        LittleEndian::write_u32(&mut bin[12..], self.magic);
        bin
    }

    fn unmarshal(&mut self, bin: &[u8]) {
        self.commit_ts = LittleEndian::read_u64(bin);
        self.num_cfs = LittleEndian::read_u32(&bin[8..]);
        self.magic = LittleEndian::read_u32(&bin[12..]);
    }
}

#[derive(Clone)]
pub struct L0Table {
    footer: L0Footer,
    file: Arc<dyn dfs::File>,
    cfs:  [Option<sstable::SSTable>; NUM_CFS],
    cf_offs: [u32; NUM_CFS],
    smallest: Bytes,
    biggest: Bytes,
}

impl L0Table {
    pub fn new(file: Arc<dyn dfs::File>, cache: SegmentedCache<BlockCacheKey, Bytes>) -> Result<Self> {
        let footer_off = file.size() - L0_FOOTER_SIZE as u64;
        let mut footer = L0Footer::default();
        let footer_buf = file.read(footer_off, L0_FOOTER_SIZE)?;
        footer.unmarshal(footer_buf.chunk());
        let cf_offs_off = footer_off - 4 * NUM_CFS as u64;
        let cf_offs_buf = file.read(cf_offs_off, 4 * NUM_CFS)?;
        let mut cf_offs = [0u32; NUM_CFS];
        for i in 0..NUM_CFS {
            cf_offs[i] = LittleEndian::read_u32(&cf_offs_buf[i*4..]);
        }
        let mut cfs: [Option<SSTable>; NUM_CFS] = [None, None, None];
        for i in 0..NUM_CFS {
            let start_off = cf_offs[i] as u64;
            let mut end_off = cf_offs_off as usize;
            if i + 1 < NUM_CFS {
                end_off = cf_offs[i+1] as usize;
            }
            let cf_data = file.read(start_off, end_off - start_off as usize)?;
            if cf_data.is_empty() {
                continue;
            }
            let mem_file = dfs::InMemFile::new(file.id(), cf_data);
            let tbl = sstable::SSTable::new(Arc::new(mem_file), cache.clone())?;
            cfs[i] = Some(tbl)
        }
        let (smallest, biggest) = Self::compute_smallest_biggest(&cfs);
        Ok(Self {
            footer,
            file,
            cfs,
            cf_offs,
            smallest,
            biggest,
        })
    }

    fn compute_smallest_biggest(cfs: &[Option<SSTable>; NUM_CFS]) -> (Bytes, Bytes) {
        let mut smallest_buf = BytesMut::new();
        let mut biggest_buf = BytesMut::new();
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &cfs[i] {
                let smallest = cf_tbl.smallest();
                if smallest.len() > 0 {
                    if smallest_buf.is_empty() || smallest_buf.chunk() < smallest {
                        smallest_buf.truncate(0);
                        smallest_buf.extend_from_slice(smallest);
                    }
                }
                let biggest = cf_tbl.biggest();
                if biggest > biggest_buf.chunk() {
                    biggest_buf.truncate(0);
                    biggest_buf.extend_from_slice(biggest);
                }
            }
        }
        assert!(smallest_buf.len() > 0);
        assert!(biggest_buf.len() > 0);
        (smallest_buf.freeze(), biggest_buf.freeze())
    }

    pub fn id(&self) -> u64 {
        self.file.id()
    }

    pub fn get_cf(&self, cf: usize) -> &Option<sstable::SSTable> {
        &self.cfs[cf]
    }

    pub fn size(&self) -> u64 {
        self.file.size()
    }

    pub fn smallest(&self) -> &[u8] {
        self.smallest.chunk()
    }

    pub fn biggest(&self) -> &[u8] {
        self.biggest.chunk()
    }

    pub fn commit_ts(&self) -> u64 {
        self.footer.commit_ts
    }
}

pub struct L0Builder {
    builders: Vec<Builder>,
    commit_ts: u64,
}

impl L0Builder {
    pub fn new(fid: u64, opt: TableBuilderOptions, commit_ts: u64) -> Self {
        let mut builders = Vec::with_capacity(4);
        for i in 0..NUM_CFS {
            let builder = Builder::new(fid, opt);
            builders.push(builder);
        }
        Self {
            builders,
            commit_ts,
        }
    }

    pub fn add(&mut self, cf: usize, key: &[u8], val: Value) {
        self.builders[cf].add(key, val);
    }

    pub fn finish(&mut self) -> Bytes {
        let mut estimated_size = 0;
        for builder in &self.builders {
            estimated_size += builder.estimated_size();
        }
        estimated_size += estimated_size / 32; // reserve a little extra buffer.
        let mut buf = BytesMut::with_capacity(estimated_size);
        let mut offsets = Vec::with_capacity(NUM_CFS);
        for builder in &mut self.builders {
            offsets.push(buf.len() as u32);
            if !builder.is_empty() {
                builder.finish(&mut buf);
            }
        }
        for offset in offsets {
            buf.put_u32_le(offset);
        }
        buf.put_u64_le(self.commit_ts);
        buf.put_u32_le(NUM_CFS as u32);
        buf.put_u32_le(MAGIC_NUMBER);
        buf.freeze()
    }

    pub fn smallest_biggest(&self) -> (Bytes, Bytes) {
        let mut smallest_buf = BytesMut::new();
        let mut biggest_buf = BytesMut::new();
        for builder in &self.builders {
            if builder.get_smallest().len() > 0 {
                if smallest_buf.len() == 0 || builder.get_smallest() < smallest_buf {
                    smallest_buf.truncate(0);
                    smallest_buf.extend_from_slice(builder.get_smallest());
                }
            }
            if builder.get_biggest() > biggest_buf {
                biggest_buf.truncate(0);
                biggest_buf.extend_from_slice(builder.get_biggest());
            }
        }
        (smallest_buf.freeze(), biggest_buf.freeze())
    }
}