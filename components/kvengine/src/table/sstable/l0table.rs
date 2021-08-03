use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BytesMut};

use crate::{NUM_CFS};

use super::table_file::TableFile;
use super::{SSTable, sstable};
use crate::table::table::Result;


const L0_FOOTER_SIZE: usize = std::mem::size_of::<L0Footer>();

#[derive(Default)]
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

pub struct L0Table {
    footer: L0Footer,
    file: TableFile,
    cfs:  [Option<sstable::SSTable>; NUM_CFS],
    cf_offs: [u32; NUM_CFS],
    smallest_buf: BytesMut,
    biggest_buf: BytesMut,
}

impl L0Table {
    pub fn new(file: TableFile) -> Result<Self> {
        let footer_off = file.size - L0_FOOTER_SIZE as u64;
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
            let mem_file = TableFile::new_in_mem(file.id, cf_data);
            let tbl = sstable::SSTable::new(mem_file)?;
            cfs[i] = Some(tbl)
        }

        let mut l0 = Self {
            footer,
            file,
            cfs,
            cf_offs,
            smallest_buf: BytesMut::new(),
            biggest_buf: BytesMut::new(),
        };
        l0.compute_smallest_biggest();
        Ok(l0)
    }

    fn compute_smallest_biggest(&mut self) {
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &self.cfs[i] {
                let smallest = cf_tbl.smallest();
                if smallest.len() > 0 {
                    if self.smallest_buf.is_empty() || self.smallest_buf.chunk() < smallest {
                        self.smallest_buf.truncate(0);
                        self.smallest_buf.extend_from_slice(smallest);
                    }
                }
                let biggest = cf_tbl.biggest();
                if biggest > self.biggest_buf.chunk() {
                    self.biggest_buf.truncate(0);
                    self.biggest_buf.extend_from_slice(biggest);
                }
            }
        }
        assert!(self.smallest_buf.len() > 0);
        assert!(self.biggest_buf.len() > 0);
    }

    pub fn id(&self) -> u64 {
        self.file.id
    }

    pub fn get_cf(&self, cf: usize) -> Option<sstable::SSTable> {
        self.cfs[cf].clone()
    }

    pub fn size(&self) -> u64 {
        self.file.size
    }

    pub fn smallest(&self) -> &[u8] {
        self.smallest_buf.chunk()
    }

    pub fn biggest(&self) -> &[u8] {
        self.biggest_buf.chunk()
    }

    pub fn commit_ts(&self) -> u64 {
        self.footer.commit_ts
    }

}