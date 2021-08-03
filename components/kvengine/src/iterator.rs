use core::slice;

use bytes::{Buf, Bytes};

use crate::table::{memtable, table};
use crate::*;
use crate::write::{L0Tables, MemTables};



pub struct Item {
    k: Bytes,
    v: table::Value,
}

impl Item {
    pub fn key(&self) -> &[u8] {
        self.k.chunk()
    }

    pub fn value(&self) -> &[u8] {
        self.v.get_value()
    }

    pub fn value_size(&self) -> usize {
        self.value().len()
    }

    pub fn has_value(&self) -> bool {
        self.v.is_empty()
    }

    pub fn is_deleted(&self) -> bool {
        table::is_deleted(self.v.meta)
    }

    pub fn estimated_size(&self) -> usize {
        todo!()
    }
}
pub struct SnapAccess {
    shard: Shard,
    cfs:   [CFConfig; NUM_CFS],
    managed_ts: u64,
    hints: [Box<memtable::Hint>; NUM_CFS],
    mem_tbls: MemTables,
    l0_tbls: L0Tables,
    splitting: Vec<memtable::CFTable>,
}

impl SnapAccess {
    pub fn new(shard: Shard, cfs: [CFConfig; NUM_CFS]) -> Self {
        todo!()
    }

    pub fn get(&self, key: &[u8], version: u64) -> Item {
        todo!()
    }

    fn get_value(&self, cf: usize, key: &[u8], version: u64) -> table::Value {
        todo!()
    }

    pub fn set_managed_ts(&mut self, managed_ts: u64) {
        self.managed_ts = managed_ts;
    }

    pub fn new_iterator(&self) -> Iterator {
        todo!()
    }

    fn new_table_iterator(&self) -> Box<dyn table::Iterator> {
        todo!()
    }

    fn append_mem_tbl_iters(tbl_iters: &mut Vec<Box<dyn table::Iterator>>, mem_tbls: MemTables) {
        todo!()
    }

    fn append_l0_tbl_iters(tbl_iters: &mut Vec<Box<dyn table::Iterator>>, l0_tbls: L0Tables) {
        todo!()
    }
}

pub struct Iterator {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub item: Item,
    pub it_inner: Box<dyn table::Iterator>,

}

impl Iterator {

    pub fn valid(&self) -> bool {
        !self.item.v.is_empty()
    }

    pub fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.item.k.starts_with(prefix)
    }

    pub fn next(&mut self) {
        todo!()
    }

    fn update_item(&mut self) {
        todo!()
    }

    fn parse_item(&mut self) {
        todo!()
    }

    pub fn seek(&mut self, key: &[u8]) {
        todo!()
    }

    pub fn rewind(&mut self) {
        todo!()
    }

    pub fn set_all_versions(&mut self, all_versions: bool) {
        self.all_versions = all_versions;
    }



}