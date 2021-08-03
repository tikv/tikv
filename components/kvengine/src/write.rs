use std::{fs::File, sync::Arc};

use bytes::{Bytes, BytesMut};

use crate::{CFConfig, Engine, Error, NUM_CFS, Shard, table::{self, memtable::{self, WriteBatchEntry}, sstable}};

#[derive(Clone)]
pub struct MemTables {
    inner: Arc<MemTablesInner>
}

struct MemTablesInner {
    tbls: Vec<memtable::CFTable>,
}

#[derive(Clone)]
pub struct L0Tables {
    inner: Arc<L0TablesInner>
}

struct L0TablesInner {
    tbls: Vec<sstable::L0Table>,
}

struct WriteBatch {
    shard: Shard,
    cf_conf: [CFConfig; NUM_CFS],
    cf_batches: [memtable::WriteBatch; NUM_CFS],
}

impl WriteBatch {
    pub fn put(&mut self, cf: usize, key: &[u8], val: &[u8], meta: u8, user_meta: &[u8], version: u64) {
        todo!()
    }

    pub fn cf_len(&self, cf: usize) -> usize {
        self.cf_batches[cf].len()
    }

    pub fn get_entry(&self, cf: usize, idx: usize) -> WriteBatchEntry {
        self.cf_batches[cf].get(idx)
    }
}

impl Engine {
    fn switch_mem_table(&self) -> memtable::CFTable {
        todo!()
    }

    fn write(&self, wb: &mut WriteBatch) {
        todo!()
    }

    fn write_splitting(&self, wb: WriteBatch, commit_ts: u64) {
        todo!()
    }

    fn create_l0_file(&self) -> Result<File, Error> {
        todo!()
    }

    fn get_property(shard: &Shard, key: &[u8]) -> Option<Bytes> {
        let props = shard.properties.lock().unwrap();
        props.get(key).map(| x | x.clone())
    }


}