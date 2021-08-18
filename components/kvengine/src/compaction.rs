

use std::sync::Arc;

use crate::table::{search, sstable::{self, SSTable}};
use crate::dfs;
use crate::*;

#[derive(Clone)]
pub(crate) struct CompactionClient {
    dfs: Arc<dyn dfs::DFS>,
    remote_url: String,
}

impl CompactionClient {
    pub(crate) fn new(dfs: Arc<dyn dfs::DFS>, remote_url: String) -> Self {
        Self {
            dfs,
            remote_url,
        }
    } 
}

pub struct CompactDef {}


impl Engine {
    pub(crate) fn run_compaction(&self) {
        
    }
}


pub(crate) struct KeyRange<'a> {
    pub start: &'a [u8],
    pub end: &'a [u8]
}

pub(crate) fn get_key_range<'a: 'b, 'b>(tables: &'a Vec<SSTable>) -> KeyRange<'b> {
    let mut smallest = tables[0].smallest();
    let mut biggest = tables[0].biggest();
    for i in 1..tables.len() {
        let tbl = &tables[i];
        if tbl.smallest() < smallest {
            smallest = tbl.smallest();
        }
        if tbl.biggest() > tbl.biggest() {
            biggest = tbl.biggest();
        }
    }
    KeyRange {
        start: smallest,
        end: biggest,
    }
}

pub(crate) fn get_tables_in_range(tables: &Vec<SSTable>, start: &[u8], end: &[u8]) -> (usize, usize) {
    let left = search(tables.len(), |i| start <= tables[i].biggest());
    let right = search(tables.len(), |i| end < tables[i].smallest());
    (left, right)
}

