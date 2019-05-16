// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use tikv_util::collections::HashSet;

use super::{scan::InnerExecutor, Row, ScanExecutor};
use crate::codec::table;
use crate::storage::Store;
use crate::{util, Result};
use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;
use tipb::schema::ColumnInfo;

pub struct TableInnerExecutor {
    col_ids: HashSet<i64>,
}

impl TableInnerExecutor {
    fn new(meta: &TableScan) -> Self {
        let col_ids = meta
            .get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(ColumnInfo::get_column_id)
            .collect();
        Self { col_ids }
    }
}

impl InnerExecutor for TableInnerExecutor {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        let row_data = box_try!(table::cut_row(value, &self.col_ids));
        let h = box_try!(table::decode_handle(&key));
        Ok(Some(Row::origin(h, row_data, columns)))
    }

    #[inline]
    fn is_point(&self, range: &KeyRange) -> bool {
        util::is_point(&range)
    }

    #[inline]
    fn scan_on(&self) -> super::super::scanner::ScanOn {
        super::super::scanner::ScanOn::Table
    }

    #[inline]
    fn key_only(&self) -> bool {
        self.col_ids.is_empty()
    }
}

impl<S: Store> ScanExecutor<S, TableInnerExecutor> {
    pub fn table_scan(
        mut meta: TableScan,
        key_ranges: Vec<KeyRange>,
        store: S,
        collect: bool,
    ) -> Result<Self> {
        let inner = TableInnerExecutor::new(&meta);
        Self::new(
            inner,
            meta.get_desc(),
            meta.take_columns().to_vec(),
            key_ranges,
            store,
            collect,
        )
    }
}

pub type TableScanExecutor<S> = ScanExecutor<S, TableInnerExecutor>;
