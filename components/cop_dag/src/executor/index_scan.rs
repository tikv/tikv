// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use super::{scan::InnerExecutor, Row, ScanExecutor};
use crate::codec::table;
use crate::storage::Store;
use crate::{util, Result};
use kvproto::coprocessor::KeyRange;
use tipb::executor::IndexScan;
use tipb::schema::ColumnInfo;

pub struct IndexInnerExecutor {
    pk_col: Option<ColumnInfo>,
    col_ids: Vec<i64>,
    unique: bool,
}

impl IndexInnerExecutor {
    fn new(meta: &mut IndexScan, unique: bool) -> Self {
        let mut pk_col = None;
        let cols = meta.mut_columns();
        if cols.last().map_or(false, ColumnInfo::get_pk_handle) {
            pk_col = Some(cols.pop().unwrap());
        }
        let col_ids = cols.iter().map(ColumnInfo::get_column_id).collect();
        Self {
            pk_col,
            col_ids,
            unique,
        }
    }
}

impl InnerExecutor for IndexInnerExecutor {
    fn decode_row(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<Option<Row>> {
        use crate::codec::datum;
        use byteorder::{BigEndian, ReadBytesExt};
        use cop_datatype::prelude::*;
        use cop_datatype::FieldTypeFlag;

        let (mut values, handle) = box_try!(table::cut_idx_key(key, &self.col_ids));
        let handle = match handle {
            None => box_try!(value.as_slice().read_i64::<BigEndian>()),
            Some(h) => h,
        };

        if let Some(ref pk_col) = self.pk_col {
            let handle_datum = if pk_col.flag().contains(FieldTypeFlag::UNSIGNED) {
                // PK column is unsigned
                datum::Datum::U64(handle as u64)
            } else {
                datum::Datum::I64(handle)
            };
            let mut bytes = box_try!(datum::encode_key(&[handle_datum]));
            values.append(pk_col.get_column_id(), &mut bytes);
        }
        Ok(Some(Row::origin(handle, values, columns)))
    }

    #[inline]
    fn is_point(&self, range: &KeyRange) -> bool {
        self.unique && util::is_point(range)
    }

    #[inline]
    fn scan_on(&self) -> super::super::scanner::ScanOn {
        super::super::scanner::ScanOn::Index
    }

    // Since the unique index wouldn't always come with
    // self.unique = true. so the key-only would always be false.
    #[inline]
    fn key_only(&self) -> bool {
        false
    }
}

impl<S: Store> ScanExecutor<S, IndexInnerExecutor> {
    pub fn index_scan(
        mut meta: IndexScan,
        key_ranges: Vec<KeyRange>,
        store: S,
        unique: bool,
        collect: bool,
    ) -> Result<Self> {
        let columns = meta.get_columns().to_vec();
        let inner = IndexInnerExecutor::new(&mut meta, unique);
        Self::new(inner, meta.get_desc(), columns, key_ranges, store, collect)
    }

    pub fn index_scan_with_cols_len(
        cols: i64,
        key_ranges: Vec<KeyRange>,
        store: S,
    ) -> Result<Self> {
        let col_ids: Vec<i64> = (0..cols).collect();
        let inner = IndexInnerExecutor {
            col_ids,
            pk_col: None,
            unique: false,
        };
        Self::new(inner, false, vec![], key_ranges, store, false)
    }
}

pub type IndexScanExecutor<S> = ScanExecutor<S, IndexInnerExecutor>;
