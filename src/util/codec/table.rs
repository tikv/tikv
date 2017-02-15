// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::io::Write;
use std::collections::{HashMap, HashSet};
use std::{cmp, u8};
use tipb::schema::ColumnInfo;

use super::number::{NumberDecoder, NumberEncoder};
use super::bytes::BytesDecoder;
use super::datum::DatumDecoder;
use super::{Result, Datum, datum};
use super::mysql::{types, Duration, Time};
use util::xeval::EvalContext;
use util::escape;

// handle or index id
pub const ID_LEN: usize = 8;
pub const PREFIX_LEN: usize = TABLE_PREFIX_LEN + ID_LEN /*table_id*/ + SEP_LEN;
pub const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
pub const TABLE_PREFIX: &'static [u8] = b"t";
pub const RECORD_PREFIX_SEP: &'static [u8] = b"_r";
pub const INDEX_PREFIX_SEP: &'static [u8] = b"_i";
pub const SEP_LEN: usize = 2;
pub const TABLE_PREFIX_LEN: usize = 1;


trait TableEncoder: NumberEncoder {
    fn append_table_record_prefix(&mut self, table_id: i64) -> Result<()> {
        try!(self.write_all(TABLE_PREFIX));
        try!(self.encode_i64(table_id));
        self.write_all(RECORD_PREFIX_SEP).map_err(From::from)
    }

    fn append_table_index_prefix(&mut self, table_id: i64) -> Result<()> {
        try!(self.write_all(TABLE_PREFIX));
        try!(self.encode_i64(table_id));
        self.write_all(INDEX_PREFIX_SEP).map_err(From::from)
    }
}

impl<T: Write> TableEncoder for T {}

fn flatten(data: Datum) -> Result<Datum> {
    match data {
        Datum::Dur(d) => Ok(Datum::I64(d.to_nanos())),
        Datum::Time(t) => Ok(Datum::U64(t.to_packed_u64())),
        _ => Ok(data),
    }
}

// `encode_row` encodes row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
pub fn encode_row(row: Vec<Datum>, col_ids: &[i64]) -> Result<Vec<u8>> {
    if row.len() != col_ids.len() {
        return Err(box_err!("data and columnID count not match {} vs {}",
                            row.len(),
                            col_ids.len()));
    }
    let mut values = Vec::with_capacity(cmp::max(row.len() * 2, 1));
    for (&id, col) in col_ids.into_iter().zip(row) {
        values.push(Datum::I64(id));
        let fc = try!(flatten(col));
        values.push(fc);
    }
    if values.is_empty() {
        values.push(Datum::Null);
    }
    datum::encode_value(&values)
}

/// `encode_row_key` encodes the table id and record handle into a byte array.
pub fn encode_row_key(table_id: i64, encoded_handle: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN);
    // can't panic
    key.append_table_record_prefix(table_id).unwrap();
    key.write_all(encoded_handle).unwrap();
    key
}

/// `encode_column_key` encodes the table id, row handle and column id into a byte array.
pub fn encode_column_key(table_id: i64, handle: i64, column_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN + ID_LEN);
    key.append_table_record_prefix(table_id).unwrap();
    key.encode_i64(handle).unwrap();
    key.encode_i64(column_id).unwrap();
    key
}

/// `decode_handle` decodes the key and gets the handle.
pub fn decode_handle(encoded: &[u8]) -> Result<i64> {
    if !encoded.starts_with(TABLE_PREFIX) {
        return Err(invalid_type!("record key expected, but got {}", escape(encoded)));
    }

    let mut remaining = &encoded[TABLE_PREFIX.len()..];
    try!(remaining.decode_i64());

    if !remaining.starts_with(RECORD_PREFIX_SEP) {
        return Err(invalid_type!("record key expected, but got {}", escape(encoded)));
    }

    remaining = &remaining[RECORD_PREFIX_SEP.len()..];
    remaining.decode_i64()
}

/// `truncate_as_row_key` truncate extra part of a tidb key and just keep the row key part.
pub fn truncate_as_row_key(key: &[u8]) -> Result<&[u8]> {
    try!(decode_handle(key));
    Ok(&key[..RECORD_ROW_KEY_LEN])
}

/// `encode_index_seek_key` encodes an index value to byte array.
pub fn encode_index_seek_key(table_id: i64, idx_id: i64, encoded: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_LEN + ID_LEN + encoded.len());
    key.append_table_index_prefix(table_id).unwrap();
    key.encode_i64(idx_id).unwrap();
    key.write_all(encoded).unwrap();
    key
}

// `decode_index_key` decodes datums from an index key.
pub fn decode_index_key(ctx: &EvalContext,
                        mut encoded: &[u8],
                        infos: &[ColumnInfo])
                        -> Result<Vec<Datum>> {
    encoded = &encoded[PREFIX_LEN + ID_LEN..];
    let mut res = vec![];

    for info in infos {
        if encoded.is_empty() {
            return Err(box_err!("{} is too short.", escape(encoded)));
        }
        let mut v = try!(encoded.decode_datum());
        v = try!(unflatten(ctx, v, info));
        res.push(v);
    }

    Ok(res)
}

/// `unflatten` converts a raw datum to a column datum.
fn unflatten(ctx: &EvalContext, datum: Datum, col: &ColumnInfo) -> Result<Datum> {
    if let Datum::Null = datum {
        return Ok(datum);
    }
    if col.get_tp() > u8::MAX as i32 || col.get_tp() < 0 {
        error!("unknown type {} {:?}", col.get_tp(), datum);
    }
    match col.get_tp() as u8 {
        types::FLOAT => Ok(Datum::F64(datum.f64() as f32 as f64)),
        types::TINY |
        types::SHORT |
        types::YEAR |
        types::INT24 |
        types::LONG |
        types::LONG_LONG |
        types::DOUBLE |
        types::TINY_BLOB |
        types::MEDIUM_BLOB |
        types::BLOB |
        types::LONG_BLOB |
        types::VARCHAR |
        types::STRING |
        types::NEW_DECIMAL => Ok(datum),
        types::DATE | types::DATETIME | types::TIMESTAMP => {
            let fsp = col.get_decimal() as i8;
            let t = try!(Time::from_packed_u64(datum.u64(), col.get_tp() as u8, fsp, &ctx.tz));
            Ok(Datum::Time(t))
        }
        types::DURATION => Duration::from_nanos(datum.i64(), 0).map(Datum::Dur),
        types::ENUM | types::SET | types::BIT => {
            Err(box_err!("unflatten column {:?} is not supported yet.", col))
        }
        t => {
            error!("unknown type {} {:?}", t, datum);
            Ok(datum)
        }
    }
}

pub trait TableDecoder: DatumDecoder {
    // `decode_col_value` decodes data to a Datum according to the column info.
    fn decode_col_value(&mut self, ctx: &EvalContext, col: &ColumnInfo) -> Result<Datum> {
        let d = try!(self.decode_datum());
        unflatten(ctx, d, col)
    }

    // `decode_row` decodes a byte slice into datums.
    // TODO: We should only decode columns in the cols map.
    // Row layout: colID1, value1, colID2, value2, .....
    fn decode_row(&mut self,
                  ctx: &EvalContext,
                  cols: &HashMap<i64, ColumnInfo>)
                  -> Result<HashMap<i64, Datum>> {
        let mut values = try!(self.decode());
        if values.get(0).map_or(true, |d| *d == Datum::Null) {
            return Ok(HashMap::new());
        }
        if values.len() & 1 == 1 {
            return Err(box_err!("decoded row values' length should be even!"));
        }
        let mut row = HashMap::with_capacity(cols.len());
        let mut drain = values.drain(..);
        loop {
            let id = match drain.next() {
                None => return Ok(row),
                Some(id) => id.i64(),
            };
            let v = drain.next().unwrap();
            if let Some(ci) = cols.get(&id) {
                let v = try!(unflatten(ctx, v, ci));
                row.insert(id, v);
            }
        }
    }
}

impl<T: BytesDecoder> TableDecoder for T {}

pub struct RowColMeta {
    offset: usize,
    length: usize,
}

pub struct RowColsDict {
    // data of current row
    value: Vec<u8>,
    // cols contains meta of each column in the format of:
    // (col_id1,(offset1,len1)),(col_id2,(offset2,len2),...)
    cols: HashMap<i64, RowColMeta>,
}

impl RowColMeta {
    pub fn new(offset: usize, length: usize) -> RowColMeta {
        RowColMeta {
            offset: offset,
            length: length,
        }
    }
}

impl RowColsDict {
    pub fn new(cols: HashMap<i64, RowColMeta>, val: Vec<u8>) -> RowColsDict {
        RowColsDict {
            value: val,
            cols: cols,
        }
    }

    pub fn len(&self) -> usize {
        self.cols.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn get(&self, key: i64) -> Option<&[u8]> {
        if let Some(meta) = self.cols.get(&key) {
            return Some(&self.value[meta.offset..(meta.offset + meta.length)]);
        }
        None
    }
}

// `cut_row` cut encoded row into (col_id,offset,length)
// and return interested columns' meta in RowColsDict
pub fn cut_row(data: Vec<u8>, cols: &HashSet<i64>) -> Result<RowColsDict> {
    if cols.is_empty() || data.is_empty() || (data.len() == 1 && data[0] == datum::NIL_FLAG) {
        return Ok(RowColsDict::new(HashMap::with_capacity(0), data));
    }

    let meta_map = {
        let mut meta_map = HashMap::with_capacity(cols.len());
        let length = data.len();
        let mut tmp_data: &[u8] = data.as_ref();
        while !tmp_data.is_empty() && meta_map.len() < cols.len() {
            let id = try!(tmp_data.decode_datum()).i64();
            let offset = length - tmp_data.len();
            let (val, rem) = try!(datum::split_datum(tmp_data, false));
            if cols.contains(&id) {
                meta_map.insert(id, RowColMeta::new(offset, val.len()));
            }
            tmp_data = rem;
        }
        meta_map
    };
    Ok(RowColsDict::new(meta_map, data))
}

// `cut_idx_key` cuts encoded index key into RowColsDict and handle .
pub fn cut_idx_key(key: Vec<u8>, col_ids: &[i64]) -> Result<(RowColsDict, Option<i64>)> {
    let mut meta_map: HashMap<i64, RowColMeta> = HashMap::with_capacity(col_ids.len());
    let handle = {
        let mut tmp_data: &[u8] = &key[PREFIX_LEN + ID_LEN..];
        let length = key.len();
        // parse cols from data
        for &id in col_ids {
            let offset = length - tmp_data.len();
            let (val, rem) = try!(datum::split_datum(tmp_data, false));
            meta_map.insert(id, RowColMeta::new(offset, val.len()));
            tmp_data = rem;
        }

        if tmp_data.is_empty() {
            None
        } else {
            Some(box_try!(tmp_data.decode_datum()).i64())
        }
    };
    Ok((RowColsDict::new(meta_map, key), handle))
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::mysql::types;
    use util::codec::datum::{self, Datum, DatumDecoder};
    use util::codec::number::NumberEncoder;

    use tipb::schema::ColumnInfo;
    use std::i64;
    use std::collections::{HashSet, HashMap};

    #[test]
    fn test_row_key_codec() {
        let tests = vec![i64::MIN, i64::MAX, -1, 0, 2, 3, 1024];
        for &t in &tests {
            let mut buf = vec![];
            buf.encode_i64(t).unwrap();
            let k = encode_row_key(1, &buf);
            assert_eq!(t, decode_handle(&k).unwrap());
        }
    }

    #[test]
    fn test_index_key_codec() {
        let tests = vec![Datum::U64(1), Datum::Bytes(b"123".to_vec()), Datum::I64(-1)];
        let types = vec![new_col_info(types::LONG_LONG),
                         new_col_info(types::VARCHAR),
                         new_col_info(types::LONG_LONG)];
        let buf = datum::encode_key(&tests).unwrap();
        let encoded = encode_index_seek_key(1, 2, &buf);
        assert_eq!(tests,
                   decode_index_key(&Default::default(), &encoded, &types).unwrap());
    }

    fn new_col_info(tp: u8) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.set_tp(tp as i32);
        col_info
    }

    fn to_hash_map(row: &RowColsDict) -> HashMap<i64, Vec<u8>> {
        let mut data = HashMap::with_capacity(row.cols.len());
        if row.is_empty() {
            return data;
        }
        for (key, meta) in &row.cols {
            data.insert(*key,
                        row.value[meta.offset..(meta.offset + meta.length)].to_vec());
        }
        data
    }

    fn cut_row_as_owned(bs: &[u8], col_id_set: &HashSet<i64>) -> HashMap<i64, Vec<u8>> {
        let res = cut_row(bs.to_vec(), col_id_set).unwrap();
        to_hash_map(&res)
    }

    fn cut_idx_key_as_owned(bs: &[u8], ids: &[i64]) -> (HashMap<i64, Vec<u8>>, Option<i64>) {
        let (res, left) = cut_idx_key(bs.to_vec(), ids).unwrap();
        (to_hash_map(&res), left)
    }

    #[test]
    fn test_row_codec() {
        let mut cols = map![
            1 => new_col_info(types::LONG_LONG),
            2 => new_col_info(types::VARCHAR),
            3 => new_col_info(types::NEW_DECIMAL)
        ];

        let mut row = map![
            1 => Datum::I64(100),
            2 => Datum::Bytes(b"abc".to_vec()),
            3 => Datum::Dec(10.into())
        ];

        let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
        let col_values: Vec<_> = row.iter().map(|(_, v)| v.clone()).collect();
        let mut col_encoded: HashMap<_, _> = row.iter()
            .map(|(k, v)| {
                let f = super::flatten(v.clone()).unwrap();
                (*k, datum::encode_value(&[f]).unwrap())
            })
            .collect();
        let mut col_id_set: HashSet<_> = col_ids.iter().cloned().collect();

        let bs = encode_row(col_values, &col_ids).unwrap();
        assert!(!bs.is_empty());

        let r = bs.as_slice().decode_row(&Default::default(), &cols).unwrap();
        assert_eq!(row, r);

        let mut datums: HashMap<_, _>;
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.insert(4, new_col_info(types::FLOAT));
        let r = bs.as_slice().decode_row(&Default::default(), &cols).unwrap();
        assert_eq!(row, r);
        col_id_set.insert(4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.remove(&4);
        cols.remove(&3);
        let r = bs.as_slice().decode_row(&Default::default(), &cols).unwrap();
        row.remove(&3);
        assert_eq!(row, r);
        col_id_set.remove(&3);
        col_id_set.remove(&4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        col_encoded.remove(&3);
        assert_eq!(col_encoded, datums);

        let bs = encode_row(vec![], &[]).unwrap();
        assert!(!bs.is_empty());
        assert!(bs.as_slice().decode_row(&Default::default(), &cols).unwrap().is_empty());
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert!(datums.is_empty());
    }

    #[test]
    fn test_idx_codec() {
        let mut col_ids = vec![1, 2, 3];
        let col_types = vec![new_col_info(types::LONG_LONG),
                             new_col_info(types::VARCHAR),
                             new_col_info(types::NEW_DECIMAL)];
        let col_values =
            vec![Datum::I64(100), Datum::Bytes(b"abc".to_vec()), Datum::Dec(10.into())];
        let mut col_encoded: HashMap<_, _> = col_ids.iter()
            .zip(&col_types)
            .zip(&col_values)
            .map(|((id, t), v)| {
                let unflattened = super::unflatten(&Default::default(), v.clone(), t).unwrap();
                let encoded = datum::encode_key(&[unflattened]).unwrap();
                (*id, encoded)
            })
            .collect();

        let key = datum::encode_key(&col_values).unwrap();
        let bs = encode_index_seek_key(1, 1, &key);
        assert!(!bs.is_empty());

        let r = decode_index_key(&Default::default(), &bs, &col_types).unwrap();
        assert_eq!(col_values, r);

        let mut res: (HashMap<_, _>, _) = cut_idx_key_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert!(res.1.is_none());

        let handle_data = col_encoded.remove(&3).unwrap();
        let handle = if handle_data.is_empty() {
            None
        } else {
            Some((handle_data.as_ref() as &[u8]).decode_datum().unwrap().i64())
        };
        col_ids.remove(2);
        res = cut_idx_key_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert_eq!(res.1, handle);

        let bs = encode_index_seek_key(1, 1, &[]);
        assert!(!bs.is_empty());
        assert!(decode_index_key(&Default::default(), &bs, &[]).unwrap().is_empty());
        res = cut_idx_key_as_owned(&bs, &[]);
        assert!(res.0.is_empty());
        assert!(res.1.is_none());
    }
}
