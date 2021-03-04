// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::TryInto;
use std::io::Write;
use std::sync::Arc;
use std::{cmp, u8};

use crate::prelude::*;
use crate::FieldTypeTp;
use kvproto::coprocessor::KeyRange;
use tipb::ColumnInfo;

use super::mysql::{Duration, Time};
use super::{datum, datum::DatumDecoder, Datum, Error, Result};
use crate::expr::EvalContext;
use codec::prelude::*;
use collections::{HashMap, HashSet};
use tikv_util::codec::BytesSlice;

// handle or index id
pub const ID_LEN: usize = 8;
pub const PREFIX_LEN: usize = TABLE_PREFIX_LEN + ID_LEN /*table_id*/ + SEP_LEN;
pub const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
pub const TABLE_PREFIX: &[u8] = b"t";
pub const RECORD_PREFIX_SEP: &[u8] = b"_r";
pub const INDEX_PREFIX_SEP: &[u8] = b"_i";
pub const SEP_LEN: usize = 2;
pub const TABLE_PREFIX_LEN: usize = 1;
pub const TABLE_PREFIX_KEY_LEN: usize = TABLE_PREFIX_LEN + ID_LEN;
// the maximum len of the old encoding of index value.
pub const MAX_OLD_ENCODED_VALUE_LEN: usize = 9;

/// Flag that indicate if the index value has common handle.
pub const INDEX_VALUE_COMMON_HANDLE_FLAG: u8 = 127;
/// Flag that indicate if the index value has partition id.
pub const INDEX_VALUE_PARTITION_ID_FLAG: u8 = 126;
/// Flag that indicate if the index values has the version information.
pub const INDEX_VALUE_VERSION_FLAG: u8 = 125;
/// Flag that indicate if the index value has restored data.
pub const INDEX_VALUE_RESTORED_DATA_FLAG: u8 = crate::codec::row::v2::CODEC_VERSION;

/// ID for partition column, see https://github.com/pingcap/parser/pull/1010
pub const EXTRA_PARTITION_ID_COL_ID: i64 = -2;

/// `TableEncoder` encodes the table record/index prefix.
trait TableEncoder: NumberEncoder {
    fn append_table_record_prefix(&mut self, table_id: i64) -> Result<()> {
        self.write_bytes(TABLE_PREFIX)?;
        self.write_i64(table_id)?;
        self.write_bytes(RECORD_PREFIX_SEP).map_err(Error::from)
    }

    fn append_table_index_prefix(&mut self, table_id: i64) -> Result<()> {
        self.write_bytes(TABLE_PREFIX)?;
        self.write_i64(table_id)?;
        self.write_bytes(INDEX_PREFIX_SEP).map_err(Error::from)
    }
}

impl<T: BufferWriter> TableEncoder for T {}

/// Extracts table prefix from table record or index.
#[inline]
pub fn extract_table_prefix(key: &[u8]) -> Result<&[u8]> {
    if !key.starts_with(TABLE_PREFIX) || key.len() < TABLE_PREFIX_KEY_LEN {
        Err(invalid_type!(
            "record key or index key expected, but got {:?}",
            key
        ))
    } else {
        Ok(&key[..TABLE_PREFIX_KEY_LEN])
    }
}

/// Checks if the range is for table record or index.
pub fn check_table_ranges(ranges: &[KeyRange]) -> Result<()> {
    for range in ranges {
        extract_table_prefix(range.get_start())?;
        extract_table_prefix(range.get_end())?;
        if range.get_start() >= range.get_end() {
            return Err(invalid_type!(
                "invalid range,range.start should be smaller than range.end, but got [{:?},{:?})",
                range.get_start(),
                range.get_end()
            ));
        }
    }
    Ok(())
}

#[inline]
pub fn check_record_key(key: &[u8]) -> Result<()> {
    check_key_type(key, RECORD_PREFIX_SEP)
}

#[inline]
pub fn check_index_key(key: &[u8]) -> Result<()> {
    check_key_type(key, INDEX_PREFIX_SEP)
}

/// `check_key_type` checks if the key is the type we want, `wanted_type` should be
/// `table::RECORD_PREFIX_SEP` or `table::INDEX_PREFIX_SEP` .
#[inline]
fn check_key_type(key: &[u8], wanted_type: &[u8]) -> Result<()> {
    let mut buf = key;
    if buf.read_bytes(TABLE_PREFIX_LEN)? != TABLE_PREFIX {
        return Err(invalid_type!(
            "record or index key expected, but got {}",
            log_wrappers::Value::key(key)
        ));
    }

    buf.read_bytes(ID_LEN)?;
    if buf.read_bytes(SEP_LEN)? != wanted_type {
        Err(invalid_type!(
            "expected key sep type {}, but got key {})",
            log_wrappers::Value::key(wanted_type),
            log_wrappers::Value::key(key)
        ))
    } else {
        Ok(())
    }
}

/// Decodes table ID from the key.
pub fn decode_table_id(key: &[u8]) -> Result<i64> {
    let mut buf = key;
    if buf.read_bytes(TABLE_PREFIX_LEN)? != TABLE_PREFIX {
        return Err(invalid_type!(
            "record key expected, but got {}",
            log_wrappers::Value::key(key)
        ));
    }
    buf.read_i64().map_err(Error::from)
}

/// `flatten` flattens the datum.
#[inline]
pub fn flatten(ctx: &mut EvalContext, data: Datum) -> Result<Datum> {
    match data {
        Datum::Dur(d) => Ok(Datum::I64(d.to_nanos())),
        Datum::Time(t) => Ok(Datum::U64(t.to_packed_u64(ctx)?)),
        _ => Ok(data),
    }
}

// `encode_row` encodes row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
pub fn encode_row(ctx: &mut EvalContext, row: Vec<Datum>, col_ids: &[i64]) -> Result<Vec<u8>> {
    if row.len() != col_ids.len() {
        return Err(box_err!(
            "data and columnID count not match {} vs {}",
            row.len(),
            col_ids.len()
        ));
    }
    let mut values = Vec::with_capacity(cmp::max(row.len() * 2, 1));
    for (&id, col) in col_ids.iter().zip(row) {
        values.push(Datum::I64(id));
        let fc = flatten(ctx, col)?;
        values.push(fc);
    }
    if values.is_empty() {
        values.push(Datum::Null);
    }
    datum::encode_value(ctx, &values)
}

/// `encode_row_key` encodes the table id and record handle into a byte array.
pub fn encode_row_key(table_id: i64, handle: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN);
    // can't panic
    key.append_table_record_prefix(table_id).unwrap();
    key.write_i64(handle).unwrap();
    key
}

pub fn encode_common_handle_for_test(table_id: i64, handle: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_LEN + handle.len());
    key.append_table_record_prefix(table_id).unwrap();
    key.extend(handle);
    key
}

/// `encode_column_key` encodes the table id, row handle and column id into a byte array.
pub fn encode_column_key(table_id: i64, handle: i64, column_id: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(RECORD_ROW_KEY_LEN + ID_LEN);
    key.append_table_record_prefix(table_id).unwrap();
    key.write_i64(handle).unwrap();
    key.write_i64(column_id).unwrap();
    key
}

/// `decode_int_handle` decodes the key and gets the int handle.
#[inline]
pub fn decode_int_handle(mut key: &[u8]) -> Result<i64> {
    check_record_key(key)?;
    key = &key[PREFIX_LEN..];
    key.read_i64().map_err(Error::from)
}

/// `decode_common_handle` decodes key key and gets the common handle.
#[inline]
pub fn decode_common_handle(mut key: &[u8]) -> Result<&[u8]> {
    check_record_key(key)?;
    key = &key[PREFIX_LEN..];
    Ok(key)
}

/// `encode_index_seek_key` encodes an index value to byte array.
pub fn encode_index_seek_key(table_id: i64, idx_id: i64, encoded: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(PREFIX_LEN + ID_LEN + encoded.len());
    key.append_table_index_prefix(table_id).unwrap();
    key.write_i64(idx_id).unwrap();
    key.write_all(encoded).unwrap();
    key
}

// `decode_index_key` decodes datums from an index key.
pub fn decode_index_key(
    ctx: &mut EvalContext,
    encoded: &[u8],
    infos: &[ColumnInfo],
) -> Result<Vec<Datum>> {
    let mut buf = &encoded[PREFIX_LEN + ID_LEN..];
    let mut res = vec![];

    for info in infos {
        if buf.is_empty() {
            return Err(box_err!(
                "{} is too short.",
                log_wrappers::Value::key(encoded)
            ));
        }
        let mut v = buf.read_datum()?;
        v = unflatten(ctx, v, info)?;
        res.push(v);
    }

    Ok(res)
}

/// `unflatten` converts a raw datum to a column datum.
fn unflatten(
    ctx: &mut EvalContext,
    datum: Datum,
    field_type: &dyn FieldTypeAccessor,
) -> Result<Datum> {
    if let Datum::Null = datum {
        return Ok(datum);
    }
    let tp = field_type.tp();
    match tp {
        FieldTypeTp::Float => Ok(Datum::F64(f64::from(datum.f64() as f32))),
        FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
            let fsp = field_type.decimal() as i8;
            let t = Time::from_packed_u64(ctx, datum.u64(), tp.try_into()?, fsp)?;
            Ok(Datum::Time(t))
        }
        FieldTypeTp::Duration => {
            Duration::from_nanos(datum.i64(), field_type.decimal() as i8).map(Datum::Dur)
        }
        FieldTypeTp::Enum | FieldTypeTp::Set | FieldTypeTp::Bit => Err(box_err!(
            "unflatten field type {} is not supported yet.",
            tp
        )),
        t => {
            debug_assert!(
                [
                    FieldTypeTp::Tiny,
                    FieldTypeTp::Short,
                    FieldTypeTp::Year,
                    FieldTypeTp::Int24,
                    FieldTypeTp::Long,
                    FieldTypeTp::LongLong,
                    FieldTypeTp::Double,
                    FieldTypeTp::TinyBlob,
                    FieldTypeTp::MediumBlob,
                    FieldTypeTp::Blob,
                    FieldTypeTp::LongBlob,
                    FieldTypeTp::VarChar,
                    FieldTypeTp::String,
                    FieldTypeTp::NewDecimal,
                    FieldTypeTp::JSON
                ]
                .contains(&t),
                "unknown type {} {}",
                t,
                datum
            );
            Ok(datum)
        }
    }
}

// `decode_col_value` decodes data to a Datum according to the column info.
pub fn decode_col_value(
    data: &mut BytesSlice<'_>,
    ctx: &mut EvalContext,
    col: &ColumnInfo,
) -> Result<Datum> {
    let d = data.read_datum()?;
    unflatten(ctx, d, col)
}

// `decode_row` decodes a byte slice into datums.
// TODO: We should only decode columns in the cols map.
// Row layout: colID1, value1, colID2, value2, .....
pub fn decode_row(
    data: &mut BytesSlice<'_>,
    ctx: &mut EvalContext,
    cols: &HashMap<i64, ColumnInfo>,
) -> Result<HashMap<i64, Datum>> {
    let mut values = datum::decode(data)?;
    if values.get(0).map_or(true, |d| *d == Datum::Null) {
        return Ok(HashMap::default());
    }
    if values.len() & 1 == 1 {
        return Err(box_err!("decoded row values' length should be even!"));
    }
    let mut row = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
    let mut drain = values.drain(..);
    loop {
        let id = match drain.next() {
            None => return Ok(row),
            Some(id) => id.i64(),
        };
        let v = drain.next().unwrap();
        if let Some(ci) = cols.get(&id) {
            let v = unflatten(ctx, v, ci)?;
            row.insert(id, v);
        }
    }
}

/// `RowColMeta` saves the column meta of the row.
#[derive(Debug)]
pub struct RowColMeta {
    offset: usize,
    length: usize,
}

/// `RowColsDict` stores the row data and a map mapping column ID to its meta.
#[derive(Debug)]
pub struct RowColsDict {
    // data of current row
    pub value: Vec<u8>,
    // cols contains meta of each column in the format of:
    // (col_id1,(offset1,len1)),(col_id2,(offset2,len2),...)
    pub cols: HashMap<i64, RowColMeta>,
}

impl RowColMeta {
    pub fn new(offset: usize, length: usize) -> RowColMeta {
        RowColMeta { offset, length }
    }
}

impl RowColsDict {
    pub fn new(cols: HashMap<i64, RowColMeta>, value: Vec<u8>) -> RowColsDict {
        RowColsDict { value, cols }
    }

    /// Returns the total count of the columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.cols.len()
    }

    /// Returns whether it has columns or not.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    /// Gets the column data from its meta if `key` exists.
    pub fn get(&self, key: i64) -> Option<&[u8]> {
        if let Some(meta) = self.cols.get(&key) {
            return Some(&self.value[meta.offset..(meta.offset + meta.length)]);
        }
        None
    }

    /// Appends a column to the row.
    pub fn append(&mut self, cid: i64, value: &mut Vec<u8>) {
        let offset = self.value.len();
        let length = value.len();
        self.value.append(value);
        self.cols.insert(cid, RowColMeta::new(offset, length));
    }

    /// Gets binary of cols, keeps the original order, and returns one slice and cols' end offsets.
    pub fn get_column_values_and_end_offsets(&self) -> (&[u8], Vec<usize>) {
        let mut start = self.value.len();
        let mut length = 0;
        for meta in self.cols.values() {
            if meta.offset < start {
                start = meta.offset;
            }
            length += meta.length;
        }
        let end_offsets = self
            .cols
            .values()
            .map(|meta| meta.offset + meta.length - start)
            .collect();
        (&self.value[start..start + length], end_offsets)
    }
}

/// `cut_row` cuts the encoded row into (col_id,offset,length)
///  and returns interested columns' meta in RowColsDict
///
/// Encoded row can be either in row format v1 or v2.
///
/// `col_ids` must be consistent with `cols`. Otherwise the result is undefined.
pub fn cut_row(
    data: Vec<u8>,
    col_ids: &HashSet<i64>,
    cols: Arc<[ColumnInfo]>,
) -> Result<RowColsDict> {
    if cols.is_empty() || data.is_empty() || (data.len() == 1 && data[0] == datum::NIL_FLAG) {
        return Ok(RowColsDict::new(HashMap::default(), data));
    }
    match data[0] {
        crate::codec::row::v2::CODEC_VERSION => cut_row_v2(data, cols),
        _ => cut_row_v1(data, col_ids),
    }
}

/// Cuts a non-empty row in row format v1.
fn cut_row_v1(data: Vec<u8>, cols: &HashSet<i64>) -> Result<RowColsDict> {
    let meta_map = {
        let mut meta_map = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
        let length = data.len();
        let mut tmp_data: &[u8] = data.as_ref();
        while !tmp_data.is_empty() && meta_map.len() < cols.len() {
            let id = tmp_data.read_datum()?.i64();
            let offset = length - tmp_data.len();
            let (val, rem) = datum::split_datum(tmp_data, false)?;
            if cols.contains(&id) {
                meta_map.insert(id, RowColMeta::new(offset, val.len()));
            }
            tmp_data = rem;
        }
        meta_map
    };
    Ok(RowColsDict::new(meta_map, data))
}

/// Cuts a non-empty row in row format v2 and encodes into v1 format.
fn cut_row_v2(data: Vec<u8>, cols: Arc<[ColumnInfo]>) -> Result<RowColsDict> {
    use crate::codec::datum_codec::{ColumnIdDatumEncoder, EvaluableDatumEncoder};
    use crate::codec::row::v2::{RowSlice, V1CompatibleEncoder};

    let mut meta_map = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
    let mut result = Vec::with_capacity(data.len() + cols.len() * 8);

    let row_slice = RowSlice::from_bytes(&data)?;
    for col in cols.iter() {
        let id = col.get_column_id();
        if let Some((start, offset)) = row_slice.search_in_non_null_ids(id)? {
            result.write_column_id_datum(id)?;
            let v2_datum = &row_slice.values()[start..offset];
            let result_offset = result.len();
            result.write_v2_as_datum(v2_datum, col)?;
            meta_map.insert(
                id,
                RowColMeta::new(result_offset, result.len() - result_offset),
            );
        } else if row_slice.search_in_null_ids(id) {
            result.write_column_id_datum(id)?;
            let result_offset = result.len();
            result.write_evaluable_datum_null()?;
            meta_map.insert(
                id,
                RowColMeta::new(result_offset, result.len() - result_offset),
            );
        } else {
            // Otherwise the column does not exist.
        }
    }
    Ok(RowColsDict::new(meta_map, result))
}

/// `cut_idx_key` cuts the encoded index key into RowColsDict and handle .
pub fn cut_idx_key(key: Vec<u8>, col_ids: &[i64]) -> Result<(RowColsDict, Option<i64>)> {
    let mut meta_map: HashMap<i64, RowColMeta> =
        HashMap::with_capacity_and_hasher(col_ids.len(), Default::default());
    let handle = {
        let mut tmp_data: &[u8] = &key[PREFIX_LEN + ID_LEN..];
        let length = key.len();
        // parse cols from data
        for &id in col_ids {
            let offset = length - tmp_data.len();
            let (val, rem) = datum::split_datum(tmp_data, false)?;
            meta_map.insert(id, RowColMeta::new(offset, val.len()));
            tmp_data = rem;
        }

        if tmp_data.is_empty() {
            None
        } else {
            Some(tmp_data.read_datum()?.i64())
        }
    };
    Ok((RowColsDict::new(meta_map, key), handle))
}

pub fn generate_index_data_for_test(
    table_id: i64,
    index_id: i64,
    handle: i64,
    col_val: &Datum,
    unique: bool,
) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
    let indice = vec![(2, (*col_val).clone()), (3, Datum::Dec(handle.into()))];
    let mut expect_row = HashMap::default();
    let mut v: Vec<_> = indice
        .iter()
        .map(|&(ref cid, ref value)| {
            expect_row.insert(
                *cid,
                datum::encode_key(&mut EvalContext::default(), &[value.clone()]).unwrap(),
            );
            value.clone()
        })
        .collect();
    if !unique {
        v.push(Datum::I64(handle));
    }
    let encoded = datum::encode_key(&mut EvalContext::default(), &v).unwrap();
    let idx_key = encode_index_seek_key(table_id, index_id, &encoded);
    (expect_row, idx_key)
}

#[cfg(test)]
mod tests {
    use std::i64;

    use tipb::ColumnInfo;

    use crate::codec::datum::{self, Datum};
    use collections::{HashMap, HashSet};
    use tikv_util::map;

    use super::*;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;

    #[test]
    fn test_row_key_codec() {
        let tests = vec![i64::MIN, i64::MAX, -1, 0, 2, 3, 1024];
        for &t in &tests {
            let k = encode_row_key(1, t);
            assert_eq!(t, decode_int_handle(&k).unwrap());
        }
    }

    #[test]
    fn test_index_key_codec() {
        let tests = vec![
            Datum::U64(1),
            Datum::Bytes(b"123".to_vec()),
            Datum::I64(-1),
            Datum::Dur(Duration::parse(&mut EvalContext::default(), "12:34:56.666", 2).unwrap()),
        ];

        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let types = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::LongLong.into(),
            duration_col,
        ];
        let mut ctx = EvalContext::default();
        let buf = datum::encode_key(&mut ctx, &tests).unwrap();
        let encoded = encode_index_seek_key(1, 2, &buf);
        assert_eq!(tests, decode_index_key(&mut ctx, &encoded, &types).unwrap());
    }

    fn to_hash_map(row: &RowColsDict) -> HashMap<i64, Vec<u8>> {
        let mut data = HashMap::with_capacity_and_hasher(row.cols.len(), Default::default());
        if row.is_empty() {
            return data;
        }
        for (key, meta) in &row.cols {
            data.insert(
                *key,
                row.value[meta.offset..(meta.offset + meta.length)].to_vec(),
            );
        }
        data
    }

    fn cut_row_as_owned(bs: &[u8], col_id_set: &HashSet<i64>) -> HashMap<i64, Vec<u8>> {
        let is_empty_row =
            col_id_set.is_empty() || bs.is_empty() || (bs.len() == 1 && bs[0] == datum::NIL_FLAG);
        let res = if is_empty_row {
            RowColsDict::new(HashMap::default(), bs.to_vec())
        } else {
            cut_row_v1(bs.to_vec(), col_id_set).unwrap()
        };
        to_hash_map(&res)
    }

    fn cut_idx_key_as_owned(bs: &[u8], ids: &[i64]) -> (HashMap<i64, Vec<u8>>, Option<i64>) {
        let (res, left) = cut_idx_key(bs.to_vec(), ids).unwrap();
        (to_hash_map(&res), left)
    }

    #[test]
    fn test_row_codec() {
        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let mut cols = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col
        ];

        let mut row = map![
            1 => Datum::I64(100),
            2 => Datum::Bytes(b"abc".to_vec()),
            3 => Datum::Dec(10.into()),
            5 => Datum::Json(r#"{"name": "John"}"#.parse().unwrap()),
            6 => Datum::Dur(Duration::parse(&mut EvalContext::default(),"23:23:23.666",2 ).unwrap())
        ];

        let mut ctx = EvalContext::default();
        let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
        let col_values: Vec<_> = row.iter().map(|(_, v)| v.clone()).collect();
        let mut col_encoded: HashMap<_, _> = row
            .iter()
            .map(|(k, v)| {
                let f = super::flatten(&mut ctx, v.clone()).unwrap();
                (*k, datum::encode_value(&mut ctx, &[f]).unwrap())
            })
            .collect();
        let mut col_id_set: HashSet<_> = col_ids.iter().cloned().collect();

        let bs = encode_row(&mut ctx, col_values, &col_ids).unwrap();
        assert!(!bs.is_empty());
        let mut ctx = EvalContext::default();
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        assert_eq!(row, r);

        let mut datums: HashMap<_, _>;
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.insert(4, FieldTypeTp::Float.into());
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        assert_eq!(row, r);

        col_id_set.insert(4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.remove(&4);
        cols.remove(&3);
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        row.remove(&3);
        assert_eq!(row, r);

        col_id_set.remove(&3);
        col_id_set.remove(&4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        col_encoded.remove(&3);
        assert_eq!(col_encoded, datums);

        let bs = encode_row(&mut ctx, vec![], &[]).unwrap();
        assert!(!bs.is_empty());
        assert!(decode_row(&mut bs.as_slice(), &mut ctx, &cols)
            .unwrap()
            .is_empty());
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert!(datums.is_empty());
    }

    #[test]
    fn test_idx_codec() {
        let mut col_ids = vec![1, 2, 3, 4];

        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let col_types = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::NewDecimal.into(),
            duration_col,
        ];

        let col_values = vec![
            Datum::I64(100),
            Datum::Bytes(b"abc".to_vec()),
            Datum::Dec(10.into()),
            Datum::Dur(Duration::parse(&mut EvalContext::default(), "23:23:23.666", 2).unwrap()),
        ];

        let mut ctx = EvalContext::default();
        let mut col_encoded: HashMap<_, _> = col_ids
            .iter()
            .zip(&col_types)
            .zip(&col_values)
            .map(|((id, t), v)| {
                let unflattened = super::unflatten(&mut ctx, v.clone(), t).unwrap();
                let encoded = datum::encode_key(&mut ctx, &[unflattened]).unwrap();
                (*id, encoded)
            })
            .collect();

        let key = datum::encode_key(&mut ctx, &col_values).unwrap();
        let bs = encode_index_seek_key(1, 1, &key);
        assert!(!bs.is_empty());
        let mut ctx = EvalContext::default();
        let r = decode_index_key(&mut ctx, &bs, &col_types).unwrap();
        assert_eq!(col_values, r);

        let mut res: (HashMap<_, _>, _) = cut_idx_key_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert!(res.1.is_none());

        let handle_data = col_encoded.remove(&4).unwrap();
        let handle = if handle_data.is_empty() {
            None
        } else {
            Some((handle_data.as_ref() as &[u8]).read_datum().unwrap().i64())
        };
        col_ids.remove(3);
        res = cut_idx_key_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert_eq!(res.1, handle);

        let bs = encode_index_seek_key(1, 1, &[]);
        assert!(!bs.is_empty());
        assert!(decode_index_key(&mut ctx, &bs, &[]).unwrap().is_empty());
        res = cut_idx_key_as_owned(&bs, &[]);
        assert!(res.0.is_empty());
        assert!(res.1.is_none());
    }

    #[test]
    fn test_extract_table_prefix() {
        let cases = vec![
            (vec![], None),
            (b"a\x80\x00\x00\x00\x00\x00\x00\x01".to_vec(), None),
            (b"t\x80\x00\x00\x00\x00\x00\x01".to_vec(), None),
            (
                b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec(),
                Some(b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec()),
            ),
            (
                b"t\x80\x00\x00\x00\x00\x00\x00\x01_r\xff\xff".to_vec(),
                Some(b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec()),
            ),
        ];
        for (input, output) in cases {
            assert_eq!(extract_table_prefix(&input).ok().map(From::from), output);
        }
    }

    #[test]
    fn test_check_table_range() {
        let small_key = b"t\x80\x00\x00\x00\x00\x00\x00\x01a".to_vec();
        let large_key = b"t\x80\x00\x00\x00\x00\x00\x00\x01b".to_vec();
        let mut range = KeyRange::default();
        range.set_start(small_key.clone());
        range.set_end(large_key.clone());
        assert!(check_table_ranges(&[range]).is_ok());
        //test range.start > range.end
        let mut range = KeyRange::default();
        range.set_end(small_key.clone());
        range.set_start(large_key);
        assert!(check_table_ranges(&[range]).is_err());

        // test invalid end
        let mut range = KeyRange::default();
        range.set_start(small_key);
        range.set_end(b"xx".to_vec());
        assert!(check_table_ranges(&[range]).is_err());
    }

    #[test]
    fn test_decode_table_id() {
        let tests = vec![0, 2, 3, 1024, i64::MAX];
        for &tid in &tests {
            let k = encode_row_key(tid, 1);
            assert_eq!(tid, decode_table_id(&k).unwrap());
            let k = encode_index_seek_key(tid, 1, &k);
            assert_eq!(tid, decode_table_id(&k).unwrap());
            assert!(decode_table_id(b"xxx").is_err());
        }
    }

    #[test]
    fn test_check_key_type() {
        let record_key = encode_row_key(TABLE_ID, 1);
        assert!(check_key_type(&record_key.as_slice(), RECORD_PREFIX_SEP).is_ok());
        assert!(check_key_type(&record_key.as_slice(), INDEX_PREFIX_SEP).is_err());

        let (_, index_key) =
            generate_index_data_for_test(TABLE_ID, INDEX_ID, 1, &Datum::I64(1), true);
        assert!(check_key_type(&index_key.as_slice(), RECORD_PREFIX_SEP).is_err());
        assert!(check_key_type(&index_key.as_slice(), INDEX_PREFIX_SEP).is_ok());

        let too_small_key = vec![0];
        assert!(check_key_type(&too_small_key.as_slice(), RECORD_PREFIX_SEP).is_err());
        assert!(check_key_type(&too_small_key.as_slice(), INDEX_PREFIX_SEP).is_err());
    }
}
