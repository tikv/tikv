#![allow(dead_code)]

use crate::coprocessor::{
    codec::{
        mysql::{decimal::DecimalEncoder, json::JsonEncoder},
        Datum,
    },
    dag::expr::EvalContext,
};
use crate::util::collections::HashMap;
use codec::{prelude::*, Error, NumberCodec, Result};
use cop_datatype::FieldTypeAccessor;
use std::io::Cursor;
use std::{mem, slice};
use tipb::schema::ColumnInfo;

const CODEC_VERSION: u8 = 128;
const LARGE_ID: i64 = 255;

const F64_SIZE: usize = 8;

// | version | flag | number_of_not_null_columns | number_of_null_columns | not_null_ids | null_ids | value_offsets | values
// version: 1 byte
// flag: 1 byte
// number of not null columns: 2 bytes
// number of null columns: 2 bytes
// column ids:  not null ids + null ids,  when flag == 1 (large), id is 4 bytes, otherwise 1 byte
// not null values offset: when large, offset is 4 bytes, otherwise 2 bytes
pub fn encode(datums: Vec<Datum>, ids: &[i64]) -> Result<Vec<u8>> {
    let mut flag = 0;

    let ids_len = ids.len();
    let mut not_null_cols = Vec::with_capacity(ids.len());
    let mut null_cols = Vec::with_capacity(ids.len());

    let mut value_approx_size = 0;
    for (datum, id) in datums.into_iter().zip(ids) {
        let datum = flatten(datum);
        if *id > LARGE_ID {
            flag = 1;
        }
        value_approx_size += datum_size(&datum);

        if datum == Datum::Null {
            null_cols.push((datum, *id));
        } else {
            not_null_cols.push((datum, *id));
        }
    }
    let (id_size, offset_size) = if flag == 1 { (4, 4) } else { (1, 2) };

    // version(1) + flag(1) + columns_length(4) + ids_length + values_offset_length + values_length
    let approximate_size =
        6 + ids_len * id_size + not_null_cols.len() * offset_size + value_approx_size;

    let mut wtr = Vec::with_capacity(approximate_size);
    wtr.push(CODEC_VERSION);
    wtr.push(flag);
    wtr.write_u16_le(not_null_cols.len() as u16)?;
    wtr.write_u16_le(null_cols.len() as u16)?;

    not_null_cols.sort_by_key(|(_, id)| *id);
    null_cols.sort_by_key(|(_, id)| *id);

    let mut offset_writer = Vec::with_capacity(offset_size * not_null_cols.len());
    let mut value_writer = Vec::with_capacity(value_approx_size);

    // write ids first, then append offsets and values
    if flag == 0 {
        for (datum, id) in not_null_cols {
            wtr.push(id as u8);
            encode_datum(datum, &mut value_writer)?;
            offset_writer.write_u16_le(value_writer.len() as u16)?;
        }

        for (_, id) in null_cols {
            wtr.push(id as u8);
        }
    } else {
        for (datum, id) in not_null_cols {
            wtr.write_u32_le(id as u32)?;
            encode_datum(datum, &mut value_writer)?;
            offset_writer.write_u32_le(value_writer.len() as u32)?;
        }

        for (_, id) in null_cols {
            wtr.write_u32_le(id as u32)?;
        }
    }
    wtr.extend_from_slice(&offset_writer);
    wtr.extend_from_slice(&value_writer);
    Ok(wtr)
}

#[derive(Debug)]
struct Row {
    not_null_cnt: u16,
    null_cnt: u16,
    ids: Vec<i64>,
    offsets: Vec<usize>,
    values: Vec<u8>,
}

impl Row {
    fn new(data: Vec<u8>) -> Result<Self> {
        if data[0] != CODEC_VERSION {
            panic!("version not matched");
        }

        // read ids count
        let not_null_cnt = NumberCodec::decode_u16_le(&data[2..]);
        let null_cnt = NumberCodec::decode_u16_le(&data[4..]);
        let mut rdr = Cursor::new(&data[6..]);

        // read ids and offsets
        let (ids, offsets) = if data[1] == 0 {
            (
                Row::read_ints(&mut rdr, (not_null_cnt + null_cnt) as usize, u8::to_i64)?,
                Row::read_ints(&mut rdr, not_null_cnt as usize, u16::to_usize)?,
            )
        } else {
            (
                Row::read_ints(&mut rdr, (not_null_cnt + null_cnt) as usize, u32::to_i64)?,
                Row::read_ints(&mut rdr, not_null_cnt as usize, u32::to_usize)?,
            )
        };
        Ok(Row {
            not_null_cnt,
            null_cnt,
            ids,
            offsets,
            values: rdr.get_ref()[rdr.position() as usize..].to_vec(),
        })
    }

    fn decode(
        &self,
        ctx: &EvalContext,
        cols: &HashMap<i64, ColumnInfo>,
    ) -> Result<HashMap<i64, Datum>> {
        let mut row = HashMap::with_capacity(cols.len());
        let (not_null_ids, null_ids) = self.ids.split_at(self.not_null_cnt as usize);
        for (id, tp) in cols {
            if let Ok(idx) = not_null_ids.binary_search(id) {
                let offset = self
                    .offsets
                    .get(idx)
                    .expect("should always have value in offsets");
                let start = if idx > 0 {
                    self.offsets[idx - 1]
                } else {
                    0usize
                };
                row.insert(*id, decode_datum(tp, ctx, &self.values[start..*offset])?);
            } else if null_ids.binary_search(id).is_ok() {
                row.insert(*id, Datum::Null);
            } else {
                row.insert(*id, decode_datum(tp, ctx, tp.get_default_val())?);
            }
        }
        Ok(row)
    }

    // batch read ints as vec
    // using *unsafe* from_raw_parts is actually safe here, because we can ensure
    //   1. the cursor had at least *len* elements
    //   2. the result of from_raw_parts is only used in the function, so no lifetime issue
    #[inline]
    fn read_ints<T, R, F>(rdr: &mut Cursor<&[u8]>, len: usize, f: F) -> Result<(Vec<R>)>
    where
        T: Converter,
        F: FnMut(&T) -> R,
    {
        let bytes_len = mem::size_of::<T>() * len;
        let pos = rdr.position() as usize;
        if rdr.get_ref().len() < pos + bytes_len {
            return Err(Error::BufferTooSmall);
        }
        let x = rdr.get_ref()[pos..pos + bytes_len].as_ptr() as *const T;
        rdr.set_position(rdr.position() + bytes_len as u64);
        let res = unsafe { slice::from_raw_parts(x, len) };
        let dst = res.iter().map(f).collect::<Vec<_>>();
        Ok(dst)
    }
}

// convert number to usize or i64
// ids need to convert i64 since the column id is i64
// offsets need to convert of usize, since it will used as range index later
trait Converter {
    fn to_usize(&self) -> usize;
    fn to_i64(&self) -> i64;
}

impl Converter for u8 {
    fn to_usize(&self) -> usize {
        *self as usize
    }

    fn to_i64(&self) -> i64 {
        i64::from(*self)
    }
}

impl Converter for u16 {
    #[inline]
    fn to_usize(&self) -> usize {
        self.to_le() as usize
    }

    #[inline]
    fn to_i64(&self) -> i64 {
        i64::from(self.to_le())
    }
}

impl Converter for u32 {
    #[inline]
    fn to_usize(&self) -> usize {
        self.to_le() as usize
    }

    #[inline]
    fn to_i64(&self) -> i64 {
        i64::from(self.to_le())
    }
}

pub fn decode(
    data: Vec<u8>,
    ctx: &EvalContext,
    cols: &HashMap<i64, ColumnInfo>,
) -> Result<HashMap<i64, Datum>> {
    Row::new(data)?.decode(ctx, cols)
}

fn approx_size(datums: &[Datum]) -> usize {
    datums.iter().map(datum_size).sum()
}

fn datum_size(datum: &Datum) -> usize {
    match *datum {
        Datum::I64(i) => i64_size(i),
        Datum::U64(u) => u64_size(u),
        Datum::Bytes(ref bs) => bs.len(),
        Datum::Dec(ref d) => d.approximate_encoded_size(),
        Datum::Json(ref j) => j.binary_len(),
        Datum::F64(_) | Datum::Time(_) | Datum::Dur(_) => {
            unreachable!("this three type should b flattened")
        }
        _ => 0,
    }
}

#[allow(clippy::match_overlapping_arm)]
fn i64_size(i: i64) -> usize {
    match i {
        -128..=127 => 1,
        -32768..=32767 => 2,
        -2_147_483_648..=2_147_483_647 => 4,
        _ => 8,
    }
}

fn u64_size(u: u64) -> usize {
    match u {
        0..=255 => 1,
        256..=65535 => 2,
        65536..=4_294_967_295 => 4,
        _ => 8,
    }
}

#[inline]
fn flatten(datum: Datum) -> Datum {
    match datum {
        Datum::F64(f) => Datum::U64(f.to_bits()),
        Datum::Time(t) => Datum::U64(t.to_packed_u64()),
        Datum::Dur(d) => Datum::I64(d.to_nanos()),
        _ => datum,
    }
}

fn encode_datum(datum: Datum, dest: &mut Vec<u8>) -> Result<()> {
    match datum {
        Datum::I64(i) => encode_i64(i, dest),
        Datum::U64(u) => encode_u64(u, dest),
        Datum::Bytes(b) => encode_bytes(b, dest),
        Datum::Dec(d) => {
            let (prec, frac) = d.prec_and_frac();
            dest.encode_decimal(&d, prec, frac)
                .expect("TODO: handle this");
            Ok(())
        }
        Datum::Json(j) => {
            dest.encode_json(&j).expect("TODO: handle this");
            Ok(())
        }
        Datum::F64(_) | Datum::Time(_) | Datum::Dur(_) | Datum::Null => unreachable!(),
        _ => unimplemented!(),
    }
}

#[allow(clippy::match_overlapping_arm)]
fn encode_i64(v: i64, dest: &mut Vec<u8>) -> Result<()> {
    match v {
        -128..=127 => {
            dest.push(v as i8 as u8);
            Ok(())
        }
        -32768..=32767 => dest.write_i16_le(v as i16),
        -2_147_483_648..=2_147_483_647 => dest.write_i32_le(v as i32),
        _ => dest.write_i64_le(v),
    }
}

fn encode_u64(v: u64, dest: &mut Vec<u8>) -> Result<()> {
    match v {
        0..=255 => {
            dest.push(v as u8);
            Ok(())
        }
        256..=65535 => dest.write_u16_le(v as u16),
        65536..=4_294_967_295 => dest.write_u32_le(v as u32),
        _ => dest.write_u64_le(v),
    }
}

fn encode_bytes(b: Vec<u8>, dest: &mut Vec<u8>) -> Result<()> {
    dest.extend_from_slice(&b);
    Ok(())
}

//    Float = 4,
//    Null = 6,
//    NewDate = 14,
//    Bit = 16,
//    Enum = 0xf7,
//    Set = 0xf8,
//    Geometry = 0xff,
fn decode_datum(col_info: &ColumnInfo, ctx: &EvalContext, data: &[u8]) -> Result<Datum> {
    use crate::coprocessor::codec::mysql::{Decimal, Duration as MysqlDuration, Json, Time};
    use cop_datatype::{FieldTypeFlag, FieldTypeTp::*};
    use core::convert::TryInto;
    match col_info.tp() {
        Tiny | Short | Long | LongLong | Int24 | Year => {
            if col_info.flag().contains(FieldTypeFlag::UNSIGNED) {
                Ok(Datum::U64(decode_u64(data)))
            } else {
                Ok(Datum::I64(decode_i64(data)))
            }
        }
        VarChar | VarString | String | TinyBlob | MediumBlob | LongBlob | Blob => {
            Ok(Datum::Bytes(data.to_vec()))
        }
        Double | Float => Ok(Datum::F64(f64::from_bits(decode_u64(data)))),
        Date | DateTime | Timestamp => {
            let t = Time::from_packed_u64(
                decode_u64(data),
                col_info.tp().try_into().unwrap(),
                col_info.decimal() as i8,
                &ctx.cfg.tz,
            )
            .expect("TODO: handle this");
            Ok(Datum::Time(t))
        }
        NewDecimal => {
            let d = Decimal::decode(&mut data.to_vec().as_slice()).expect("TODO: handle this");
            Ok(Datum::Dec(d))
        }
        JSON => {
            let j = Json::decode(&mut data.to_vec().as_slice()).expect("TODO: handle this");
            Ok(Datum::Json(j))
        }
        Duration => {
            let d = MysqlDuration::from_nanos(decode_i64(data), col_info.decimal() as i8)
                .expect("TODO: handle this");
            Ok(Datum::Dur(d))
        }
        _ => unimplemented!(),
    }
}

fn decode_u64(data: &[u8]) -> u64 {
    match data.len() {
        1 => u64::from(data[0]),
        2 => u64::from(NumberCodec::decode_u16_le(data)),
        4 => u64::from(NumberCodec::decode_u32_le(data)),
        _ => NumberCodec::decode_u64_le(data),
    }
}

fn decode_i64(data: &[u8]) -> i64 {
    match data.len() {
        1 => i64::from(data[0]),
        2 => i64::from(NumberCodec::decode_i16_le(data)),
        4 => i64::from(NumberCodec::decode_i32_le(data)),
        _ => NumberCodec::decode_i64_le(data),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::coprocessor::codec::{
        mysql::{duration::NANOS_PER_SEC, Duration as MysqlDuration, Json, Time},
        Datum,
    };
    use crate::coprocessor::dag::expr::EvalContext;
    use cop_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp, FieldTypeTp::*};
    use tipb::schema::ColumnInfo;

    use super::{decode, encode};

    #[test]
    fn test_encode() {
        let datums = vec![
            Datum::I64(1000),
            Datum::I64(2),
            Datum::Null,
            Datum::U64(3),
            Datum::I64(32767),
            Datum::Bytes(b"abc".to_vec()),
            Datum::F64(1.8),
            Datum::F64(-1.8),
            Datum::Time(Time::parse_utc_datetime("2018-01-19 03:14:07", 0).unwrap()),
            Datum::Dec(1i64.into()),
            Datum::Json(Json::from_str(r#"{"key":"value"}"#).unwrap()),
            Datum::Dur(MysqlDuration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
        ];
        let ids = vec![1, 12, 33, 3, 8, 7, 9, 6, 13, 14, 15, 16];

        let exp = vec![
            128, 0, 11, 0, 1, 0, 1, 3, 6, 7, 8, 9, 12, 13, 14, 15, 16, 33, 2, 0, 3, 0, 11, 0, 14,
            0, 16, 0, 24, 0, 25, 0, 33, 0, 36, 0, 65, 0, 69, 0, 232, 3, 3, 205, 204, 204, 204, 204,
            204, 252, 191, 97, 98, 99, 255, 127, 205, 204, 204, 204, 204, 204, 252, 63, 2, 0, 0, 0,
            135, 51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 3, 0, 12,
            22, 0, 0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 0, 202, 154, 59,
        ];

        assert_eq!(exp, encode(datums, &ids).unwrap());
    }

    #[test]
    fn test_encode_large() {
        let datums = vec![
            Datum::I64(1000),
            Datum::I64(2),
            Datum::Null,
            Datum::I64(3),
            Datum::I64(32767),
        ];
        let ids = vec![1, 12, 335, 3, 8];
        let exp = vec![
            128, 1, 4, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 79, 1, 0, 0, 2, 0,
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 232, 3, 3, 255, 127, 2,
        ];

        assert_eq!(exp, encode(datums, &ids).unwrap());
    }

    fn col_info(tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.as_mut_accessor().set_tp(tp);
        col_info
    }

    #[test]
    fn test_decode() {
        let data = vec![
            128, 0, 11, 0, 1, 0, 1, 3, 6, 7, 8, 9, 12, 13, 14, 15, 16, 33, 2, 0, 3, 0, 11, 0, 14,
            0, 16, 0, 24, 0, 25, 0, 33, 0, 36, 0, 65, 0, 69, 0, 232, 3, 3, 205, 204, 204, 204, 204,
            204, 252, 191, 97, 98, 99, 255, 127, 205, 204, 204, 204, 204, 204, 252, 63, 2, 0, 0, 0,
            135, 51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 3, 0, 12,
            22, 0, 0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 0, 202, 154, 59,
        ];

        let mut unsigned_tp = col_info(Long);
        unsigned_tp
            .as_mut_accessor()
            .set_flag(FieldTypeFlag::UNSIGNED);

        let mut with_default_value = col_info(Long);
        // TODO: test more complex value
        with_default_value.set_default_val(vec![2]);

        let v = decode(
            data,
            &EvalContext::default(),
            &map![
                1 => col_info(Long),
                12 => col_info(Long),
                33 => col_info(Long),
                3 => unsigned_tp,
                8 => col_info(Long),
                7 => col_info(String),
                9 => col_info(Double),
                6 => col_info(Double),
                13 => col_info(DateTime),
                14 => col_info(NewDecimal),
                15 => col_info(JSON),
                16 => col_info(Duration),
                17 => with_default_value
            ],
        )
        .unwrap();

        let exp = map![
            1 => Datum::I64(1000),
            12 => Datum::I64(2),
            33 => Datum::Null,
            3 => Datum::U64(3),
            8 => Datum::I64(32767),
            7 => Datum::Bytes(b"abc".to_vec()),
            9 => Datum::F64(1.8),
            6 => Datum::F64(-1.8),
            13 => Datum::Time(Time::parse_utc_datetime("2018-01-19 03:14:07", 0).unwrap()),
            14 => Datum::Dec(1i64.into()),
            15 => Datum::Json(Json::from_str(r#"{"key":"value"}"#).unwrap()),
            16 => Datum::Dur(MysqlDuration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
            17 => Datum::I64(2)
        ];
        assert_eq!(exp, v);
    }

    #[test]
    fn test_decode_large() {
        let data = vec![
            128, 1, 4, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 79, 1, 0, 0, 2, 0,
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 232, 3, 3, 255, 127, 2,
        ];
        let v = decode(
            data,
            &EvalContext::default(),
            &map![
                12 => col_info(Long),
                3 => col_info(Long),
                335 => col_info(Long),
                8 => col_info(Long)
            ],
        )
        .unwrap();

        let exp = map![
            12 => Datum::I64(2),
            3 => Datum::I64(3),
            335 => Datum::Null,
            8 => Datum::I64(32767)
        ];
        assert_eq!(exp, v);
    }

    // not_null_ids => [1,5,4] , null_ids = [2,3]
    #[test]
    fn test_order_mix() {
        let ids = vec![1, 2, 3, 4, 5];
        let datums = vec![
            Datum::I64(1),
            Datum::Null,
            Datum::Null,
            Datum::I64(2),
            Datum::I64(3),
        ];
        let data = encode(datums, &ids).unwrap();

        let cols = map![
            1 => col_info(Long),
            2 => col_info(Long),
            3 => col_info(Long),
            4 => col_info(Long),
            5 => col_info(Long)
        ];
        let got = decode(data, &EvalContext::default(), &cols).unwrap();
        let exp = map![
            1 => Datum::I64(1),
            2 => Datum::Null,
            3 => Datum::Null,
            4 => Datum::I64(2),
            5 => Datum::I64(3)
        ];
        assert_eq!(got, exp);
    }
}
