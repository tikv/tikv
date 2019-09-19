use crate::codec::{
    mysql::{self, DecimalDecoder, JsonDecoder, Time},
    Datum, Result,
};
use crate::expr::EvalContext;
use codec::number::NumberCodec;
use codec::prelude::*;
use tidb_query_datatype::FieldTypeAccessor;
use tikv_util::collections::HashMap;
use tipb::ColumnInfo;

pub trait RowDecoder: NumberDecoder + IntDecoder {
    // | version | flag | number_of_not_null_values | number_of_null_values | not_null_value_ids | null_value_ids | value_offsets | values
    fn decode_row(
        &mut self,
        ctx: &EvalContext,
        cols: &HashMap<i64, ColumnInfo>,
    ) -> Result<HashMap<i64, Datum>> {
        let mut row = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
        if self.read_u8()? != super::CODEC_VERSION {
            panic!("version not matched");
        }
        let is_large_id = self.read_u8()? == 1;

        // read ids count
        let not_null_cnt = self.read_u16_le()? as usize;
        let null_cnt = self.read_u16_le()? as usize;

        let ids: Vec<i64>;
        let offsets: Vec<usize>;
        if is_large_id {
            ids = self.read_ints_le(not_null_cnt + null_cnt, u32::to_i64)?;
            offsets = self.read_ints_le(not_null_cnt, u32::to_usize)?;
        } else {
            ids = self.read_ints_le(not_null_cnt + null_cnt, u8::to_i64)?;
            offsets = self.read_ints_le(not_null_cnt, u16::to_usize)?;
        };

        let values = self.bytes();
        let (not_null_ids, null_ids) = ids.split_at(not_null_cnt);
        for (id, tp) in cols {
            if let Ok(idx) = not_null_ids.binary_search(id) {
                let offset = offsets
                    .get(idx)
                    .expect("should always have value in offsets");
                let start = if idx > 0 { offsets[idx - 1] } else { 0usize };
                row.insert(*id, (&values[start..*offset]).decode_datum(ctx, tp)?);
            } else if null_ids.binary_search(id).is_ok() {
                row.insert(*id, Datum::Null);
            } else {
                row.insert(*id, tp.get_default_val().decode_datum(ctx, tp)?);
            }
        }
        Ok(row)
    }
}

impl<T: BufferReader> RowDecoder for T {}

trait DatumDecoder: NumberDecoder + DecimalDecoder + JsonDecoder {
    #[inline]
    fn decode_datum(&mut self, ctx: &EvalContext, ft: &dyn FieldTypeAccessor) -> Result<Datum> {
        use core::convert::TryInto;
        use tidb_query_datatype::FieldTypeTp::*;
        match ft.tp() {
            Tiny | Short | Long | LongLong | Int24 | Year => {
                if ft.is_unsigned() {
                    Ok(Datum::U64(self.decode_u64()))
                } else {
                    Ok(Datum::I64(self.decode_i64()))
                }
            }
            VarChar | VarString | String | TinyBlob | MediumBlob | LongBlob | Blob => {
                Ok(Datum::Bytes(self.bytes().to_vec()))
            }
            Double | Float => Ok(Datum::F64(f64::from_bits(self.decode_u64()))),
            Date | DateTime | Timestamp => Time::from_packed_u64(
                self.decode_u64(),
                ft.tp().try_into().unwrap(),
                ft.decimal() as i8,
                &ctx.cfg.tz,
            )
            .map(Datum::Time),
            NewDecimal => self.decode_decimal().map(Datum::Dec),
            JSON => self.decode_json().map(Datum::Json),
            Duration => {
                mysql::Duration::from_nanos(self.decode_i64(), ft.decimal() as i8).map(Datum::Dur)
            }
            tp => return Err(invalid_type!("unsupported data type `{:?}`", tp)),
        }
    }

    #[inline]
    fn decode_u64(&mut self) -> u64 {
        let data = self.bytes();
        match data.len() {
            1 => u64::from(data[0]),
            2 => u64::from(NumberCodec::decode_u16_le(data)),
            4 => u64::from(NumberCodec::decode_u32_le(data)),
            _ => NumberCodec::decode_u64_le(data),
        }
    }

    #[inline]
    fn decode_i64(&mut self) -> i64 {
        let data = self.bytes();
        match data.len() {
            1 => i64::from(data[0]),
            2 => i64::from(NumberCodec::decode_i16_le(data)),
            4 => i64::from(NumberCodec::decode_i32_le(data)),
            _ => NumberCodec::decode_i64_le(data),
        }
    }
}

impl<T: BufferReader> DatumDecoder for T {}

// convert number to usize or i64
// ids need to convert i64 since the column id is i64
// offsets need to convert of usize, since it will used as range index later
pub trait Converter {
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

pub trait IntDecoder: BufferReader {
    #[inline]
    fn read_ints_le<T, R, F>(&mut self, len: usize, f: F) -> codec::Result<Vec<R>>
    where
        T: Converter,
        F: FnMut(&T) -> R,
    {
        use std::{mem, slice};
        let bytes_len = mem::size_of::<T>() * len;
        let bytes = self.read_bytes(bytes_len)?.as_ptr() as *const T;
        let buf = unsafe { slice::from_raw_parts(bytes, len) };
        let res = buf.iter().map(f).collect::<Vec<_>>();
        Ok(res)
    }
}

impl<T: BufferReader> IntDecoder for T {}

#[cfg(test)]
mod tests {

    use super::super::encode;
    use super::{Converter, Datum, IntDecoder, RowDecoder};
    use crate::codec::mysql::{self, duration::NANOS_PER_SEC, Json, Time};
    use crate::expr::EvalContext;
    use std::str::FromStr;
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp, FieldTypeTp::*};
    use tikv_util::map;
    use tipb::ColumnInfo;

    #[test]
    fn test_read_ints() {
        let data: Vec<u8> = vec![1, 3, 6, 7, 8, 9, 12, 13];
        {
            let mut buf = data.as_slice();
            let ids: Vec<usize> = buf.read_ints_le(3, u8::to_usize).unwrap();
            assert_eq!(ids, vec![1, 3, 6]);
        }
        {
            let mut buf = data.as_slice();
            let ids: Vec<usize> = buf.read_ints_le(2, u16::to_usize).unwrap();
            assert_eq!(ids, vec![769, 1798]);
        }
        i64::from(1u8);
    }

    fn col_info(tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::default();
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

        let v = data
            .as_slice()
            .decode_row(
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
            16 => Datum::Dur(mysql::Duration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
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
        let v = data
            .as_slice()
            .decode_row(
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
        let got = data
            .as_slice()
            .decode_row(&EvalContext::default(), &cols)
            .unwrap();
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
