#![allow(dead_code)]
mod v_128;
use crate::coprocessor::codec::{table, Datum};
use crate::coprocessor::dag::expr::EvalContext;
use crate::util::collections::HashMap;
use codec::Result;
use tipb::schema::ColumnInfo;

// TODO: 1. compatible with RowColsDict and remain mehtods from codec::table
//       2. more tests and benches
//       3. adapt more methods from codec::table
//       4. codec::table should be the entry point
pub fn encode(datums: Vec<Datum>, col_ids: &[i64], version: u8) -> Result<Vec<u8>> {
    let data = match version {
        0..=127 => table::encode_row(datums, col_ids).expect("TOOD: handle this"),
        128 => v_128::encode(datums, col_ids)?,
        _ => unimplemented!("version not supported"),
    };
    Ok(data)
}

// TODO: data as ByteSlice ?
pub fn decode(
    data: Vec<u8>,
    ctx: &mut EvalContext,
    cols: &HashMap<i64, ColumnInfo>,
) -> Result<HashMap<i64, Datum>> {
    let version = data[0];
    let datums = match version {
        0..=127 => table::decode_row(&mut data.as_slice(), ctx, cols).expect("TODO: handl this"),
        128 => v_128::decode(data, ctx, cols)?,
        _ => unimplemented!("version not supported"),
    };
    Ok(datums)
}

#[cfg(test)]
mod benches {
    use super::{decode, encode};
    use crate::coprocessor::codec::{
        mysql::{duration::NANOS_PER_SEC, Duration as MysqlDuration, Json, Time},
        Datum,
    };
    use crate::coprocessor::dag::expr::EvalContext;
    use crate::test;
    use crate::util::collections::HashMap;
    use cop_datatype::{FieldTypeAccessor, FieldTypeTp, FieldTypeTp::*};
    use std::str::FromStr;
    use tipb::schema::ColumnInfo;

    #[inline]
    fn datums() -> Vec<Datum> {
        vec![
            Datum::I64(127),
            Datum::I64(32767),
            Datum::I64(12),
            Datum::Null,
            Datum::Bytes(b"abc".to_vec()),
            Datum::F64(1.8),
            Datum::Time(Time::parse_utc_datetime("2018-01-19 03:14:07", 0).unwrap()),
            Datum::Dec(1i64.into()),
            Datum::Json(Json::from_str(r#"{"key":"value"}"#).unwrap()),
            Datum::Dur(MysqlDuration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
        ]
    }

    #[inline]
    fn ids() -> Vec<i64> {
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }

    #[inline]
    fn col_info(tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::new();
        col_info.as_mut_accessor().set_tp(tp);
        col_info
    }

    #[inline]
    fn cols() -> HashMap<i64, ColumnInfo> {
        map![
            1 => col_info(Long),
            2 => col_info(Long),
            3 => col_info(Long),
            4 => col_info(Long),
            5 => col_info(String),
            6 => col_info(Double),
            7 => col_info(DateTime),
            8 => col_info(NewDecimal),
            9 => col_info(JSON),
            10 => col_info(Duration)
        ]
    }

    #[inline]
    fn v1_data() -> Vec<u8> {
        // encode(datums(), &ids(), 1).unwrap();
        vec![
            8, 2, 8, 254, 1, 8, 4, 8, 254, 255, 3, 8, 6, 8, 254, 255, 255, 255, 15, 8, 8, 0, 8, 10,
            2, 6, 97, 98, 99, 8, 12, 5, 191, 252, 204, 204, 204, 204, 204, 205, 8, 14, 9, 128, 128,
            128, 184, 184, 198, 185, 207, 25, 8, 16, 6, 1, 0, 129, 8, 18, 10, 1, 1, 0, 0, 0, 28, 0,
            0, 0, 19, 0, 0, 0, 3, 0, 12, 22, 0, 0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 8,
            20, 8, 128, 168, 214, 185, 7,
        ]
    }

    #[inline]
    fn v2_data() -> Vec<u8> {
        // encode(datums(), &ids(), 128).unwrap();
        vec![
            128, 0, 9, 0, 1, 0, 1, 2, 3, 5, 6, 7, 8, 9, 10, 4, 1, 0, 3, 0, 7, 0, 10, 0, 18, 0, 26,
            0, 29, 0, 58, 0, 62, 0, 127, 255, 127, 255, 255, 255, 127, 97, 98, 99, 205, 204, 204,
            204, 204, 204, 252, 63, 0, 0, 0, 135, 51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28,
            0, 0, 0, 19, 0, 0, 0, 3, 0, 12, 22, 0, 0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101,
            0, 202, 154, 59,
        ]
    }

    #[bench]
    fn bench_v1_encode(b: &mut test::Bencher) {
        b.iter(|| {
            encode(datums(), &ids(), 1).unwrap();
        })
    }

    #[bench]
    fn bench_v128_encode(b: &mut test::Bencher) {
        b.iter(|| {
            encode(datums(), &ids(), 128).unwrap();
        })
    }

    #[bench]
    fn bench_v1_decode(b: &mut test::Bencher) {
        b.iter(|| {
            decode(v1_data(), &mut EvalContext::default(), &cols()).unwrap();
        })
    }

    #[bench]
    fn bench_v128_decode(b: &mut test::Bencher) {
        b.iter(|| {
            decode(v2_data(), &mut EvalContext::default(), &cols()).unwrap();
        })
    }

}
