// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use cop_datatype::FieldTypeTp;

use super::{Json, Result};
use crate::coprocessor::codec::convert::{
    self, convert_bytes_to_int, convert_bytes_to_uint, convert_float_to_int, convert_float_to_uint,
    convert_int_to_uint, convert_uint_to_int,
};
use crate::coprocessor::dag::expr::EvalContext;

impl Json {
    pub fn cast_to_int(&self, ctx: &mut EvalContext) -> Result<i64> {
        // Casts json to int has different behavior in TiDB/MySQL when the json
        // value is a `Json::Double` and we will keep compitable with TiDB
        // **Note**: select cast(cast('4.5' as json) as signed)
        // TiDB:  5
        // MySQL: 4
        match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => Ok(0),
            Json::Boolean(true) => Ok(1),
            Json::I64(d) => Ok(d),
            Json::U64(d) => convert_uint_to_int(ctx, d, FieldTypeTp::LongLong),
            Json::Double(d) => convert_float_to_int(ctx, d, FieldTypeTp::LongLong),
            Json::String(ref s) => convert_bytes_to_int(ctx, s.as_bytes(), FieldTypeTp::LongLong),
        }
    }

    pub fn cast_to_uint(&self, ctx: &mut EvalContext) -> Result<u64> {
        match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => Ok(0u64),
            Json::Boolean(true) => Ok(1u64),
            Json::I64(d) => convert_int_to_uint(ctx, d, FieldTypeTp::LongLong),
            Json::U64(d) => Ok(d),
            Json::Double(d) => convert_float_to_uint(ctx, d, FieldTypeTp::LongLong),
            Json::String(ref s) => convert_bytes_to_uint(ctx, s.as_bytes(), FieldTypeTp::LongLong),
        }
    }

    ///  Keep compatible with TiDB's `ConvertJSONToFloat` function.
    pub fn cast_to_real(&self, ctx: &mut EvalContext) -> Result<f64> {
        let d = match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => 0f64,
            Json::Boolean(true) => 1f64,
            Json::I64(d) => d as f64,
            Json::U64(d) => d as f64,
            Json::Double(d) => d,
            Json::String(ref s) => convert::bytes_to_f64(ctx, s.as_bytes())?,
        };
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
    use std::f64;
    use std::sync::Arc;

    #[test]
    fn test_cast_to_int() {
        let test_cases = vec![
            ("{}", 0),
            ("[]", 0),
            ("3", 3),
            ("-3", -3),
            ("4.1", 4),
            ("4.5", 5),
            ("true", 1),
            ("false", 0),
            ("null", 0),
            (r#""hello""#, 0),
            (r#""1234""#, 1234),
        ];

        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_int(&mut ctx).unwrap();
            assert_eq!(get, exp, "json.as_i64 get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_cast_to_uint() {
        let test_cases = vec![
            ("{}", 0u64),
            ("[]", 0u64),
            ("3", 3u64),
            ("4.1", 4u64),
            ("4.5", 5u64),
            ("true", 1u64),
            ("false", 0u64),
            ("null", 0u64),
            (r#""hello""#, 0u64),
            (r#""1234""#, 1234u64),
        ];

        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_uint(&mut ctx).unwrap();
            assert_eq!(get, exp, "json.as_u64 get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_cast_to_real() {
        let test_cases = vec![
            ("{}", 0f64),
            ("[]", 0f64),
            ("3", 3f64),
            ("-3", -3f64),
            ("4.5", 4.5),
            ("true", 1f64),
            ("false", 0f64),
            ("null", 0f64),
            (r#""hello""#, 0f64),
            (r#""1234""#, 1234f64),
        ];
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_real(&mut ctx).unwrap();
            assert!(
                (get - exp).abs() < f64::EPSILON,
                "json.as_f64 get: {}, exp: {}",
                get,
                exp
            );
        }
    }
}
