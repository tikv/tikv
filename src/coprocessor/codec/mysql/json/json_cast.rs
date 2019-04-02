// Copyright 2017 TiKV Project Authors.
use super::{Json, Result};
use crate::coprocessor::codec::convert;
use crate::coprocessor::dag::expr::EvalContext;

impl Json {
    pub fn cast_to_int(&self) -> i64 {
        match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => 0,
            Json::Boolean(true) => 1,
            Json::I64(d) => d,
            Json::U64(d) => d as i64,
            Json::Double(d) => d as i64,
            Json::String(ref s) => s.parse::<i64>().unwrap_or(0),
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
            ("4.5", 4),
            ("true", 1),
            ("false", 0),
            ("null", 0),
            (r#""hello""#, 0),
            (r#""1234""#, 1234),
        ];

        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_int();
            assert_eq!(get, exp, "cast_to_int get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_cast_to_f64() {
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
                "cast_to_int get: {}, exp: {}",
                get,
                exp
            );
        }
    }
}
