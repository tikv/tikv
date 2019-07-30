// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Json, Result};
use crate::codec::convert::*;
use crate::codec::data_type::Decimal;
use crate::expr::EvalContext;

impl Json {
    ///  Keep compatible with TiDB's `ConvertJSONToFloat` function.
    pub fn cast_to_real(&self, ctx: &mut EvalContext) -> Result<f64> {
        let d = match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => 0f64,
            Json::Boolean(true) => 1f64,
            Json::I64(d) => d as f64,
            Json::U64(d) => d as f64,
            Json::Double(d) => d,
            Json::String(ref s) => convert_bytes_to_f64(ctx, s.as_bytes())?,
        };
        Ok(d)
    }

    /// Converts a `Json` to a `Decimal`
    #[inline]
    pub fn cast_to_decimal(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        let f = self.cast_to_real(ctx)?;
        Decimal::from_f64(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::{EvalConfig, EvalContext};
    use std::f64;
    use std::sync::Arc;

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
