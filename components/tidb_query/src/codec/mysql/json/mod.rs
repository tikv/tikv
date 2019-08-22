// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// FIXME(shirly): remove following later
#![allow(dead_code)]

mod binary;
mod comparison;
mod path_expr;
mod serde;
// json functions
mod json_extract;
mod json_merge;
mod json_modify;
mod json_remove;
mod json_type;
mod json_unquote;

pub use self::binary::JsonEncoder;
pub use self::json_modify::ModifyType;
pub use self::path_expr::{parse_json_path_expr, PathExpression};

use std::collections::BTreeMap;
use std::str::FromStr;

use tikv_util::is_even;

use super::super::datum::Datum;
use super::super::{Error, Result};
use crate::codec::convert::ConvertTo;
use crate::codec::data_type::Decimal;
use crate::expr::EvalContext;

const ERR_CONVERT_FAILED: &str = "Can not covert from ";

/// Json implements type json used in tikv, it specifies the following
/// implementations:
/// 1. Serialize `json` values into binary representation, and reading values
///  back from the binary representation.
/// 2. Serialize `json` values into readable string representation, and reading
/// values back from string representation.
/// 3. sql functions like `JSON_TYPE`, etc
#[derive(Clone, Debug)]
pub enum Json {
    Object(BTreeMap<String, Json>),
    Array(Vec<Json>),
    I64(i64),
    U64(u64),
    Double(f64),
    String(String),
    Boolean(bool),
    None,
}

// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-array
pub fn json_array(elems: Vec<Datum>) -> Result<Json> {
    let mut a = Vec::with_capacity(elems.len());
    for elem in elems {
        a.push(elem.into_json()?);
    }
    Ok(Json::Array(a))
}

// https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-object
pub fn json_object(kvs: Vec<Datum>) -> Result<Json> {
    let len = kvs.len();
    if !is_even(len) {
        return Err(Error::Other(box_err!(
            "Incorrect parameter count in the call to native \
             function 'JSON_OBJECT'"
        )));
    }
    let mut map = BTreeMap::new();
    let mut key = None;
    for elem in kvs {
        if key.is_none() {
            // take elem as key
            if elem == Datum::Null {
                return Err(invalid_type!(
                    "JSON documents may not contain NULL member names"
                ));
            }
            key = Some(elem.into_string()?);
        } else {
            // take elem as value
            let val = elem.into_json()?;
            map.insert(key.take().unwrap(), val);
        }
    }
    Ok(Json::Object(map))
}

impl Json {
    /// Port from TiDB's types.ConvertJSONToInt
    fn as_int_with_ctx(&self, ctx: &mut EvalContext, is_res_unsigned: bool) -> Result<i64> {
        let d = match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => 0,
            Json::Boolean(true) => 1,
            Json::I64(d) => d,
            Json::U64(d) => d as i64,
            Json::Double(d) => {
                if is_res_unsigned {
                    let r: u64 = d.convert(ctx)?;
                    r as i64
                } else {
                    d.convert(ctx)?
                }
            }
            Json::String(ref s) => {
                let bs = s.as_bytes();
                if is_res_unsigned {
                    let r: u64 = bs.convert(ctx)?;
                    r as i64
                } else {
                    bs.convert(ctx)?
                }
            }
        };
        Ok(d)
    }
}

impl ConvertTo<i64> for Json {
    /// Port from TiDB's types.ConvertJSONToInt
    fn convert(&self, ctx: &mut EvalContext) -> Result<i64> {
        self.as_int_with_ctx(ctx, false)
    }
}

impl ConvertTo<u64> for Json {
    /// Port from TiDB's types.ConvertJSONToInt
    fn convert(&self, ctx: &mut EvalContext) -> Result<u64> {
        self.as_int_with_ctx(ctx, true).map(|x| x as u64)
    }
}

impl ConvertTo<f64> for Json {
    /// Port from TiDB's types.ConvertJSONToFloat
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        let d = match *self {
            Json::Object(_) | Json::Array(_) | Json::None | Json::Boolean(false) => 0f64,
            Json::Boolean(true) => 1f64,
            Json::I64(d) => d as f64,
            Json::U64(d) => d as f64,
            Json::Double(d) => d,
            Json::String(ref s) => s.as_str().convert(ctx)?,
        };
        Ok(d)
    }
}

impl ConvertTo<Decimal> for Json {
    #[inline]
    /// Port from TiDB's types.ConvertJSONToDecimal
    fn convert(&self, ctx: &mut EvalContext) -> Result<Decimal> {
        match self {
            Json::String(s) => {
                match Decimal::from_str(s.as_str()) {
                    Ok(d) => Ok(d),
                    Err(e) => {
                        ctx.handle_truncate_err(e)?;
                        // TODO, if TiDB's MyDecimal::FromString return err,
                        //  it may has res. However, if TiKV's Decimal::from_str
                        //  return err, it has no res, so I return zero here,
                        //  but it may different from TiDB's MyDecimal::FromString
                        Ok(Decimal::zero())
                    }
                }
            }
            _ => {
                let r: f64 = self.convert(ctx)?;
                Decimal::from_f64(r)
            }
        }
    }
}

impl crate::codec::data_type::AsMySQLBool for Json {
    #[inline]
    fn as_mysql_bool(&self, _context: &mut crate::expr::EvalContext) -> crate::Result<bool> {
        // TODO: This logic is not correct. See pingcap/tidb#9593
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::expr::{EvalConfig, EvalContext};

    #[test]
    fn test_json_array() {
        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"sdf".to_vec()),
                    Datum::U64(2),
                    Datum::Json(r#"[3,4]"#.parse().unwrap()),
                ],
                r#"[1,"sdf",2,[3,4]]"#.parse().unwrap(),
            ),
            (vec![], "[]".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_array(d).unwrap(), ep_json);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            vec![Datum::I64(1)],
            vec![
                Datum::I64(1),
                Datum::Bytes(b"sdf".to_vec()),
                Datum::Null,
                Datum::U64(2),
            ],
        ];
        for d in cases {
            assert!(json_object(d).is_err());
        }

        let cases = vec![
            (
                vec![
                    Datum::I64(1),
                    Datum::Bytes(b"sdf".to_vec()),
                    Datum::Bytes(b"asd".to_vec()),
                    Datum::Bytes(b"qwe".to_vec()),
                    Datum::I64(2),
                    Datum::Json(r#"{"3":4}"#.parse().unwrap()),
                ],
                r#"{"1":"sdf","2":{"3":4},"asd":"qwe"}"#.parse().unwrap(),
            ),
            (vec![], "{}".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_object(d).unwrap(), ep_json);
        }
    }

    // TODO, add more test case
    #[test]
    fn test_as_int_with_ctx() {
        let test_cases = vec![
            ("{}", 0),
            ("[]", 0),
            ("3", 3),
            ("-3", -3),
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
            let get: i64 = json.as_int_with_ctx(&mut ctx, false).unwrap();
            assert_eq!(get, exp, "json.as_f64 get: {}, exp: {}", get, exp);
        }
    }

    // TODO, add more test case
    #[test]
    fn test_as_f64_with_ctx() {
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
            let get: f64 = json.convert(&mut ctx).unwrap();
            assert!(
                (get - exp).abs() < std::f64::EPSILON,
                "json.as_f64 get: {}, exp: {}",
                get,
                exp
            );
        }
    }
}
