// Copyright 2017 PingCAP, Inc.
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

use super::Json;
use super::Result;

impl Json {
    pub fn cast_to_int(self) -> Result<i64> {
        match self {
            Json::Object(_) |
            Json::Array(_) |
            Json::None |
            Json::Boolean(false) => Ok(0),
            Json::Boolean(true) => Ok(1),
            Json::I64(d) => Ok(d),
            Json::U64(d) => Ok(d as i64),
            Json::Double(d) => Ok(d as i64),
            Json::String(s) => {
                let ret = s.parse::<i64>().unwrap_or(0);
                Ok(ret)
            }
        }
    }

    pub fn cast_to_real(self) -> Result<f64> {
        match self {
            Json::Object(_) |
            Json::Array(_) |
            Json::None |
            Json::Boolean(false) => Ok(0f64),
            Json::Boolean(true) => Ok(1f64),
            Json::I64(d) => Ok(d as f64),
            Json::U64(d) => Ok(d as f64),
            Json::Double(d) => Ok(d),
            Json::String(s) => {
                let ret = s.parse::<f64>().unwrap_or(0f64);
                Ok(ret)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::f64;

    use super::*;

    #[test]
    fn test_cast_to_int() {
        let test_cases = vec![("{}", 0),
                              ("[]", 0),
                              ("3", 3),
                              ("-3", -3),
                              ("4.5", 4),
                              ("true", 1),
                              ("false", 0),
                              ("null", 0),
                              (r#""hello""#, 0),
                              (r#""1234""#, 1234)];

        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_int().unwrap();
            assert_eq!(get, exp, "cast_to_int get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_cast_to_f64() {
        let test_cases = vec![("{}", 0f64),
                              ("[]", 0f64),
                              ("3", 3f64),
                              ("-3", -3f64),
                              ("4.5", 4.5),
                              ("true", 1f64),
                              ("false", 0f64),
                              ("null", 0f64),
                              (r#""hello""#, 0f64),
                              (r#""1234""#, 1234f64)];

        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.cast_to_real().unwrap();
            assert!((get - exp).abs() < f64::EPSILON,
                    "cast_to_int get: {}, exp: {}",
                    get,
                    exp);
        }
    }
}
