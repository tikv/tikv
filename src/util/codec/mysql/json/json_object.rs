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

// FIXME: remove following later
#![allow(dead_code)]

use std::collections::BTreeMap;

use super::Json;
use super::super::super::datum::Datum;
use super::super::super::{Result, Error};

impl Json {
    // https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-object
    pub fn to_object(kvs: &[Datum]) -> Result<Json> {
        let len = kvs.len();
        if len & 1 == 1 {
            return Err(Error::Other(box_err!("Incorrect parameter count in the call to native \
                                              function 'JSON_OBJECT'")));
        }
        let mut map = BTreeMap::new();
        let mut idx = 0;
        while idx < len {
            let key = match kvs[idx] {
                Datum::Null => {
                    return Err(invalid_type!("JSON documents may not contain NULL member names"));
                }
                _ => try!(kvs[idx].to_string()),
            };
            let val = try!(kvs[idx + 1].clone().into_json());
            map.insert(key, val);
            idx += 2;
        }
        Ok(Json::Object(map))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_object() {
        let d = vec![Datum::I64(1)];
        assert!(Json::to_object(&d).is_err());

        let d = vec![Datum::I64(1), Datum::Bytes(b"sdf".to_vec()), Datum::Null, Datum::U64(2)];
        assert!(Json::to_object(&d).is_err());

        let d = vec![Datum::I64(1),
                     Datum::Bytes(b"sdf".to_vec()),
                     Datum::Bytes(b"asd".to_vec()),
                     Datum::Bytes(b"qwe".to_vec()),
                     Datum::I64(2),
                     Datum::Json(r#"{"3":4}"#.parse().unwrap())];
        let ep_json = r#"{"1":"sdf","2":{"3":4},"asd":"qwe"}"#.parse().unwrap();
        assert_eq!(Json::to_object(&d).unwrap(), ep_json);

        let d = vec![];
        let ep_json = "{}".parse().unwrap();
        assert_eq!(Json::to_object(&d).unwrap(), ep_json);
    }
}
