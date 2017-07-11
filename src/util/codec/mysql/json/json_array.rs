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

use super::Json;
use super::super::super::datum::Datum;
use super::super::super::Result;

impl Json {
    // https://dev.mysql.com/doc/refman/5.7/en/json-creation-functions.html#function_json-array
    pub fn to_array(elems: &[Datum]) -> Result<Json> {
        let mut arr = Vec::with_capacity(elems.len());
        for elem in elems {
            arr.push(try!(elem.clone().into_json()));
        }
        Ok(Json::Array(arr))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_json_array() {
        let d = vec![Datum::I64(1),
                     Datum::Bytes(b"sdf".to_vec()),
                     Datum::U64(2),
                     Datum::Json(r#"[3,4]"#.parse().unwrap())];
        let ep_json = r#"[1,"sdf",2,[3,4]]"#.parse().unwrap();
        assert_eq!(Json::to_array(&d).unwrap(), ep_json);

        let d = vec![];
        let ep_json = "[]".parse().unwrap();
        assert_eq!(Json::to_array(&d).unwrap(), ep_json);
    }
}
