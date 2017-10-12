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

/// Common functions for table encoding and decoding.
// TODO: unify this mod and `coprocessor::codec::table`.

use std::io::Write;

use super::number::{NumberDecoder, NumberEncoder};

const TABLE_PREFIX: &'static [u8] = b"t";
const TABLE_PREFIX_LEN: usize = 1;
const ID_LEN: usize = 8;
const PREFIX_LEN: usize = TABLE_PREFIX_LEN + ID_LEN;

/// Composes table record and index prefix: "t[tableID]".
pub fn gen_table_prefix(table_id: i64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(PREFIX_LEN);
    buf.write_all(TABLE_PREFIX).unwrap();
    buf.encode_i64(table_id).unwrap();
    buf
}

/// Decodes the table ID of the key, if the key is not table key, returns 0.
pub fn decode_table_id(key: &[u8]) -> i64 {
    if key.len() < PREFIX_LEN || !key.starts_with(TABLE_PREFIX) {
        0
    } else {
        let mut key = &key[TABLE_PREFIX_LEN..PREFIX_LEN];
        key.decode_i64().unwrap_or(0)
    }
}

/// Test if the left and right are in the same table.
pub fn is_same_table<'a>(left: &'a [u8], right: &'a [u8]) -> Result<bool, &'a [u8]> {
    if left.len() < PREFIX_LEN || !left.starts_with(TABLE_PREFIX) {
        Err(left)
    } else if right.len() < PREFIX_LEN || !right.starts_with(TABLE_PREFIX) {
        Err(right)
    } else if left[..PREFIX_LEN] == right[..PREFIX_LEN] {
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod test {
    use std::i64;

    use super::*;

    #[test]
    fn test_encode_and_decode_table_perfix() {
        let good = vec![i64::MIN, i64::MAX, -1, 0, 2, 3, 1024];
        for t in good {
            let key = gen_table_prefix(t);
            println!("{}: {:?}", line!(), key);
            assert_eq!(decode_table_id(&key), t);
        }

        assert_eq!(decode_table_id(&[]), 0);
        assert_eq!(decode_table_id(TABLE_PREFIX), 0);
        assert_eq!(decode_table_id(b"t123"), 0);
    }

    #[test]
    fn test_is_same_table() {
        let (mut left, mut right) = (gen_table_prefix(1), gen_table_prefix(1));
        left.extend_from_slice(b"foo");
        right.extend_from_slice(b"bar");
        assert_eq!(is_same_table(&left, &right), Ok(true));

        assert_eq!(
            is_same_table(&gen_table_prefix(1), &gen_table_prefix(1)),
            Ok(true)
        );

        assert_eq!(
            is_same_table(&gen_table_prefix(1), &gen_table_prefix(2)),
            Ok(false)
        );

        let vacant = vec![];
        let empty = vacant.as_slice();
        assert_eq!(is_same_table(empty, &gen_table_prefix(1)), Err(empty));
        assert_eq!(is_same_table(&gen_table_prefix(1), empty), Err(empty));
        assert_eq!(is_same_table(empty, empty), Err(empty));
    }
}
