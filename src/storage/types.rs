// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

//! Core data types.
use std::u64;

use crate::storage::mvcc::{Lock, Write};
pub use cop_dag::storage::{Value, KvPair, Key};

/// `MvccInfo` stores all mvcc information of given key.
/// Used by `MvccGetByKey` and `MvccGetByStartTs`.
#[derive(Debug, Default)]
pub struct MvccInfo {
    pub lock: Option<Lock>,
    /// commit_ts and write
    pub writes: Vec<(u64, Write)>,
    /// start_ts and value
    pub values: Vec<(u64, Value)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_ts() {
        let k = b"k";
        let ts = 123;
        assert!(Key::split_on_ts_for(k).is_err());
        let enc = Key::from_encoded_slice(k).append_ts(ts);
        let res = Key::split_on_ts_for(enc.as_encoded()).unwrap();
        assert_eq!(res, (k.as_ref(), ts));
    }

    #[test]
    fn test_is_user_key_eq() {
        // make a short name to keep format for the test.
        fn eq(a: &[u8], b: &[u8]) -> bool {
            Key::is_user_key_eq(a, b)
        }
        assert_eq!(false, eq(b"", b""));
        assert_eq!(false, eq(b"12345", b""));
        assert_eq!(true, eq(b"12345678", b""));
        assert_eq!(true, eq(b"x12345678", b"x"));
        assert_eq!(false, eq(b"x12345", b"x"));
        // user key len == 3
        assert_eq!(true, eq(b"xyz12345678", b"xyz"));
        assert_eq!(true, eq(b"xyz________", b"xyz"));
        assert_eq!(false, eq(b"xyy12345678", b"xyz"));
        assert_eq!(false, eq(b"yyz12345678", b"xyz"));
        assert_eq!(false, eq(b"xyz12345678", b"xy"));
        assert_eq!(false, eq(b"xyy12345678", b"xy"));
        assert_eq!(false, eq(b"yyz12345678", b"xy"));
        // user key len == 7
        assert_eq!(true, eq(b"abcdefg12345678", b"abcdefg"));
        assert_eq!(true, eq(b"abcdefgzzzzzzzz", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefg12345678", b"abcdef"));
        assert_eq!(false, eq(b"abcdefg12345678", b"bcdefg"));
        assert_eq!(false, eq(b"abcdefv12345678", b"abcdefg"));
        assert_eq!(false, eq(b"vbcdefg12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abccefg12345678", b"abcdefg"));
        // user key len == 8
        assert_eq!(true, eq(b"abcdefgh12345678", b"abcdefgh"));
        assert_eq!(true, eq(b"abcdefghyyyyyyyy", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"abcdefg"));
        assert_eq!(false, eq(b"abcdefgh12345678", b"bcdefgh"));
        assert_eq!(false, eq(b"abcdefgz12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"zbcdefgh12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcddfgh12345678", b"abcdefgh"));
        // user key len == 9
        assert_eq!(true, eq(b"abcdefghi12345678", b"abcdefghi"));
        assert_eq!(true, eq(b"abcdefghixxxxxxxx", b"abcdefghi"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"abcdefgh"));
        assert_eq!(false, eq(b"abcdefghi12345678", b"bcdefghi"));
        assert_eq!(false, eq(b"abcdefghy12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"ybcdefghi12345678", b"abcdefghi"));
        assert_eq!(false, eq(b"abcddfghi12345678", b"abcdefghi"));
        // user key len == 11
        assert_eq!(true, eq(b"abcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(true, eq(b"abcdefghijkabcdefgh", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"abcdefghij"));
        assert_eq!(false, eq(b"abcdefghijk87654321", b"bcdefghijk"));
        assert_eq!(false, eq(b"abcdefghijx87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"xbcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abxdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"axcdefghijk87654321", b"abcdefghijk"));
        assert_eq!(false, eq(b"abcdeffhijk87654321", b"abcdefghijk"));
    }
}
