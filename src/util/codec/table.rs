// Copyright 2016 PingCAP, Inc.
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


use std::io::Write;

use super::{number, Result, Error, Datum, datum};

const ID_LEN: usize = 8;
const PREFIX_LEN: usize = 1 + ID_LEN /*table_id*/ + 2;
const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
const TABLE_PREFIX: &'static [u8] = b"t";
const RECORD_PREFIX_SEP: &'static [u8] = b"_r";
const INDEX_PREFIX_SEP: &'static [u8] = b"_i";

fn append_table_record_prefix(mut buf: &mut [u8], table_id: i64) -> Result<()> {
    try!(buf.write_all(TABLE_PREFIX));
    try!(number::encode_i64(buf, table_id));
    try!((&mut buf[8..]).write_all(RECORD_PREFIX_SEP));
    Ok(())
}

fn append_table_index_prefix(mut buf: &mut [u8], table_id: i64) -> Result<()> {
    try!(buf.write_all(TABLE_PREFIX));
    try!(number::encode_i64(buf, table_id));
    try!((&mut buf[8..]).write_all(INDEX_PREFIX_SEP));
    Ok(())
}

/// `encode_row_key` encodes the table id and record handle into a byte array.
pub fn encode_row_key(table_id: i64, encoded_handle: &[u8]) -> Result<Vec<u8>> {
    let mut key = vec![0; RECORD_ROW_KEY_LEN];
    try!(append_table_record_prefix(&mut key, table_id));
    try!((&mut key[PREFIX_LEN..]).write_all(encoded_handle));
    Ok(key)
}

/// `encode_column_key` encodes the table id, row handle and column id into a byte array.
pub fn encode_column_key(table_id: i64, handle: i64, column_id: i64) -> Result<Vec<u8>> {
    let mut key = vec![0; RECORD_ROW_KEY_LEN + ID_LEN];
    try!(append_table_record_prefix(&mut key, table_id));
    try!(number::encode_i64(&mut key[PREFIX_LEN..], handle));
    try!(number::encode_i64(&mut key[RECORD_ROW_KEY_LEN..], column_id));
    Ok(key)
}

/// `decode_handle` decodes the key and gets the handle.
pub fn decode_handle(encoded: &[u8]) -> Result<i64> {
    if !encoded.starts_with(TABLE_PREFIX) {
        return Err(Error::InvalidDataType(format!("record key expected, but got {:?}", encoded)));
    }

    let mut remaining = &encoded[TABLE_PREFIX.len()..];
    try!(number::decode_i64(remaining));

    if !remaining[ID_LEN..].starts_with(RECORD_PREFIX_SEP) {
        return Err(Error::InvalidDataType(format!("record key expected, but got {:?}", encoded)));
    }

    remaining = &remaining[ID_LEN + RECORD_PREFIX_SEP.len()..];
    number::decode_i64(remaining)
}

/// `encode_index_seek_key` encodes an index value to byte array.
pub fn encode_index_seek_key(table_id: i64, idx_id: i64, encoded: &[u8]) -> Result<Vec<u8>> {
    let mut key = vec![0; PREFIX_LEN + ID_LEN + encoded.len()];
    try!(append_table_index_prefix(&mut key, table_id));
    try!(number::encode_i64(&mut key[PREFIX_LEN..], idx_id));
    try!((&mut key[PREFIX_LEN + ID_LEN..]).write_all(encoded));
    Ok(key)
}

// `decode_index_key` decodes datums from an index key.
pub fn decode_index_key(encoded: &[u8]) -> Result<Vec<Datum>> {
    datum::decode(&encoded[PREFIX_LEN + ID_LEN..]).map(|o| o.0)
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::{number, datum};
    use util::codec::datum::Datum;
    use std::i64;

    #[test]
    fn test_row_key_codec() {
        let tests = vec![i64::MIN, i64::MAX, -1, 0, 2, 3, 1024];
        for &t in &tests {
            let mut buf = vec![0; 8];
            number::encode_i64(&mut buf, t).unwrap();
            let k = encode_row_key(1, &buf).unwrap();
            assert_eq!(t, decode_handle(&k).unwrap());
        }
    }

    #[test]
    fn test_index_key_codec() {
        let tests = vec![Datum::U64(1), Datum::Bytes(b"123".to_vec()), Datum::I64(-1)];
        let mut buf = vec![0; datum::approximate_size(&tests, true)];
        let written = datum::encode_key(&mut buf, &tests).unwrap();
        let encoded = encode_index_seek_key(1, 2, &buf[..written]).unwrap();
        assert_eq!(tests, decode_index_key(&encoded).unwrap());
    }
}
