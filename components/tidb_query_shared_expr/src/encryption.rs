use flate2::read::{ZlibDecoder, ZlibEncoder};
use flate2::Compression;
use std::io::Read;
use tidb_query_datatype::codec::data_type::*;

pub fn compress(input: &[u8]) -> Option<Bytes> {
    use byteorder::{ByteOrder, LittleEndian};
    // according to MySQL doc: Empty strings are stored as empty strings.
    if input.is_empty() {
        return Some(vec![]);
    }
    let mut e = ZlibEncoder::new(input, Compression::default());
    // preferred capacity is input length plus four bytes length header and one extra end "."
    // max capacity is isize::max_value(), or will panic with "capacity overflow"
    let mut vec = Vec::with_capacity((input.len() + 5).min(isize::max_value() as usize));
    vec.resize(4, 0);
    LittleEndian::write_u32(&mut vec, input.len() as u32);
    match e.read_to_end(&mut vec) {
        Ok(_) => {
            // according to MySQL doc: append "." if ends with space
            if vec[vec.len() - 1] == 32 {
                vec.push(b'.');
            }
            Some(vec)
        }
        _ => None,
    }
}

pub fn uncompress(input: &[u8]) -> Option<Bytes> {
    use byteorder::{ByteOrder, LittleEndian};

    if input.is_empty() {
        return Some(Vec::new());
    }
    if input.len() <= 4 {
        return None;
    }

    let len = LittleEndian::read_u32(&input[0..4]) as usize;
    let mut decoder = ZlibDecoder::new(&input[4..]);
    let mut vec = Vec::with_capacity(len);
    // if the length of uncompressed string is greater than the length we read from the first
    //     four bytes, return null and generate a length corrupted warning.
    // if the length of uncompressed string is zero or uncompress fail, return null and generate
    //     a data corrupted warning
    match decoder.read_to_end(&mut vec) {
        Ok(decoded_len) if len >= decoded_len && decoded_len != 0 => Some(vec),
        Ok(decoded_len) if len < decoded_len => None,
        _ => None,
    }
}
