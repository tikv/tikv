use std::io::{Cursor, Write};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use super::{Result, MvccErrorKind};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
fn encode_bytes(key: &[u8]) -> Vec<u8> {
	let cap = key.len() / (ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1);
	let mut encoded = Vec::<u8>::with_capacity(cap);
    let len = key.len();
    let mut index = 0;
    while index <= len {
        let remain = len - index;
        let mut pad: usize = 0;
        if remain > ENC_GROUP_SIZE {
            encoded.write(&key[index..index + ENC_GROUP_SIZE]).unwrap();
        } else {
            pad = ENC_GROUP_SIZE - remain;
            encoded.write(&key[index..]).unwrap();
            encoded.write(&ENC_PADDING[..pad]).unwrap();
        }
        encoded.push(ENC_MARKER - (pad as u8));
        index += ENC_GROUP_SIZE;
    }
    encoded
}

fn decode_bytes(data: &[u8]) -> Result<(Vec<u8>, usize)> {
	let mut key = Vec::<u8>::with_capacity(data.len());
    let mut read: usize = 0;
    for chunk in data.chunks(ENC_GROUP_SIZE + 1) {
        if chunk.len() != ENC_GROUP_SIZE + 1 {
            return MvccErrorKind::KeyLength.as_result();
        }
        read += ENC_GROUP_SIZE + 1;
        let marker = chunk.last().unwrap();
        let pad = (ENC_MARKER - marker) as usize;
        key.write(&chunk[..ENC_GROUP_SIZE - pad]).unwrap();
        if pad > 0 {
            break;
        }
    }
    Ok((key, read))
}

pub fn encode_key(key: &[u8], version: u64) -> Vec<u8> {
    let mut v = encode_bytes(key);
    v.write_u64::<BigEndian>(version).unwrap();
    v
}

#[allow(dead_code)]
pub fn decode_key(data: &[u8]) -> Result<(Vec<u8>, u64)> {
    let (key, read_size) = try!(decode_bytes(data));
    let mut rdr = Cursor::new(&data[read_size..]);
    match rdr.read_u64::<BigEndian>() {
        Ok(ver) => Ok((key, ver)),
        Err(..) => MvccErrorKind::KeyVersion.as_result(),
    }
}

#[cfg(test)]
mod tests {
    use super::{encode_key, encode_bytes, decode_key, decode_bytes};

    #[test]
    fn test_encode_bytes() {
        let pairs: Vec<(Vec<u8>,Vec<u8>)> = vec![
            (vec![], vec![0, 0, 0, 0, 0, 0, 0, 0, 247]),
            (vec![1, 2, 3], vec![1, 2, 3, 0, 0, 0, 0, 0, 250]),
            (vec![1, 2, 3, 0], vec![1, 2, 3, 0, 0, 0, 0, 0, 251]),
            (vec![1, 2, 3, 4, 5, 6, 7, 8], vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]),
        ];

        for (x, y) in pairs {
            assert_eq!(encode_bytes(&x), y);
            let (key, size) = decode_bytes(&y).unwrap();
            assert_eq!(key, x);
            assert_eq!(size, y.len());
        }
    }

    #[test]
    fn test_encode_key() {
        let pairs: Vec<(&'static [u8], u64)> = vec![
          (b"abc", 0),
          (b"\x00\x00", 100),
        ];

        for (x, y) in pairs {
            let data = encode_key(x, y);
            let (k, ver) = decode_key(&data).unwrap();
            assert_eq!(k, x);
            assert_eq!(ver, y);
        }
    }

    use test::Bencher;

    #[bench]
    fn bench_encode(b: &mut Bencher) {
        let key = [b'x'; 20];
        b.iter(|| encode_bytes(&key));
    }

    #[bench]
    fn bench_decode(b: &mut Bencher) {
        let key = [b'x'; 20];
        let encoded = encode_bytes(&key);
        b.iter(|| decode_bytes(&encoded));
    }
}
