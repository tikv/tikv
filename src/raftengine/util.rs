
use crc::crc32::{self, Digest, Hasher32};

pub fn calc_crc32(buf: &[u8]) -> u32 {
    let mut digest = Digest::new(crc32::IEEE);
    digest.write(buf);
    digest.sum32()
}
