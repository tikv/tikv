// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::Write;

use crate::{Error, Result};
use byteorder::{BigEndian, ByteOrder};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Version {
    V1 = 1,
}

impl Version {
    fn from(input: u8) -> Result<Version> {
        if input == 1 {
            Ok(Version::V1)
        } else {
            Err(Error::Other(format!("unknown version {:x}", input).into()))
        }
    }
}

/// Header of encrypted file.
///
/// ```ignore
///  0 1 2 3 4 5 6 7 0 1 2 3 4 5 6 7 0 1 2
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | |     |       |              |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///  ^   ^      ^         ^           ^
///  |   |      |         |           | Serialized content (variable size)
///  |   |      |         | Content size (8 bytes)
///  |   |      | Crc32  (4 bytes)
///  |   | Reserved  (3 bytes)
///  | Version (1 bytes)
/// ```
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Header {
    version: Version,
    crc32: u32,
    size: u64,
}

impl Header {
    // Version (1 bytes) | Reserved  (3 bytes)
    // Crc32  (4 bytes)
    // Content size (8 bytes)
    const SIZE: usize = 1 + 3 + 4 + 8;

    pub fn new(content: &[u8]) -> Header {
        let size = content.len() as u64;
        let mut digest = crc32fast::Hasher::new();
        digest.update(content);
        let crc32 = digest.finalize();
        Header {
            version: Version::V1,
            crc32,
            size,
        }
    }

    pub fn parse(buf: &[u8]) -> Result<(Header, &[u8])> {
        if buf.len() < Header::SIZE {
            return Err(Error::Other(
                format!(
                    "file corrupted! header size mismatch {} != {}",
                    Header::SIZE,
                    buf.len(),
                )
                .into(),
            ));
        }
        // Version (1 bytes) | Reserved  (3 bytes)
        let version = Version::from(buf[0])?;
        // Crc32  (4 bytes)
        let crc32 = BigEndian::read_u32(&buf[4..8]);
        // Content size (8 bytes)
        let size = BigEndian::read_u64(&buf[8..Header::SIZE]);
        let content = &buf[Header::SIZE..];
        let header = Header {
            version,
            crc32,
            size,
        };
        Ok((header, content))
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = [0; Header::SIZE];

        // Version (1 bytes) | Reserved  (3 bytes)
        (&mut buf[0..4])
            .write_all(&[self.version as u8, 0, 0, 0])
            .unwrap();
        // Crc32  (4 bytes)
        BigEndian::write_u32(&mut buf[4..8], self.crc32);
        // Content size (8 bytes)
        BigEndian::write_u64(&mut buf[8..Header::SIZE], self.size);

        buf.to_vec()
    }

    pub fn verify(&self, content: &[u8]) -> Result<()> {
        if self.size != content.len() as u64 {
            return Err(Error::EncryptedFile(
                format!("content size mismatch {} != {}", self.size, content.len()).into(),
            ));
        }
        let mut digest = crc32fast::Hasher::new();
        digest.update(content);
        let crc32 = digest.finalize();
        if self.crc32 != crc32 {
            return Err(Error::EncryptedFile(
                format!("crc mismatch {} != {}", self.crc32, crc32).into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_header() {
        let empty_header = Header {
            version: Version::V1,
            crc32: 0,
            size: 0,
        };

        let bytes = empty_header.to_bytes();
        let (header1, content1) = Header::parse(&bytes).unwrap();
        assert_eq!(empty_header, header1);
        let empty: Vec<u8> = vec![];
        assert_eq!(content1, empty.as_slice())
    }
}
