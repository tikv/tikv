use std::path::{Path, PathBuf};

use crate::{Error, Result};

pub enum Version {
    Unknown = 0,
    V1 = 1,
}

/// A reference to an open file on the filesystem.
///
/// ```ignore
/// 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
/// | |     |       |              |
/// +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
///  ^   ^      ^         ^           ^
///  |   |      |         |           | Serialized content (variable size)
///  |   |      |         | Content size (8 Byte)
///  |   |      | Crc32  (4 Byte)
///  |   | Reserved  (5 Byte)
///  | Version (1 Byte)
/// ```
pub struct File {
    version: Version,
    crc32: u32,
    size: u32,
    path: PathBuf,

    content: Option<Vec<u8>>,
}

impl File {
    /// Open or create a file at the path.
    ///
    /// Note: It's different from `std::fs::File`, it does not hold a reference
    /// to the file. Instead it reads the whole content into memory.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<File> {
        // TODO open and deserialize the content.
        unimplemented!()
    }

    pub fn read_once(&mut self) -> Result<Vec<u8>> {
        self.content
            .take()
            .ok_or_else(|| Error::Other(String::from("already read").into()))
    }

    pub fn write_through(&mut self, content: &[u8]) -> Result<()> {
        unimplemented!();
    }
}
