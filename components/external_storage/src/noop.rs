// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::marker::Unpin;

use futures_executor::block_on;
use futures_io::AsyncRead;
use futures_util::io::{copy, AllowStdIo};

use super::ExternalStorage;

/// A storage saves files into void.
/// It is mainly for test use.
#[derive(Clone, Default)]
pub struct NoopStorage {}

impl ExternalStorage for NoopStorage {
    fn write(
        &self,
        _name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        _content_length: u64,
    ) -> io::Result<()> {
        // we must still process the entire reader to run the SHA-256 hasher.
        block_on(copy(reader, &mut AllowStdIo::new(io::sink()))).map(drop)
    }

    fn read(&self, _name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        Box::new(AllowStdIo::new(io::empty()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::io::AsyncReadExt;

    #[test]
    fn test_noop_storage() {
        let noop = NoopStorage::default();

        // Test save_file
        let magic_contents: &[u8] = b"5678";
        noop.write(
            "a.log",
            Box::new(magic_contents),
            magic_contents.len() as u64,
        )
        .unwrap();
        let mut reader = noop.read("a.log");
        let mut buf = vec![];
        block_on(reader.read_to_end(&mut buf)).unwrap();
        assert!(buf.is_empty());
    }
}
