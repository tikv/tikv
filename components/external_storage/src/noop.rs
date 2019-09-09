// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self, Read};

use super::ExternalStorage;

/// A storage saves files into void.
/// It is mainly for test use.
#[derive(Clone)]
pub struct NoopStorage {}

impl NoopStorage {
    /// The url scheme of the `NoopStorage`.
    pub const SCHEME: &'static str = "noop";

    /// Create a new noop storage in the given path.
    pub fn new() -> NoopStorage {
        info!("create noop storage");
        NoopStorage {}
    }
}

impl ExternalStorage for NoopStorage {
    fn write(&self, _name: &str, _reader: &mut dyn Read) -> io::Result<()> {
        Ok(())
    }

    fn read(&self, _name: &str) -> io::Result<Box<dyn Read>> {
        Ok(Box::new(io::empty()) as _)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_storage() {
        let noop = NoopStorage::new();

        // Test save_file
        let mut magic_contents: &[u8] = b"5678";
        noop.write("a.log", &mut magic_contents).unwrap();
        let mut reader = noop.read("a.log").unwrap();
        let mut buf = vec![];
        reader.read_to_end(&mut buf).unwrap();
        assert!(buf.is_empty());
    }
}
