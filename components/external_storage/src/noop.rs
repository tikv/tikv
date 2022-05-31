// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::Unpin;

use async_trait::async_trait;
use futures_io::AsyncRead;
use tokio::io;
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use super::ExternalStorage;
use crate::UnpinReader;

/// A storage saves files into void.
/// It is mainly for test use.
#[derive(Clone, Default)]
pub struct NoopStorage {}

impl NoopStorage {}

fn url_for() -> url::Url {
    url::Url::parse("noop:///").unwrap()
}

const STORAGE_NAME: &str = "noop";

#[async_trait]
impl ExternalStorage for NoopStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        Ok(url_for())
    }

    async fn write(
        &self,
        _name: &str,
        reader: UnpinReader,
        _content_length: u64,
    ) -> io::Result<()> {
        // we must still process the entire reader to run the SHA-256 hasher.
        io::copy(&mut reader.0.compat(), &mut io::sink()).await?;
        Ok(())
    }

    fn read(&self, _name: &str) -> Box<dyn AsyncRead + Unpin> {
        Box::new(io::empty().compat())
    }
}

#[cfg(test)]
mod tests {
    use futures_util::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn test_noop_storage() {
        let noop = NoopStorage::default();

        // Test save_file
        let magic_contents: &[u8] = b"5678";
        noop.write(
            "a.log",
            UnpinReader(Box::new(magic_contents)),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();
        let mut reader = noop.read("a.log");
        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();
        assert!(buf.is_empty());
    }

    #[test]
    fn test_url_of_backend() {
        assert_eq!(url_for().to_string(), "noop:///");
    }
}
