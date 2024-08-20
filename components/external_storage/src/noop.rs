// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use cloud::blob::BlobObject;
use futures_util::{
    future::{self, LocalBoxFuture},
    stream::{self, LocalBoxStream},
};
use tokio::io;
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

use super::ExternalStorage;
use crate::{ExternalData, UnpinReader};

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
        reader: UnpinReader<'_>,
        _content_length: u64,
    ) -> io::Result<()> {
        // we must still process the entire reader to run the SHA-256 hasher.
        io::copy(&mut reader.0.compat(), &mut io::sink()).await?;
        Ok(())
    }

    fn read(&self, _name: &str) -> ExternalData<'_> {
        Box::new(io::empty().compat())
    }

    fn read_part(&self, _name: &str, _off: u64, _len: u64) -> ExternalData<'_> {
        Box::new(io::empty().compat())
    }

    fn iter_prefix(&self, _prefix: &str) -> LocalBoxStream<'_, io::Result<BlobObject>> {
        Box::pin(stream::empty())
    }

    fn delete(&self, _name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(future::ok(()))
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
