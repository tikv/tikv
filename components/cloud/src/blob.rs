// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt::Display, io, marker::Unpin, pin::Pin, task::Poll};

use async_trait::async_trait;
use futures::{
    future::{ok, BoxFuture, FutureExt, LocalBoxFuture, TryFutureExt},
    stream::{Stream, TryStreamExt},
};
use futures_io::AsyncRead;
use uuid::Uuid;

pub trait BlobConfig: 'static + Send + Sync {
    fn name(&self) -> &'static str;
    fn url(&self) -> io::Result<url::Url>;
}

/// PutResource is a simple wrapper for put.
/// It is identity to [external_storage::UnpinReader],
/// only for decoupling external_storage and cloud package.
///
/// See the documentation of [external_storage::UnpinReader] for why those
/// wrappers exists.
pub struct PutResource<'a>(pub Box<dyn AsyncRead + Send + Unpin + 'a>);

pub type BlobStream<'a> = Box<dyn AsyncRead + Unpin + Send + 'a>;

impl<'a> AsyncRead for PutResource<'a> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::result::Result<usize, futures_io::Error>> {
        Pin::new(&mut *self.get_mut().0).poll_read(cx, buf)
    }
}

impl<'a> From<Box<dyn AsyncRead + Send + Unpin + 'a>> for PutResource<'a> {
    fn from(s: Box<dyn AsyncRead + Send + Unpin + 'a>) -> Self {
        Self(s)
    }
}

/// An abstraction for blob storage.
/// Currently the same as ExternalStorage
#[async_trait]
pub trait BlobStorage: 'static + Send + Sync {
    fn config(&self) -> Box<dyn BlobConfig>;

    /// Write all contents of the read to the given path.
    async fn put(&self, name: &str, reader: PutResource<'_>, content_length: u64)
    -> io::Result<()>;

    /// Read all contents of the given path.
    fn get(&self, name: &str) -> BlobStream<'_>;

    /// Read part of contents of the given path.
    fn get_part(&self, name: &str, off: u64, len: u64) -> BlobStream<'_>;
}

pub trait DeletableStorage: 'static + Send + Sync {
    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>>;
}

#[derive(Debug)]
pub struct BlobObject {
    pub key: String,
}

impl Display for BlobObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.key)
    }
}

/// An storage that its content can be enumerated by prefix.
pub trait IterableStorage: 'static + Send + Sync {
    /// Walk the prefix of the blob storage.
    /// It returns the stream of items.
    fn iter_prefix(
        &self,
        prefix: &str,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<BlobObject, io::Error>> + '_>>;
}

#[derive(Clone, Copy)]
pub struct ExclusiveWriteCtx<'a> {
    file: &'a str,
    txn_id: uuid::Uuid,
    storage: &'a dyn IterableStorage,
}

pub mod requirements {
    use std::io;

    pub fn only(expect: &str) -> impl (Fn(&str) -> io::Result<()>) + '_ {
        move |v| {
            if v != expect {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("there is a file {}", v),
                ))
            } else {
                Ok(())
            }
        }
    }

    pub fn nothing(v: &str) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("there is a file {}", v),
        ))
    }
}

impl<'a> ExclusiveWriteCtx<'a> {
    pub fn txn_id(&self) -> uuid::Uuid {
        self.txn_id
    }

    pub async fn check_files_of_prefix(
        &self,
        prefix: &str,
        mut requires: impl FnMut(&str) -> io::Result<()>,
    ) -> io::Result<()> {
        self.storage
            .iter_prefix(prefix)
            .try_for_each(|v| futures::future::ready(requires(&v.key)))
            .await
    }

    pub async fn verify_only_my_intent(&self) -> io::Result<()> {
        self.check_files_of_prefix(self.file, requirements::only(&self.intent_file_name()))
            .await
    }

    pub fn intent_file_name(&self) -> String {
        format!("{}.INTENT.{:032X}", self.file, self.txn_id)
    }
}

#[allow(async_fn_in_trait)]
pub trait ExclusiveWriteTxn {
    fn path(&self) -> &str;
    fn content(&self, cx: ExclusiveWriteCtx<'_>) -> io::Result<Vec<u8>>;
    fn verify<'cx: 'ret, 's: 'ret, 'ret>(
        &'s self,
        _cx: ExclusiveWriteCtx<'cx>,
    ) -> LocalBoxFuture<'ret, io::Result<()>> {
        ok(()).boxed_local()
    }
}

/// An storage that supports atomically write a file if the file not exists.
pub trait ExclusiveWritableStorage: 'static + Send + Sync {
    fn exclusive_write<'s: 'ret, 'txn: 'ret, 'ret>(
        &'s self,
        w: &'txn dyn ExclusiveWriteTxn,
    ) -> LocalBoxFuture<'ret, io::Result<uuid::Uuid>>;
}

// NOTE: maybe add `StrongConsistency`.
impl<T: BlobStorage + IterableStorage + DeletableStorage> ExclusiveWritableStorage for T {
    fn exclusive_write<'s: 'ret, 'txn: 'ret, 'ret>(
        &'s self,
        w: &'txn dyn ExclusiveWriteTxn,
    ) -> LocalBoxFuture<'ret, io::Result<uuid::Uuid>> {
        async move {
            let txn_id = Uuid::new_v4();
            let cx = ExclusiveWriteCtx {
                file: w.path(),
                txn_id,
                storage: self,
            };
            futures::future::try_join(cx.verify_only_my_intent(), w.verify(cx)).await?;
            let target = cx.intent_file_name();
            self.put(&target, PutResource(Box::new(futures::io::empty())), 0)
                .await?;

            let result = async {
                futures::future::try_join(cx.verify_only_my_intent(), w.verify(cx)).await?;
                let content = w.content(cx)?;
                self.put(
                    w.path(),
                    PutResource(Box::new(futures::io::Cursor::new(&content))),
                    content.len() as _,
                )
                .await?;
                io::Result::Ok(txn_id)
            }
            .await;

            let _ = self.delete(&target).await;
            result
        }
        .boxed_local()
    }
}

impl BlobConfig for dyn BlobStorage {
    fn name(&self) -> &'static str {
        self.config().name()
    }

    fn url(&self) -> io::Result<url::Url> {
        self.config().url()
    }
}

#[async_trait]
impl BlobStorage for Box<dyn BlobStorage> {
    fn config(&self) -> Box<dyn BlobConfig> {
        (**self).config()
    }

    async fn put(
        &self,
        name: &str,
        reader: PutResource<'_>,
        content_length: u64,
    ) -> io::Result<()> {
        let fut = (**self).put(name, reader, content_length);
        fut.await
    }

    fn get(&self, name: &str) -> BlobStream<'_> {
        (**self).get(name)
    }

    fn get_part(&self, name: &str, off: u64, len: u64) -> BlobStream<'_> {
        (**self).get_part(name, off, len)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct StringNonEmpty(String);
impl StringNonEmpty {
    pub fn opt(s: String) -> Option<Self> {
        if s.is_empty() { None } else { Some(Self(s)) }
    }

    pub fn opt2(s1: String, s2: String) -> Option<Self> {
        Self::opt(s1).or_else(|| Self::opt(s2))
    }

    pub fn required_field(s: String, field: &str) -> io::Result<Self> {
        Self::required_msg(s, &format!("field {}", field))
    }

    pub fn required_field2(s1: String, s2: String, field: &str) -> io::Result<Self> {
        match Self::opt2(s1, s2) {
            Some(sne) => Ok(sne),
            None => Err(Self::error_required(&format!("field {}", field))),
        }
    }

    pub fn required_msg(s: String, msg: &str) -> io::Result<Self> {
        if !s.is_empty() {
            Ok(Self(s))
        } else {
            Err(Self::error_required(&format!("Empty {}", msg)))
        }
    }

    fn error_required(msg: &str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, msg)
    }

    pub fn required(s: String) -> io::Result<Self> {
        Self::required_msg(s, "string")
    }

    pub fn static_str(s: &'static str) -> Self {
        Self::required_msg(s.to_owned(), "static str").unwrap()
    }
}

impl std::ops::Deref for StringNonEmpty {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for StringNonEmpty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Debug)]
pub struct BucketConf {
    pub endpoint: Option<StringNonEmpty>,
    pub region: Option<StringNonEmpty>,
    pub bucket: StringNonEmpty,
    pub prefix: Option<StringNonEmpty>,
    pub storage_class: Option<StringNonEmpty>,
}

impl BucketConf {
    pub fn default(bucket: StringNonEmpty) -> Self {
        BucketConf {
            bucket,
            endpoint: None,
            region: None,
            prefix: None,
            storage_class: None,
        }
    }

    pub fn url(&self, scheme: &str) -> Result<url::Url, String> {
        let path = none_to_empty(self.prefix.clone());
        if let Some(ep) = &self.endpoint {
            let mut u =
                url::Url::parse(ep).map_err(|e| format!("invalid endpoint {}: {}", &ep, e))?;
            u.set_path(&format!(
                "{}/{}",
                &self.bucket.trim_end_matches('/'),
                &path.trim_start_matches('/')
            ));
            Ok(u)
        } else {
            let mut u = url::Url::parse(&format!("{}://{}", &scheme, &self.bucket))
                .map_err(|e| format!("{}", e))?;
            u.set_path(&path);
            Ok(u)
        }
    }
}

pub fn none_to_empty(opt: Option<StringNonEmpty>) -> String {
    if let Some(s) = opt {
        s.0
    } else {
        "".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_of_bucket() {
        let bucket_name = StringNonEmpty::required("bucket".to_owned()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = StringNonEmpty::opt("/backup 01/prefix/".to_owned());
        assert_eq!(
            bucket.url("s3").unwrap().to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
        bucket.endpoint = Some(StringNonEmpty::static_str("http://endpoint.com"));
        assert_eq!(
            bucket.url("s3").unwrap().to_string(),
            "http://endpoint.com/bucket/backup%2001/prefix/"
        );
    }
}
