// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! `meta_storage` is the API set for storing generic KV pairs.
//! It is a trimmed version of the KV service of etcd, along with some metrics.

use std::{pin::Pin, sync::Arc, task::ready};

use futures::{FutureExt, Stream};
use kvproto::meta_storagepb as pb;
use tikv_util::{box_err, codec};

use crate::{Error, PdFuture, Result};

/// The etcd INF end key.
/// Unlike TiKV, they have chosen the slice `[0u8]` as the infinity.
const INF: [u8; 1] = [0u8];

/// A Get request to the meta storage.
#[derive(Clone, Debug)]
pub struct Get {
    pub(crate) inner: pb::GetRequest,
}

impl From<Get> for pb::GetRequest {
    fn from(value: Get) -> Self {
        value.inner
    }
}

impl Get {
    /// Create a new get request, querying for exactly one key.
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::GetRequest::default();
        inner.set_key(key.into());
        Self { inner }
    }

    /// Enhance the query, make it be able to query the prefix of keys.
    /// The prefix is the key passed to the method [`of`](Get::of).
    pub fn prefixed(mut self) -> Self {
        let mut next = codec::next_prefix_of(self.inner.key.clone());
        if next.is_empty() {
            next = INF.to_vec();
        }
        self.inner.set_range_end(next);
        self
    }

    /// Enhance the query, make it be able to query a range of keys.
    /// The prefix is the key passed to the method [`of`](Get::of).
    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }

    /// Specify the revision of the query.
    pub fn rev(mut self, rev: i64) -> Self {
        self.inner.set_revision(rev);
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.inner.set_limit(limit);
        self
    }
}

/// A Put request to the meta store.
#[derive(Clone, Debug)]
pub struct Put {
    pub(crate) inner: pb::PutRequest,
}

impl Put {
    /// Create a put request of the key value.
    pub fn of(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::PutRequest::default();
        inner.set_key(key.into());
        inner.set_value(value.into());
        Self { inner }
    }

    /// Enhance the put request, allow it to return the previous kv pair.
    pub fn fetch_prev_kv(mut self) -> Self {
        self.inner.prev_kv = true;
        self
    }
}

impl From<Put> for pb::PutRequest {
    fn from(value: Put) -> Self {
        value.inner
    }
}

#[derive(Clone, Debug)]
pub struct Watch {
    pub(crate) inner: pb::WatchRequest,
}

impl Watch {
    /// Create a watch request for a key.
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::WatchRequest::default();
        inner.set_key(key.into());

        Self { inner }
    }

    /// Enhance the request to allow it watch keys with the same prefix.
    pub fn prefixed(mut self) -> Self {
        let mut next = codec::next_prefix_of(self.inner.key.clone());
        if next.is_empty() {
            next = INF.to_vec();
        }
        self.inner.set_range_end(next);
        self
    }

    /// Enhance the request to allow it watch keys until the range end.
    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }

    /// Enhance the request to make it watch from a specified revision.
    pub fn from_rev(mut self, rev: i64) -> Self {
        self.inner.set_start_revision(rev);
        self
    }

    /// Enhance the request to get the previous KV before the event happens.
    pub fn with_prev_kv(mut self) -> Self {
        self.inner.set_prev_kv(true);
        self
    }
}

impl From<Watch> for pb::WatchRequest {
    fn from(value: Watch) -> Self {
        value.inner
    }
}

/// A Delete request to the meta storage.
#[derive(Clone, Debug)]
pub struct Delete {
    pub(crate) inner: pb::DeleteRequest,
}

impl From<Delete> for pb::DeleteRequest {
    fn from(value: Delete) -> Self {
        value.inner
    }
}

impl Delete {
    /// Create a new delete request, deleting for exactly one key.
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::DeleteRequest::default();
        inner.set_key(key.into());
        Self { inner }
    }

    /// Enhance the delete, make it be able to delete the prefix of keys.
    /// The prefix is the key passed to the method [`of`](Delete::of).
    pub fn prefixed(mut self) -> Self {
        let mut next = codec::next_prefix_of(self.inner.key.clone());
        if next.is_empty() {
            next = INF.to_vec();
        }
        self.inner.set_range_end(next);
        self
    }

    /// Enhance the query, make it be able to query a range of keys.
    /// The prefix is the key passed to the method [`of`](Get::of).
    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }
}

/// The descriptor of source (caller) of the requests.
#[derive(Clone, Copy)]
pub enum Source {
    LogBackup = 0,
    ResourceControl = 1,
    RegionLabel = 2,
    KeysapceLevelGC = 3,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::LogBackup => f.write_str("log_backup"),
            Source::ResourceControl => f.write_str("resource_control"),
            Source::RegionLabel => f.write_str("region_label"),
            Source::KeysapceLevelGC =>f.write_str("keyspace_level_gc")
        }
    }
}

/// A wrapper over client which would fill the source field in the header for
/// all requests.
#[derive(Clone)]
pub struct Sourced<S> {
    inner: S,
    source: Source,
}

impl<S> Sourced<S> {
    pub fn new(inner: S, source: Source) -> Self {
        Self { inner, source }
    }

    fn prepare_header(&self, h: &mut pb::RequestHeader) {
        h.set_source(self.source.to_string());
    }
}

impl<S: MetaStorageClient> MetaStorageClient for Sourced<S> {
    type WatchStream = S::WatchStream;

    fn get(&self, mut req: Get) -> PdFuture<pb::GetResponse> {
        self.prepare_header(req.inner.mut_header());
        self.inner.get(req)
    }

    fn put(&self, mut req: Put) -> PdFuture<pb::PutResponse> {
        self.prepare_header(req.inner.mut_header());
        self.inner.put(req)
    }

    fn watch(&self, mut req: Watch) -> Self::WatchStream {
        self.prepare_header(req.inner.mut_header());
        self.inner.watch(req)
    }

    fn delete(&self, mut req: Delete) -> PdFuture<pb::DeleteResponse> {
        self.prepare_header(req.inner.mut_header());
        self.inner.delete(req)
    }
}

/// A wrapper that makes every response and stream event get checked.
/// When there is an error in the header, this client would return a [`Err`]
/// variant directly.
#[derive(Clone)]
pub struct Checked<S>(S);

impl<S> Checked<S> {
    pub fn new(client: S) -> Self {
        Self(client)
    }
}

/// A wrapper that checks every event in the stream and returns an error
/// variant when there is error in the header.
pub struct CheckedStream<S>(S);

fn check_resp_header(header: &pb::ResponseHeader) -> Result<()> {
    if header.has_error() {
        match header.get_error().get_type() {
            pb::ErrorType::Ok => Ok(()),
            pb::ErrorType::Unknown => Err(Error::Other(box_err!(
                "{}",
                header.get_error().get_message()
            ))),
            pb::ErrorType::DataCompacted => Err(Error::DataCompacted(
                header.get_error().get_message().to_owned(),
            )),
        }?;
    }
    Ok(())
}

impl<S: Stream<Item = Result<pb::WatchResponse>>> Stream for CheckedStream<S> {
    type Item = Result<pb::WatchResponse>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // SAFETY: trivial projection.
        let inner = unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().0) };
        let item = ready!(inner.poll_next(cx));
        item.map(|r| {
            r.and_then(|resp| {
                check_resp_header(resp.get_header())?;
                Ok(resp)
            })
        })
        .into()
    }
}

impl<S: MetaStorageClient> MetaStorageClient for Checked<S> {
    type WatchStream = CheckedStream<S::WatchStream>;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse> {
        self.0
            .get(req)
            .map(|resp| {
                resp.and_then(|r| {
                    check_resp_header(r.get_header())?;
                    Ok(r)
                })
            })
            .boxed()
    }

    fn put(&self, req: Put) -> PdFuture<pb::PutResponse> {
        self.0
            .put(req)
            .map(|resp| {
                resp.and_then(|r| {
                    check_resp_header(r.get_header())?;
                    Ok(r)
                })
            })
            .boxed()
    }

    fn watch(&self, req: Watch) -> Self::WatchStream {
        CheckedStream(self.0.watch(req))
    }

    fn delete(&self, req: Delete) -> PdFuture<pb::DeleteResponse> {
        self.0
            .delete(req)
            .map(|resp| {
                resp.and_then(|r| {
                    check_resp_header(r.get_header())?;
                    Ok(r)
                })
            })
            .boxed()
    }
}

impl<S: MetaStorageClient> MetaStorageClient for Arc<S> {
    type WatchStream = S::WatchStream;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse> {
        Arc::as_ref(self).get(req)
    }

    fn put(&self, req: Put) -> PdFuture<pb::PutResponse> {
        Arc::as_ref(self).put(req)
    }

    fn watch(&self, req: Watch) -> Self::WatchStream {
        Arc::as_ref(self).watch(req)
    }

    fn delete(&self, req: Delete) -> PdFuture<pb::DeleteResponse> {
        Arc::as_ref(self).delete(req)
    }
}

/// A client which is able to play with the `meta_storage` service.
pub trait MetaStorageClient: Send + Sync + 'static {
    // Note: Perhaps we'd better make it generic over response here, however that
    // would make `CheckedStream` impossible(How can we check ALL types? Or we may
    // make traits like `MetaStorageResponse` and constraint over the T), thankfully
    // there is only one streaming RPC in this service.
    /// The stream that yielded by the watch RPC.
    type WatchStream: Stream<Item = Result<pb::WatchResponse>>;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse>;
    fn put(&self, req: Put) -> PdFuture<pb::PutResponse>;
    fn delete(&self, req: Delete) -> PdFuture<pb::DeleteResponse>;
    fn watch(&self, req: Watch) -> Self::WatchStream;
}
