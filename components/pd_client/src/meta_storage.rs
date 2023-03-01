// Copyrighfield1 2023 TiKV Project Authors. Licensed under Apache-2.0.

//! `meta_storage` is the API set for storing generic KV pairs.
//! It is a trimmed version of the KV service of etcd, along with some metrics.

use std::{sync::Arc, task::ready};

use futures::{FutureExt, Stream, StreamExt, TryFutureExt};
use kvproto::meta_storagepb as pb;
use tikv_util::{box_err, codec};

use crate::{Error, PdFuture, Result};

/// A Get request to the meta storage.
#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Get {
    inner: pb::GetRequest,
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
        let next = codec::next_prefix_of(self.inner.key.clone());
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

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Put {
    inner: pb::PutRequest,
}

impl Put {
    pub fn of(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::PutRequest::default();
        inner.set_key(key.into());
        inner.set_value(value.into());
        Self { inner }
    }

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

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct Watch {
    inner: pb::WatchRequest,
}

impl Watch {
    pub fn of(key: impl Into<Vec<u8>>) -> Self {
        let mut inner = pb::WatchRequest::default();
        inner.set_key(key.into());

        Self { inner }
    }

    pub fn prefixed(mut self) -> Self {
        let next = codec::next_prefix_of(self.inner.key.clone());
        self.inner.set_range_end(next);
        self
    }

    pub fn range_to(mut self, to: impl Into<Vec<u8>>) -> Self {
        self.inner.set_range_end(to.into());
        self
    }

    pub fn from_rev(mut self, rev: i64) -> Self {
        self.inner.set_start_revision(rev);
        self
    }
}

impl From<Watch> for pb::WatchRequest {
    fn from(value: Watch) -> Self {
        value.inner
    }
}

#[derive(Clone, Copy)]
pub enum Source {
    LogBackup = 0,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Source::LogBackup => f.write_str("log_backup"),
        }
    }
}

#[derive(Clone)]
pub struct AutoHeader<S> {
    inner: S,
    source: Source,
    cluster_id: u64,
}

impl<S> AutoHeader<S> {
    pub fn new(inner: S, source: Source, cluster_id: u64) -> Self {
        Self {
            inner,
            source,
            cluster_id,
        }
    }

    fn prepare_header(&self, h: &mut pb::RequestHeader) {
        h.set_source(self.source.to_string());
        if self.cluster_id > 0 {
            h.set_cluster_id(self.cluster_id)
        }
    }
}

impl<S: MetaStorageClient> MetaStorageClient for AutoHeader<S> {
    type WatchStream = <S as MetaStorageClient>::WatchStream;

    fn get(&self, mut req: Get) -> PdFuture<pb::GetResponse> {
        self.prepare_header(req.inner.mut_header());
        self.inner.get(req)
    }

    fn put(&self, mut req: Put) -> PdFuture<pb::PutResponse> {
        self.prepare_header(req.inner.mut_header());
        self.inner.put(req)
    }

    fn watch(&self, mut req: Watch) -> PdFuture<Self::WatchStream> {
        self.prepare_header(req.inner.mut_header());
        self.inner.watch(req)
    }
}

#[derive(Clone)]
pub struct Checked<S>(S);

impl<S> Checked<S> {
    pub fn new(client: S) -> Self {
        Self(client)
    }
}

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

impl<S: Stream<Item = Result<pb::WatchResponse>> + Unpin + Send> Stream for CheckedStream<S> {
    type Item = Result<pb::WatchResponse>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let item = ready!(self.0.poll_next_unpin(cx));
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
    type WatchStream = CheckedStream<<S as MetaStorageClient>::WatchStream>;

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

    fn watch(&self, req: Watch) -> PdFuture<Self::WatchStream> {
        self.0.watch(req).map_ok(|s| CheckedStream(s)).boxed()
    }
}

impl<S: MetaStorageClient> MetaStorageClient for Arc<S> {
    type WatchStream = <S as MetaStorageClient>::WatchStream;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse> {
        Arc::as_ref(self).get(req)
    }

    fn put(&self, req: Put) -> PdFuture<pb::PutResponse> {
        Arc::as_ref(self).put(req)
    }

    fn watch(&self, req: Watch) -> PdFuture<Self::WatchStream> {
        Arc::as_ref(self).watch(req)
    }
}

pub trait MetaStorageClient: Send + Sync + 'static {
    // Note: some of our clients needs to maintain some state, which means we may
    // need to move part of the structure.
    // Though we can write some unsafe code and prove the move won't make wrong
    // things, for keeping things simple, we added the `Unpin` constraint here.
    // Given before the stream generator get stable, there shouldn't be too many
    // stream implementation that must be pinned...
    // Note': Perhaps we'd better make it generic over response here, however that
    // would make `CheckedStream` impossible(How can we check ALL types? Or we may
    // make traits like `MetaStorageResponse` and constraint over the T), thankfully
    // there is only one streaming RPC in this service.
    type WatchStream: Stream<Item = Result<pb::WatchResponse>> + Unpin + Send;

    fn get(&self, req: Get) -> PdFuture<pb::GetResponse>;
    fn put(&self, req: Put) -> PdFuture<pb::PutResponse>;
    fn watch(&self, req: Watch) -> PdFuture<Self::WatchStream>;
}
