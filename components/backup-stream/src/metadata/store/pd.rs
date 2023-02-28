use std::{
    collections::VecDeque,
    fmt::Display,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::ready,
    time::Duration,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use grpcio::ClientSStreamReceiver;
use kvproto::{
    meta_storagepb as mpb,
    pdpb::{self, WatchGlobalConfigResponse},
};
use pd_client::{Get, MetaStorageClient, Put, Watch};
use tikv_util::{box_err, info, warn};

use super::{
    GetResponse, Keys, KvChangeSubscription, KvEvent, KvEventType, MetaStore, Snapshot,
    TransactionOp, WithRevision,
};
use crate::{
    annotate,
    errors::{ContextualResultExt, Error, Result},
    metadata::keys::{KeyValue, MetaKey, PREFIX},
};

fn convert_kv(mut kv: mpb::KeyValue) -> KeyValue {
    let k = kv.take_key();
    let v = kv.take_value();
    KeyValue(MetaKey(k), v)
}

pub struct PdStore<M> {
    client: Arc<M>,
}

impl<M> Clone for PdStore<M> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
        }
    }
}

impl<M> PdStore<M> {
    pub fn new(s: Arc<M>) -> Self {
        Self { client: s }
    }
}

fn unimplemented(name: impl Display) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("the behavior {} hasn't been implemented yet.", name),
    ))
}

enum PdWatchStream<S> {
    Running {
        inner: S,
        buf: VecDeque<KvEvent>,
        canceled: Arc<AtomicBool>,
    },
    Canceled,
}

impl<S> PdWatchStream<S> {
    /// Create a new Watch Stream from PD, with a function to cancel the stream.
    fn new(inner: S) -> (Self, impl FnOnce()) {
        let cancel = Arc::default();
        let s = Self::Running {
            inner,
            buf: Default::default(),
            canceled: Arc::clone(&cancel),
        };

        (s, move || cancel.store(true, Ordering::SeqCst))
    }
}

impl<S: Stream<Item = grpcio::Result<mpb::WatchResponse>> + Unpin> Stream for PdWatchStream<S> {
    type Item = Result<KvEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // Safety: trivial projection.
        let (inner, buf, canceled) = match &mut *self {
            PdWatchStream::Running {
                inner,
                buf,
                canceled,
            } => (inner, buf, canceled),
            PdWatchStream::Canceled => return None.into(),
        };
        loop {
            if let Some(x) = buf.pop_front() {
                return Some(Ok(x)).into();
            }
            if canceled.load(Ordering::SeqCst) {
                *self.get_mut() = Self::Canceled;
                return None.into();
            }
            let resp = ready!(inner.poll_next_unpin(cx));
            match resp {
                None => return None.into(),
                Some(Err(err)) => return Some(Err(Error::Grpc(err))).into(),
                Some(Ok(mut x)) => {
                    if x.get_header().has_error() {
                        return Some(Err(Error::Other(box_err!(
                            "watch stream returns error: {:?}",
                            x.get_header().get_error()
                        ))))
                        .into();
                    }
                    assert!(buf.is_empty());
                    for mut e in x.take_events().into_iter() {
                        let ty = match e.get_type() {
                            kvproto::meta_storagepb::EventEventType::Put => KvEventType::Put,
                            kvproto::meta_storagepb::EventEventType::Delete => KvEventType::Delete,
                        };
                        let kv = KvEvent {
                            kind: ty,
                            pair: convert_kv(e.take_kv()),
                        };
                        buf.push_back(kv);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Snapshot for RevOnly {
    async fn get_extra(&self, _keys: Keys, _extra: super::GetExtra) -> Result<GetResponse> {
        Err(unimplemented("PdStore::snapshot::get"))
    }

    fn revision(&self) -> i64 {
        self.0
    }
}

pub struct RevOnly(i64);

#[async_trait]
impl<PD: MetaStorageClient> MetaStore for PdStore<PD> {
    type Snap = RevOnly;

    async fn snapshot(&self) -> Result<Self::Snap> {
        // hacking here: when we are doing point querying, the server won't return
        // revision. So we are going to query a non-exist prefix here.
        let random_key = format!("/{}", rand::random::<u64>());
        let rev = self
            .client
            .get(Get::of(PREFIX.as_bytes().to_vec()).prefixed().limit(0))
            .await?
            .get_header()
            .get_revision();
        Ok(RevOnly(rev))
    }

    async fn watch(
        &self,
        keys: super::Keys,
        start_rev: i64,
    ) -> Result<super::KvChangeSubscription> {
        match keys {
            Keys::Prefix(k) => {
                use futures::stream::StreamExt;
                let stream = self
                    .client
                    .watch(Watch::of(k).prefixed().from_rev(start_rev))
                    .await?;
                let (stream, cancel) = PdWatchStream::new(stream);
                Ok(KvChangeSubscription {
                    stream: stream.boxed(),
                    cancel: Box::pin(async { cancel() }),
                })
            }
            _ => Err(unimplemented("watch distinct keys or range of keys")),
        }
    }

    async fn txn(&self, txn: super::Transaction) -> Result<()> {
        Err(unimplemented("PdStore::txn"))
    }

    async fn txn_cond(&self, _txn: super::CondTransaction) -> Result<()> {
        Err(unimplemented("PdStore::txn_cond"))
    }

    async fn set(&self, mut kv: KeyValue) -> Result<()> {
        self.client
            .put(Put::of(kv.take_key(), kv.take_value()))
            .await?;
        Ok(())
    }

    async fn get_latest(&self, keys: Keys) -> Result<WithRevision<Vec<KeyValue>>> {
        let spec = match keys {
            Keys::Prefix(p) => Get::of(p).prefixed(),
            Keys::Key(k) => Get::of(k),
            Keys::Range(s, e) => Get::of(s).range_to(e),
        };
        // Note: we skipped check `more` here, because we haven't make pager.
        let mut resp = self.client.get(spec).await?;
        let inner = resp
            .take_kvs()
            .into_iter()
            .map(convert_kv)
            .collect::<Vec<_>>();
        let revision = resp.get_header().get_revision();
        Ok(WithRevision { inner, revision })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::{Future, StreamExt};
    use pd_client::RpcClient;
    use test_pd::{mocker::Service, util::*, Server as PdServer};
    use tikv_util::config::ReadableDuration;

    use super::PdStore;
    use crate::metadata::{
        keys::{KeyValue, MetaKey},
        store::{Keys, KvEventType, MetaStore},
    };

    fn new_test_server_and_client() -> (PdServer<Service>, PdStore<RpcClient>) {
        let server = PdServer::with_case(1);
        let eps = server.bind_addrs();
        let client =
            new_client_with_update_interval(eps, None, ReadableDuration(Duration::from_secs(99)));
        (server, PdStore::new(Arc::new(client)))
    }

    fn w<T>(f: impl Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn test_query() {
        let (_s, c) = new_test_server_and_client();

        let kv = |k, v: &str| KeyValue(MetaKey::task_of(k), v.as_bytes().to_vec());
        let insert = |k, v| w(c.set(kv(k, v))).unwrap();
        insert("a", "the signpost of flowers");
        insert("b", "the milky hills");
        insert("c", "the rusty sky");

        let k = w(c.get_latest(Keys::Key(MetaKey::task_of("a")))).unwrap();
        assert_eq!(
            k.inner.as_slice(),
            [KeyValue(
                MetaKey::task_of("a"),
                b"the signpost of flowers".to_vec()
            )]
            .as_slice()
        );
        let k = w(c.get_latest(Keys::Key(MetaKey::task_of("d")))).unwrap();
        assert_eq!(k.inner.as_slice(), [].as_slice());

        let k = w(c.get_latest(Keys::Prefix(MetaKey::tasks()))).unwrap();
        assert_eq!(
            k.inner.as_slice(),
            [
                kv("a", "the signpost of flowers"),
                kv("b", "the milky hills"),
                kv("c", "the rusty sky"),
            ]
            .as_slice()
        )
    }

    #[test]
    fn test_watch() {
        let (_s, c) = new_test_server_and_client();
        let kv = |k, v: &str| KeyValue(MetaKey::task_of(k), v.as_bytes().to_vec());
        let insert = |k, v| w(c.set(kv(k, v))).unwrap();

        insert("a", "the guest in vermilion");
        let res = w(c.get_latest(Keys::Prefix(MetaKey::tasks()))).unwrap();
        assert_eq!(res.inner.as_slice(), &[kv("a", "the guest in vermilion")]);
        let mut ws = w(c.watch(Keys::Prefix(MetaKey::tasks()), res.revision + 1)).unwrap();
        let mut items = vec![];
        insert("a", "looking up at the ocean");
        items.push(w(ws.stream.next()).unwrap().unwrap());
        insert("b", "a folktale in the polar day");
        items.push(w(ws.stream.next()).unwrap().unwrap());
        w(ws.cancel);
        assert!(w(ws.stream.next()).is_none());

        assert_eq!(items[0].pair, kv("a", "looking up at the ocean"));
        assert_eq!(items[1].pair, kv("b", "a folktale in the polar day"));
    }
}
