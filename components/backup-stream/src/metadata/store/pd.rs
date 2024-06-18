// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::VecDeque, fmt::Display, pin::Pin, task::ready};

use async_trait::async_trait;
use futures::{stream, Stream};
use kvproto::meta_storagepb::{self as mpb, WatchResponse};
use pd_client::meta_storage::{Get, MetaStorageClient, Put, Watch};
use pin_project::pin_project;
use tikv_util::{box_err, info};

use super::{
    GetResponse, Keys, KvChangeSubscription, KvEvent, KvEventType, MetaStore, Snapshot,
    WithRevision,
};
use crate::{
    debug,
    errors::{Error, Result},
    metadata::keys::{KeyValue, MetaKey, PREFIX},
};

fn convert_kv(mut kv: mpb::KeyValue) -> KeyValue {
    let k = kv.take_key();
    let v = kv.take_value();
    KeyValue(MetaKey(k), v)
}

#[derive(Clone)]
pub struct PdStore<M> {
    client: M,
}

impl<M> PdStore<M> {
    pub fn new(s: M) -> Self {
        Self { client: s }
    }
}

fn unimplemented(name: impl Display) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("the behavior {} hasn't been implemented yet.", name),
    ))
}

#[pin_project]
struct PdWatchStream<S> {
    #[pin]
    inner: S,
    buf: VecDeque<KvEvent>,
}

impl<S> PdWatchStream<S> {
    /// Create a new Watch Stream from PD, with a function to cancel the stream.
    fn new(inner: S) -> Self {
        Self {
            inner,
            buf: Default::default(),
        }
    }
}

impl<S: Stream<Item = pd_client::Result<mpb::WatchResponse>>> Stream for PdWatchStream<S> {
    type Item = Result<KvEvent>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            let buf = this.buf;
            if let Some(x) = buf.pop_front() {
                return Some(Ok(x)).into();
            }
            let resp = ready!(this.inner.poll_next(cx));
            match resp {
                None => return None.into(),
                Some(Err(err)) => return Some(Err(Error::Pd(err))).into(),
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
impl<
    St: Stream<Item = pd_client::Result<WatchResponse>> + Send + 'static,
    PD: MetaStorageClient<WatchStream = St> + Clone,
> MetaStore for PdStore<PD>
{
    type Snap = RevOnly;

    async fn snapshot(&self) -> Result<Self::Snap> {
        // hacking here: when we are doing point querying, the server won't return
        // revision. So we are going to query a non-exist prefix here.
        let rev = self
            .client
            .get(Get::of(PREFIX.as_bytes().to_vec()).prefixed().limit(0))
            .await?
            .get_header()
            .get_revision();
        info!("pd meta client getting snapshot."; "rev" => %rev);
        Ok(RevOnly(rev))
    }

    async fn watch(
        &self,
        keys: super::Keys,
        start_rev: i64,
    ) -> Result<super::KvChangeSubscription> {
        info!("pd meta client creating watch stream."; "keys" => ?keys, "rev" => %start_rev);
        match keys {
            Keys::Prefix(k) => {
                use futures::stream::StreamExt;
                let stream = self
                    .client
                    .watch(Watch::of(k).prefixed().from_rev(start_rev));
                let (stream, cancel) = stream::abortable(PdWatchStream::new(stream));
                Ok(KvChangeSubscription {
                    stream: stream.boxed(),
                    cancel: Box::pin(async move { cancel.abort() }),
                })
            }
            _ => Err(unimplemented("watch distinct keys or range of keys")),
        }
    }

    async fn txn(&self, _txn: super::Transaction) -> Result<()> {
        Err(unimplemented("PdStore::txn"))
    }

    async fn txn_cond(&self, _txn: super::CondTransaction) -> Result<()> {
        Err(unimplemented("PdStore::txn_cond"))
    }

    async fn set(&self, mut kv: KeyValue) -> Result<()> {
        debug!("pd meta client setting."; "pair" => ?kv);
        self.client
            .put(Put::of(kv.take_key(), kv.take_value()))
            .await?;
        Ok(())
    }

    async fn get_latest(&self, keys: Keys) -> Result<WithRevision<Vec<KeyValue>>> {
        let spec = match keys.clone() {
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
        debug!("pd meta client getting."; "range" => ?keys, "rev" => %revision, "result" => ?inner);
        Ok(WithRevision { inner, revision })
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::{Future, StreamExt};
    use pd_client::{
        meta_storage::{Checked, Source, Sourced},
        RpcClient,
    };
    use test_pd::{mocker::MetaStorage, util::*, Server as PdServer};
    use tikv_util::config::ReadableDuration;

    use super::PdStore;
    use crate::metadata::{
        keys::{KeyValue, MetaKey},
        store::{Keys, MetaStore},
    };

    fn new_test_server_and_client<C>(
        factory: impl FnOnce(RpcClient) -> C,
    ) -> (PdServer<MetaStorage>, PdStore<C>) {
        let server = PdServer::with_case(1, Arc::<MetaStorage>::default());
        let eps = server.bind_addrs();
        let client =
            new_client_with_update_interval(eps, None, ReadableDuration(Duration::from_secs(99)));
        (server, PdStore::new(factory(client)))
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
        let (_s, c) = new_test_server_and_client(|c| Sourced::new(Arc::new(c), Source::LogBackup));

        let kv = |k, v: &str| KeyValue(MetaKey::task_of(k), v.as_bytes().to_vec());
        let insert = |k, v| w(c.set(kv(k, v))).unwrap();
        insert("a", "the signpost of flowers");
        insert("b", "the milky hills");
        insert("c", "the rusty sky");

        let k = w(c.get_latest(Keys::Key(MetaKey::task_of("a")))).unwrap();
        assert_eq!(
            k.inner.as_slice(),
            [kv("a", "the signpost of flowers")].as_slice()
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
        let (_s, c) = new_test_server_and_client(|c| Sourced::new(Arc::new(c), Source::LogBackup));
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

    #[test]
    fn test_check_error() {
        // Without AutoHeader, it will fail due to the source is empty.
        let (_s, c) = new_test_server_and_client(|c| Checked::new(Arc::new(c)));
        let kv = |k, v: &str| KeyValue(MetaKey::task_of(k), v.as_bytes().to_vec());
        let insert = |k, v| w(c.set(kv(k, v)));

        insert("c", "the rainbow-like summer").unwrap_err();
        w(c.get_latest(Keys::Key(MetaKey(vec![42u8])))).unwrap_err();
        assert!(w(c.watch(Keys::Key(MetaKey(vec![42u8])), 42)).is_err());
    }

    #[test]
    fn test_retry() {
        use tikv_util::defer;

        defer! {{
            fail::remove("meta_storage_get");
        }};
        let (_s, c) = new_test_server_and_client(|c| Sourced::new(Arc::new(c), Source::LogBackup));

        let kv = |k, v: &str| KeyValue(MetaKey::task_of(k), v.as_bytes().to_vec());
        let insert = |k, v| w(c.set(kv(k, v))).unwrap();
        insert("rejectme", "this key would be rejected by the failpoint.");

        fail::cfg("meta_storage_get", "4*return").unwrap();
        let res = w(c.get_latest(Keys::Key(MetaKey::task_of("rejectme"))))
            .expect("should success when temporary failing");
        assert_eq!(res.inner.len(), 1);
        assert_eq!(
            res.inner[0],
            kv("rejectme", "this key would be rejected by the failpoint.")
        );

        // FIXME: this would take about 10s to run and influences unit tests run...
        fail::cfg("meta_storage_get", "return").unwrap();
        w(c.get_latest(Keys::Key(MetaKey::task_of("rejectme"))))
            .expect_err("should fail when ever failing");
    }
}
