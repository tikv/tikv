use std::{
    collections::VecDeque, convert::TryInto, fmt::Display, pin::Pin, sync::Arc, task::ready,
    time::Duration,
};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use grpcio::ClientSStreamReceiver;
use kvproto::pdpb::{self, GlobalConfigItem, WatchGlobalConfigResponse};
use pd_client::{GlobalConfigSelector, PdClient};
use tikv_util::{box_err, warn};

use super::{
    GetResponse, Keys, KvChangeSubscription, KvEvent, KvEventType, MetaStore, Snapshot,
    TransactionOp, WithRevision,
};
use crate::{
    annotate,
    errors::{ContextualResultExt, Error, Result},
    metadata::keys::{KeyValue, MetaKey, PREFIX},
};

pub struct PdStore<PD> {
    client: Arc<PD>,
}

impl<PD> PdStore<PD> {
    pub fn new(s: Arc<PD>) -> Self {
        Self { client: s }
    }
}

impl<PD> Clone for PdStore<PD> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
        }
    }
}

fn unimplemented(name: impl Display) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("the behavior {} hasn't been implemented yet.", name),
    ))
}

fn to_config_key(name: Vec<u8>) -> Result<String> {
    String::from_utf8(name)
        .map_err(|err| annotate!(err, "arbitrary binary key isn't support now(utf-8 only)"))
}

fn try_make_pd_selector(k: Keys) -> Result<GlobalConfigSelector> {
    match k {
        Keys::Prefix(p) => Ok(GlobalConfigSelector::of_prefix(to_config_key(p.0)?)),
        Keys::Range(..) => Err(unimplemented("Keys::Range::try_into<ConfigPath>")),
        Keys::Key(k) => Ok(GlobalConfigSelector::of_prefix(to_config_key(k.0)?).exactly()),
    }
}

fn make_config_name(key: MetaKey) -> Result<String> {
    let striped = key.0.strip_prefix(PREFIX.as_bytes()).ok_or_else(|| {
        Error::Other(box_err!(
            "the key {} doesn't starts with prefix {}",
            String::from_utf8_lossy(key.0.as_slice()),
            PREFIX
        ))
    })?;
    to_config_key(striped.to_vec())
}

fn try_make_item(op: TransactionOp) -> Result<pdpb::GlobalConfigItem> {
    let mut i = pdpb::GlobalConfigItem::default();
    match op {
        TransactionOp::Put(kv, opt) => {
            if opt.ttl > Duration::ZERO {
                return Err(unimplemented("Txn::Put::WithTtl"));
            }

            i.set_kind(pdpb::EventType::Put);
            i.set_name(make_config_name(kv.0)?);
            i.set_payload(kv.1);
            Ok(i)
        }
        TransactionOp::Delete(Keys::Key(k)) => {
            i.set_kind(pdpb::EventType::Delete);
            i.set_name(make_config_name(k)?);
            Ok(i)
        }
        _ => Err(unimplemented("Remove(Keys::{Range,Prefix})")),
    }
}

struct PdWatchStream {
    inner: ClientSStreamReceiver<WatchGlobalConfigResponse>,
    buf: VecDeque<KvEvent>,
}

impl PdWatchStream {
    fn new(inner: ClientSStreamReceiver<WatchGlobalConfigResponse>) -> Self {
        Self {
            inner,
            buf: Default::default(),
        }
    }
}

impl Stream for PdWatchStream {
    type Item = Result<KvEvent>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(x) = this.buf.pop_front() {
                return Some(Ok(x)).into();
            }

            let resp = ready!(this.inner.poll_next_unpin(cx));
            match resp {
                None => return None.into(),
                Some(Err(err)) => return Some(Err(Error::Grpc(err))).into(),
                Some(Ok(x)) => {
                    pd_client::check_resp_header(x.get_header())?;
                    this.buf.clear();
                    for e in x.get_changes() {
                        let ty = match e.get_kind() {
                            pdpb::EventType::Put => KvEventType::Put,
                            pdpb::EventType::Delete => KvEventType::Delete,
                        };
                        let k = e.get_name().to_string().into_bytes();
                        let v = e.get_payload().to_vec();
                        let kv = KvEvent {
                            kind: ty,
                            pair: KeyValue(MetaKey(k), v),
                        };
                        this.buf.push_back(kv);
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
impl<PD: PdClient> MetaStore for PdStore<PD> {
    type Snap = RevOnly;

    async fn snapshot(&self) -> Result<Self::Snap> {
        // hacking here: when we are doing point querying, the server won't return
        // revision. So we are going to query a non-exist prefix here.
        let random_key = format!("/{}", rand::random::<u64>());
        let (items, rev) = self
            .client
            .load_global_config(GlobalConfigSelector::of_prefix(random_key.clone()))
            .await?;
        if !items.is_empty() {
            warn!("random key returned something."; "len" => %items.len(), "key" => %random_key);
        }
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
                    .watch_global_config(to_config_key(k.0)?, start_rev)?;
                Ok(KvChangeSubscription {
                    stream: PdWatchStream::new(stream).boxed(),
                    cancel: Box::pin(async { todo!() }),
                })
            }
            _ => Err(unimplemented("watch distinct keys or range of keys")),
        }
    }

    async fn txn(&self, txn: super::Transaction) -> Result<()> {
        let mut r = Vec::with_capacity(txn.ops.len());
        for (i, o) in txn.ops.into_iter().enumerate() {
            let item = try_make_item(o).context_with(|| format!("in the {}th item of txn", i))?;
            r.push(item);
        }
        self.client
            .store_global_config(PREFIX.to_owned(), r)
            .await?;
        Ok(())
    }

    async fn txn_cond(&self, _txn: super::CondTransaction) -> Result<()> {
        Err(unimplemented("PdStore::txn_cond"))
    }

    async fn get_latest(&self, keys: Keys) -> Result<WithRevision<Vec<KeyValue>>> {
        let c = self.client.as_ref();
        let cfg = try_make_pd_selector(keys)?;
        let (ks, rev) = c.load_global_config(cfg).await?;
        Ok(WithRevision {
            revision: rev,
            inner: ks
                .into_iter()
                .filter_map(|ks| {
                    if ks.get_kind() == pdpb::EventType::Delete {
                        return None;
                    }

                    let key = ks.get_name().as_bytes().to_vec();
                    let value = if !ks.get_value().is_empty() {
                        ks.get_value().as_bytes()
                    } else {
                        ks.get_payload()
                    };
                    Some(KeyValue(MetaKey(key), value.to_vec()))
                })
                .collect(),
        })
    }
}
