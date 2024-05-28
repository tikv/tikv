// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use futures::{executor::block_on, SinkExt, StreamExt};
use grpcio::{RpcStatus, RpcStatusCode};
use kvproto::meta_storagepb as mpb;

use super::etcd::{Etcd, KeyValue, Keys, KvEventType, MetaKey};
use crate::PdMocker;

#[derive(Default)]
pub struct MetaStorage {
    store: Arc<Mutex<Etcd>>,
}

fn convert_kv(from: KeyValue) -> mpb::KeyValue {
    let mut kv = mpb::KeyValue::default();
    kv.set_key(from.0.0);
    kv.set_value(from.1);
    kv
}

fn check_header(h: &mpb::RequestHeader) -> super::Result<()> {
    if h.get_source().is_empty() {
        return Err(format!("Please provide header.source; req = {:?}", h));
    }
    Ok(())
}

fn header_of_revision(r: i64) -> mpb::ResponseHeader {
    let mut h = mpb::ResponseHeader::default();
    h.set_revision(r);
    h
}

impl PdMocker for MetaStorage {
    fn meta_store_get(&self, req: mpb::GetRequest) -> Option<super::Result<mpb::GetResponse>> {
        if let Err(err) = check_header(req.get_header()) {
            return Some(Err(err));
        }

        let store = self.store.lock().unwrap();
        let key = if req.get_range_end().is_empty() {
            Keys::Key(MetaKey(req.get_key().to_vec()))
        } else {
            Keys::Range(
                MetaKey(req.get_key().to_vec()),
                MetaKey(req.get_range_end().to_vec()),
            )
        };
        let (items, rev) = store.get_key(key);
        let mut resp = mpb::GetResponse::new();
        resp.set_kvs(items.into_iter().map(convert_kv).collect());
        resp.set_header(header_of_revision(rev));
        Some(Ok(resp))
    }

    fn meta_store_put(&self, mut req: mpb::PutRequest) -> Option<super::Result<mpb::PutResponse>> {
        if let Err(err) = check_header(req.get_header()) {
            return Some(Err(err));
        }

        let mut store = self.store.lock().unwrap();
        block_on(store.set(KeyValue(MetaKey(req.take_key()), req.take_value()))).unwrap();
        Some(Ok(Default::default()))
    }

    fn meta_store_delete(
        &self,
        req: mpb::DeleteRequest,
    ) -> Option<super::Result<mpb::DeleteResponse>> {
        if let Err(err) = check_header(req.get_header()) {
            return Some(Err(err));
        }

        let mut store = self.store.lock().unwrap();
        block_on(store.delete(Keys::Key(MetaKey(req.get_key().into())))).unwrap();
        Some(Ok(Default::default()))
    }

    fn meta_store_watch(
        &self,
        req: mpb::WatchRequest,
        mut sink: grpcio::ServerStreamingSink<mpb::WatchResponse>,
        ctx: &grpcio::RpcContext<'_>,
    ) -> bool {
        if let Err(err) = check_header(req.get_header()) {
            ctx.spawn(async move {
                sink.fail(RpcStatus::with_message(
                    RpcStatusCode::INVALID_ARGUMENT,
                    err,
                ))
                .await
                .unwrap()
            });
            return true;
        }

        let mut store = self.store.lock().unwrap();
        let key = if req.get_range_end().is_empty() {
            Keys::Key(MetaKey(req.get_key().to_vec()))
        } else {
            Keys::Range(
                MetaKey(req.get_key().to_vec()),
                MetaKey(req.get_range_end().to_vec()),
            )
        };
        let mut watcher =
            block_on(store.watch(key, req.get_start_revision())).expect("should be infallible");
        ctx.spawn(async move {
            while let Some(x) = watcher.next().await {
                let mut event = mpb::Event::new();
                event.set_kv(convert_kv(x.pair.clone()));
                match x.kind {
                    KvEventType::Put => event.set_type(mpb::event::EventType::Put),
                    KvEventType::Delete => {
                        event.set_type(mpb::event::EventType::Delete);
                        event.set_prev_kv(convert_kv(x.pair));
                    }
                }

                let mut resp = mpb::WatchResponse::default();
                resp.set_events(vec![event].into());
                sink.send((resp, Default::default())).await.unwrap();

                #[cfg(feature = "failpoints")]
                {
                    use futures::executor::block_on;
                    let cli_clone = cli.clone();
                    fail_point!("watch_meta_storage_return", |_| {
                        block_on(async move { cli_clone.lock().await.clear_subs() });
                        watcher.close();
                    });
                }
            }
        });
        true
    }
}
