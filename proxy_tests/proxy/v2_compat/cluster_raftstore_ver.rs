// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{atomic::AtomicUsize, Arc, RwLock},
};

use hyper::{
    service::{make_service_fn, service_fn},
    Body, Response, Server,
};
use proxy_ffi::interfaces_ffi::RaftstoreVer;
use tokio::{runtime::Runtime, sync::oneshot, task::JoinHandle};

use crate::utils::v1::*;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq)]
enum ReturnState {
    V1,
    V2,
    C404,
    C403,
    TimeoutV1,
}

#[derive(Clone)]
pub struct SharedState {
    inner: Arc<RwLock<ReturnState>>,
}

impl SharedState {
    fn new(t: ReturnState) -> Self {
        Self {
            inner: Arc::new(RwLock::new(t)),
        }
    }
}

async fn handle_request(shared: SharedState) -> Result<Response<Body>, BoxError> {
    let x = shared.inner.read().unwrap().clone();
    match x {
        ReturnState::C403 => Ok(Response::builder()
            .status(403)
            .body("raft-kv".into())
            .unwrap()),
        ReturnState::C404 => Ok(Response::builder()
            .status(404)
            .body("raft-kv".into())
            .unwrap()),
        ReturnState::V1 => Ok(Response::new("raft-kv".into())),
        ReturnState::V2 => Ok(Response::new("partitioned-raft-kv".into())),
        ReturnState::TimeoutV1 => {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            Ok(Response::new("raft-kv".into()))
        }
    }
}

pub async fn serve(rx: tokio::sync::oneshot::Receiver<u64>, port: u16, state: SharedState) {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let state = state.clone();
    let make_svc = make_service_fn(move |_| {
        let state = state.clone();
        async move {
            // This is the request handler.
            Ok::<_, Infallible>(service_fn(move |_| {
                let state = state.clone();
                handle_request(state)
            }))
        }
    });

    let server = Server::bind(&addr).serve(make_svc);

    let sig = async move || {
        rx.await.unwrap();
    };
    let graceful = server.with_graceful_shutdown(sig());

    if let Err(e) = graceful.await {
        eprintln!("server error: {}", e);
    }
}

struct MockServer {
    rt: Runtime,
    tx: Vec<tokio::sync::oneshot::Sender<u64>>,
    res: Vec<SharedState>,
    handle: Vec<JoinHandle<()>>,
    index: Arc<AtomicUsize>,
    ports: Vec<u16>,
}

impl MockServer {
    fn new(ports: Vec<u16>) -> Self {
        let rt = Runtime::new().unwrap();
        let n = ports.len();
        let mut tx = vec![];
        let mut res = vec![];
        let mut handle = vec![];
        let index = Arc::new(AtomicUsize::new(0));
        for i in 0..n {
            let (tx1, rx1) = oneshot::channel();
            let res1 = SharedState::new(ReturnState::V2);
            let f1 = rt.spawn(serve(rx1, ports[i], res1.clone()));
            tx.push(tx1);
            res.push(res1);
            handle.push(f1);
        }
        Self {
            rt,
            tx,
            res,
            handle,
            index,
            ports,
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        let tx = std::mem::take(&mut self.tx);
        let handle = std::mem::take(&mut self.handle);
        for x in tx.into_iter() {
            x.send(1).unwrap()
        }
        for x in handle.into_iter() {
            self.rt.block_on(async { x });
        }
    }
}

#[test]
fn test_maybe_substitute_addr() {
    let b = || "http://111.111.111.111:100".to_string();
    let b2 = || "111.111.111.111:100".to_string();
    let b3 = || "tikv2:100".to_string();
    fn test_with_b(b: impl Fn() -> String + Clone, ba: &str) {
        let o = proxy_ffi::maybe_use_backup_addr(
            "http://tc-tikv-9.tc-tikv-peer.mutiple-rocksdb-btdpb.svc:20160/engine_type",
            b.clone(),
        );
        assert_eq!(o, None);
        let o = proxy_ffi::maybe_use_backup_addr("http://1.2.3.4:5/engine_type", b.clone());
        assert_eq!(o, None);
        let o = proxy_ffi::maybe_use_backup_addr("http://127.0.0.1/engine_type", b.clone());
        assert_eq!(o.unwrap(), format!("http://{}/engine_type", ba));
        let o = proxy_ffi::maybe_use_backup_addr("http://localhost:222/a", b.clone());
        assert_eq!(o.unwrap(), format!("http://{}:222/a", ba));
        let o = proxy_ffi::maybe_use_backup_addr("http://0.0.0.0:333/engine_type", b.clone());
        assert_eq!(o.unwrap(), format!("http://{}:333/engine_type", ba));
    }
    test_with_b(b, "111.111.111.111");
    test_with_b(b2, "111.111.111.111");
    test_with_b(b3, "tikv2");
}

#[test]
fn test_with_error_status_addr() {
    let mock_server = MockServer::new(vec![1111, 1112]);
    let (mut cluster_v1, _) = new_mock_cluster(1, 3);
    let index = mock_server.index.clone();
    cluster_v1.cluster_ext.pre_run_node_callback =
        Some(Box::new(move |cfg: &mut MixedClusterConfig| {
            cfg.server.labels.clear();
            match index.load(Ordering::Relaxed) {
                0 => cfg.server.status_addr = "error$#_string".to_string(),
                1 => cfg.server.status_addr = "".to_string(),
                _ => cfg.server.status_addr = "localhost:1119".to_string(),
            }
            index.fetch_add(1, Ordering::Relaxed);
        }));
    cluster_v1.run();
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
            assert_eq!(
                ffi.proxy.cluster_raftstore_version(),
                RaftstoreVer::Uncertain
            );
        });
    fail::cfg("proxy_fetch_cluster_version_retry", "return(5)").unwrap();
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
            assert_eq!(
                ffi.proxy.cluster_raftstore_version(),
                RaftstoreVer::Uncertain
            );
        });
    fail::remove("proxy_fetch_cluster_version_retry");
    cluster_v1.shutdown();
}

#[test]
fn test_with_tiflash() {
    let mock_server = MockServer::new(vec![1111, 1112]);
    let (mut cluster_v1, _) = new_mock_cluster(1, 2);
    let addrs = mock_server.ports.iter().map(|e| format!("127.0.0.1:{}", e));
    let status_addrs = Arc::new(addrs.collect::<Vec<_>>());
    let index = mock_server.index.clone();
    cluster_v1.cluster_ext.pre_run_node_callback =
        Some(Box::new(move |cfg: &mut MixedClusterConfig| {
            if index.load(Ordering::Relaxed) == 0 {
                cfg.server.labels.clear();
            } else {
                cfg.server
                    .labels
                    .insert("engine".to_string(), "tiflash".to_string());
            }
            cfg.server.status_addr = (*status_addrs)[index.load(Ordering::Relaxed)].to_string();
            index.fetch_add(1, Ordering::Relaxed);
        }));
    cluster_v1.run();

    // TiFlash will always output as V1, however, we should neglect that.

    *mock_server.res[0].inner.write().unwrap() = ReturnState::C403;
    // Node 1 is TiFlash node, its result will be neglected.
    *mock_server.res[1].inner.write().unwrap() = ReturnState::V2;
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
            assert_eq!(
                ffi.proxy.cluster_raftstore_version(),
                RaftstoreVer::Uncertain
            );
        });

    cluster_v1.shutdown();
}

#[test]
fn test_normal() {
    let mock_server = MockServer::new(vec![1111, 1112]);
    let (mut cluster_v1, _) = new_mock_cluster(1, 2);
    // Will switch from the ipv6 localhost address into store.get_addr().
    let addrs = mock_server.ports.iter().map(|e| format!("[::]:{}", e));
    let status_addrs = Arc::new(addrs.collect::<Vec<_>>());
    let index = mock_server.index.clone();
    cluster_v1.cluster_ext.pre_run_node_callback =
        Some(Box::new(move |cfg: &mut MixedClusterConfig| {
            cfg.server.labels.clear();
            cfg.server.status_addr = (*status_addrs)[index.load(Ordering::Relaxed)].to_string();
            index.fetch_add(1, Ordering::Relaxed);
        }));
    cluster_v1.run();

    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            assert_eq!(ffi.proxy.cluster_raftstore_version(), RaftstoreVer::V2);
        });

    *mock_server.res[0].inner.write().unwrap() = ReturnState::C403;
    *mock_server.res[1].inner.write().unwrap() = ReturnState::V1;
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
            assert_eq!(ffi.proxy.cluster_raftstore_version(), RaftstoreVer::V1);
        });

    *mock_server.res[0].inner.write().unwrap() = ReturnState::TimeoutV1;
    *mock_server.res[1].inner.write().unwrap() = ReturnState::V2;
    assert_eq!(
        *mock_server.res[0].inner.read().unwrap(),
        ReturnState::TimeoutV1
    );
    assert_eq!(*mock_server.res[1].inner.read().unwrap(), ReturnState::V2);
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(500);
            assert_eq!(ffi.proxy.cluster_raftstore_version(), RaftstoreVer::V2);
        });

    // All timeout result in uncertain state.
    *mock_server.res[0].inner.write().unwrap() = ReturnState::TimeoutV1;
    *mock_server.res[1].inner.write().unwrap() = ReturnState::TimeoutV1;
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(500);
            assert_eq!(
                ffi.proxy.cluster_raftstore_version(),
                RaftstoreVer::Uncertain
            );
        });

    // If returns 404, means the server is an old v1 TiKV which doesn't have this
    // service.
    *mock_server.res[0].inner.write().unwrap() = ReturnState::C404;
    *mock_server.res[1].inner.write().unwrap() = ReturnState::C404;
    cluster_v1
        .cluster_ext
        .iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
            assert_eq!(ffi.proxy.cluster_raftstore_version(), RaftstoreVer::V1);
        });

    cluster_v1.shutdown();
}
