// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, Future};
use hyper::service::service_fn;
use hyper::{self, Body, Method, Request, Response, Server, StatusCode};
use tempdir::TempDir;
use tokio_threadpool::{Builder, ThreadPool};

use std::net::SocketAddr;
use std::str::FromStr;

use super::Result;
use tikv_alloc::error::ProfError;
use tikv_util::collections::HashMap;
use tikv_util::metrics::dump;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

mod profiler_guard {
    use tikv_alloc::error::ProfResult;
    use tikv_alloc::{activate_prof, deactivate_prof};

    use futures::{Future, Poll};
    use futures_locks::{Mutex, MutexFut, MutexGuard};

    lazy_static! {
        static ref PROFILER_MUTEX: Mutex<u32> = Mutex::new(0);
    }

    pub struct ProfGuard(MutexGuard<u32>);

    pub struct ProfLock(MutexFut<u32>);

    impl ProfLock {
        pub fn new() -> ProfResult<ProfLock> {
            let guard = PROFILER_MUTEX.lock();
            match activate_prof() {
                Ok(_) => Ok(ProfLock(guard)),
                Err(e) => Err(e),
            }
        }
    }

    impl Drop for ProfGuard {
        fn drop(&mut self) {
            match deactivate_prof() {
                _ => {} // TODO: handle error here
            }
        }
    }

    impl Future for ProfLock {
        type Item = ProfGuard;
        type Error = ();
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.0.poll().map(|item| item.map(|guard| ProfGuard(guard)))
        }
    }
}

pub struct StatusServer {
    thread_pool: ThreadPool,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
}

impl StatusServer {
    pub fn new(status_thread_pool_size: usize) -> Self {
        let thread_pool = Builder::new()
            .pool_size(status_thread_pool_size)
            .name_prefix("status-server-")
            .after_start(|| {
                info!("Status server started");
            })
            .before_stop(|| {
                info!("stopping status server");
            })
            .build();
        let (tx, rx) = futures::sync::oneshot::channel::<()>();
        StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
        }
    }

    pub fn dump_prof(seconds: u64) -> Box<dyn Future<Item = Vec<u8>, Error = ProfError> + Send> {
        let lock = match profiler_guard::ProfLock::new() {
            Err(e) => return Box::new(err(e)),
            Ok(lock) => lock,
        };
        info!("start memory profiling {} seconds", seconds);

        let timer = GLOBAL_TIMER_HANDLE.clone();
        Box::new(lock.then(move |guard| {
            timer
                .delay(std::time::Instant::now() + std::time::Duration::from_secs(seconds))
                .then(|_| match TempDir::new("") {
                    Ok(tmp_dir) => ok(tmp_dir),
                    Err(e) => err(e.into()),
                })
                .and_then(move |tmp_dir| {
                    let os_path = tmp_dir.path().join("tikv_dump_profile").into_os_string();
                    let path = os_path.into_string().unwrap();

                    tikv_alloc::dump_prof(Some(&path));
                    drop(guard);
                    tokio_fs::file::File::open(path)
                        .and_then(|file| {
                            let buf: Vec<u8> = Vec::new();
                            tokio_io::io::read_to_end(file, buf)
                        })
                        .and_then(move |(_, buf)| {
                            drop(tmp_dir);
                            ok(buf)
                        })
                        .map_err(|e| -> ProfError { e.into() })
                })
        }))
    }

    pub fn dump_prof_to_resp(
        req: Request<Body>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let url = url::Url::parse(&format!("http://host{}", req.uri().to_string())).unwrap(); // Add scheme and host to parse query
        let query_pairs: HashMap<_, _> = url.query_pairs().collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(_) => {
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(Body::empty())
                        .unwrap();
                    return Box::new(ok(response));
                }
            },
            None => {
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::empty())
                    .unwrap();
                return Box::new(ok(response));
            }
        };

        Box::new(
            Self::dump_prof(seconds)
                .and_then(|buf| {
                    let response = Response::builder().body(buf.into()).unwrap();
                    ok(response)
                })
                .or_else(|_| {
                    let response = Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::empty())
                        .unwrap();
                    ok(response)
                }),
        )
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        // TODO: support TLS for the status server.
        let builder = Server::try_bind(&addr)?;

        // Create a status service.
        let service = |req: Request<Body>| -> Box<dyn Future<Item=Response<Body>, Error=hyper::Error> + Send> {
            match (req.method(), req.uri().path()) {
                (&Method::GET, "/metrics") => {
                    let response = Response::builder().body(Body::from(dump())).unwrap();
                    Box::new(ok(response))
                }
                (&Method::GET, "/pprof/profile") => {
                    Self::dump_prof_to_resp(req)
                }
                (&Method::GET, "/status") => {
                    let response = Response::builder().body(Body::empty()).unwrap();
                    Box::new(ok(response))
                },
                _ => {
                    let response = Response::builder().status(StatusCode::NOT_FOUND).body(Body::empty()).unwrap();
                    Box::new(ok(response))
                }
            }
        };

        // Start to serve.
        let server = builder.serve(move || service_fn(service));
        self.addr = Some(server.local_addr());
        let graceful = server
            .with_graceful_shutdown(self.rx.take().unwrap())
            .map_err(|e| error!("Status server error: {:?}", e));
        self.thread_pool.spawn(graceful);
        Ok(())
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool
            .shutdown_now()
            .wait()
            .unwrap_or_else(|e| error!("failed to stop the status server, error: {:?}", e));
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use crate::server::status_server::StatusServer;
    use futures::future::{lazy, Future};
    use hyper::{Client, StatusCode, Uri};

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(1);
        let _ = status_server.start("127.0.0.1:0".to_string());
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/metrics")
            .build()
            .unwrap();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            client
                .get(uri)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));
        handle.wait().unwrap();
        status_server.stop();
    }
}
