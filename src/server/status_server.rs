// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::future::{ok, FutureResult};
use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, Future};
use hyper::service::service_fn;
use hyper::{self, Body, Method, Request, Response, Server, StatusCode};
use tokio_threadpool::{Builder, ThreadPool};

use std::net::SocketAddr;
use std::str::FromStr;

use super::Result;
use crate::util::metrics::dump;

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

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        // TODO: support TLS for the status server.
        let builder = Server::try_bind(&addr)?;

        // Create a status service.
        let service = |req: Request<Body>| -> FutureResult<Response<Body>, hyper::Error> {
            let mut response = Response::new(Body::empty());

            match (req.method(), req.uri().path()) {
                (&Method::GET, "/metrics") => {
                    *response.body_mut() = Body::from(dump());
                }
                (&Method::GET, "/status") => return ok(response),
                _ => {
                    *response.status_mut() = StatusCode::NOT_FOUND;
                }
            };
            ok(response)
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
