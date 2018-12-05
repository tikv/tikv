// Copyright 2018 PingCAP, Inc.
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

use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, future, Future};
use hyper::service::service_fn;
use hyper::{self, Body, Error as HttpError, Method, Request, Response, Server, StatusCode};
use prometheus::{self, Encoder, TextEncoder};
use tokio_threadpool::{Builder, ThreadPool};

use std::net::{AddrParseError, SocketAddr};
use std::str::FromStr;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        AddrParse(err: AddrParseError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
        Http(err: HttpError) {
            from()
            cause(err)
            display("{:?}", err)
            description(err.description())
        }
    }
}

pub struct HttpServer {
    thread_pool: ThreadPool,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
}

impl HttpServer {
    pub fn new(thread_pool_size: usize) -> Self {
        let thread_pool = Builder::new()
            .pool_size(thread_pool_size)
            .name_prefix("tikv-http-server-")
            .after_start(|| {
                info!("HTTP server started");
            })
            .before_stop(|| {
                info!("stopping HTTP server");
            })
            .build();
        let (tx, rx) = futures::sync::oneshot::channel::<()>();
        HttpServer {
            thread_pool,
            tx,
            rx: Some(rx),
        }
    }

    pub fn start(&mut self, http_addr: String) -> Result<(), Error> {
        let addr = SocketAddr::from_str(&http_addr)?;

        // TODO: support TLS for HTTP server.
        let builder = Server::try_bind(&addr)?;

        // Create an HTTP service.
        let service =
        |req: Request<Body>| -> Box<Future<Item = Response<Body>, Error = hyper::Error> + Send> {
            let mut response = Response::new(Body::empty());

            match (req.method(), req.uri().path()) {
                (&Method::GET, "/metrics") => {
                    let encoder = TextEncoder::new();
                    let metric_familys = prometheus::gather();
                    let mut buffer = vec![];
                    if let Err(e) = encoder.encode(&metric_familys, &mut buffer) {
                        error!("failed to get metrics: {:?}", e)
                    } else {
                        *response.body_mut() = Body::from(buffer);
                    }
                }
                _ => {
                    *response.status_mut() = StatusCode::NOT_FOUND;
                }
            };

            Box::new(future::ok(response))
        };

        // Start to serve.
        let graceful = builder
            .serve(move || service_fn(service))
            .with_graceful_shutdown(self.rx.take().unwrap())
            .map_err(|e| error!("HTTP server error: {:?}", e));
        self.thread_pool.spawn(graceful);
        Ok(())
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool
            .shutdown_now()
            .wait()
            .unwrap_or_else(|e| error!("failed to stop HTTP server, error: {:?}", e));
    }
}
