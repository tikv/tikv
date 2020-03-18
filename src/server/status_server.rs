// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::ok;
use futures::stream::Stream;
use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, Future};
use hyper::server::Builder as HyperBuilder;
use hyper::service::service_fn;
use hyper::{self, Body, Method, Request, Response, Server, StatusCode};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
#[cfg(target_os = "linux")]
use pprof;
#[cfg(target_os = "linux")]
use prost::Message;
#[cfg(target_os = "linux")]
use regex::Regex;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_openssl::SslAcceptorExt;
use tokio_tcp::TcpListener;
use tokio_threadpool::{Builder, ThreadPool};

use std::error::Error as StdError;
use std::net::SocketAddr;
use std::str::FromStr;

use super::Result;
use tikv_util::metrics::dump;
use tikv_util::security::SecurityConfig;

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

    fn err_response<T>(status_code: StatusCode, message: T) -> Response<Body>
    where
        T: Into<Body>,
    {
        Response::builder()
            .status(status_code)
            .body(message.into())
            .unwrap()
    }

    #[cfg(target_os = "linux")]
    fn extract_thread_name(thread_name: &str) -> String {
        lazy_static! {
            static ref THREAD_NAME_RE: Regex =
                Regex::new(r"^(?P<thread_name>[a-z-_ :]+?)(-?\d)*$").unwrap();
            static ref THREAD_NAME_REPLACE_SEPERATOR_RE: Regex = Regex::new(r"[_ ]").unwrap();
        }

        THREAD_NAME_RE
            .captures(thread_name)
            .and_then(|cap| {
                cap.name("thread_name").map(|thread_name| {
                    THREAD_NAME_REPLACE_SEPERATOR_RE
                        .replace_all(thread_name.as_str(), "-")
                        .into_owned()
                })
            })
            .unwrap_or_else(|| thread_name.to_owned())
    }

    #[cfg(target_os = "linux")]
    fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
        move |frames| {
            let name = Self::extract_thread_name(&frames.thread_name);
            frames.thread_name = name;
        }
    }

    #[cfg(target_os = "linux")]
    pub fn dump_rsprof(
        seconds: u64,
        frequency: i32,
    ) -> Box<dyn Future<Item = pprof::Report, Error = pprof::Error> + Send> {
        match pprof::ProfilerGuard::new(frequency) {
            Ok(guard) => {
                info!(
                    "start profiling {} seconds with frequency {} /s",
                    seconds, frequency
                );

                let timer = GLOBAL_TIMER_HANDLE.clone();
                Box::new(
                    timer
                        .delay(std::time::Instant::now() + std::time::Duration::from_secs(seconds))
                        .then(
                            move |_| -> Box<
                                dyn Future<Item = pprof::Report, Error = pprof::Error> + Send,
                            > {
                                let _ = guard;
                                Box::new(
                                    match guard
                                        .report()
                                        .frames_post_processor(Self::frames_post_processor())
                                        .build()
                                    {
                                        Ok(report) => ok(report),
                                        Err(e) => err(e),
                                    },
                                )
                            },
                        ),
                )
            }
            Err(e) => Box::new(err(e)),
        }
    }

    #[cfg(target_os = "linux")]
    pub fn dump_rsperf_to_resp(
        req: Request<Body>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Box::new(ok(StatusServer::err_response(StatusCode::BAD_REQUEST, "")));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Box::new(ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    )));
                }
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Box::new(ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    )));
                }
            },
            None => 99, // Default frequency of sampling. 99Hz to avoid coincide with special periods
        };
        drop(query_pairs);

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        Box::new(
            Self::dump_rsprof(seconds, frequency)
                .and_then(move |report| {
                    let mut body: Vec<u8> = Vec::new();
                    if req.headers().get("Content-Type") == Some(&prototype_content_type) {
                        match report.pprof() {
                            Ok(profile) => match profile.encode(&mut body) {
                                Ok(()) => {
                                    info!("write report successfully");
                                    Box::new(ok(StatusServer::err_response(StatusCode::OK, body)))
                                }
                                Err(err) => Box::new(ok(StatusServer::err_response(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    err.to_string(),
                                ))),
                            },
                            Err(err) => Box::new(ok(StatusServer::err_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                err.to_string(),
                            ))),
                        }
                    } else {
                        match report.flamegraph(&mut body) {
                            Ok(_) => {
                                info!("write report successfully");
                                Box::new(ok(StatusServer::err_response(StatusCode::OK, body)))
                            }
                            Err(err) => Box::new(ok(StatusServer::err_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                err.to_string(),
                            ))),
                        }
                    }
                })
                .or_else(|err| {
                    ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    ))
                }),
        )
    }

    fn start_serve<I>(&mut self, builder: HyperBuilder<I>)
    where
        I: Stream + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Item: AsyncRead + AsyncWrite + Send + 'static,
    {
        // Start to serve.
        let server = builder.serve(move || {
            // Create a status service.
            service_fn(
                move |req: Request<Body>| -> Box<
                    dyn Future<Item = Response<Body>, Error = hyper::Error> + Send,
                > {
                    let path = req.uri().path().to_owned();
                    let method = req.method().to_owned();

                    #[cfg(feature = "failpoints")]
                        {
                            if path.starts_with(FAIL_POINTS_REQUEST_PATH) {
                                return handle_fail_points_request(req);
                            }
                        }

                    match (method, path.as_ref()) {
                        (Method::GET, "/metrics") => Box::new(ok(Response::new(dump().into()))),
                        (Method::GET, "/status") => Box::new(ok(Response::default())),
                        (Method::GET, "/debug/pprof/profile") => {
                            #[cfg(target_os = "linux")]
                                { Self::dump_rsperf_to_resp(req) }
                            #[cfg(not(target_os = "linux"))]
                                { Box::new(ok(Response::default())) }
                        }
                        _ => Box::new(ok(StatusServer::err_response(
                            StatusCode::NOT_FOUND,
                            "path not found",
                        ))),
                    }
                },
            )
        });

        let graceful = server
            .with_graceful_shutdown(self.rx.take().unwrap())
            .map_err(|e| error!("Status server error: {:?}", e));
        self.thread_pool.spawn(graceful);
    }

    pub fn start(&mut self, status_addr: String, security_config: &SecurityConfig) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        let tcp_listener = TcpListener::bind(&addr)?;
        self.addr = Some(tcp_listener.local_addr()?);

        if !security_config.cert_path.is_empty()
            && !security_config.key_path.is_empty()
            && !security_config.ca_path.is_empty()
        {
            let mut acceptor = SslAcceptor::mozilla_modern(SslMethod::tls())?;
            acceptor.set_ca_file(&security_config.ca_path)?;
            acceptor.set_certificate_chain_file(&security_config.cert_path)?;
            acceptor.set_private_key_file(&security_config.key_path, SslFiletype::PEM)?;
            let acceptor = acceptor.build();

            let tls_stream = tcp_listener
                .incoming()
                .and_then(move |stream| {
                    acceptor.accept_async(stream).then(|r| match r {
                        Ok(stream) => Ok(Some(stream)),
                        Err(e) => {
                            error!("failed to accept TLS connection"; "err" => ?e);
                            Ok(None)
                        }
                    })
                })
                .filter_map(|x| x);
            let server = Server::builder(tls_stream);
            self.start_serve(server);
        } else {
            let tcp_stream = tcp_listener.incoming();
            let server = Server::builder(tcp_stream);
            self.start_serve(server);
        }
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
    use hyper::client::HttpConnector;
    use hyper::{Body, Client, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use openssl::ssl::{SslConnector, SslMethod};

    use std::env;
    use std::path::PathBuf;

    use test_util::new_security_cfg;
    use tikv_util::security::SecurityConfig;

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(1);
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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

    #[test]
    fn test_security_status_service() {
        let mut status_server = StatusServer::new(1);
        let _ = status_server.start("127.0.0.1:0".to_string(), &new_security_cfg());
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let mut connector = HttpConnector::new(1);
        connector.enforce_http(false);
        let mut ssl = SslConnector::builder(SslMethod::tls()).unwrap();
        ssl.set_ca_file(format!(
            "{}",
            p.join("components/test_util/data/ca.pem").display()
        ))
        .unwrap();

        let ssl = HttpsConnector::with_connector(connector, ssl).unwrap();
        let client = Client::builder().build::<_, Body>(ssl);

        let uri = Uri::builder()
            .scheme("https")
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
