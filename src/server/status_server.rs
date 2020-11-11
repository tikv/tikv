// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::sync::oneshot::{Receiver, Sender};
use futures::{self, Future};
use hyper::service::service_fn;
use hyper::{self, Body, Method, Request, Response, Server, StatusCode};
#[cfg(target_os = "linux")]
use pprof;
#[cfg(target_os = "linux")]
use prost::Message;
#[cfg(target_os = "linux")]
use regex::Regex;
use tokio_threadpool::{Builder, ThreadPool};

use std::borrow::Cow;
use std::fmt::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::{error, result};

use super::Result;
use tikv_util::collections::HashMap;
use tikv_util::config::ReadableSize;
use tikv_util::metrics::dump;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

fn trace_to_json(buffer: &mut String, snapshot: &tikv_alloc::trace::MemoryTrace) {
    write!(buffer, r#""{}":"#, snapshot.id()).unwrap();
    if snapshot.children().is_empty() {
        buffer.push('"');
        ReadableSize(snapshot.size() as u64).round_and_format(buffer);
        buffer.push('"');
    } else {
        buffer.push('{');
        let mut total = 0;
        for c in snapshot.children() {
            trace_to_json(buffer, c);
            buffer.push(',');
            total += c.size();
        }
        buffer.push_str(r#""total":""#);
        ReadableSize(snapshot.size() as u64).round_and_format(buffer);
        if snapshot.size() > total {
            buffer.push_str(r#"","size":""#);
            ReadableSize((snapshot.size() - total) as u64).round_and_format(buffer);
        }
        buffer.push_str(r#""}"#);
    }
}

fn trace_to_flamegraph(
    buffer: &mut String,
    trace: &mut String,
    snapshot: &tikv_alloc::trace::MemoryTrace,
) {
    let origin_len = trace.len();
    if trace.is_empty() {
        write!(trace, "{}-", snapshot.id()).unwrap();
    } else {
        write!(trace, ";{}-", snapshot.id()).unwrap();
    }
    ReadableSize(snapshot.size() as u64).round_and_format(trace);
    let mut total = 0;
    for c in snapshot.children() {
        trace_to_flamegraph(buffer, trace, c);
        total += c.size();
    }
    // When merging frame, size will of children will also be counted.
    if snapshot.size() > total {
        writeln!(buffer, "{} {}", trace, snapshot.size() - total).unwrap();
    }
    trace.truncate(origin_len);
}

fn dump_memory_trace(
    snapshot: &tikv_alloc::trace::MemoryTrace,
    is_json: bool,
) -> result::Result<Vec<u8>, Box<dyn error::Error>> {
    let mut buffer = String::new();
    if is_json {
        buffer.push('{');
        trace_to_json(&mut buffer, snapshot);
        buffer.push('}');
        Ok(buffer.into_bytes())
    } else {
        let mut trace = String::new();
        trace_to_flamegraph(&mut buffer, &mut trace, &snapshot);
        let mut out = Vec::default();
        if let Err(e) = inferno::flamegraph::from_lines(
            &mut inferno::flamegraph::Options::default(),
            buffer.lines(),
            &mut out,
        ) {
            return Err(format!("failed to generate flamegraph: {:?}", e).into());
        }
        Ok(out)
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

    fn memory_trace_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Ok(StatusServer::err_response(
                    StatusCode::BAD_REQUEST,
                    "request should have the query part",
                ));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let is_json = match query_pairs.get("type") {
            Some(Cow::Borrowed("json")) => true,
            _ => false,
        };

        match dump_memory_trace(&tikv_alloc::trace::snapshot(), is_json) {
            Ok(buf) => {
                if is_json {
                    Ok(Response::builder()
                        .header("Content-Type", "application/json")
                        .body(Body::from(buf))
                        .unwrap())
                } else {
                    Ok(StatusServer::err_response(StatusCode::OK, buf))
                }
            }
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )),
        }
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        // TODO: support TLS for the status server.
        let builder = Server::try_bind(&addr)?;

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
                                {
                                    Self::dump_rsperf_to_resp(req)
                                }
                                #[cfg(not(target_os = "linux"))]
                                {
                                    Box::new(ok(Response::default()))
                                }
                            }
                            (Method::GET, "/debug/trace/memory") => {
                                Box::new(futures::future::result(Self::memory_trace_to_resp(req)))
                            }
                            _ => Box::new(ok(StatusServer::err_response(
                                StatusCode::NOT_FOUND,
                                "path not found",
                            ))),
                        }
                    },
                )
        });
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
