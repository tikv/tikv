// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::stream::Stream;
use futures::{self, Future};
use hyper::server::Builder as HyperBuilder;
use hyper::service::service_fn;
use hyper::{self, header, Body, Method, Request, Response, Server, StatusCode};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use openssl::x509::X509StoreContextRef;
use pprof;
use pprof::protos::Message;
use regex::Regex;
use tempfile::TempDir;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_openssl::SslAcceptorExt;
use tokio_sync::oneshot::{Receiver, Sender};
use tokio_tcp::TcpListener;
use tokio_threadpool::{Builder, ThreadPool};

use std::error::Error as StdError;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use super::Result;
use crate::config::ConfigController;
use tikv_alloc::error::ProfError;
use tikv_util::collections::HashMap;
use tikv_util::metrics::dump;
use tikv_util::security::{self, SecurityConfig};
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

#[cfg(feature = "failpoints")]
static MISSING_NAME: &[u8] = b"Missing param name";
#[cfg(feature = "failpoints")]
static MISSING_ACTIONS: &[u8] = b"Missing param actions";
#[cfg(feature = "failpoints")]
static FAIL_POINTS_REQUEST_PATH: &str = "/fail";

pub struct StatusServer {
    thread_pool: ThreadPool,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    cfg_controller: Option<ConfigController>,
}

impl StatusServer {
    pub fn new(status_thread_pool_size: usize, cfg_controller: ConfigController) -> Self {
        let thread_pool = Builder::new()
            .pool_size(status_thread_pool_size)
            .name_prefix("status-server-")
            .after_start(|| {
                debug!("Status server started");
            })
            .before_stop(|| {
                debug!("stopping status server");
            })
            .build();
        let (tx, rx) = tokio_sync::oneshot::channel::<()>();
        StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            cfg_controller: Some(cfg_controller),
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
                .then(
                    move |_| -> Box<dyn Future<Item = Vec<u8>, Error = ProfError> + Send> {
                        let tmp_dir = match TempDir::new() {
                            Ok(tmp_dir) => tmp_dir,
                            Err(e) => return Box::new(err(e.into())),
                        };
                        let os_path = tmp_dir.path().join("tikv_dump_profile").into_os_string();
                        let path = match os_path.into_string() {
                            Ok(path) => path,
                            Err(path) => return Box::new(err(ProfError::PathEncodingError(path))),
                        };

                        if let Err(e) = tikv_alloc::dump_prof(&path) {
                            return Box::new(err(e));
                        }
                        drop(guard);
                        Box::new(
                            tokio_fs::file::File::open(path)
                                .and_then(|file| {
                                    let buf: Vec<u8> = Vec::new();
                                    tokio_io::io::read_to_end(file, buf)
                                })
                                .and_then(move |(_, buf)| {
                                    drop(tmp_dir);
                                    ok(buf)
                                })
                                .map_err(|e| -> ProfError { e.into() }),
                        )
                    },
                )
        }))
    }

    pub fn dump_prof_to_resp(
        req: Request<Body>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Box::new(ok(StatusServer::err_response(
                    StatusCode::BAD_REQUEST,
                    "request should have the query part",
                )));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(_) => {
                    return Box::new(ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        "request should have seconds argument",
                    )));
                }
            },
            None => 10,
        };

        Box::new(
            Self::dump_prof(seconds)
                .and_then(|buf| {
                    let response = Response::builder()
                        .header("X-Content-Type-Options", "nosniff")
                        .header("Content-Disposition", "attachment; filename=\"profile\"")
                        .header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                        .header("Content-Length", buf.len())
                        .body(buf.into())
                        .unwrap();
                    ok(response)
                })
                .or_else(|err| {
                    ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    ))
                }),
        )
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

    fn get_config(
        cfg_controller: &ConfigController,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let res = match serde_json::to_string(cfg_controller.get_current()) {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
            ),
        };
        Box::new(ok(res))
    }

    fn update_config(
        cfg_controller: Arc<RwLock<ConfigController>>,
        req: Request<Body>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let res = req.into_body().concat2().and_then(move |body| {
            let res = match serde_json::from_slice(body.into_bytes().as_ref()) {
                Ok(change) => match cfg_controller.write().unwrap().update(change) {
                    Err(e) => StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("fail to update, error: {:?}", e),
                    ),
                    Ok(_) => {
                        let mut resp = Response::default();
                        *resp.status_mut() = StatusCode::OK;
                        resp
                    }
                },
                Err(_) => StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "fail to decode".to_owned(),
                ),
            };
            ok(res)
        });
        Box::new(res)
    }

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

    fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
        move |frames| {
            let name = Self::extract_thread_name(&frames.thread_name);
            frames.thread_name = name;
        }
    }

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
        // TODO: use future lock
        let cfg_controller = Arc::new(RwLock::new(self.cfg_controller.take().unwrap()));
        // Start to serve.
        let server = builder.serve(move || {
            let cfg_controller = cfg_controller.clone();
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
                            (Method::GET, "/debug/pprof/heap") => Self::dump_prof_to_resp(req),
                            (Method::GET, "/config") => {
                                Self::get_config(&cfg_controller.read().unwrap())
                            }
                            (Method::POST, "/config") => {
                                Self::update_config(cfg_controller.clone(), req)
                            }
                            (Method::GET, "/debug/pprof/profile") => Self::dump_rsperf_to_resp(req),
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

            if !security_config.cert_allowed_cn.is_empty() {
                let allowed_cn = security_config.cert_allowed_cn.clone();
                // The verification callback to check if the peer CN is allowed.
                let verify_cb = move |flag: bool, x509_ctx: &mut X509StoreContextRef| {
                    if !flag || x509_ctx.error_depth() != 0 {
                        return flag;
                    }
                    if let Some(chains) = x509_ctx.chain() {
                        if chains.len() != 0 {
                            if let Some(pattern) = chains
                                .get(0)
                                .unwrap()
                                .subject_name()
                                .entries_by_nid(openssl::nid::Nid::COMMONNAME)
                                .next()
                            {
                                let data = pattern.data().as_slice();
                                return security::match_peer_names(
                                    &allowed_cn,
                                    std::str::from_utf8(data).unwrap(),
                                );
                            }
                        }
                    }
                    false
                };
                // Request and require cert from client-side.
                acceptor.set_verify_callback(
                    SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
                    verify_cb,
                );
            }

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

// For handling fail points related requests
#[cfg(feature = "failpoints")]
fn handle_fail_points_request(
    req: Request<Body>,
) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
    let path = req.uri().path().to_owned();
    let method = req.method().to_owned();
    let fail_path = format!("{}/", FAIL_POINTS_REQUEST_PATH);
    let fail_path_has_sub_path: bool = path.starts_with(&fail_path);

    match (method, fail_path_has_sub_path) {
        (Method::PUT, true) => Box::new(req.into_body().concat2().map(move |chunk| {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap();
            };

            let actions = chunk.into_iter().collect::<Vec<u8>>();
            let actions = String::from_utf8(actions).unwrap();
            if actions.is_empty() {
                return Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_ACTIONS.into())
                    .unwrap();
            };

            if let Err(e) = fail::cfg(name.to_owned(), &actions) {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(e.into())
                    .unwrap();
            }
            let body = format!("Added fail point with name: {}, actions: {}", name, actions);
            Response::new(body.into())
        })),
        (Method::DELETE, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Box::new(ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap()));
            };

            fail::remove(name);
            let body = format!("Deleted fail point with name: {}", name);
            Box::new(ok(Response::new(body.into())))
        }
        (Method::GET, _) => {
            // In this scope the path must be like /fail...(/...), which starts with FAIL_POINTS_REQUEST_PATH and may or may not have a sub path
            // Now we return 404 when path is neither /fail nor /fail/
            if path != FAIL_POINTS_REQUEST_PATH && path != fail_path {
                return Box::new(ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()));
            }

            // From here path is either /fail or /fail/, return lists of fail points
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            let list = list.join("\n");
            Box::new(ok(Response::new(list.into())))
        }
        _ => Box::new(ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap())),
    }
}

#[cfg(test)]
mod tests {
    use futures::future::{lazy, Future};
    use futures::Stream;
    use hyper::client::HttpConnector;
    use hyper::{Body, Client, Method, Request, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use openssl::ssl::SslFiletype;
    use openssl::ssl::{SslConnector, SslMethod};

    use std::env;
    use std::path::PathBuf;

    use crate::config::{ConfigController, TiKvConfig};
    use crate::server::status_server::StatusServer;
    use test_util::new_security_cfg;
    use tikv_util::collections::HashSet;
    use tikv_util::security::SecurityConfig;

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(1, ConfigController::default());
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
    fn test_security_status_service_without_cn() {
        do_test_security_status_service(HashSet::default(), true);
    }

    #[test]
    fn test_security_status_service_with_cn() {
        let mut allowed_cn = HashSet::default();
        allowed_cn.insert("tikv-server".to_owned());
        do_test_security_status_service(allowed_cn, true);
    }

    #[test]
    fn test_security_status_service_with_cn_fail() {
        let mut allowed_cn = HashSet::default();
        allowed_cn.insert("invaild-cn".to_owned());
        do_test_security_status_service(allowed_cn, false);
    }

    #[test]
    fn test_config_endpoint() {
        let mut status_server = StatusServer::new(1, ConfigController::default());
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/config")
            .build()
            .unwrap();
        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            client
                .get(uri)
                .and_then(|resp| {
                    assert_eq!(resp.status(), StatusCode::OK);
                    resp.into_body().concat2()
                })
                .map(|body| {
                    let v = body.to_vec();
                    let resp_json = String::from_utf8_lossy(&v).to_string();
                    let cfg = TiKvConfig::default();
                    serde_json::to_string(&cfg)
                        .map(|cfg_json| {
                            assert_eq!(resp_json, cfg_json);
                        })
                        .expect("Could not convert TiKvConfig to string");
                })
                .map_err(|err| panic!("response status is not OK: {:?}", err))
        }));
        handle.wait().unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(1, ConfigController::default());
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let future_1_add_fail_point = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(list.len(), 1);
                    let list = list.join(";");
                    assert_eq!("test_fail_point_name=panic", list);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test add another fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/and_another_name")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            let future_2_add_fail_point = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(2, list.len());
                    let list = list.join(";");
                    assert!(list.contains("test_fail_point_name=panic"));
                    assert!(list.contains("and_another_name=panic"))
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test list fail points
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::GET;
            *req.uri_mut() = uri;

            let future_3_list_fail_points = client
                .request(req)
                .and_then(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                    res.into_body().concat2()
                })
                .map(|chunk| {
                    let body = chunk.iter().cloned().collect::<Vec<u8>>();
                    let body = String::from_utf8(body).unwrap();
                    assert!(body.contains("test_fail_point_name=panic"));
                    assert!(body.contains("and_another_name=panic"))
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            // test delete fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/test_fail_point_name")
                .build()
                .unwrap();
            let mut req = Request::default();
            *req.method_mut() = Method::DELETE;
            *req.uri_mut() = uri;

            let future_4_delete_fail_points = client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);

                    let list: Vec<String> = fail::list()
                        .into_iter()
                        .map(move |(name, actions)| format!("{}={}", name, actions))
                        .collect();
                    assert_eq!(1, list.len());
                    let list = list.join(";");
                    assert_eq!("and_another_name=panic", list);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                });

            future_1_add_fail_point
                .then(|_| future_2_add_fail_point)
                .then(|_| future_3_list_fail_points)
                .then(|_| future_4_delete_fail_points)
        }));

        handle.wait().unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints_can_trigger_fails() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(1, ConfigController::default());
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("return"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            client
                .request(req)
                .map(|res| {
                    assert_eq!(res.status(), StatusCode::OK);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));

        handle.wait().unwrap();
        status_server.stop();

        let true_only_if_fail_point_triggered = || {
            fail_point!("a_test_fail_name_nobody_else_is_using", |_| { true });
            false
        };
        assert!(true_only_if_fail_point_triggered());
    }

    #[cfg(not(feature = "failpoints"))]
    #[test]
    fn test_status_service_fail_endpoints_should_give_404_when_failpoints_are_disable() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(1, ConfigController::default());
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn_handle(lazy(move || {
            // test add fail point
            let uri = Uri::builder()
                .scheme("http")
                .authority(addr.as_str())
                .path_and_query("/fail/a_test_fail_name_nobody_else_is_using")
                .build()
                .unwrap();
            let mut req = Request::new(Body::from("panic"));
            *req.method_mut() = Method::PUT;
            *req.uri_mut() = uri;

            client
                .request(req)
                .map(|res| {
                    // without feature "failpoints", this PUT endpoint should return 404
                    assert_eq!(res.status(), StatusCode::NOT_FOUND);
                })
                .map_err(|err| {
                    panic!("response status is not OK: {:?}", err);
                })
        }));

        handle.wait().unwrap();
        status_server.stop();
    }

    #[test]
    fn test_extract_thread_name() {
        assert_eq!(
            &StatusServer::extract_thread_name("test-name-1"),
            "test-name"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("grpc-server-5"),
            "grpc-server"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("rocksdb:bg1000"),
            "rocksdb:bg"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("raftstore-1-100"),
            "raftstore"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap sender1000"),
            "snap-sender"
        );
        assert_eq!(
            &StatusServer::extract_thread_name("snap_sender1000"),
            "snap-sender"
        );
    }

    fn do_test_security_status_service(allowed_cn: HashSet<String>, expected: bool) {
        let mut status_server = StatusServer::new(1, ConfigController::default());
        let _ = status_server.start(
            "127.0.0.1:0".to_string(),
            &new_security_cfg(Some(allowed_cn)),
        );

        let mut connector = HttpConnector::new(1);
        connector.enforce_http(false);
        let mut ssl = SslConnector::builder(SslMethod::tls()).unwrap();
        ssl.set_certificate_file(
            format!(
                "{}",
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("components/test_util/data/server.pem")
                    .display()
            ),
            SslFiletype::PEM,
        )
        .unwrap();
        ssl.set_private_key_file(
            format!(
                "{}",
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("components/test_util/data/key.pem")
                    .display()
            ),
            SslFiletype::PEM,
        )
        .unwrap();
        ssl.set_ca_file(format!(
            "{}",
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("components/test_util/data/ca.pem")
                .display()
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

        if expected {
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
        } else {
            let handle = status_server.thread_pool.spawn_handle(lazy(move || {
                client
                    .get(uri)
                    .map(|_| {
                        panic!("response status should be err");
                    })
                    .map_err(|err| {
                        assert!(err.is_connect());
                    })
            }));
            let _ = handle.wait();
        }
        status_server.stop();
    }
}
