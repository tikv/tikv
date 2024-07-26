// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

pub mod profile;
pub mod vendored_utils;

use std::{
    error::Error as StdError,
    marker::PhantomData,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    str::{self, FromStr},
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_stream::stream;
use collections::HashMap;
use engine_store_ffi::ffi::interfaces_ffi::{EngineStoreServerHelper, HttpRequestStatus};
use engine_traits::KvEngine;
use futures::{
    compat::{Compat01As03, Stream01CompatExt},
    future::{ok, poll_fn},
    prelude::*,
};
use hyper::{
    self, header,
    server::{
        accept::Accept,
        conn::{AddrIncoming, AddrStream},
        Builder as HyperBuilder,
    },
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server, StatusCode,
};
use online_config::OnlineConfig;
use openssl::{
    ssl::{Ssl, SslAcceptor, SslContext, SslFiletype, SslMethod, SslVerifyMode},
    x509::X509,
};
use pin_project::pin_project;
use profile::{
    activate_heap_profile, deactivate_heap_profile, jeprof_heap_profile, list_heap_profiles,
    read_file, start_one_cpu_profile, start_one_heap_profile,
};
use raftstore::store::{transport::CasualRouter, CasualMessage};
use regex::Regex;
use security::{self, SecurityConfig};
use serde_json::Value;
use tikv::{
    config::{ConfigController, LogLevel},
    server::Result,
};
use tikv_util::{
    error, logger::set_log_level, metrics::dump, sys::thread::ThreadBuildWrapper,
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    runtime::{Builder, Handle, Runtime},
    sync::oneshot::{self, Receiver, Sender},
};
use tokio_openssl::SslStream;

static TIMER_CANCELED: &str = "tokio timer canceled";

#[cfg(feature = "failpoints")]
static MISSING_NAME: &[u8] = b"Missing param name";
#[cfg(feature = "failpoints")]
static MISSING_ACTIONS: &[u8] = b"Missing param actions";
#[cfg(feature = "failpoints")]
static FAIL_POINTS_REQUEST_PATH: &str = "/fail";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct LogLevelRequest {
    pub log_level: LogLevel,
}

pub struct StatusServer<E, R> {
    engine_store_server_helper: &'static EngineStoreServerHelper,
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    cfg_controller: ConfigController,
    router: R,
    security_config: Arc<SecurityConfig>,
    store_path: PathBuf,
    _snap: PhantomData<E>,
}

impl StatusServer<(), ()> {
    #[cfg_attr(not(target_arch = "x86_64"), allow(dead_code))]
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

    #[cfg(target_arch = "x86_64")]
    fn frames_post_processor() -> impl Fn(&mut pprof::Frames) {
        move |frames| {
            let name = Self::extract_thread_name(&frames.thread_name);
            frames.thread_name = name;
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
}

impl<E, R> StatusServer<E, R>
where
    E: 'static,
    R: 'static + Send,
{
    pub fn new(
        engine_store_server_helper: &'static EngineStoreServerHelper,
        status_thread_pool_size: usize,
        cfg_controller: ConfigController,
        security_config: Arc<SecurityConfig>,
        router: R,
        store_path: PathBuf,
    ) -> Result<Self> {
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(status_thread_pool_size)
            .thread_name("status-server")
            .with_sys_and_custom_hooks(
                || debug!("Proxy's Status server started"),
                || debug!("stopping status server"),
            )
            .build()?;

        let (tx, rx) = oneshot::channel::<()>();
        Ok(StatusServer {
            engine_store_server_helper,
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            cfg_controller,
            router,
            security_config,
            store_path,
            _snap: PhantomData,
        })
    }

    fn list_heap_prof(_req: Request<Body>) -> hyper::Result<Response<Body>> {
        let profiles = match list_heap_profiles() {
            Ok(s) => s,
            Err(e) => return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e)),
        };

        let text = profiles
            .into_iter()
            .map(|(f, ct)| format!("{}\t\t{}", f, ct))
            .collect::<Vec<_>>()
            .join("\n")
            .into_bytes();

        let response = Response::builder()
            .header("Content-Type", mime::TEXT_PLAIN.to_string())
            .header("Content-Length", text.len())
            .body(text.into())
            .unwrap();
        Ok(response)
    }

    async fn activate_heap_prof(
        req: Request<Body>,
        store_path: PathBuf,
    ) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let interval: u64 = match query_pairs.get("interval") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            },
            None => 60,
        };

        let interval = Duration::from_secs(interval);
        let period = GLOBAL_TIMER_HANDLE
            .interval(Instant::now() + interval, interval)
            .compat()
            .map_ok(|_| ())
            .map_err(|_| TIMER_CANCELED.to_owned())
            .into_stream();
        let (tx, rx) = oneshot::channel();
        let callback = move || tx.send(()).unwrap_or_default();
        let res = Handle::current().spawn(activate_heap_profile(period, store_path, callback));
        if rx.await.is_ok() {
            let msg = "activate heap profile success";
            Ok(make_response(StatusCode::OK, msg))
        } else {
            let errmsg = format!("{:?}", res.await);
            Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, errmsg))
        }
    }

    fn deactivate_heap_prof(_req: Request<Body>) -> hyper::Result<Response<Body>> {
        let body = if deactivate_heap_profile() {
            "deactivate heap profile success"
        } else {
            "no heap profile is running"
        };
        Ok(make_response(StatusCode::OK, body))
    }

    #[allow(dead_code)]
    async fn dump_heap_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let use_jeprof = query_pairs.get("jeprof").map(|x| x.as_ref()) == Some("true");

        let result = if let Some(name) = query_pairs.get("name") {
            if use_jeprof {
                jeprof_heap_profile(name)
            } else {
                read_file(name)
            }
        } else {
            let mut seconds = 10;
            if let Some(s) = query_pairs.get("seconds") {
                match s.parse() {
                    Ok(val) => seconds = val,
                    Err(_) => {
                        let errmsg = "request should have seconds argument".to_owned();
                        return Ok(make_response(StatusCode::BAD_REQUEST, errmsg));
                    }
                }
            }
            let timer = GLOBAL_TIMER_HANDLE.delay(Instant::now() + Duration::from_secs(seconds));
            let end = Compat01As03::new(timer)
                .map_err(|_| TIMER_CANCELED.to_owned())
                .into_future();
            start_one_heap_profile(end, use_jeprof).await
        };

        match result {
            Ok(body) => {
                info!("dump or get heap profile successfully");
                let mut response = Response::builder()
                    .header("X-Content-Type-Options", "nosniff")
                    .header("Content-Disposition", "attachment; filename=\"profile\"")
                    .header("Content-Length", body.len());
                response = if use_jeprof {
                    response.header("Content-Type", mime::IMAGE_SVG.to_string())
                } else {
                    response.header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                };
                Ok(response.body(body.into()).unwrap())
            }
            Err(e) => {
                info!("dump or get heap profile fail: {}", e);
                Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        }
    }

    async fn get_config(
        req: Request<Body>,
        cfg_controller: &ConfigController,
        engine_store_server_helper: &'static EngineStoreServerHelper,
    ) -> hyper::Result<Response<Body>> {
        let mut full = false;
        if let Some(query) = req.uri().query() {
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            full = match query_pairs.get("full") {
                Some(val) => match val.parse() {
                    Ok(val) => val,
                    Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
                },
                None => false,
            };
        }
        let encode_res = if full {
            // Get all config
            serde_json::to_string(&cfg_controller.get_current())
        } else {
            // Filter hidden config
            serde_json::to_string(&cfg_controller.get_current().get_encoder())
        };

        let engine_store_config = engine_store_server_helper.get_config(full);
        let engine_store_config =
            unsafe { String::from_utf8_unchecked(engine_store_config) }.parse::<toml::Value>();

        let engine_store_config = match engine_store_config {
            Ok(c) => serde_json::to_string(&c),
            Err(_) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error: fail to parse config from engine-store",
                ));
            }
        };

        Ok(match (encode_res, engine_store_config) {
            (Ok(json), Ok(store_config)) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    "{{\"raftstore-proxy\":{},\"engine-store\":{}}}",
                    json, store_config,
                )))
                .unwrap(),
            _ => StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error",
            ),
        })
    }

    async fn update_config(
        cfg_controller: ConfigController,
        req: Request<Body>,
    ) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;
        Ok(match decode_json(&body) {
            Ok(change) => match cfg_controller.update(change) {
                Err(e) => {
                    if let Some(e) = e.downcast_ref::<std::io::Error>() {
                        make_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!(
                                "config changed, but failed to persist change due to err: {:?}",
                                e
                            ),
                        )
                    } else {
                        make_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to update, error: {:?}", e),
                        )
                    }
                }
                Ok(_) => {
                    let mut resp = Response::default();
                    *resp.status_mut() = StatusCode::OK;
                    resp
                }
            },
            Err(e) => make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode, error: {:?}", e),
            ),
        })
    }

    #[cfg(target_arch = "x86_64")]
    pub async fn dump_rsprof(seconds: u64, frequency: i32) -> pprof::Result<pprof::Report> {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(frequency)
            .blocklist(&["libc", "libgcc", "pthread"])
            .build()?;
        info!(
            "start profiling {} seconds with frequency {} /s",
            seconds, frequency
        );
        let timer = GLOBAL_TIMER_HANDLE.clone();
        let _ = Compat01As03::new(timer.delay(Instant::now() + Duration::from_secs(seconds))).await;
        guard
            .report()
            .frames_post_processor(StatusServer::frames_post_processor())
            .build()
    }

    async fn update_config_from_toml_file(
        cfg_controller: ConfigController,
        _req: Request<Body>,
    ) -> hyper::Result<Response<Body>> {
        match cfg_controller.update_from_toml_file() {
            Err(e) => Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to update, error: {:?}", e),
            )),
            Ok(_) => {
                let mut resp = Response::default();
                *resp.status_mut() = StatusCode::OK;
                Ok(resp)
            }
        }
    }

    pub async fn dump_cpu_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = req.uri().query().unwrap_or("");
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();

        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => return Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
            },
            None => 99, /* Default frequency of sampling. 99Hz to avoid coincide with special
                         * periods */
        };

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        let output_protobuf = req.headers().get("Content-Type") == Some(&prototype_content_type);

        let timer = GLOBAL_TIMER_HANDLE.delay(Instant::now() + Duration::from_secs(seconds));
        let end = async move {
            Compat01As03::new(timer)
                .await
                .map_err(|_| TIMER_CANCELED.to_owned())
        };
        match start_one_cpu_profile(end, frequency, output_protobuf).await {
            Ok(body) => {
                info!("dump cpu profile successfully");
                let mut response = Response::builder()
                    .header(
                        "Content-Disposition",
                        "attachment; filename=\"cpu_profile\"",
                    )
                    .header("Content-Length", body.len());
                response = if output_protobuf {
                    response.header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                } else {
                    response.header("Content-Type", mime::IMAGE_SVG.to_string())
                };
                Ok(response.body(body.into()).unwrap())
            }
            Err(e) => {
                info!("dump cpu profile fail: {}", e);
                Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e))
            }
        }
    }

    async fn change_log_level(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let mut body = Vec::new();
        req.into_body()
            .try_for_each(|bytes| {
                body.extend(bytes);
                ok(())
            })
            .await?;

        let log_level_request: std::result::Result<LogLevelRequest, serde_json::error::Error> =
            serde_json::from_slice(&body);

        match log_level_request {
            Ok(req) => {
                set_log_level(req.log_level.into());
                Ok(Response::new(Body::empty()))
            }
            Err(err) => Ok(make_response(StatusCode::BAD_REQUEST, err.to_string())),
        }
    }

    pub fn stop(self) {
        let _ = self.tx.send(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(3));
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }
}

impl<E, R> StatusServer<E, R>
where
    E: KvEngine,
    R: 'static + Send + CasualRouter<E> + Clone,
{
    pub async fn dump_region_meta(req: Request<Body>, router: R) -> hyper::Result<Response<Body>> {
        lazy_static! {
            static ref REGION: Regex = Regex::new(r"/region/(?P<id>\d+)").unwrap();
        }

        fn not_found(msg: impl Into<Body>) -> hyper::Result<Response<Body>> {
            Ok(make_response(StatusCode::NOT_FOUND, msg))
        }

        let cap = match REGION.captures(req.uri().path()) {
            Some(cap) => cap,
            None => return not_found(format!("path {} not found", req.uri().path())),
        };

        let id: u64 = match cap["id"].parse() {
            Ok(id) => id,
            Err(err) => {
                return Ok(make_response(
                    StatusCode::BAD_REQUEST,
                    format!("invalid region id: {}", err),
                ));
            }
        };
        let (tx, rx) = oneshot::channel();
        match router.send(
            id,
            CasualMessage::AccessPeer(Box::new(move |peer| {
                if let Err(meta) = tx.send(peer) {
                    error!("receiver dropped, region meta: {:?}", meta)
                }
            })),
        ) {
            Ok(_) => (),
            Err(raftstore::Error::RegionNotFound(_)) => {
                return not_found(format!("region({}) not found", id));
            }
            Err(err) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("channel pending or disconnect: {}", err),
                ));
            }
        }

        let meta = match rx.await {
            Ok(meta) => meta,
            Err(_) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "query cancelled",
                ));
            }
        };

        let body = match serde_json::to_vec(&meta) {
            Ok(body) => body,
            Err(err) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("fails to json: {}", err),
                ));
            }
        };
        match Response::builder()
            .header("content-type", "application/json")
            .body(hyper::Body::from(body))
        {
            Ok(resp) => Ok(resp),
            Err(err) => Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to build response: {}", err),
            )),
        }
    }

    pub async fn handle_http_request(
        req: Request<Body>,
        engine_store_server_helper: &'static EngineStoreServerHelper,
    ) -> hyper::Result<Response<Body>> {
        let (head, body) = req.into_parts();
        let body = hyper::body::to_bytes(body).await;

        match body {
            Ok(s) => {
                let res = engine_store_server_helper.handle_http_request(
                    head.uri.path(),
                    head.uri.query(),
                    &s,
                );
                if res.status != HttpRequestStatus::Ok {
                    return Ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "engine-store fails to build response".to_string(),
                    ));
                }

                let data = res.res.view.to_slice().to_vec();

                match Response::builder().body(hyper::Body::from(data)) {
                    Ok(resp) => Ok(resp),
                    Err(err) => Ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("fails to build response: {}", err),
                    )),
                }
            }
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to build response: {}", err),
            )),
        }
    }

    fn start_serve<I, C>(&mut self, builder: HyperBuilder<I>)
    where
        I: Accept<Conn = C, Error = std::io::Error> + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        C: ServerConnection,
    {
        let security_config = self.security_config.clone();
        let cfg_controller = self.cfg_controller.clone();
        let router = self.router.clone();
        let engine_store_server_helper = self.engine_store_server_helper;
        let store_path = self.store_path.clone();
        // Start to serve.
        let server = builder.serve(make_service_fn(move |conn: &C| {
            let x509 = conn.get_x509();
            let security_config = security_config.clone();
            let cfg_controller = cfg_controller.clone();
            let router = router.clone();
            let store_path = store_path.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let x509 = x509.clone();
                    let security_config = security_config.clone();
                    let cfg_controller = cfg_controller.clone();
                    let router = router.clone();
                    let store_path = store_path.clone();
                    async move {
                        let path = req.uri().path().to_owned();
                        let method = req.method().to_owned();

                        #[cfg(feature = "failpoints")]
                        {
                            if path.starts_with(FAIL_POINTS_REQUEST_PATH) {
                                return handle_fail_points_request(req).await;
                            }
                        }

                        // 1. POST "/config" will modify the configuration of TiKV.
                        // 2. GET "/region" will get start key and end key. These keys could be
                        // actual user data since in some cases the data
                        // itself is stored in the key.
                        let should_check_cert = !matches!(
                            (&method, path.as_ref()),
                            (&Method::GET, "/metrics")
                                | (&Method::GET, "/status")
                                | (&Method::GET, "/config")
                                | (&Method::GET, "/debug/pprof/profile")
                        );

                        if should_check_cert && !check_cert(security_config, x509) {
                            return Ok(make_response(
                                StatusCode::FORBIDDEN,
                                "certificate role error",
                            ));
                        }

                        match (method, path.as_ref()) {
                            (Method::GET, "/metrics") => Ok(Response::new(
                                dump(cfg_controller.get_current().server.simplify_metrics).into(),
                            )),
                            (Method::GET, "/status") => Ok(Response::default()),
                            (Method::GET, "/debug/pprof/heap_list") => Self::list_heap_prof(req),
                            (Method::GET, "/debug/pprof/heap_activate") => {
                                Self::activate_heap_prof(req, store_path).await
                            }
                            (Method::GET, "/debug/pprof/heap_deactivate") => {
                                Self::deactivate_heap_prof(req)
                            }
                            (Method::GET, "/debug/pprof/heap") => {
                                Self::dump_heap_prof_to_resp(req).await
                            }
                            (Method::GET, "/config") => {
                                Self::get_config(req, &cfg_controller, engine_store_server_helper)
                                    .await
                            }
                            (Method::POST, "/config") => {
                                Self::update_config(cfg_controller.clone(), req).await
                            }
                            // This interface is used for configuration file hosting scenarios,
                            // TiKV will not update configuration files, and this interface will
                            // silently ignore configration items that cannot be updated online,
                            // hand it over to the hosting platform for processing.
                            (Method::PUT, "/config/reload") => {
                                Self::update_config_from_toml_file(cfg_controller.clone(), req)
                                    .await
                            }
                            (Method::GET, "/debug/pprof/profile") => {
                                Self::dump_cpu_prof_to_resp(req).await
                            }
                            (Method::GET, "/debug/fail_point") => {
                                info!("debug fail point API start");
                                fail_point!("debug_fail_point");
                                info!("debug fail point API finish");
                                Ok(Response::default())
                            }
                            (Method::GET, path) if path.starts_with("/region") => {
                                Self::dump_region_meta(req, router).await
                            }
                            (Method::PUT, path) if path.starts_with("/log-level") => {
                                Self::change_log_level(req).await
                            }

                            (Method::GET, path)
                                if engine_store_server_helper.check_http_uri_available(path) =>
                            {
                                Self::handle_http_request(req, engine_store_server_helper).await
                            }

                            _ => Ok(make_response(StatusCode::NOT_FOUND, "path not found")),
                        }
                    }
                }))
            }
        }));

        let rx = self.rx.take().unwrap();
        let graceful = server
            .with_graceful_shutdown(async move {
                let _ = rx.await;
            })
            .map_err(|e| error!("Status server error: {:?}", e));
        self.thread_pool.spawn(graceful);
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        let incoming = {
            let _enter = self.thread_pool.enter();
            AddrIncoming::bind(&addr)
        }?;
        self.addr = Some(incoming.local_addr());
        if !self.security_config.cert_path.is_empty()
            && !self.security_config.key_path.is_empty()
            && !self.security_config.ca_path.is_empty()
        {
            let tls_incoming = tls_incoming(self.security_config.clone(), incoming)?;
            let server = Server::builder(tls_incoming);
            self.start_serve(server);
        } else {
            let server = Server::builder(incoming);
            self.start_serve(server);
        }
        Ok(())
    }
}

// To unify TLS/Plain connection usage in start_serve function
trait ServerConnection {
    fn get_x509(&self) -> Option<X509>;
}

impl ServerConnection for SslStream<AddrStream> {
    fn get_x509(&self) -> Option<X509> {
        self.ssl().peer_certificate()
    }
}

impl ServerConnection for AddrStream {
    fn get_x509(&self) -> Option<X509> {
        None
    }
}

// Check if the peer's x509 certificate meets the requirements, this should
// be called where the access should be controlled.
//
// For now, the check only verifies the role of the peer certificate.
fn check_cert(security_config: Arc<SecurityConfig>, cert: Option<X509>) -> bool {
    // if `cert_allowed_cn` is empty, skip check and return true
    if !security_config.cert_allowed_cn.is_empty() {
        if let Some(x509) = cert {
            if let Some(name) = x509
                .subject_name()
                .entries_by_nid(openssl::nid::Nid::COMMONNAME)
                .next()
            {
                let data = name.data().as_slice();
                // Check common name in peer cert
                return security::match_peer_names(
                    &security_config.cert_allowed_cn,
                    std::str::from_utf8(data).unwrap(),
                );
            }
        }
        false
    } else {
        true
    }
}

fn tls_acceptor(security_config: &SecurityConfig) -> Result<SslAcceptor> {
    let mut acceptor = SslAcceptor::mozilla_modern(SslMethod::tls())?;
    acceptor.set_ca_file(&security_config.ca_path)?;
    acceptor.set_certificate_chain_file(&security_config.cert_path)?;
    acceptor.set_private_key_file(&security_config.key_path, SslFiletype::PEM)?;
    if !security_config.cert_allowed_cn.is_empty() {
        acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    }
    Ok(acceptor.build())
}

fn tls_incoming(
    security_config: Arc<SecurityConfig>,
    mut incoming: AddrIncoming,
) -> Result<impl Accept<Conn = SslStream<AddrStream>, Error = std::io::Error>> {
    let mut context = tls_acceptor(&security_config)?.into_context();
    let mut cert_last_modified_time = None;
    let mut handle_ssl_error = move |context: &mut SslContext| {
        match security_config.is_modified(&mut cert_last_modified_time) {
            Ok(true) => match tls_acceptor(&security_config) {
                Ok(acceptor) => {
                    *context = acceptor.into_context();
                }
                Err(e) => {
                    error!("Failed to reload TLS certificate: {}", e);
                }
            },
            Ok(false) => {
                // TLS certificate is not changed, do nothing
            }
            Err(e) => {
                error!("Failed to load certificate file metadata: {}", e);
            }
        }
    };
    let s = stream! {
        loop {
            let stream = match poll_fn(|cx| Pin::new(&mut incoming).poll_accept(cx)).await {
                Some(Ok(stream)) => stream,
                Some(Err(e)) => {
                    yield Err(e);
                    continue;
                }
                None => break,
            };
            let ssl = match Ssl::new(&context) {
                Ok(ssl) => ssl,
                Err(err) => {
                    error!("Status server error: {}", err);
                    handle_ssl_error(&mut context);
                    continue;
                }
            };
            match tokio_openssl::SslStream::new(ssl, stream) {
                Ok(mut ssl_stream) => match Pin::new(&mut ssl_stream).accept().await {
                    Err(_) => {
                        error!("Status server error: TLS handshake error");
                        handle_ssl_error(&mut context);
                        continue;
                    },
                    Ok(()) => {
                        yield Ok(ssl_stream);
                    },
                }
                Err(err) => {
                    error!("Status server error: {}", err);
                    handle_ssl_error(&mut context);
                    continue;
                }
            };
        }
    };
    Ok(TlsIncoming(s))
}

#[pin_project]
struct TlsIncoming<S>(#[pin] S);

impl<S> Accept for TlsIncoming<S>
where
    S: Stream<Item = std::io::Result<SslStream<AddrStream>>>,
{
    type Conn = SslStream<AddrStream>;
    type Error = std::io::Error;

    fn poll_accept(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::io::Result<Self::Conn>>> {
        self.project().0.poll_next(cx)
    }
}

// For handling fail points related requests
#[cfg(feature = "failpoints")]
async fn handle_fail_points_request(req: Request<Body>) -> hyper::Result<Response<Body>> {
    let path = req.uri().path().to_owned();
    let method = req.method().to_owned();
    let fail_path = format!("{}/", FAIL_POINTS_REQUEST_PATH);
    let fail_path_has_sub_path: bool = path.starts_with(&fail_path);

    match (method, fail_path_has_sub_path) {
        (Method::PUT, true) => {
            let mut buf = Vec::new();
            req.into_body()
                .try_for_each(|bytes| {
                    buf.extend(bytes);
                    ok(())
                })
                .await?;
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            let actions = String::from_utf8(buf).unwrap_or_default();
            if actions.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_ACTIONS.into())
                    .unwrap());
            };

            if let Err(e) = fail::cfg(name.to_owned(), &actions) {
                return Ok(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(e.into())
                    .unwrap());
            }
            let body = format!("Added fail point with name: {}, actions: {}", name, actions);
            Ok(Response::new(body.into()))
        }
        (Method::DELETE, true) => {
            let (_, name) = path.split_at(fail_path.len());
            if name.is_empty() {
                return Ok(Response::builder()
                    .status(StatusCode::UNPROCESSABLE_ENTITY)
                    .body(MISSING_NAME.into())
                    .unwrap());
            };

            fail::remove(name);
            let body = format!("Deleted fail point with name: {}", name);
            Ok(Response::new(body.into()))
        }
        (Method::GET, _) => {
            // In this scope the path must be like /fail...(/...), which starts with
            // FAIL_POINTS_REQUEST_PATH and may or may not have a sub path
            // Now we return 404 when path is neither /fail nor /fail/
            if path != FAIL_POINTS_REQUEST_PATH && path != fail_path {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap());
            }

            // From here path is either /fail or /fail/, return lists of fail points
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            let list = list.join("\n");
            Ok(Response::new(list.into()))
        }
        _ => Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap()),
    }
}

// Decode different type of json value to string value
fn decode_json(
    data: &[u8],
) -> std::result::Result<std::collections::HashMap<String, String>, Box<dyn std::error::Error>> {
    let json: Value = serde_json::from_slice(data)?;
    if let Value::Object(map) = json {
        let mut dst = std::collections::HashMap::new();
        for (k, v) in map.into_iter() {
            let v = match v {
                Value::Bool(v) => format!("{}", v),
                Value::Number(v) => format!("{}", v),
                Value::String(v) => v,
                Value::Array(_) => return Err("array type are not supported".to_owned().into()),
                _ => return Err("wrong format".to_owned().into()),
            };
            dst.insert(k, v);
        }
        Ok(dst)
    } else {
        Err("wrong format".to_owned().into())
    }
}

fn make_response<T>(status_code: StatusCode, message: T) -> Response<Body>
where
    T: Into<Body>,
{
    Response::builder()
        .status(status_code)
        .body(message.into())
        .unwrap()
}
