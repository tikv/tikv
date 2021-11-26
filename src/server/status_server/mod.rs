// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error as StdError;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use async_stream::stream;
use engine_traits::KvEngine;
use futures::compat::Compat01As03;
use futures::executor::block_on;
use futures::future::{ok, poll_fn};
use futures::prelude::*;
use hyper::client::HttpConnector;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::server::Builder as HyperBuilder;
use hyper::service::{make_service_fn, service_fn};
use hyper::{self, header, Body, Client, Method, Request, Response, Server, StatusCode, Uri};
use hyper_openssl::HttpsConnector;
use openssl::ssl::{
    Ssl, SslAcceptor, SslConnector, SslConnectorBuilder, SslFiletype, SslMethod, SslVerifyMode,
};
use openssl::x509::X509;
use pin_project::pin_project;
use pprof::protos::Message;
use raftstore::store::{transport::CasualRouter, CasualMessage};
use regex::Regex;
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio_openssl::SslStream;

use collections::HashMap;
use online_config::OnlineConfig;
use pd_client::{RpcClient, REQUEST_RECONNECT_INTERVAL};
use security::{self, SecurityConfig};
use tikv_alloc::error::ProfError;
use tikv_util::logger::set_log_level;
use tikv_util::metrics::dump;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

use super::Result;
use crate::config::{log_level_serde, ConfigController};

pub mod region_meta;

mod profiler_guard {
    use tikv_alloc::error::ProfResult;
    use tikv_alloc::{activate_prof, deactivate_prof};

    use tokio::sync::{Mutex, MutexGuard};

    lazy_static! {
        static ref PROFILER_MUTEX: Mutex<u32> = Mutex::new(0);
    }

    pub struct ProfGuard(MutexGuard<'static, u32>);

    pub async fn new_prof() -> ProfResult<ProfGuard> {
        let guard = PROFILER_MUTEX.lock().await;
        match activate_prof() {
            Ok(_) => Ok(ProfGuard(guard)),
            Err(e) => Err(e),
        }
    }

    impl Drop for ProfGuard {
        fn drop(&mut self) {
            // TODO: handle error here
            let _ = deactivate_prof();
        }
    }
}

const COMPONENT_REQUEST_RETRY: usize = 5;

static COMPONENT: &str = "tikv";

#[cfg(feature = "failpoints")]
static MISSING_NAME: &[u8] = b"Missing param name";
#[cfg(feature = "failpoints")]
static MISSING_ACTIONS: &[u8] = b"Missing param actions";
#[cfg(feature = "failpoints")]
static FAIL_POINTS_REQUEST_PATH: &str = "/fail";

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct LogLevelRequest {
    #[serde(with = "log_level_serde")]
    pub log_level: slog::Level,
}

pub struct StatusServer<E, R> {
    engine_store_server_helper: &'static raftstore::engine_store_ffi::EngineStoreServerHelper,
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    advertise_addr: Option<String>,
    pd_client: Option<Arc<RpcClient>>,
    cfg_controller: ConfigController,
    router: R,
    security_config: Arc<SecurityConfig>,
    _snap: PhantomData<E>,
}

impl StatusServer<(), ()> {
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
        engine_store_server_helper: &'static raftstore::engine_store_ffi::EngineStoreServerHelper,
        status_thread_pool_size: usize,
        pd_client: Option<Arc<RpcClient>>,
        cfg_controller: ConfigController,
        security_config: Arc<SecurityConfig>,
        router: R,
    ) -> Result<Self> {
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(status_thread_pool_size)
            .thread_name("status-server")
            .on_thread_start(|| {
                debug!("Status server started");
            })
            .on_thread_stop(|| {
                debug!("stopping status server");
            })
            .build()?;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        Ok(StatusServer {
            engine_store_server_helper,
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            advertise_addr: None,
            pd_client,
            cfg_controller,
            router,
            security_config,
            _snap: PhantomData,
        })
    }

    pub async fn dump_prof(seconds: u64) -> std::result::Result<Vec<u8>, ProfError> {
        let guard = profiler_guard::new_prof().await?;
        info!("start memory profiling {} seconds", seconds);

        let timer = GLOBAL_TIMER_HANDLE.clone();
        let _ = Compat01As03::new(timer.delay(Instant::now() + Duration::from_secs(seconds))).await;
        let tmp_dir = TempDir::new()?;
        let os_path = tmp_dir.path().join("tikv_dump_profile").into_os_string();
        let path = os_path
            .into_string()
            .map_err(ProfError::PathEncodingError)?;
        tikv_alloc::dump_prof(&path)?;
        drop(guard);
        let mut file = tokio::fs::File::open(path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(buf)
    }

    pub async fn dump_prof_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
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
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(_) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        "request should have seconds argument",
                    ));
                }
            },
            None => 10,
        };

        match Self::dump_prof(seconds).await {
            Ok(buf) => {
                let response = Response::builder()
                    .header("X-Content-Type-Options", "nosniff")
                    .header("Content-Disposition", "attachment; filename=\"profile\"")
                    .header("Content-Type", mime::APPLICATION_OCTET_STREAM.to_string())
                    .header("Content-Length", buf.len())
                    .body(buf.into())
                    .unwrap();
                Ok(response)
            }
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string(),
            )),
        }
    }

    async fn get_config(
        req: Request<Body>,
        cfg_controller: &ConfigController,
        engine_store_server_helper: &'static raftstore::engine_store_ffi::EngineStoreServerHelper,
    ) -> hyper::Result<Response<Body>> {
        let mut full = false;
        if let Some(query) = req.uri().query() {
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();
            full = match query_pairs.get("full") {
                Some(val) => match val.parse() {
                    Ok(val) => val,
                    Err(err) => {
                        return Ok(StatusServer::err_response(
                            StatusCode::BAD_REQUEST,
                            err.to_string(),
                        ));
                    }
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
            Err(e) => {
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
                Err(e) => StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to update, error: {:?}", e),
                ),
                Ok(_) => {
                    let mut resp = Response::default();
                    *resp.status_mut() = StatusCode::OK;
                    resp
                }
            },
            Err(e) => StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to decode, error: {:?}", e),
            ),
        })
    }

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

    pub async fn dump_rsperf_to_resp(req: Request<Body>) -> hyper::Result<Response<Body>> {
        let query = match req.uri().query() {
            Some(query) => query,
            None => {
                return Ok(StatusServer::err_response(StatusCode::BAD_REQUEST, ""));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
        let seconds: u64 = match query_pairs.get("seconds") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    ));
                }
            },
            None => 10,
        };

        let frequency: i32 = match query_pairs.get("frequency") {
            Some(val) => match val.parse() {
                Ok(val) => val,
                Err(err) => {
                    return Ok(StatusServer::err_response(
                        StatusCode::BAD_REQUEST,
                        err.to_string(),
                    ));
                }
            },
            None => 99, // Default frequency of sampling. 99Hz to avoid coincide with special periods
        };

        let prototype_content_type: hyper::http::HeaderValue =
            hyper::http::HeaderValue::from_str("application/protobuf").unwrap();
        let report = match Self::dump_rsprof(seconds, frequency).await {
            Ok(report) => report,
            Err(err) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                ));
            }
        };

        let mut body: Vec<u8> = Vec::new();
        if req.headers().get("Content-Type") == Some(&prototype_content_type) {
            match report.pprof() {
                Ok(profile) => match profile.encode(&mut body) {
                    Ok(()) => {
                        info!("write report successfully");
                        Ok(StatusServer::err_response(StatusCode::OK, body))
                    }
                    Err(err) => Ok(StatusServer::err_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        err.to_string(),
                    )),
                },
                Err(err) => Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )),
            }
        } else {
            match report.flamegraph(&mut body) {
                Ok(_) => {
                    info!("write report successfully");
                    Ok(StatusServer::err_response(StatusCode::OK, body))
                }
                Err(err) => Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string(),
                )),
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
                set_log_level(req.log_level);
                Ok(Response::new(Body::empty()))
            }
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::BAD_REQUEST,
                err.to_string(),
            )),
        }
    }

    pub fn stop(mut self) {
        // unregister the status address to pd
        // self.unregister_addr();
        let _ = self.tx.send(());
        self.thread_pool.shutdown_background();
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }

    // Conveniently generate ssl connector according to SecurityConfig for https client
    // Return `None` if SecurityConfig is not set up.
    fn generate_ssl_connector(&self) -> Option<SslConnectorBuilder> {
        if !self.security_config.cert_path.is_empty()
            && !self.security_config.key_path.is_empty()
            && !self.security_config.ca_path.is_empty()
        {
            let mut ssl = SslConnector::builder(SslMethod::tls()).unwrap();
            ssl.set_ca_file(&self.security_config.ca_path).unwrap();
            ssl.set_certificate_file(&self.security_config.cert_path, SslFiletype::PEM)
                .unwrap();
            ssl.set_private_key_file(&self.security_config.key_path, SslFiletype::PEM)
                .unwrap();
            Some(ssl)
        } else {
            None
        }
    }

    fn register_addr(&mut self, advertise_addr: String) {
        if let Some(ssl) = self.generate_ssl_connector() {
            let mut connector = HttpConnector::new();
            connector.enforce_http(false);
            let https_conn = HttpsConnector::with_connector(connector, ssl).unwrap();
            self.register_addr_core(https_conn, advertise_addr);
        } else {
            self.register_addr_core(HttpConnector::new(), advertise_addr);
        }
    }

    fn register_addr_core<C>(&mut self, conn: C, advertise_addr: String)
    where
        C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
    {
        if self.pd_client.is_none() {
            return;
        }
        let pd_client = self.pd_client.as_ref().unwrap();
        let client = Client::builder().build::<_, Body>(conn);

        let json = {
            let mut body = std::collections::HashMap::new();
            body.insert("component".to_owned(), COMPONENT.to_owned());
            body.insert("addr".to_owned(), advertise_addr.clone());
            serde_json::to_string(&body).unwrap()
        };

        for _ in 0..COMPONENT_REQUEST_RETRY {
            for pd_addr in pd_client.get_leader().get_client_urls() {
                let client = client.clone();
                let uri: Uri = (pd_addr.to_owned() + "/pd/api/v1/component")
                    .parse()
                    .unwrap();
                let req = Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json.clone()))
                    .expect("construct post request failed");
                let req_handle = self
                    .thread_pool
                    .spawn(async move { client.request(req).await });

                match block_on(req_handle).unwrap() {
                    Ok(resp) if resp.status() == StatusCode::OK => {
                        self.advertise_addr = Some(advertise_addr);
                        return;
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        warn!("failed to register addr to pd"; "status code" => status.as_str(), "body" => ?resp.body());
                    }
                    Err(e) => warn!("failed to register addr to pd"; "error" => ?e),
                }
            }
            // refresh the pd leader
            if let Err(e) = pd_client.reconnect() {
                warn!("failed to reconnect pd client"; "err" => ?e);
                thread::sleep(REQUEST_RECONNECT_INTERVAL);
            }
        }
        warn!(
            "failed to register addr to pd after {} tries",
            COMPONENT_REQUEST_RETRY
        );
    }

    fn unregister_addr(&mut self) {
        if let Some(ssl) = self.generate_ssl_connector() {
            let mut connector = HttpConnector::new();
            connector.enforce_http(false);
            let https_conn = HttpsConnector::with_connector(connector, ssl).unwrap();
            self.unregister_addr_core(https_conn);
        } else {
            self.unregister_addr_core(HttpConnector::new());
        }
    }

    fn unregister_addr_core<C>(&mut self, conn: C)
    where
        C: hyper::client::connect::Connect + Clone + Send + Sync + 'static,
    {
        if self.pd_client.is_none() || self.advertise_addr.is_none() {
            return;
        }
        let advertise_addr = self.advertise_addr.as_ref().unwrap().to_owned();
        let pd_client = self.pd_client.as_ref().unwrap();
        let client = Client::builder().build::<_, Body>(conn);

        for _ in 0..COMPONENT_REQUEST_RETRY {
            for pd_addr in pd_client.get_leader().get_client_urls() {
                let client = client.clone();
                let uri: Uri = (pd_addr.to_owned()
                    + &format!("/pd/api/v1/component/{}/{}", COMPONENT, advertise_addr))
                    .parse()
                    .unwrap();
                let req = Request::delete(uri)
                    .body(Body::empty())
                    .expect("construct delete request failed");
                let req_handle = self
                    .thread_pool
                    .spawn(async move { client.request(req).await });

                match block_on(req_handle).unwrap() {
                    Ok(resp) if resp.status() == StatusCode::OK => {
                        self.advertise_addr = None;
                        return;
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        warn!("failed to unregister addr to pd"; "status code" => status.as_str(), "body" => ?resp.body());
                    }
                    Err(e) => warn!("failed to unregister addr to pd"; "error" => ?e),
                }
            }
            // refresh the pd leader
            if let Err(e) = pd_client.reconnect() {
                warn!("failed to reconnect pd client"; "err" => ?e);
                thread::sleep(REQUEST_RECONNECT_INTERVAL);
            }
        }
        warn!(
            "failed to unregister addr to pd after {} tries",
            COMPONENT_REQUEST_RETRY
        );
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

        fn err_resp(
            status_code: StatusCode,
            msg: impl Into<Body>,
        ) -> hyper::Result<Response<Body>> {
            Ok(StatusServer::err_response(status_code, msg))
        }

        fn not_found(msg: impl Into<Body>) -> hyper::Result<Response<Body>> {
            err_resp(StatusCode::NOT_FOUND, msg)
        }

        let cap = match REGION.captures(req.uri().path()) {
            Some(cap) => cap,
            None => return not_found(format!("path {} not found", req.uri().path())),
        };

        let id: u64 = match cap["id"].parse() {
            Ok(id) => id,
            Err(err) => {
                return err_resp(
                    StatusCode::BAD_REQUEST,
                    format!("invalid region id: {}", err),
                );
            }
        };
        let (tx, rx) = oneshot::channel();
        match router.send(
            id,
            CasualMessage::AccessPeer(Box::new(move |peer| {
                if let Err(meta) = tx.send(region_meta::RegionMeta::new(peer)) {
                    error!("receiver dropped, region meta: {:?}", meta)
                }
            })),
        ) {
            Ok(_) => (),
            Err(raftstore::Error::RegionNotFound(_)) => {
                return not_found(format!("region({}) not found", id));
            }
            Err(err) => {
                return err_resp(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("channel pending or disconnect: {}", err),
                );
            }
        }

        let meta = match rx.await {
            Ok(meta) => meta,
            Err(_) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "query cancelled",
                ));
            }
        };

        let body = match serde_json::to_vec(&meta) {
            Ok(body) => body,
            Err(err) => {
                return Ok(StatusServer::err_response(
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
            Err(err) => Ok(StatusServer::err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to build response: {}", err),
            )),
        }
    }

    pub async fn thread_stats(_req: Request<Body>) -> hyper::Result<Response<Body>> {
        Ok(Response::new(
            tikv_util::metrics::dump_thread_stats().into(),
        ))
    }

    pub async fn handle_http_request(
        req: Request<Body>,
        engine_store_server_helper: &'static raftstore::engine_store_ffi::EngineStoreServerHelper,
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
                if res.status != raftstore::engine_store_ffi::HttpRequestStatus::Ok {
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
        // Start to serve.
        let server = builder.serve(make_service_fn(move |conn: &C| {
            let x509 = conn.get_x509();
            let security_config = security_config.clone();
            let cfg_controller = cfg_controller.clone();
            let router = router.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let x509 = x509.clone();
                    let security_config = security_config.clone();
                    let cfg_controller = cfg_controller.clone();
                    let router = router.clone();
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
                        // 2. GET "/region" will get start key and end key. These keys could be actual
                        // user data since in some cases the data itself is stored in the key.
                        let should_check_cert = !matches!(
                            (&method, path.as_ref()),
                            (&Method::GET, "/metrics")
                                | (&Method::GET, "/status")
                                | (&Method::GET, "/config")
                                | (&Method::GET, "/debug/pprof/profile")
                        );

                        if should_check_cert && !check_cert(security_config, x509) {
                            return Ok(StatusServer::err_response(
                                StatusCode::FORBIDDEN,
                                "certificate role error",
                            ));
                        }

                        match (method, path.as_ref()) {
                            (Method::GET, "/metrics") => Ok(Response::new(dump().into())),
                            (Method::GET, "/status") => Ok(Response::default()),
                            (Method::GET, "/debug/pprof/heap") => {
                                Self::dump_prof_to_resp(req).await
                            }
                            (Method::GET, "/config") => {
                                Self::get_config(req, &cfg_controller, engine_store_server_helper)
                                    .await
                            }
                            (Method::POST, "/config") => {
                                Self::update_config(cfg_controller.clone(), req).await
                            }
                            (Method::GET, "/debug/pprof/profile") => {
                                Self::dump_rsperf_to_resp(req).await
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
                            (Method::GET, "/thread_stats") => Self::thread_stats(req).await,
                            (Method::PUT, path) if path.starts_with("/log-level") => {
                                Self::change_log_level(req).await
                            }

                            (Method::GET, path)
                                if engine_store_server_helper.check_http_uri_available(path) =>
                            {
                                Self::handle_http_request(req, engine_store_server_helper).await
                            }

                            _ => Ok(StatusServer::err_response(
                                StatusCode::NOT_FOUND,
                                "path not found",
                            )),
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

    pub fn start(&mut self, status_addr: String, advertise_status_addr: String) -> Result<()> {
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
            let mut acceptor = SslAcceptor::mozilla_modern(SslMethod::tls())?;
            acceptor.set_ca_file(&self.security_config.ca_path)?;
            acceptor.set_certificate_chain_file(&self.security_config.cert_path)?;
            acceptor.set_private_key_file(&self.security_config.key_path, SslFiletype::PEM)?;
            if !self.security_config.cert_allowed_cn.is_empty() {
                acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
            }
            let acceptor = acceptor.build();
            let tls_incoming = tls_incoming(acceptor, incoming);
            let server = Server::builder(tls_incoming);
            self.start_serve(server);
        } else {
            let server = Server::builder(incoming);
            self.start_serve(server);
        }
        // register the advertise status address to pd
        // self.register_addr(advertise_status_addr);
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

fn tls_incoming(
    acceptor: SslAcceptor,
    mut incoming: AddrIncoming,
) -> impl Accept<Conn = SslStream<AddrStream>, Error = std::io::Error> {
    let context = acceptor.into_context();
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
                    continue;
                }
            };
            match tokio_openssl::SslStream::new(ssl, stream) {
                Ok(mut ssl_stream) => match Pin::new(&mut ssl_stream).accept().await {
                    Err(_) => {
                        error!("Status server error: TLS handshake error");
                        continue;
                    },
                    Ok(()) => {
                        yield Ok(ssl_stream);
                    },
                }
                Err(err) => {
                    error!("Status server error: {}", err);
                    continue;
                }
            };
        }
    };
    TlsIncoming(s)
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
        cx: &mut Context,
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
            // In this scope the path must be like /fail...(/...), which starts with FAIL_POINTS_REQUEST_PATH and may or may not have a sub path
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

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::future::ok;
    use futures::prelude::*;
    use hyper::client::HttpConnector;
    use hyper::{Body, Client, Method, Request, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use openssl::ssl::SslFiletype;
    use openssl::ssl::{SslConnector, SslMethod};

    use std::env;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::config::{ConfigController, TiKvConfig};
    use crate::server::status_server::{LogLevelRequest, StatusServer};
    use collections::HashSet;
    use engine_test::kv::KvTestEngine;
    use online_config::OnlineConfig;
    use raftstore::store::transport::CasualRouter;
    use raftstore::store::CasualMessage;
    use security::SecurityConfig;
    use test_util::new_security_cfg;
    use tikv_util::logger::get_log_level;

    #[derive(Clone)]
    struct MockRouter;

    impl CasualRouter<KvTestEngine> for MockRouter {
        fn send(&self, region_id: u64, _: CasualMessage<KvTestEngine>) -> raftstore::Result<()> {
            Err(raftstore::Error::RegionNotFound(region_id))
        }
    }

    #[test]
    fn test_status_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/metrics")
            .build()
            .unwrap();

        let handle = status_server.thread_pool.spawn(async move {
            let res = client.get(uri).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        });
        block_on(handle).unwrap();
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
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/config")
            .build()
            .unwrap();
        let handle = status_server.thread_pool.spawn(async move {
            let resp = client.get(uri).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let mut v = Vec::new();
            resp.into_body()
                .try_for_each(|bytes| {
                    v.extend(bytes);
                    ok(())
                })
                .await
                .unwrap();
            let resp_json = String::from_utf8_lossy(&v).to_string();
            let cfg = TiKvConfig::default();
            serde_json::to_string(&cfg.get_encoder())
                .map(|cfg_json| {
                    assert_eq!(resp_json, cfg_json);
                })
                .expect("Could not convert TiKvConfig to string");
        });
        block_on(handle).unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
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

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(list.len(), 1);
            let list = list.join(";");
            assert_eq!("test_fail_point_name=panic", list);

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

            let res = client.request(req).await.unwrap();

            assert_eq!(res.status(), StatusCode::OK);

            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(2, list.len());
            let list = list.join(";");
            assert!(list.contains("test_fail_point_name=panic"));
            assert!(list.contains("and_another_name=panic"));

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

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let mut body = Vec::new();
            res.into_body()
                .try_for_each(|bytes| {
                    body.extend(bytes);
                    ok(())
                })
                .await
                .unwrap();
            let body = String::from_utf8(body).unwrap();
            assert!(body.contains("test_fail_point_name=panic"));
            assert!(body.contains("and_another_name=panic"));

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

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let list: Vec<String> = fail::list()
                .into_iter()
                .map(move |(name, actions)| format!("{}={}", name, actions))
                .collect();
            assert_eq!(1, list.len());
            let list = list.join(";");
            assert_eq!("and_another_name=panic", list);
        });

        block_on(handle).unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints_can_trigger_fails() {
        let _guard = fail::FailScenario::setup();
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
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

            let res = client.request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
        });

        block_on(handle).unwrap();
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
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let addr = status_server.listening_addr().to_string();

        let handle = status_server.thread_pool.spawn(async move {
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

            let res = client.request(req).await.unwrap();
            // without feature "failpoints", this PUT endpoint should return 404
            assert_eq!(res.status(), StatusCode::NOT_FOUND);
        });

        block_on(handle).unwrap();
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
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(new_security_cfg(Some(allowed_cn))),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);

        let mut connector = HttpConnector::new();
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
            .path_and_query("/region")
            .build()
            .unwrap();

        if expected {
            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::NOT_FOUND);
            });
            block_on(handle).unwrap();
        } else {
            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::FORBIDDEN);
            });
            let _ = block_on(handle);
        }
        status_server.stop();
    }

    #[cfg(feature = "mem-profiling")]
    #[test]
    #[ignore]
    fn test_pprof_heap_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/debug/pprof/heap?seconds=1")
            .build()
            .unwrap();
        let handle = status_server
            .thread_pool
            .spawn(async move { client.get(uri).await.unwrap() });
        let resp = block_on(handle).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        status_server.stop();
    }

    #[test]
    fn test_pprof_profile_service() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);
        let client = Client::new();
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/debug/pprof/profile?seconds=1&frequency=99")
            .build()
            .unwrap();
        let handle = status_server
            .thread_pool
            .spawn(async move { client.get(uri).await.unwrap() });
        let resp = block_on(handle).unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        status_server.stop();
    }

    #[test]
    fn test_change_log_level() {
        let mut status_server = StatusServer::new(
            1,
            None,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr.clone(), addr);

        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/log-level")
            .build()
            .unwrap();

        let new_log_level = slog::Level::Debug;
        let mut log_level_request = Request::new(Body::from(
            serde_json::to_string(&LogLevelRequest {
                log_level: new_log_level,
            })
            .unwrap(),
        ));
        *log_level_request.method_mut() = Method::PUT;
        *log_level_request.uri_mut() = uri;
        log_level_request.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/json"),
        );

        let handle = status_server.thread_pool.spawn(async move {
            Client::new()
                .request(log_level_request)
                .await
                .map(move |res| {
                    assert_eq!(res.status(), StatusCode::OK);
                    assert_eq!(get_log_level(), Some(new_log_level));
                })
                .unwrap()
        });
        block_on(handle).unwrap();
        status_server.stop();
    }
}
