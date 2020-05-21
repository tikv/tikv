// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use async_stream::stream;
use engine_traits::Snapshot;
use futures03::compat::Compat01As03;
use futures03::future::{ok, poll_fn};
use futures03::prelude::*;
use hyper::server::accept::Accept;
use hyper::server::conn::{AddrIncoming, AddrStream};
use hyper::server::Builder as HyperBuilder;
use hyper::service::{make_service_fn, service_fn};
use hyper::{self, header, Body, Method, Request, Response, Server, StatusCode};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use openssl::x509::X509StoreContextRef;
use pin_project::pin_project;
use pprof::protos::Message;
use raftstore::store::{transport::CasualRouter, CasualMessage};
use regex::Regex;
use reqwest::{self, blocking::Client};
use serde_json::Value;
use tempfile::TempDir;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio_openssl::SslStream;

use std::error::Error as StdError;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::Result;
use crate::config::ConfigController;
use pd_client::RpcClient;
use security::{self, SecurityConfig};
use tikv_alloc::error::ProfError;
use tikv_util::collections::HashMap;
use tikv_util::metrics::dump;
use tikv_util::timer::GLOBAL_TIMER_HANDLE;

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

pub struct StatusServer<S, R> {
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    pd_client: Option<Arc<RpcClient>>,
    cfg_controller: ConfigController,
    router: R,
    _snap: PhantomData<S>,
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

impl<S, R> StatusServer<S, R>
where
    S: 'static,
    R: 'static + Send,
{
    pub fn new(
        status_thread_pool_size: usize,
        pd_client: Option<Arc<RpcClient>>,
        cfg_controller: ConfigController,
        router: R,
    ) -> Result<Self> {
        let thread_pool = Builder::new()
            .threaded_scheduler()
            .enable_all()
            .core_threads(status_thread_pool_size)
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
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            pd_client,
            cfg_controller,
            router,
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
            .map_err(|path| ProfError::PathEncodingError(path))?;
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

    async fn get_config(cfg_controller: &ConfigController) -> hyper::Result<Response<Body>> {
        Ok(match serde_json::to_string(&cfg_controller.get_current()) {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => StatusServer::err_response(
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
        let guard = pprof::ProfilerGuard::new(frequency)?;
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
                ))
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

    pub fn stop(self) {
        // unregister the status address to pd
        self.unregister_addr();
        let _ = self.tx.send(());
        self.thread_pool.shutdown_timeout(Duration::from_secs(10));
    }

    // Return listening address, this may only be used for outer test
    // to get the real address because we may use "127.0.0.1:0"
    // in test to avoid port conflict.
    pub fn listening_addr(&self) -> SocketAddr {
        self.addr.unwrap()
    }

    fn register_addr(&self, status_addr: String) {
        if self.pd_client.is_none() {
            return;
        }
        let pd_client = self.pd_client.as_ref().unwrap();
        let client = Client::new();
        let json = {
            let mut body = std::collections::HashMap::new();
            body.insert("component".to_owned(), COMPONENT.to_owned());
            body.insert("addr".to_owned(), status_addr);
            serde_json::to_string(&body).unwrap()
        };
        for _ in 0..COMPONENT_REQUEST_RETRY {
            for pd_addr in pd_client.get_leader().get_client_urls() {
                let mut url = url::Url::parse(pd_addr).unwrap();
                url.set_path("pd/api/v1/component");
                let res = client
                    .post(url.as_str())
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(json.clone())
                    .send();
                match res {
                    Ok(resp) if resp.status() == reqwest::StatusCode::OK => return,
                    Ok(resp) => error!("failed to register addr to pd"; "response" => ?resp),
                    Err(e) => error!("failed to register addr to pd"; "error" => ?e),
                }
            }
            // refresh the pd leader
            if let Err(e) = pd_client.reconnect() {
                error!("failed to reconnect pd client"; "err" => ?e);
            }
        }
        error!(
            "failed to register addr to pd after {} tries",
            COMPONENT_REQUEST_RETRY
        );
    }

    fn unregister_addr(&self) {
        if self.pd_client.is_none() {
            return;
        }
        let status_addr = format!("{}", self.listening_addr());
        let pd_client = self.pd_client.as_ref().unwrap();
        let client = Client::new();
        for _ in 0..COMPONENT_REQUEST_RETRY {
            for pd_addr in pd_client.get_leader().get_client_urls() {
                let mut url = url::Url::parse(pd_addr).unwrap();
                url.set_path(format!("pd/api/v1/component/{}/{}", COMPONENT, status_addr).as_str());
                match client.delete(url.as_str()).send() {
                    Ok(resp) if resp.status() == reqwest::StatusCode::OK => return,
                    Ok(resp) => error!("failed to unregister addr to pd"; "response" => ?resp),
                    Err(e) => error!("failed to unregister addr to pd"; "error" => ?e),
                }
            }
            // refresh the pd leader
            if let Err(e) = pd_client.reconnect() {
                error!("failed to reconnect pd client"; "err" => ?e);
            }
        }
        error!(
            "failed to unregister addr to pd after {} tries",
            COMPONENT_REQUEST_RETRY
        );
    }
}

impl<S, R> StatusServer<S, R>
where
    S: Snapshot,
    R: 'static + Send + CasualRouter<S> + Clone,
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
                if let Err(meta) = tx.send(region_meta::RegionMeta::new(&peer.peer)) {
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
                ))
            }
        };

        let body = match serde_json::to_vec(&meta) {
            Ok(body) => body,
            Err(err) => {
                return Ok(StatusServer::err_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("fails to json: {}", err),
                ))
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

    fn start_serve<I>(&mut self, builder: HyperBuilder<I>)
    where
        I: Accept + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let cfg_controller = self.cfg_controller.clone();
        let router = self.router.clone();
        // Start to serve.
        let server = builder.serve(make_service_fn(move |_| {
            let cfg_controller = cfg_controller.clone();
            let router = router.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
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

                        match (method, path.as_ref()) {
                            (Method::GET, "/metrics") => Ok(Response::new(dump().into())),
                            (Method::GET, "/status") => Ok(Response::default()),
                            (Method::GET, "/debug/pprof/heap") => {
                                Self::dump_prof_to_resp(req).await
                            }
                            (Method::GET, "/config") => Self::get_config(&cfg_controller).await,
                            (Method::POST, "/config") => {
                                Self::update_config(cfg_controller.clone(), req).await
                            }
                            (Method::GET, "/debug/pprof/profile") => {
                                Self::dump_rsperf_to_resp(req).await
                            }
                            (Method::GET, path) if path.starts_with("/region") => {
                                Self::dump_region_meta(req, router).await
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

    pub fn start(&mut self, status_addr: String, security_config: &SecurityConfig) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        let incoming = self.thread_pool.enter(|| AddrIncoming::bind(&addr))?;
        self.addr = Some(incoming.local_addr());
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
            let tls_incoming = tls_incoming(acceptor, incoming);
            let server = Server::builder(tls_incoming);
            self.start_serve(server);
        } else {
            let server = Server::builder(incoming);
            self.start_serve(server);
        }
        // register the status address to pd
        self.register_addr(status_addr);
        Ok(())
    }
}

fn tls_incoming(
    acceptor: SslAcceptor,
    mut incoming: AddrIncoming,
) -> impl Accept<Conn = SslStream<AddrStream>, Error = std::io::Error> {
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
            yield tokio_openssl::accept(&acceptor, stream)
                .await
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "TLS handshake error"));
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
    use futures03::executor::block_on;
    use futures03::future::ok;
    use futures03::prelude::*;
    use hyper::client::HttpConnector;
    use hyper::{Body, Client, Method, Request, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use openssl::ssl::SslFiletype;
    use openssl::ssl::{SslConnector, SslMethod};

    use std::env;
    use std::path::PathBuf;

    use crate::config::{ConfigController, TiKvConfig};
    use crate::server::status_server::StatusServer;
    use engine_rocks::RocksSnapshot;
    use raftstore::store::transport::CasualRouter;
    use raftstore::store::CasualMessage;
    use security::SecurityConfig;
    use test_util::new_security_cfg;
    use tikv_util::collections::HashSet;

    #[derive(Clone)]
    struct MockRouter;

    impl CasualRouter<RocksSnapshot> for MockRouter {
        fn send(&self, region_id: u64, _: CasualMessage<RocksSnapshot>) -> raftstore::Result<()> {
            Err(raftstore::Error::RegionNotFound(region_id))
        }
    }

    #[test]
    fn test_status_service() {
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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
            serde_json::to_string(&cfg)
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
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start("127.0.0.1:0".to_string(), &SecurityConfig::default());
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
        let mut status_server =
            StatusServer::new(1, None, ConfigController::default(), MockRouter).unwrap();
        let _ = status_server.start(
            "127.0.0.1:0".to_string(),
            &new_security_cfg(Some(allowed_cn)),
        );

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
            .path_and_query("/metrics")
            .build()
            .unwrap();

        if expected {
            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::OK);
            });
            block_on(handle).unwrap();
        } else {
            let handle = status_server.thread_pool.spawn(async move {
                client
                    .get(uri)
                    .await
                    .expect_err("response status should be err");
            });
            let _ = block_on(handle);
        }
        status_server.stop();
    }
}
