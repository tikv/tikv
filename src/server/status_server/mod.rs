// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

/// Provides profilers for TiKV.
mod profile;
use std::{
    error::Error as StdError,
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
use flate2::{write::GzEncoder, Compression};
use futures::{
    compat::{Compat01As03, Stream01CompatExt},
    future::{ok, poll_fn},
    prelude::*,
};
use http::header::{HeaderValue, ACCEPT_ENCODING, CONTENT_ENCODING, CONTENT_TYPE};
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
use kvproto::resource_manager::ResourceGroup;
use online_config::OnlineConfig;
use openssl::{
    ssl::{Ssl, SslAcceptor, SslContext, SslFiletype, SslMethod, SslVerifyMode},
    x509::X509,
};
use pin_project::pin_project;
pub use profile::{
    activate_heap_profile, deactivate_heap_profile, heap_profiles_dir, jeprof_heap_profile,
    list_heap_profiles, read_file, start_one_cpu_profile, start_one_heap_profile,
    HEAP_PROFILE_REGEX,
};
use prometheus::TEXT_FORMAT;
use regex::Regex;
use resource_control::ResourceGroupManager;
use security::{self, SecurityConfig};
use serde::Serialize;
use serde_json::Value;
use service::service_manager::GrpcServiceManager;
use tikv_kv::RaftExtension;
use tikv_util::{
    logger::set_log_level,
    metrics::{dump, dump_to},
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    runtime::{Builder, Handle, Runtime},
    sync::oneshot::{self, Receiver, Sender},
};
use tokio_openssl::SslStream;

use crate::{
    config::{ConfigController, LogLevel},
    server::Result,
    tikv_util::sys::thread::ThreadBuildWrapper,
};

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

pub struct StatusServer<R> {
    thread_pool: Runtime,
    tx: Sender<()>,
    rx: Option<Receiver<()>>,
    addr: Option<SocketAddr>,
    cfg_controller: ConfigController,
    router: R,
    security_config: Arc<SecurityConfig>,
    store_path: PathBuf,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    grpc_service_mgr: GrpcServiceManager,
}

impl<R> StatusServer<R>
where
    R: 'static + Send,
{
    pub fn new(
        status_thread_pool_size: usize,
        cfg_controller: ConfigController,
        security_config: Arc<SecurityConfig>,
        router: R,
        store_path: PathBuf,
        resource_manager: Option<Arc<ResourceGroupManager>>,
        grpc_service_mgr: GrpcServiceManager,
    ) -> Result<Self> {
        let thread_pool = Builder::new_multi_thread()
            .enable_all()
            .worker_threads(status_thread_pool_size)
            .thread_name("status-server")
            .with_sys_and_custom_hooks(
                || debug!("Status server started"),
                || debug!("stopping status server"),
            )
            .build()?;

        let (tx, rx) = oneshot::channel::<()>();
        Ok(StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            cfg_controller,
            router,
            security_config,
            store_path,
            resource_manager,
            grpc_service_mgr,
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
            let re = Regex::new(HEAP_PROFILE_REGEX).unwrap();
            if !re.is_match(name) {
                let errmsg = format!("heap profile name {} is invalid", name);
                return Ok(make_response(StatusCode::BAD_REQUEST, errmsg));
            }
            let profiles = match list_heap_profiles() {
                Ok(s) => s,
                Err(e) => return Ok(make_response(StatusCode::INTERNAL_SERVER_ERROR, e)),
            };
            if profiles.iter().any(|(f, _)| f == name) {
                let dir = match heap_profiles_dir() {
                    Some(path) => path,
                    None => {
                        return Ok(make_response(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "heap profile is not active",
                        ));
                    }
                };
                let path = dir.join(name.as_ref());
                if use_jeprof {
                    jeprof_heap_profile(path.to_str().unwrap())
                } else {
                    read_file(path.to_str().unwrap())
                }
            } else {
                let errmsg = format!("heap profile {} not found", name);
                return Ok(make_response(StatusCode::BAD_REQUEST, errmsg));
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
        Ok(match encode_res {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => make_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error"),
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

    async fn get_engine_type(cfg_controller: &ConfigController) -> hyper::Result<Response<Body>> {
        let engine_type = cfg_controller.get_engine_type();
        let response = Response::builder()
            .header("Content-Type", mime::TEXT_PLAIN.to_string())
            .header("Content-Length", engine_type.len())
            .body(engine_type.into())
            .unwrap();
        Ok(response)
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

impl<R> StatusServer<R>
where
    R: 'static + Send + RaftExtension + Clone,
{
    async fn handle_pause_grpc(
        mut grpc_service_mgr: GrpcServiceManager,
    ) -> hyper::Result<Response<Body>> {
        if let Err(err) = grpc_service_mgr.pause() {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to pause grpc: {}", err),
            ));
        }
        Ok(make_response(
            StatusCode::OK,
            "Successfully pause grpc service",
        ))
    }

    async fn handle_resume_grpc(
        mut grpc_service_mgr: GrpcServiceManager,
    ) -> hyper::Result<Response<Body>> {
        if let Err(err) = grpc_service_mgr.resume() {
            return Ok(make_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("fails to resume grpc: {}", err),
            ));
        }
        Ok(make_response(
            StatusCode::OK,
            "Successfully resume grpc service",
        ))
    }

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
        let f = router.query_region(id);
        let meta = match f.await {
            Ok(meta) => meta,
            Err(tikv_kv::Error(box tikv_kv::ErrorInner::Request(header)))
                if header.has_region_not_found() =>
            {
                return not_found(format!("region({}) not found", id));
            }
            Err(err) => {
                return Ok(make_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query failed: {}", err),
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

        #[cfg(feature = "trace-tablet-lifetime")]
        let body = {
            let query = req.uri().query().unwrap_or("");
            let query_pairs: HashMap<_, _> =
                url::form_urlencoded::parse(query.as_bytes()).collect();

            let mut body = body;
            if query_pairs.contains_key("trace-tablet") {
                for s in engine_rocks::RocksEngine::trace(id) {
                    body.push(b'\n');
                    body.extend_from_slice(s.as_bytes());
                }
            };
            body
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

    fn handle_get_metrics(
        req: Request<Body>,
        mgr: &ConfigController,
    ) -> hyper::Result<Response<Body>> {
        let should_simplify = mgr.get_current().server.simplify_metrics;
        let gz_encoding = client_accept_gzip(&req);
        let metrics = if gz_encoding {
            // gzip can reduce the body size to less than 1/10.
            let mut encoder = GzEncoder::new(vec![], Compression::default());
            dump_to(&mut encoder, should_simplify);
            encoder.finish().unwrap()
        } else {
            dump(should_simplify).into_bytes()
        };
        let mut resp = Response::new(metrics.into());
        resp.headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static(TEXT_FORMAT));
        if gz_encoding {
            resp.headers_mut()
                .insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }

        Ok(resp)
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
        let store_path = self.store_path.clone();
        let resource_manager = self.resource_manager.clone();
        let grpc_service_mgr = self.grpc_service_mgr.clone();
        // Start to serve.
        let server = builder.serve(make_service_fn(move |conn: &C| {
            let x509 = conn.get_x509();
            let security_config = security_config.clone();
            let cfg_controller = cfg_controller.clone();
            let router = router.clone();
            let store_path = store_path.clone();
            let resource_manager = resource_manager.clone();
            let grpc_service_mgr = grpc_service_mgr.clone();
            async move {
                // Create a status service.
                Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                    let x509 = x509.clone();
                    let security_config = security_config.clone();
                    let cfg_controller = cfg_controller.clone();
                    let router = router.clone();
                    let store_path = store_path.clone();
                    let resource_manager = resource_manager.clone();
                    let grpc_service_mgr = grpc_service_mgr.clone();
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
                        // actual user data since in some cases the data itself is stored in the
                        // key.
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
                            (Method::GET, "/metrics") => {
                                Self::handle_get_metrics(req, &cfg_controller)
                            }
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
                                Self::get_config(req, &cfg_controller).await
                            }
                            (Method::POST, "/config") => {
                                Self::update_config(cfg_controller.clone(), req).await
                            }
                            (Method::GET, "/engine_type") => {
                                Self::get_engine_type(&cfg_controller).await
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
                            (Method::GET, "/resource_groups") => {
                                Self::handle_get_all_resource_groups(resource_manager.as_ref())
                            }
                            (Method::PUT, "/pause_grpc") => {
                                Self::handle_pause_grpc(grpc_service_mgr).await
                            }
                            (Method::PUT, "/resume_grpc") => {
                                Self::handle_resume_grpc(grpc_service_mgr).await
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

    pub fn handle_get_all_resource_groups(
        mgr: Option<&Arc<ResourceGroupManager>>,
    ) -> hyper::Result<Response<Body>> {
        let groups = if let Some(mgr) = mgr {
            mgr.get_all_resource_groups()
                .into_iter()
                .map(into_debug_request_group)
                .collect()
        } else {
            vec![]
        };
        let body = match serde_json::to_vec(&groups) {
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
}

#[derive(Serialize)]
struct BackgroundSetting {
    task_types: Vec<String>,
}

#[derive(Serialize)]
struct ResourceGroupSetting {
    name: String,
    ru: u64,
    priority: u32,
    burst_limit: i64,
    background: BackgroundSetting,
}

fn into_debug_request_group(mut rg: ResourceGroup) -> ResourceGroupSetting {
    ResourceGroupSetting {
        name: rg.name,
        ru: rg
            .r_u_settings
            .get_ref()
            .get_r_u()
            .get_settings()
            .get_fill_rate(),
        priority: rg.priority,
        burst_limit: rg
            .r_u_settings
            .get_ref()
            .get_r_u()
            .get_settings()
            .get_burst_limit(),
        background: BackgroundSetting {
            task_types: rg
                .background_settings
                .as_mut()
                .map_or(vec![], |s| s.take_job_types().into()),
        },
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

// check if the client allow return response with gzip compression
// the following logic is port from prometheus's golang:
// https://github.com/prometheus/client_golang/blob/24172847e35ba46025c49d90b8846b59eb5d9ead/prometheus/promhttp/http.go#L155-L176
fn client_accept_gzip(req: &Request<Body>) -> bool {
    let encoding = req
        .headers()
        .get(ACCEPT_ENCODING)
        .map(|enc| enc.to_str().unwrap_or_default())
        .unwrap_or_default();
    encoding
        .split(',')
        .map(|s| s.trim())
        .any(|s| s == "gzip" || s.starts_with("gzip;"))
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

#[cfg(test)]
mod tests {
    use std::{env, io::Read, path::PathBuf, sync::Arc};

    use collections::HashSet;
    use flate2::read::GzDecoder;
    use futures::{
        executor::block_on,
        future::{ok, BoxFuture},
        prelude::*,
    };
    use http::header::{HeaderValue, ACCEPT_ENCODING};
    use hyper::{body::Buf, client::HttpConnector, Body, Client, Method, Request, StatusCode, Uri};
    use hyper_openssl::HttpsConnector;
    use online_config::OnlineConfig;
    use openssl::ssl::{SslConnector, SslFiletype, SslMethod};
    use raftstore::store::region_meta::RegionMeta;
    use security::SecurityConfig;
    use service::service_manager::GrpcServiceManager;
    use test_util::new_security_cfg;
    use tikv_kv::RaftExtension;
    use tikv_util::logger::get_log_level;

    use crate::{
        config::{ConfigController, TikvConfig},
        server::status_server::{profile::TEST_PROFILE_MUTEX, LogLevelRequest, StatusServer},
        storage::config::EngineType,
    };

    #[derive(Clone)]
    struct MockRouter;

    impl RaftExtension for MockRouter {
        fn query_region(&self, region_id: u64) -> BoxFuture<'static, tikv_kv::Result<RegionMeta>> {
            Box::pin(async move { Err(raftstore::Error::RegionNotFound(region_id).into()) })
        }
    }

    #[test]
    fn test_status_service() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
            let cfg = TikvConfig::default();
            serde_json::to_string(&cfg.get_encoder())
                .map(|cfg_json| {
                    assert_eq!(resp_json, cfg_json);
                })
                .expect("Could not convert TikvConfig to string");
        });
        block_on(handle).unwrap();
        status_server.stop();
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn test_status_service_fail_endpoints() {
        let _guard = fail::FailScenario::setup();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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

    fn do_test_security_status_service(allowed_cn: HashSet<String>, expected: bool) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(new_security_cfg(Some(allowed_cn))),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);

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
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);
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
        assert_eq!(
            resp.headers().get("Content-Type").unwrap(),
            &mime::IMAGE_SVG.to_string()
        );
        status_server.stop();
    }

    #[test]
    fn test_metrics() {
        let _test_guard = TEST_PROFILE_MUTEX.lock().unwrap();
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);

        // test plain test
        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/metrics")
            .build()
            .unwrap();

        let client = Client::new();
        let url_cloned = uri.clone();
        let handle = status_server
            .thread_pool
            .spawn(async move { client.get(url_cloned).await.unwrap() });
        let resp = block_on(handle).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body_bytes = block_on(hyper::body::to_bytes(resp.into_body())).unwrap();
        String::from_utf8(body_bytes.as_ref().to_owned()).unwrap();

        // test gzip
        let handle = status_server.thread_pool.spawn(async move {
            let body = Body::default();
            let mut req = Request::new(body);
            req.headers_mut()
                .insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
            *req.uri_mut() = uri;
            let client = Client::new();
            client.request(req).await.unwrap()
        });
        let resp = block_on(handle).unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get("Content-Encoding").unwrap(), "gzip");
        let body_bytes = block_on(hyper::body::to_bytes(resp.into_body())).unwrap();
        let mut decoded_bytes = vec![];
        GzDecoder::new(body_bytes.reader())
            .read_to_end(&mut decoded_bytes)
            .unwrap();
        String::from_utf8(decoded_bytes).unwrap();

        status_server.stop();
    }

    #[test]
    fn test_change_log_level() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut status_server = StatusServer::new(
            1,
            ConfigController::default(),
            Arc::new(SecurityConfig::default()),
            MockRouter,
            temp_dir.path().to_path_buf(),
            None,
            GrpcServiceManager::dummy(),
        )
        .unwrap();
        let addr = "127.0.0.1:0".to_owned();
        let _ = status_server.start(addr);

        let uri = Uri::builder()
            .scheme("http")
            .authority(status_server.listening_addr().to_string().as_str())
            .path_and_query("/log-level")
            .build()
            .unwrap();

        let new_log_level = slog::Level::Debug.into();
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
                    assert_eq!(get_log_level(), Some(new_log_level.into()));
                })
                .unwrap()
        });
        block_on(handle).unwrap();
        status_server.stop();
    }

    #[test]
    fn test_get_engine_type() {
        let mut multi_rocks_cfg = TikvConfig::default();
        multi_rocks_cfg.storage.engine = EngineType::RaftKv2;
        let cfgs = [TikvConfig::default(), multi_rocks_cfg];
        let resp_strs = ["raft-kv", "partitioned-raft-kv"];
        for (cfg, resp_str) in IntoIterator::into_iter(cfgs).zip(resp_strs) {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut status_server = StatusServer::new(
                1,
                ConfigController::new(cfg),
                Arc::new(SecurityConfig::default()),
                MockRouter,
                temp_dir.path().to_path_buf(),
                None,
                GrpcServiceManager::dummy(),
            )
            .unwrap();
            let addr = "127.0.0.1:0".to_owned();
            let _ = status_server.start(addr);
            let client = Client::new();
            let uri = Uri::builder()
                .scheme("http")
                .authority(status_server.listening_addr().to_string().as_str())
                .path_and_query("/engine_type")
                .build()
                .unwrap();

            let handle = status_server.thread_pool.spawn(async move {
                let res = client.get(uri).await.unwrap();
                assert_eq!(res.status(), StatusCode::OK);
                let body_bytes = hyper::body::to_bytes(res.into_body()).await.unwrap();
                let engine_type = String::from_utf8(body_bytes.as_ref().to_owned()).unwrap();
                assert_eq!(engine_type, resp_str);
            });
            block_on(handle).unwrap();
            status_server.stop();
        }
    }

    #[test]
    fn test_control_grpc_service() {
        let mut multi_rocks_cfg = TikvConfig::default();
        multi_rocks_cfg.storage.engine = EngineType::RaftKv2;
        let cfgs = [TikvConfig::default(), multi_rocks_cfg];
        for cfg in IntoIterator::into_iter(cfgs) {
            let temp_dir = tempfile::TempDir::new().unwrap();
            let mut status_server = StatusServer::new(
                1,
                ConfigController::new(cfg),
                Arc::new(SecurityConfig::default()),
                MockRouter,
                temp_dir.path().to_path_buf(),
                None,
                GrpcServiceManager::dummy(),
            )
            .unwrap();
            let addr = "127.0.0.1:0".to_owned();
            let _ = status_server.start(addr);
            for req in ["/pause_grpc", "/resume_grpc"] {
                let client = Client::new();
                let uri = Uri::builder()
                    .scheme("http")
                    .authority(status_server.listening_addr().to_string().as_str())
                    .path_and_query(req)
                    .build()
                    .unwrap();

                let mut grpc_req = Request::default();
                *grpc_req.method_mut() = Method::PUT;
                *grpc_req.uri_mut() = uri;
                let handle = status_server.thread_pool.spawn(async move {
                    let res = client.request(grpc_req).await.unwrap();
                    // Dummy grpc service manager, should return error.
                    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
                });
                block_on(handle).unwrap();
            }
            status_server.stop();
        }
    }
}
