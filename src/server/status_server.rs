// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use futures::future::{err, ok};
use futures::sync::oneshot::{Receiver, Sender};
#[cfg(feature = "failpoints")]
use futures::Stream;
use futures::{self, Future};
use hyper::service::service_fn;
use hyper::{self, header, Body, Method, Request, Response, Server, StatusCode};
use std::sync::Arc;
use tempfile::TempDir;
use tokio_threadpool::{Builder, ThreadPool};

use std::net::SocketAddr;
use std::str::FromStr;

use super::Result;
use crate::config::TiKvConfig;
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
    config: Arc<TiKvConfig>,
}

impl StatusServer {
    pub fn new(status_thread_pool_size: usize, tikv_config: TiKvConfig) -> Self {
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
        let (tx, rx) = futures::sync::oneshot::channel::<()>();
        StatusServer {
            thread_pool,
            tx,
            rx: Some(rx),
            addr: None,
            config: Arc::new(tikv_config),
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
                            Err(path) => return Box::new(err(ProfError::PathError(path))),
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
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::empty())
                    .unwrap();
                return Box::new(ok(response));
            }
        };
        let query_pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
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
                    let response = Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Body::from(err.to_string()))
                        .unwrap();
                    ok(response)
                }),
        )
    }

    fn config_handler(
        config: Arc<TiKvConfig>,
    ) -> Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send> {
        let res = match serde_json::to_string(config.as_ref()) {
            Ok(json) => Response::builder()
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json))
                .unwrap(),
            Err(_) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from("Internal Server Error"))
                .unwrap(),
        };
        Box::new(ok(res))
    }

    pub fn start(&mut self, status_addr: String) -> Result<()> {
        let addr = SocketAddr::from_str(&status_addr)?;

        // TODO: support TLS for the status server.
        let builder = Server::try_bind(&addr)?;
        let config = self.config.clone();

        // Start to serve.
        let server = builder.serve(move || {
            let config = config.clone();
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
                            (Method::GET, "/pprof/profile") => Self::dump_prof_to_resp(req),
                            (Method::GET, "/config") => Self::config_handler(config.clone()),
                            _ => Box::new(ok(Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Body::empty())
                                .unwrap())),
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
                    .body(e.to_string().into())
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
    use crate::config::TiKvConfig;
    use crate::server::status_server::StatusServer;
    use futures::future::{lazy, Future};
    use futures::Stream;
    use hyper::{Body, Client, Method, Request, StatusCode, Uri};

    #[test]
    fn test_status_service() {
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
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

    #[test]
    fn test_config_endpoint() {
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
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
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
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
            *req.uri_mut() = uri.clone();

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
            *req.uri_mut() = uri.clone();

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
            *req.uri_mut() = uri.clone();

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
            *req.uri_mut() = uri.clone();

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
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
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
            *req.uri_mut() = uri.clone();

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
        let config = TiKvConfig::default();
        let mut status_server = StatusServer::new(1, config);
        let _ = status_server.start("127.0.0.1:0".to_string());
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
            *req.uri_mut() = uri.clone();

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
}
