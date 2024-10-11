//! A striped version for the status server. It supports a subset of the status
//! server. Basically, it exports the mertrics and process information. But
//! won't provide TiKV server related stuffs, like reloading config or dump
//! region info.
//!
//! This will be used to improve the of obserbility some short-term tasks of
//! `tikv-ctl`.

use std::{error::Error as StdError, net::SocketAddr, str::FromStr, sync::Arc};

use futures_util::future::TryFutureExt;
use http::{Method, Request, Response, StatusCode};
use hyper::{
    server::{accept::Accept, conn::AddrIncoming},
    service::service_fn,
    Body, Server as HyperSrv,
};
use openssl::x509::X509;
use security::SecurityConfig;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{make_response, tls_incoming, StatusServer};
use crate::server::Result;

/// Svc is a type alias that help us to call static methods in the full status
/// server.
type Svc = StatusServer<()>;

/// Server manages how we accept the incoming requests and how we handle them.
///
/// After creating and configurating this, you may use [`start`] to start
/// serving and detach this. You can control the server then by the [`Handle`].
pub struct Server {
    security_config: Arc<SecurityConfig>,
}

/// Handle is the controller for the sever.
pub struct Handle {
    addr: SocketAddr,
}

impl Handle {
    /// Return the bound address of the server.
    pub fn address(&self) -> &SocketAddr {
        &self.addr
    }
}

impl Server {
    pub fn new(sec: Arc<SecurityConfig>) -> Self {
        Server {
            security_config: sec,
        }
    }

    /// Start the server.
    ///
    /// # Panic
    ///
    /// This must be run in a tokio context. Or this will panic.
    pub fn start(self, status_addr: &str) -> Result<Handle> {
        let addr = SocketAddr::from_str(status_addr)?;

        let incoming = AddrIncoming::bind(&addr)?;
        let hnd = Handle {
            addr: incoming.local_addr(),
        };
        if !self.security_config.cert_path.is_empty()
            && !self.security_config.key_path.is_empty()
            && !self.security_config.ca_path.is_empty()
        {
            let tls_incoming = tls_incoming(self.security_config.clone(), incoming)?;
            let server = HyperSrv::builder(tls_incoming);
            self.start_serve(server);
        } else {
            let server = HyperSrv::builder(incoming);
            self.start_serve(server);
        }
        Ok(hnd)
    }

    fn start_serve<I, C>(self, builder: hyper::server::Builder<I>)
    where
        I: Accept<Conn = C, Error = std::io::Error> + Send + 'static,
        I::Error: Into<Box<dyn StdError + Send + Sync>>,
        I::Conn: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        C: super::ServerConnection,
    {
        let mut svc = LiteService;

        let server = builder.serve(super::make_service_fn(move |conn: &C| {
            let client_cert = conn.get_x509();
            let security = self.security_config.clone();
            std::future::ready(hyper::Result::Ok(service_fn(move |req| {
                let client_cert = client_cert.clone();
                let security = security.clone();
                async move {
                    svc.call(RequestCtx {
                        req,
                        client_cert,
                        security,
                    })
                    .await
                }
            })))
        }));

        let svc =
            server.map_err(|err| warn!("status server lite encountered error"; "err" => %err));
        tokio::spawn(svc);
    }
}

#[derive(Copy, Clone)]
pub struct LiteService;

struct RequestCtx {
    req: Request<Body>,
    client_cert: Option<X509>,
    security: Arc<SecurityConfig>,
}

impl LiteService {
    async fn call(&mut self, cx: RequestCtx) -> std::result::Result<Response<Body>, hyper::Error> {
        let path = cx.req.uri().path().to_owned();
        let method = cx.req.method().to_owned();

        let should_check_cert = !matches!(
            (&method, path.as_ref()),
            (&Method::GET, "/metrics") | (&Method::GET, "/debug/pprof/profile")
        );

        if should_check_cert && !super::check_cert(cx.security, cx.client_cert) {
            return Ok(make_response(
                StatusCode::FORBIDDEN,
                "certificate role error",
            ));
        }

        match (&method, path.as_str()) {
            (&Method::GET, "/metrics") => Svc::metrics_to_resp(cx.req, true),
            (&Method::GET, "/debug/pprof/profile") => Svc::dump_cpu_prof_to_resp(cx.req).await,
            (&Method::GET, "/async_tasks") => Svc::dump_async_trace(),
            (&Method::GET, "/debug/pprof/heap") => Svc::dump_heap_prof_to_resp(cx.req),
            _ => Ok(make_response(StatusCode::NOT_FOUND, "path not found")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use hyper::{Body, Request, StatusCode};
    use security::SecurityConfig;

    use super::*;

    impl super::Server {
        fn insecure() -> Self {
            Self::new(Arc::default())
        }
    }

    #[tokio::test]
    async fn test_server_start_insecure() {
        let server = Server::insecure();
        let handle = server.start("127.0.0.1:0").unwrap();
        assert!(handle.address().is_ipv4());
    }

    #[tokio::test]
    async fn test_lite_service_call_metrics() {
        let mut service = LiteService;
        let req = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let ctx = RequestCtx {
            req,
            client_cert: None,
            security: Arc::new(SecurityConfig::default()),
        };
        let resp = service.call(ctx).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_lite_service_call_profile() {
        let mut service = LiteService;
        let req = Request::builder()
            .method("GET")
            .uri("/debug/pprof/profile")
            .body(Body::empty())
            .unwrap();
        let ctx = RequestCtx {
            req,
            client_cert: None,
            security: Arc::new(SecurityConfig::default()),
        };
        let resp = service.call(ctx).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_lite_service_call_forbidden() {
        let mut service = LiteService;
        let req = Request::builder()
            .method("GET")
            .uri("/forbidden")
            .body(Body::empty())
            .unwrap();
        let ctx = RequestCtx {
            req,
            client_cert: None,
            security: Arc::new(SecurityConfig::default()),
        };
        let resp = service.call(ctx).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
