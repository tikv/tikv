use cloud::blob::{BlobStorage, PutResource};
use futures_util::io::AsyncReadExt as _;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use kvproto::brpb::Gcs;
use std::convert::Infallible;
use std::io;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn make_cfg(endpoint: &str, bucket: &str, prefix: &str, storage_class: &str) -> Gcs {
    let mut cfg = Gcs::default();
    cfg.endpoint = endpoint.to_string();
    cfg.bucket = bucket.to_string();
    cfg.prefix = prefix.to_string();
    cfg.storage_class = storage_class.to_string();
    cfg
}

async fn read_all(mut r: cloud::blob::BlobStream<'_>) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    r.read_to_end(&mut buf).await?;
    Ok(buf)
}

fn query_has_upload_type_resumable(req: &Request<Body>) -> bool {
    req.uri()
        .query()
        .map(|q| q.split('&').any(|kv| kv == "uploadType=resumable"))
        .unwrap_or(false)
}

async fn handle(req: Request<Body>, addr: String) -> Result<Response<Body>, Infallible> {
    match *req.method() {
        hyper::Method::GET => {
            let has_range = req.headers().contains_key("range");
            let (status, body) = if has_range {
                (StatusCode::PARTIAL_CONTENT, "eta")
            } else {
                (StatusCode::OK, "alpha")
            };
            let mut resp = Response::new(Body::from(body));
            *resp.status_mut() = status;
            resp.headers_mut()
                .insert("x-goog-generation", hyper::header::HeaderValue::from_static("1"));
            if has_range {
                resp.headers_mut().insert(
                    hyper::header::CONTENT_RANGE,
                    hyper::header::HeaderValue::from_static("bytes 1-3/5"),
                );
                resp.headers_mut().insert(
                    hyper::header::CONTENT_LENGTH,
                    hyper::header::HeaderValue::from_static("3"),
                );
            }
            resp.headers_mut().insert(
                "x-goog-stored-content-length",
                hyper::header::HeaderValue::from_static("5"),
            );
            Ok(resp)
        }
        hyper::Method::POST => {
            if query_has_upload_type_resumable(&req) {
                let mut resp = Response::new(Body::empty());
                *resp.status_mut() = StatusCode::OK;
                resp.headers_mut().insert(
                    "Location",
                    hyper::header::HeaderValue::from_str(&format!("http://{addr}/upload/resumable"))
                        .unwrap(),
                );
                return Ok(resp);
            }
            let mut resp = Response::new(Body::from(
                r#"{"name":"ignored","bucket":"test-bucket","generation":"1","metageneration":"1","size":"0"}"#,
            ));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/json"),
            );
            Ok(resp)
        }
        hyper::Method::PUT => {
            let mut resp = Response::new(Body::from(
                r#"{"name":"ignored","bucket":"test-bucket","generation":"1","metageneration":"1","size":"0"}"#,
            ));
            *resp.status_mut() = StatusCode::OK;
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                hyper::header::HeaderValue::from_static("application/json"),
            );
            Ok(resp)
        }
        _ => Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()),
    }
}

async fn start_server() -> io::Result<(String, oneshot::Sender<()>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let addr_string = addr.to_string();

    let make_svc = make_service_fn(move |_| {
        let addr_string = addr_string.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle(req, addr_string.clone())
            }))
        }
    });

    let server = Server::from_tcp(listener.into_std()?)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
        .serve(make_svc)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.await;
        });
    tokio::spawn(server);

    Ok((addr.to_string(), shutdown_tx))
}

#[tokio::test]
async fn compare_gcs_v1_v2_read_and_read_part() -> Result<(), Box<dyn std::error::Error>> {
    let (addr, shutdown_tx) = start_server().await?;
    let endpoint = format!("http://{addr}");
    let cfg_v1 = make_cfg(&endpoint, "test-bucket", "pfx", "STANDARD");
    let cfg_v2 = make_cfg(&endpoint, "test-bucket", "pfx", "");

    let s1 = gcp::GcsStorage::from_input(cfg_v1)?;
    let s2 = gcs_v2::GcsStorage::from_input(cfg_v2)?;

    s1.put("a", PutResource(Box::new(futures::io::Cursor::new(b"alpha".to_vec()))), 5)
        .await?;
    s2.put("a", PutResource(Box::new(futures::io::Cursor::new(b"alpha".to_vec()))), 5)
        .await?;

    // get("a")
    let r1 = s1.get("a");
    let r2 = s2.get("a");
    let (b1, b2) = (read_all(r1).await?, read_all(r2).await?);
    assert_eq!(b1, b2);
    assert_eq!(b1, b"alpha");

    s1.put("b", PutResource(Box::new(futures::io::Cursor::new(b"beta".to_vec()))), 4)
        .await?;
    s2.put("b", PutResource(Box::new(futures::io::Cursor::new(b"beta".to_vec()))), 4)
        .await?;

    // get_part("b", 1, 3) -> "eta"
    let r1 = s1.get_part("b", 1, 3);
    let r2 = s2.get_part("b", 1, 3);
    let b1 = read_all(r1).await?;
    let b2 = read_all(r2).await?;
    assert_eq!(b1, b"eta");
    assert_eq!(b2, b"eta");

    let _ = shutdown_tx.send(());
    Ok(())
}
