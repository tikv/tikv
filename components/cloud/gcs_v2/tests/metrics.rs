use cloud::blob::{BlobStorage, PutResource};
use kvproto::brpb::Gcs;
use prometheus::proto::MetricType;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

fn histogram_sample_count(cloud: &str, req: &str) -> u64 {
    let families = prometheus::gather();
    for mf in families {
        if mf.get_name() != "tikv_cloud_request_duration_seconds" {
            continue;
        }
        if mf.get_field_type() != MetricType::HISTOGRAM {
            continue;
        }
        for m in mf.get_metric() {
            let mut cloud_ok = false;
            let mut req_ok = false;
            for lp in m.get_label() {
                if lp.get_name() == "cloud" && lp.get_value() == cloud {
                    cloud_ok = true;
                }
                if lp.get_name() == "req" && lp.get_value() == req {
                    req_ok = true;
                }
            }
            if cloud_ok && req_ok {
                return m.get_histogram().get_sample_count();
            }
        }
        return 0;
    }
    0
}

fn build_http_response(body: &str, extra_headers: &[(&str, &str)]) -> Vec<u8> {
    let body_bytes = body.as_bytes();
    let mut headers = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body_bytes.len()
    );
    for (k, v) in extra_headers {
        headers.push_str(&format!("{k}: {v}\r\n"));
    }
    headers.push_str("\r\n");
    headers
        .into_bytes()
        .into_iter()
        .chain(body_bytes.iter().copied())
        .collect()
}

fn response_for_target(target: &str, addr: &str) -> Vec<u8> {
    if target.contains("uploadType=resumable") {
        let location = format!("http://{addr}/upload/resumable");
        return build_http_response("", &[("Location", location.as_str())]);
    }
    let body = r#"{"name":"ignored","bucket":"test-bucket","generation":"1","metageneration":"1","size":"0"}"#;
    build_http_response(body, &[])
}

async fn start_server() -> io::Result<(String, oneshot::Sender<()>, Arc<Mutex<Vec<Vec<u8>>>>)> {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let captured = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let captured_in_server = captured.clone();
    let addr_string = addr.to_string();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => { break; }
                res = listener.accept() => {
                    let Ok((mut socket, _)) = res else { break; };
                    let captured = captured_in_server.clone();
                    let addr_string = addr_string.clone();
                    tokio::spawn(async move {
                        let mut buf = Vec::with_capacity(4096);
                        let mut tmp = [0u8; 1024];
                        loop {
                            let Ok(n) = socket.read(&mut tmp).await else { return; };
                            if n == 0 { return; }
                            buf.extend_from_slice(&tmp[..n]);
                            let header_end = buf.windows(4).position(|w| w == b"\r\n\r\n");
                            if let Some(header_end) = header_end {
                                let header_end = header_end + 4;
                                let headers = &buf[..header_end];
                                let mut content_length = 0usize;
                                if let Ok(s) = std::str::from_utf8(headers) {
                                    for line in s.lines() {
                                        let line = line.trim();
                                        let lower = line.to_ascii_lowercase();
                                        let Some(v) = lower.strip_prefix("content-length:") else { continue; };
                                        content_length = v.trim().parse::<usize>().unwrap_or(0);
                                    }
                                }
                                while buf.len() < header_end + content_length {
                                    let Ok(n) = socket.read(&mut tmp).await else { return; };
                                    if n == 0 { break; }
                                    buf.extend_from_slice(&tmp[..n]);
                                }
                                break;
                            }
                            if buf.len() > 64 * 1024 { return; }
                        }

                        let request = String::from_utf8_lossy(&buf).to_string();
                        let mut lines = request.lines();
                        let Some(first) = lines.next() else { return; };
                        let mut parts = first.split_whitespace();
                        let _method = parts.next();
                        let target = parts.next().unwrap_or("/");

                        captured.lock().unwrap().push(buf);
                        let response = response_for_target(target, &addr_string);
                        let _ = socket.write_all(&response).await;
                        let _ = socket.shutdown().await;
                    });
                }
            }
        }
    });

    Ok((format!("http://{addr}"), shutdown_tx, captured))
}

fn make_cfg(endpoint: &str, bucket: &str, prefix: &str, storage_class: &str, predefined_acl: &str) -> Gcs {
    let mut cfg = Gcs::default();
    cfg.endpoint = endpoint.to_string();
    cfg.bucket = bucket.to_string();
    cfg.prefix = prefix.to_string();
    cfg.storage_class = storage_class.to_string();
    cfg.predefined_acl = predefined_acl.to_string();
    cfg
}

#[tokio::test]
async fn gcs_v2_put_emits_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, shutdown, captured) = start_server().await?;
    let cfg = make_cfg(&endpoint, "test-bucket", "pfx", "COLDLINE", "projectPrivate");
    let s = gcs_v2::GcsStorage::from_input(cfg)?;

    let before_read_local = histogram_sample_count("gcp", "read_local");
    let before_insert = histogram_sample_count("gcp", "insert_multipart");

    s.put(
        "a",
        PutResource(Box::new(futures::io::Cursor::new(b"alpha".to_vec()))),
        5,
    )
    .await?;

    let after_read_local = histogram_sample_count("gcp", "read_local");
    let after_insert = histogram_sample_count("gcp", "insert_multipart");

    assert!(after_read_local >= before_read_local + 1);
    assert!(after_insert >= before_insert + 1);

    let captured = captured.lock().unwrap();
    let mut saw_predefined = false;
    let mut saw_storage_class = false;
    for req in captured.iter() {
        let s = String::from_utf8_lossy(req);
        if s.contains("predefinedAcl=projectPrivate") {
            saw_predefined = true;
        }
        if s.contains("\"storageClass\":\"COLDLINE\"") {
            saw_storage_class = true;
        }
    }
    assert!(saw_predefined);
    assert!(saw_storage_class);

    let _ = shutdown.send(());
    Ok(())
}
