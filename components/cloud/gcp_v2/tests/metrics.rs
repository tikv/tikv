use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

use cloud::blob::{BlobStorage, PutResource};
use kvproto::brpb::Gcs;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
};

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
    if target.contains("/token") {
        return build_http_response(
            r#"{"access_token":"test-token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","token_type":"Bearer","expires_in":3600}"#,
            &[],
        );
    }
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

fn make_cfg(
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    storage_class: &str,
    predefined_acl: &str,
) -> Gcs {
    let mut cfg = Gcs::default();
    cfg.endpoint = endpoint.to_string();
    cfg.bucket = bucket.to_string();
    cfg.prefix = prefix.to_string();
    cfg.storage_class = storage_class.to_string();
    cfg.predefined_acl = predefined_acl.to_string();
    cfg
}

fn external_account_credentials_blob(token_url: &str, subject_token_file: &Path) -> String {
    format!(
        r#"{{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/1/locations/global/workloadIdentityPools/pool/providers/provider",
  "subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
  "token_url": "{token_url}",
  "credential_source": {{
    "file": "{}"
  }}
}}"#,
        subject_token_file.display()
    )
}

#[tokio::test]
async fn gcp_v2_put_uses_resumable_upload_with_requested_options()
-> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, shutdown, captured) = start_server().await?;
    let cfg = make_cfg(
        &endpoint,
        "test-bucket",
        "pfx",
        "COLDLINE",
        "projectPrivate",
    );
    let mut cfg = cfg;
    let token_file = tempfile::NamedTempFile::new()?;
    std::fs::write(token_file.path(), "header.payload.signature")?;
    cfg.credentials_blob =
        external_account_credentials_blob(&format!("{endpoint}/token"), token_file.path());
    let s = gcp_v2::GcsStorage::from_input(cfg)?;

    s.put(
        "a",
        PutResource(Box::new(futures::io::Cursor::new(b"alpha".to_vec()))),
        5,
    )
    .await?;

    let captured = captured.lock().unwrap();
    let mut saw_predefined = false;
    let mut saw_storage_class = false;
    let mut saw_resumable = false;
    for req in captured.iter() {
        let s = String::from_utf8_lossy(req);
        if s.contains("uploadType=resumable") {
            saw_resumable = true;
        }
        if s.contains("predefinedAcl=projectPrivate") {
            saw_predefined = true;
        }
        if s.contains("\"storageClass\":\"COLDLINE\"") {
            saw_storage_class = true;
        }
    }
    assert!(saw_resumable);
    assert!(saw_predefined);
    assert!(saw_storage_class);

    let _ = shutdown.send(());
    Ok(())
}

#[tokio::test]
async fn gcp_v2_zero_length_put_uses_resumable_upload() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, shutdown, captured) = start_server().await?;
    let cfg = make_cfg(
        &endpoint,
        "test-bucket",
        "pfx",
        "COLDLINE",
        "projectPrivate",
    );
    let mut cfg = cfg;
    let token_file = tempfile::NamedTempFile::new()?;
    std::fs::write(token_file.path(), "header.payload.signature")?;
    cfg.credentials_blob =
        external_account_credentials_blob(&format!("{endpoint}/token"), token_file.path());
    let s = gcp_v2::GcsStorage::from_input(cfg)?;

    s.put(
        "zero",
        PutResource(Box::new(futures::io::Cursor::new(Vec::<u8>::new()))),
        0,
    )
    .await?;

    let captured = captured.lock().unwrap();
    assert!(
        captured
            .iter()
            .any(|req| String::from_utf8_lossy(req).contains("uploadType=resumable"))
    );

    let _ = shutdown.send(());
    Ok(())
}
