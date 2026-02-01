use async_trait::async_trait;
use cloud::{
    error::{Error as CloudError, KmsError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KmsProvider, PlainKey},
    metrics,
};
use google_cloud_gax::{
    client_builder::Error as GaxBuildError,
    error::{Error as GaxError, rpc::Code as RpcCode},
};
use google_cloud_auth::credentials::Credentials;
use google_cloud_kms_v1::{client::KeyManagementService, model::ProtectionLevel};
use tikv_util::box_err;
use tikv_util::stream::RetryError;
use tikv_util::time::Instant;
use tokio::sync::OnceCell;

const DEFAULT_DATAKEY_SIZE: usize = 32;

pub struct GcpKms {
    config: Config,
    location: String,
    endpoint: Option<String>,
    credentials: Option<Credentials>,
    client: OnceCell<KeyManagementService>,
}

impl std::fmt::Debug for GcpKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpKms")
            .field("key_id", &self.config.key_id)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl GcpKms {
    pub fn new(mut config: Config) -> Result<Self> {
        if config.gcp.is_none() {
            return Err(CloudError::KmsError(KmsError::Other(
                cloud::error::OtherError::from_box(box_err!("invalid configurations for GCP KMS")),
            )));
        }
        if config.key_id.ends_with('/') {
            let mut key = config.key_id.into_inner();
            key.pop();
            config.key_id = cloud::KeyId::new(key)?;
        }

        let (location, _) = parse_key_id(config.key_id.as_str())?;

        let endpoint = if config.location.endpoint.is_empty() {
            None
        } else {
            Some(config.location.endpoint.clone())
        };

        let credentials = match config.gcp.as_ref().and_then(|c| c.credential_file_path.as_deref())
        {
            Some(path) => {
                let json_data = std::fs::read(path).map_err(|e| {
                    CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
                        box_err!("read credential file: {}", e),
                    )))
                })?;
                let creds_json: serde_json::Value = serde_json::from_slice(&json_data).map_err(|e| {
                    CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
                        box_err!("parse credential file as json: {}", e),
                    )))
                })?;
                let creds = google_cloud_auth::credentials::service_account::Builder::new(creds_json)
                    .build()
                    .map_err(|e| {
                        CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
                            box_err!("build service account credentials: {}", e),
                        )))
                    })?;
                Some(creds)
            }
            None if endpoint.is_some() => Some(google_cloud_auth::credentials::anonymous::Builder::new().build()),
            None => None,
        };

        Ok(Self {
            config,
            location,
            endpoint,
            credentials,
            client: OnceCell::new(),
        })
    }

    async fn get_client(&self) -> Result<KeyManagementService> {
        let client = self
            .client
            .get_or_try_init(|| async {
                let mut builder = KeyManagementService::builder();
                if let Some(ep) = self.endpoint.as_ref() {
                    builder = builder.with_endpoint(ep);
                }
                if let Some(creds) = self.credentials.clone() {
                    builder = builder.with_credentials(creds);
                }
                builder
                    .build()
                    .await
                    .map_err(|e| CloudError::KmsError(KmsError::Other(GaxBuildKmsError(e).into())))
            })
            .await?;
        Ok(client.clone())
    }
}

fn map_kms_call_error(e: GaxError) -> CloudError {
    if let Some(status) = e.status() {
        if matches!(
            status.code,
            RpcCode::Unauthenticated | RpcCode::PermissionDenied
        ) {
            return CloudError::KmsError(KmsError::WrongMasterKey(Box::new(GaxKmsError(e))));
        }
    }
    if let Some(code) = e.http_status_code() {
        if code == 401 || code == 403 {
            return CloudError::KmsError(KmsError::WrongMasterKey(Box::new(GaxKmsError(e))));
        }
    }
    CloudError::KmsError(KmsError::Other(GaxKmsError(e).into()))
}

#[async_trait]
impl KmsProvider for GcpKms {
    fn name(&self) -> &str {
        "gcp"
    }

    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let req = google_cloud_kms_v1::model::DecryptRequest::new()
            .set_name(self.config.key_id.as_str())
            .set_ciphertext(bytes::Bytes::from(data_key.as_raw().to_vec()))
            .set_ciphertext_crc32c(crc32c::crc32c(data_key.as_raw()) as i64);

        let client = self.get_client().await?;
        let begin = Instant::now_coarse();
        let resp = client
            .decrypt()
            .with_request(req)
            .send()
            .await
            .map_err(map_kms_call_error)?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", "decrypt"])
            .observe(begin.saturating_elapsed_secs());

        check_crc32(resp.plaintext.as_ref(), resp.plaintext_crc32c)
            .map_err(CloudError::from)?;
        Ok(resp.plaintext.to_vec())
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        let client = self.get_client().await?;
        let begin = Instant::now_coarse();
        let rb_resp = client
            .generate_random_bytes()
            .set_location(self.location.clone())
            .set_length_bytes(DEFAULT_DATAKEY_SIZE as i32)
            .set_protection_level(ProtectionLevel::Hsm)
            .send()
            .await
            .map_err(map_kms_call_error)?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", "generateRandomBytes"])
            .observe(begin.saturating_elapsed_secs());
        check_crc32(rb_resp.data.as_ref(), rb_resp.data_crc32c).map_err(CloudError::from)?;

        let plaintext = rb_resp.data.to_vec();
        let enc_req = google_cloud_kms_v1::model::EncryptRequest::new()
            .set_name(self.config.key_id.as_str())
            .set_plaintext(rb_resp.data)
            .set_plaintext_crc32c(crc32c::crc32c(&plaintext) as i64);

        let begin = Instant::now_coarse();
        let enc_resp = client
            .encrypt()
            .with_request(enc_req)
            .send()
            .await
            .map_err(map_kms_call_error)?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", "encrypt"])
            .observe(begin.saturating_elapsed_secs());

        if !enc_resp.verified_plaintext_crc32c {
            return Err(CloudError::from(Crc32Error::missing(
                "kms encrypt response missing verified_plaintext_crc32c",
            )));
        }
        check_crc32(enc_resp.ciphertext.as_ref(), enc_resp.ciphertext_crc32c)
            .map_err(CloudError::from)?;

        Ok(DataKeyPair {
            plaintext: PlainKey::new(plaintext, CryptographyType::AesGcm256)?,
            encrypted: EncryptedKey::new(enc_resp.ciphertext.to_vec())?,
        })
    }
}

fn parse_key_id(key_id: &str) -> Result<(String, String)> {
    let parts: Vec<_> = key_id.split('/').collect();
    if parts.len() != 8
        || parts[0] != "projects"
        || parts[2] != "locations"
        || parts[4] != "keyRings"
        || parts[6] != "cryptoKeys"
    {
        return Err(CloudError::KmsError(KmsError::WrongMasterKey(box_err!(
            "invalid key: '{}'",
            key_id
        ))));
    }
    let location = format!("{}/{}/{}/{}", parts[0], parts[1], parts[2], parts[3]);
    let key_name = key_id.to_string();
    Ok((location, key_name))
}

fn check_crc32(data: &[u8], expected: Option<i64>) -> std::result::Result<(), Crc32Error> {
    let expected = expected.ok_or_else(|| Crc32Error::missing("missing crc32c in response"))?;
    let expected = expected as u32;
    let got = crc32c::crc32c(data);
    if got != expected {
        return Err(Crc32Error::mismatch(expected as i64, got as i64));
    }
    Ok(())
}

#[derive(Debug)]
pub struct Crc32Error {
    retryable: bool,
    message: String,
}

impl Crc32Error {
    fn mismatch(expected: i64, got: i64) -> Self {
        Self {
            retryable: true,
            message: format!("crc32c mismatch expected={} got={}", expected, got),
        }
    }

    fn missing(msg: &str) -> Self {
        Self {
            retryable: true,
            message: msg.to_string(),
        }
    }
}

impl std::fmt::Display for Crc32Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Crc32Error {}

impl RetryError for Crc32Error {
    fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl From<Crc32Error> for CloudError {
    fn from(e: Crc32Error) -> Self {
        CloudError::KmsError(KmsError::Other(e.into()))
    }
}

#[derive(Debug)]
struct GaxKmsError(GaxError);

impl std::fmt::Display for GaxKmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GaxKmsError {}

impl RetryError for GaxKmsError {
    fn is_retryable(&self) -> bool {
        let e = &self.0;
        if e.is_timeout() || e.is_exhausted() || e.is_transient_and_before_rpc() {
            return true;
        }
        if let Some(code) = e.http_status_code() {
            return code == 408 || code == 429 || (500..=599).contains(&code);
        }
        if let Some(status) = e.status() {
            return matches!(
                status.code,
                RpcCode::Unavailable
                    | RpcCode::DeadlineExceeded
                    | RpcCode::ResourceExhausted
                    | RpcCode::Internal
                    | RpcCode::Unknown
            );
        }
        false
    }
}

#[derive(Debug)]
struct GaxBuildKmsError(GaxBuildError);

impl std::fmt::Display for GaxBuildKmsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for GaxBuildKmsError {}

impl RetryError for GaxBuildKmsError {
    fn is_retryable(&self) -> bool {
        self.0.is_transport()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use prometheus::proto::MetricType;
    use std::io;
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

    fn build_http_response(body: &str) -> Vec<u8> {
        let body_bytes = body.as_bytes();
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body_bytes.len()
        )
        .into_bytes()
        .into_iter()
        .chain(body_bytes.iter().copied())
        .collect()
    }

    fn build_http_404() -> Vec<u8> {
        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
    }

    fn response_for_target(target: &str) -> Vec<u8> {
        if target.contains(":generateRandomBytes") {
            let data = vec![0x42_u8; DEFAULT_DATAKEY_SIZE];
            let crc = crc32c::crc32c(&data) as i64;
            let body = serde_json::json!({
                "data": base64::engine::general_purpose::STANDARD.encode(&data),
                "dataCrc32c": crc.to_string(),
            });
            return build_http_response(&body.to_string());
        }
        if target.contains(":encrypt") {
            let ciphertext = b"ciphertext".to_vec();
            let crc = crc32c::crc32c(&ciphertext) as i64;
            let body = serde_json::json!({
                "ciphertext": base64::engine::general_purpose::STANDARD.encode(&ciphertext),
                "ciphertextCrc32c": crc.to_string(),
                "verifiedPlaintextCrc32c": true,
            });
            return build_http_response(&body.to_string());
        }
        if target.contains(":decrypt") {
            let plaintext = vec![0x42_u8; DEFAULT_DATAKEY_SIZE];
            let crc = crc32c::crc32c(&plaintext) as i64;
            let body = serde_json::json!({
                "plaintext": base64::engine::general_purpose::STANDARD.encode(&plaintext),
                "plaintextCrc32c": crc.to_string(),
            });
            return build_http_response(&body.to_string());
        }
        build_http_404()
    }

    async fn start_server() -> io::Result<(String, oneshot::Sender<()>)> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => { break; }
                    res = listener.accept() => {
                        let Ok((mut socket, _)) = res else { break; };
                        tokio::spawn(async move {
                            let mut buf = Vec::with_capacity(4096);
                            let mut tmp = [0u8; 1024];
                            loop {
                                let Ok(n) = socket.read(&mut tmp).await else { return; };
                                if n == 0 { return; }
                                buf.extend_from_slice(&tmp[..n]);
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                                if buf.len() > 64 * 1024 { return; }
                            }

                            let request = match std::str::from_utf8(&buf) {
                                Ok(s) => s,
                                Err(_) => return,
                            };
                            let mut lines = request.lines();
                            let Some(first) = lines.next() else { return; };
                            let mut parts = first.split_whitespace();
                            let _method = parts.next();
                            let target = parts.next().unwrap_or("/");

                            let response = response_for_target(target);
                            let _ = socket.write_all(&response).await;
                            let _ = socket.shutdown().await;
                        });
                    }
                }
            }
        });

        Ok((format!("http://{addr}"), shutdown_tx))
    }

    #[tokio::test]
    async fn roundtrip_generate_and_decrypt() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (endpoint, shutdown) = start_server().await?;
        let cfg = cloud::kms::Config {
            key_id: cloud::KeyId::new(
                "projects/p/locations/l/keyRings/r/cryptoKeys/k".to_string(),
            )?,
            location: cloud::kms::Location {
                region: "l".to_string(),
                endpoint,
            },
            vendor: "gcp".to_string(),
            azure: None,
            gcp: Some(cloud::kms::SubConfigGcp {
                credential_file_path: None,
            }),
            aws: None,
        };

        let kms = GcpKms::new(cfg)?;
        let before_generate = histogram_sample_count("gcp", "generateRandomBytes");
        let before_encrypt = histogram_sample_count("gcp", "encrypt");
        let before_decrypt = histogram_sample_count("gcp", "decrypt");
        let dk = kms.generate_data_key().await?;
        let pt = kms.decrypt_data_key(&dk.encrypted).await?;
        assert_eq!(pt, dk.plaintext.as_slice());
        let after_generate = histogram_sample_count("gcp", "generateRandomBytes");
        let after_encrypt = histogram_sample_count("gcp", "encrypt");
        let after_decrypt = histogram_sample_count("gcp", "decrypt");
        assert!(after_generate >= before_generate + 1);
        assert!(after_encrypt >= before_encrypt + 1);
        assert!(after_decrypt >= before_decrypt + 1);

        let _ = shutdown.send(());
        Ok(())
    }

    fn build_http_unauthorized() -> Vec<u8> {
        b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
    }

    async fn start_unauthorized_server() -> io::Result<(String, oneshot::Sender<()>)> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => { break; }
                    res = listener.accept() => {
                        let Ok((mut socket, _)) = res else { break; };
                        tokio::spawn(async move {
                            let mut buf = Vec::with_capacity(4096);
                            let mut tmp = [0u8; 1024];
                            loop {
                                let Ok(n) = socket.read(&mut tmp).await else { return; };
                                if n == 0 { return; }
                                buf.extend_from_slice(&tmp[..n]);
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
                                if buf.len() > 64 * 1024 { return; }
                            }
                            let _ = socket.write_all(&build_http_unauthorized()).await;
                            let _ = socket.shutdown().await;
                        });
                    }
                }
            }
        });

        Ok((format!("http://{addr}"), shutdown_tx))
    }

    #[tokio::test]
    async fn unauthenticated_maps_to_wrong_master_key() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let (endpoint, shutdown) = start_unauthorized_server().await?;
        let cfg = cloud::kms::Config {
            key_id: cloud::KeyId::new(
                "projects/p/locations/l/keyRings/r/cryptoKeys/k".to_string(),
            )?,
            location: cloud::kms::Location {
                region: "l".to_string(),
                endpoint,
            },
            vendor: "gcp".to_string(),
            azure: None,
            gcp: Some(cloud::kms::SubConfigGcp {
                credential_file_path: None,
            }),
            aws: None,
        };

        let kms = GcpKms::new(cfg)?;
        let err = kms.generate_data_key().await.unwrap_err();
        assert!(matches!(err, CloudError::KmsError(KmsError::WrongMasterKey(_))), "err={err:?}");
        let _ = shutdown.send(());
        Ok(())
    }
}
