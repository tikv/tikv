// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use cloud::{
    STORAGE_VENDOR_NAME_GCP_V2,
    error::{Error as CloudError, KmsError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KmsProvider, PlainKey},
    metrics,
};
use google_cloud_gax::{
    client_builder::Error as GaxBuildError,
    error::{Error as GaxError, rpc::Code as RpcCode},
};
use google_cloud_kms_v1::{client::KeyManagementService, model::ProtectionLevel};
use tikv_util::{box_err, stream::RetryError, time::Instant};
use tokio::sync::OnceCell;

use crate::credentials::{
    CredentialsMode, build_credentials, ensure_rustls_fips_provider, validate_credentials_json,
};

const DEFAULT_DATAKEY_SIZE: usize = 32;

pub struct GcpKms {
    config: Config,
    location: String,
    endpoint: Option<String>,
    credentials_mode: CredentialsMode,
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
    fn map_credential_error(e: std::io::Error) -> CloudError {
        CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
            box_err!("build credentials: {}", e),
        )))
    }

    pub fn new(mut config: Config) -> Result<Self> {
        if config.gcp.is_none() {
            return Err(CloudError::KmsError(KmsError::Other(
                cloud::error::OtherError::from_box(box_err!("invalid configurations for GCP KMS")),
            )));
        }
        ensure_rustls_fips_provider().map_err(Self::map_credential_error)?;
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

        let credentials_mode = match config
            .gcp
            .as_ref()
            .and_then(|c| c.credential_file_path.as_deref())
        {
            Some(path) => {
                let json_data = std::fs::read(path).map_err(|e| {
                    CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
                        box_err!("read credential file: {}", e),
                    )))
                })?;
                let json_string = String::from_utf8(json_data).map_err(|e| {
                    CloudError::KmsError(KmsError::Other(cloud::error::OtherError::from_box(
                        box_err!("credential file is not valid utf-8: {}", e),
                    )))
                })?;
                validate_credentials_json(&json_string).map_err(Self::map_credential_error)?;
                CredentialsMode::Json(json_string)
            }
            None => CredentialsMode::Default,
        };

        Ok(Self {
            config,
            location,
            endpoint,
            credentials_mode,
            client: OnceCell::new(),
        })
    }

    async fn get_client(&self) -> Result<KeyManagementService> {
        let client =
            self.client
                .get_or_try_init(|| async {
                    let mut builder = KeyManagementService::builder();
                    if let Some(ep) = self.endpoint.as_ref() {
                        builder = builder.with_endpoint(ep);
                    }
                    if let Some(creds) = build_credentials(&self.credentials_mode)
                        .map_err(Self::map_credential_error)?
                    {
                        builder = builder.with_credentials(creds);
                    }
                    builder.build().await.map_err(|e| {
                        CloudError::KmsError(KmsError::Other(GaxBuildKmsError(e).into()))
                    })
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

#[async_trait::async_trait]
impl KmsProvider for GcpKms {
    fn name(&self) -> &str {
        STORAGE_VENDOR_NAME_GCP_V2
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

        check_crc32(resp.plaintext.as_ref(), resp.plaintext_crc32c).map_err(CloudError::from)?;
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
    use std::future::{self, Future};

    use google_cloud_gax::{
        error::{Error as GaxError, rpc::Status},
        options::RequestOptions,
        response::Response,
    };
    use google_cloud_kms_v1::{
        model::{
            DecryptRequest, DecryptResponse, EncryptRequest, EncryptResponse,
            GenerateRandomBytesRequest, GenerateRandomBytesResponse,
        },
        stub,
    };
    use prometheus::proto::MetricType;

    use super::*;

    const TEST_KEY_ID: &str = "projects/p/locations/l/keyRings/r/cryptoKeys/k";
    const TEST_LOCATION: &str = "projects/p/locations/l";
    const TEST_CIPHERTEXT_PREFIX: &[u8] = b"stub-kms:";

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

    fn test_config() -> std::result::Result<cloud::kms::Config, Box<dyn std::error::Error>> {
        Ok(cloud::kms::Config {
            key_id: cloud::KeyId::new(TEST_KEY_ID.to_string())?,
            location: cloud::kms::Location {
                region: "l".to_string(),
                endpoint: String::new(),
            },
            vendor: "gcp".to_string(),
            azure: None,
            gcp: Some(cloud::kms::SubConfigGcp {
                credential_file_path: None,
            }),
            aws: None,
        })
    }

    fn kms_with_stub<T>(stub: T) -> std::result::Result<GcpKms, Box<dyn std::error::Error>>
    where
        T: stub::KeyManagementService + 'static,
    {
        let kms = GcpKms::new(test_config()?)?;
        kms.client
            .set(KeyManagementService::from_stub(stub))
            .unwrap();
        Ok(kms)
    }

    fn wrap_ciphertext(plaintext: &[u8]) -> Vec<u8> {
        let mut ciphertext = TEST_CIPHERTEXT_PREFIX.to_vec();
        ciphertext.extend(plaintext.iter().rev().copied());
        ciphertext
    }

    fn unwrap_ciphertext(ciphertext: &[u8]) -> Vec<u8> {
        ciphertext
            .strip_prefix(TEST_CIPHERTEXT_PREFIX)
            .expect("ciphertext should be produced by the test KMS stub")
            .iter()
            .rev()
            .copied()
            .collect()
    }

    #[derive(Debug)]
    struct RoundTripKmsStub;

    impl stub::KeyManagementService for RoundTripKmsStub {
        fn generate_random_bytes(
            &self,
            req: GenerateRandomBytesRequest,
            _options: RequestOptions,
        ) -> impl Future<
            Output = google_cloud_kms_v1::Result<Response<GenerateRandomBytesResponse>>,
        > + Send {
            assert_eq!(req.location, TEST_LOCATION);
            assert_eq!(req.length_bytes, DEFAULT_DATAKEY_SIZE as i32);
            assert_eq!(req.protection_level, ProtectionLevel::Hsm);

            let data = vec![0x42_u8; DEFAULT_DATAKEY_SIZE];
            let response = GenerateRandomBytesResponse::new()
                .set_data(data.clone())
                .set_data_crc32c(crc32c::crc32c(&data) as i64);
            future::ready(Ok(Response::from(response)))
        }

        fn encrypt(
            &self,
            req: EncryptRequest,
            _options: RequestOptions,
        ) -> impl Future<Output = google_cloud_kms_v1::Result<Response<EncryptResponse>>> + Send
        {
            assert_eq!(req.name, TEST_KEY_ID);
            assert_eq!(
                req.plaintext_crc32c,
                Some(crc32c::crc32c(req.plaintext.as_ref()) as i64)
            );

            let ciphertext = wrap_ciphertext(req.plaintext.as_ref());
            let response = EncryptResponse::new()
                .set_name(format!("{TEST_KEY_ID}/cryptoKeyVersions/1"))
                .set_ciphertext(ciphertext.clone())
                .set_ciphertext_crc32c(crc32c::crc32c(&ciphertext) as i64)
                .set_verified_plaintext_crc32c(true)
                .set_protection_level(ProtectionLevel::Hsm);
            future::ready(Ok(Response::from(response)))
        }

        fn decrypt(
            &self,
            req: DecryptRequest,
            _options: RequestOptions,
        ) -> impl Future<Output = google_cloud_kms_v1::Result<Response<DecryptResponse>>> + Send
        {
            assert_eq!(req.name, TEST_KEY_ID);
            assert_eq!(
                req.ciphertext_crc32c,
                Some(crc32c::crc32c(req.ciphertext.as_ref()) as i64)
            );

            let plaintext = unwrap_ciphertext(req.ciphertext.as_ref());
            let response = DecryptResponse::new()
                .set_plaintext(plaintext.clone())
                .set_plaintext_crc32c(crc32c::crc32c(&plaintext) as i64)
                .set_used_primary(true)
                .set_protection_level(ProtectionLevel::Hsm);
            future::ready(Ok(Response::from(response)))
        }
    }

    #[derive(Debug)]
    struct UnauthorizedKmsStub;

    impl stub::KeyManagementService for UnauthorizedKmsStub {
        fn generate_random_bytes(
            &self,
            _req: GenerateRandomBytesRequest,
            _options: RequestOptions,
        ) -> impl Future<
            Output = google_cloud_kms_v1::Result<Response<GenerateRandomBytesResponse>>,
        > + Send {
            let error = GaxError::service(
                Status::default()
                    .set_code(google_cloud_gax::error::rpc::Code::PermissionDenied)
                    .set_message("permission denied"),
            );
            future::ready(Err(error))
        }
    }

    #[tokio::test]
    async fn roundtrip_generate_and_decrypt() -> std::result::Result<(), Box<dyn std::error::Error>>
    {
        let kms = kms_with_stub(RoundTripKmsStub)?;
        let before_generate = histogram_sample_count("gcp", "generateRandomBytes");
        let before_encrypt = histogram_sample_count("gcp", "encrypt");
        let before_decrypt = histogram_sample_count("gcp", "decrypt");
        let dk = kms.generate_data_key().await?;
        let pt = kms.decrypt_data_key(&dk.encrypted).await?;
        assert_eq!(pt, dk.plaintext.as_slice());
        let after_generate = histogram_sample_count("gcp", "generateRandomBytes");
        let after_encrypt = histogram_sample_count("gcp", "encrypt");
        let after_decrypt = histogram_sample_count("gcp", "decrypt");
        assert!(after_generate > before_generate);
        assert!(after_encrypt > before_encrypt);
        assert!(after_decrypt > before_decrypt);
        Ok(())
    }

    #[tokio::test]
    async fn unauthenticated_maps_to_wrong_master_key()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let kms = kms_with_stub(UnauthorizedKmsStub)?;
        let err = kms.generate_data_key().await.unwrap_err();
        assert!(
            matches!(err, CloudError::KmsError(KmsError::WrongMasterKey(_))),
            "err={err:?}"
        );
        Ok(())
    }

    #[test]
    fn external_account_credentials_are_recognized() {
        let creds = serde_json::json!({
            "type": "external_account",
        });
        let err = crate::credentials::build_credentials(
            &crate::credentials::CredentialsMode::Json(creds.to_string()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            !msg.contains("unsupported credentials_blob type"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn custom_endpoint_without_credentials_uses_default_credentials_mode()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut config = test_config()?;
        config.location.endpoint = "http://127.0.0.1:1".to_owned();
        let kms = GcpKms::new(config)?;
        assert!(matches!(kms.credentials_mode, CredentialsMode::Default));
        Ok(())
    }
}
