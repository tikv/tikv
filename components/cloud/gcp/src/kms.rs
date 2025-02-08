// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, result::Result as StdResult};

use async_trait::async_trait;
use cloud::{
    error::{Error as CloudError, KmsError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KmsProvider, PlainKey},
    metrics, KeyId,
};
use futures_util::stream::StreamExt;
use http::Method;
use hyper::Body;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Deserializer, Serialize};
use tame_gcs::error::HttpStatusError;
use tikv_util::{box_err, stream::RetryError, time::Instant};

use crate::{
    client::{GcpClient, RequestError},
    STORAGE_VENDOR_NAME_GCP,
};

// generated random encryption data key length.
const DEFAULT_DATAKEY_SIZE: usize = 32;
// google kms endpoint.
const GCP_KMS_ENDPOINT: &str = "https://cloudkms.googleapis.com/v1/";

// following are related kms api method names:
const METHOD_ENCRYPT: &str = "encrypt";
const METHOD_DECRYPT: &str = "decrypt";
const METHOD_GEN_RANDOM_BYTES: &str = "generateRandomBytes";

/// Protection level of the generated random key, always using HSM(Hardware
/// Security Module).
const RANDOMIZE_PROTECTION_LEVEL: &str = "HSM";

/// The encryption key_id pattern of gcp ksm:
///   projects/{project_name}/locations/{location}/keyRings/{key_ring}/
/// cryptoKeys/{key}
const KEY_ID_PATTERN: &str =
    r"^projects/([^/]+)/locations/([^/]+)/keyRings/([^/]+)/cryptoKeys/([^/]+)/?$";

lazy_static! {
    //The encryption key_id pattern regexp.
    static ref KEY_ID_REGEX: Regex = Regex::new(KEY_ID_PATTERN).unwrap();
}

pub struct GcpKms {
    config: Config,
    // the location prefix of key id,
    // format: projects/{project_name}/locations/{location}
    location: String,
    client: GcpClient,
}

impl GcpKms {
    pub fn new(mut config: Config) -> Result<Self> {
        assert!(config.gcp.is_some());
        if !KEY_ID_REGEX.is_match(&config.key_id) {
            return Err(CloudError::KmsError(KmsError::WrongMasterKey(box_err!(
                "invalid key: '{}'",
                &config.key_id
            ))));
        }
        // remove the end '/'
        if config.key_id.ends_with('/') {
            let mut key = config.key_id.into_inner();
            key.pop();
            config.key_id = KeyId::new(key)?;
        }
        let location = {
            let key = config.key_id.as_str();
            key.match_indices('/')
                .nth(3)
                .map(|(index, _)| key[..index].to_owned())
                .unwrap()
        };

        let client = GcpClient::load_from(
            config
                .gcp
                .as_ref()
                .and_then(|c| c.credential_file_path.as_deref()),
        )?;
        Ok(Self {
            config,
            location,
            client,
        })
    }

    async fn do_json_request<Q, R>(
        &self,
        key_name: &str,
        method: &'static str,
        data: Q,
    ) -> std::result::Result<R, RequestError>
    where
        Q: Serialize + Send + Sync,
        R: for<'a> Deserialize<'a> + Send + Sync,
    {
        let begin = Instant::now_coarse();
        let url = self.format_call_url(key_name, method);
        let req_builder = http::Request::builder().header(
            http::header::CONTENT_TYPE,
            http::header::HeaderValue::from_static("application/json"),
        );

        let body = serde_json::to_string(&data).unwrap();
        let req = req_builder
            .method(Method::POST)
            .uri(url.clone())
            .body(Body::from(body))
            .map_err(|e| {
                RequestError::Gcs(tame_gcs::error::Error::Http(tame_gcs::error::HttpError(e)))
            })?;
        let resp = self
            .client
            .make_request(req, tame_gcs::Scopes::CloudPlatform)
            .await?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", method])
            .observe(begin.saturating_elapsed_secs());
        if !resp.status().is_success() {
            return Err(RequestError::Gcs(tame_gcs::Error::HttpStatus(
                HttpStatusError(resp.status()),
            )));
        }
        let mut data: Vec<_> = vec![];
        let mut body = resp.into_body();
        while let Some(bytes) = body.next().await {
            match bytes {
                Ok(b) => data.extend(b),
                Err(e) => {
                    return Err(RequestError::Hyper(e, "fetch encrypt resp failed".into()));
                }
            }
        }
        serde_json::from_slice(&data).map_err(|e| RequestError::Gcs(e.into()))
    }

    fn format_call_url(&self, key: &str, method: &str) -> String {
        format!("{}{}/:{}?alt=json", GCP_KMS_ENDPOINT, key, method)
    }
}

impl fmt::Debug for GcpKms {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GcpKmsClient")
            .field("key", &self.config.key_id)
            .finish()
    }
}

#[async_trait]
impl KmsProvider for GcpKms {
    fn name(&self) -> &str {
        STORAGE_VENDOR_NAME_GCP
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is
    // possible that a wrong master key has been used, or other error
    // otherwise.
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let decrypt_req = DecryptRequest {
            ciphertext: data_key.clone().into_inner(),
            ciphertext_crc32c: crc32c::crc32c(data_key.as_raw()),
        };
        let resp: DecryptResp = self
            .do_json_request(self.config.key_id.as_str(), METHOD_DECRYPT, decrypt_req)
            .await
            .map_err(|e| KmsError::Other(e.into()))?;
        check_crc32(&resp.plaintext, resp.plaintext_crc32c)?;
        Ok(resp.plaintext)
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        let random_bytes_req = GenRandomBytesReq {
            length_bytes: DEFAULT_DATAKEY_SIZE,
            protection_level: RANDOMIZE_PROTECTION_LEVEL.into(),
        };
        let rb_resp: GenRandomBytesResp = self
            .do_json_request(&self.location, METHOD_GEN_RANDOM_BYTES, random_bytes_req)
            .await
            .map_err(|e| KmsError::Other(e.into()))?;
        check_crc32(&rb_resp.data, rb_resp.data_crc32c)?;

        let encrypt_request = EncryptRequest {
            plaintext: rb_resp.data.clone(),
            plaintext_crc32c: crc32c::crc32c(&rb_resp.data),
        };
        let resp: EncryptResp = self
            .do_json_request(self.config.key_id.as_str(), METHOD_ENCRYPT, encrypt_request)
            .await
            .map_err(|e| KmsError::Other(e.into()))?;
        check_crc32(&resp.ciphertext, resp.ciphertext_crc32c)?;

        to_data_key(resp, rb_resp.data)
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EncryptRequest {
    #[serde(with = "serde_base64_bytes")]
    plaintext: Vec<u8>,
    plaintext_crc32c: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EncryptResp {
    #[serde(with = "serde_base64_bytes")]
    ciphertext: Vec<u8>,
    #[serde(deserialize_with = "deseralize_u32_from_str")]
    ciphertext_crc32c: u32,
}

fn to_data_key(encrypt_resp: EncryptResp, raw_bytes: Vec<u8>) -> Result<DataKeyPair> {
    Ok(DataKeyPair {
        encrypted: EncryptedKey::new(encrypt_resp.ciphertext)?,
        plaintext: PlainKey::new(raw_bytes, CryptographyType::AesGcm256)?,
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DecryptRequest {
    #[serde(with = "serde_base64_bytes")]
    ciphertext: Vec<u8>,
    ciphertext_crc32c: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DecryptResp {
    #[serde(with = "serde_base64_bytes")]
    plaintext: Vec<u8>,
    #[serde(deserialize_with = "deseralize_u32_from_str")]
    plaintext_crc32c: u32,
}

fn check_crc32(data: &[u8], expected: u32) -> StdResult<(), Crc32Error> {
    let crc = crc32c::crc32c(data);
    if crc != expected {
        return Err(Crc32Error { expected, got: crc });
    }
    Ok(())
}

#[derive(Debug)]
pub struct Crc32Error {
    expected: u32,
    got: u32,
}

impl fmt::Display for Crc32Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "crc32c mismatch, expected: {}, got: {}",
            self.expected, self.got
        )
    }
}

impl std::error::Error for Crc32Error {}

impl RetryError for Crc32Error {
    fn is_retryable(&self) -> bool {
        true
    }
}

impl From<Crc32Error> for CloudError {
    fn from(e: Crc32Error) -> Self {
        Self::KmsError(KmsError::Other(e.into()))
    }
}

mod serde_base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    // deserialize bytes from base64 encoded string.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
        D::Error: serde::de::Error,
    {
        let v = String::deserialize(deserializer)?;
        base64::decode(v)
            .map_err(|e| serde::de::Error::custom(format!("base64 decode failed: {:?}", e,)))
    }

    // serialize bytes with base64 encoding.
    pub fn serialize<S>(data: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let str_data = base64::encode(data);
        serializer.serialize_str(&str_data)
    }
}

fn deseralize_u32_from_str<'de, D>(deserializer: D) -> StdResult<u32, D::Error>
where
    D: Deserializer<'de>,
    D::Error: serde::de::Error,
{
    let v = String::deserialize(deserializer)?;
    v.parse().map_err(|e| {
        serde::de::Error::custom(format!("case crc32 string '{}' as u32 failed: {:?}", &v, e,))
    })
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GenRandomBytesReq {
    length_bytes: usize,
    // we always use "HSM" currently, maybe export it as
    // a config in the future.
    protection_level: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GenRandomBytesResp {
    #[serde(with = "serde_base64_bytes")]
    data: Vec<u8>,
    #[serde(deserialize_with = "deseralize_u32_from_str")]
    data_crc32c: u32,
}

#[cfg(test)]
mod tests {
    use cloud::kms::{Location, SubConfigGcp};

    use super::*;

    #[test]
    fn test_new_gcp_kms() {
        for bad_key in [
            "abc",
            "projects/test-project/locations/us-west-2/keyRings/tikv-gpc-kms-test/cryptoKeys/gl-dev-test//",
            // key with version
            "projects/test-project/locations/us-west-2/keyRings/tikv-gpc-kms-test/cryptoKeys/gl-dev-test/cryptoKeyVersions/1",
        ] {
            let cfg = Config {
                key_id: KeyId::new(bad_key.into()).unwrap(),
                location: Location {
                    region: "".into(),
                    endpoint: "".into(),
                },
                vendor: "gcp".into(),
                azure: None,
                gcp: Some(SubConfigGcp {
                    credential_file_path: None,
                }),
                aws: None,
            };

            _ = GcpKms::new(cfg).unwrap_err();
        }

        for key in [
            "projects/test-project/locations/us-east-1/keyRings/tikv-gpc-kms-test/cryptoKeys/test",
            "projects/test-project/locations/us-east-1/keyRings/tikv-gpc-kms-test/cryptoKeys/test/",
        ] {
            let cfg = Config {
                key_id: KeyId::new(key.into()).unwrap(),
                location: Location {
                    region: "".into(),
                    endpoint: "".into(),
                },
                vendor: "gcp".into(),
                azure: None,
                gcp: Some(SubConfigGcp {
                    credential_file_path: None,
                }),
                aws: None,
            };

            let res = GcpKms::new(cfg).unwrap();
            assert_eq!(&res.location, "projects/test-project/locations/us-east-1");
            assert_eq!(
                res.config.key_id.as_str(),
                "projects/test-project/locations/us-east-1/keyRings/tikv-gpc-kms-test/cryptoKeys/test"
            );
        }
    }

    #[test]
    fn test_serde_base64() {
        #[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
        struct S {
            #[serde(with = "serde_base64_bytes")]
            data: Vec<u8>,
        }

        let st = S {
            data: "abcdedfa\r中文😅".into(),
        };
        let str_data = serde_json::to_string(&st).unwrap();
        assert_eq!(
            &str_data,
            &format!("{{\"data\":\"{}\"}}", base64::encode(&st.data))
        );

        let restored: S = serde_json::from_str(&str_data).unwrap();
        assert_eq!(restored, st);
    }
}
