// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{str, sync::Arc, time::Duration};

use azure_core::{
    auth::{AccessToken, TokenCredential, TokenResponse},
    base64, content_type,
    error::{Error, ErrorKind},
    headers, new_http_client, HttpClient, Method, Request,
};
use azure_identity::authority_hosts::AZURE_PUBLIC_CLOUD;
use openssl::{
    error::ErrorStack,
    hash::{hash, DigestBytes, MessageDigest},
    pkcs12::Pkcs12,
    pkey::{PKey, Private},
    sign::Signer,
    x509::X509,
};
use serde::Deserialize;
use time::OffsetDateTime;
use url::{form_urlencoded, Url};

/// Refresh time to use in seconds
const DEFAULT_REFRESH_TIME: i64 = 300;

/// Provides options to configure how the Identity library makes authentication
/// requests to Azure Active Directory.
#[derive(Clone, Debug, PartialEq)]
struct CertificateCredentialOptions {
    authority_host: String,
    send_certificate_chain: bool,
}

impl Default for CertificateCredentialOptions {
    fn default() -> Self {
        Self {
            authority_host: AZURE_PUBLIC_CLOUD.to_owned(),
            send_certificate_chain: true,
        }
    }
}

impl CertificateCredentialOptions {
    fn authority_host(&self) -> &str {
        &self.authority_host
    }
}

#[derive(Deserialize, Debug, Default)]
#[serde(default)]
struct AadTokenResponse {
    token_type: String,
    expires_in: u64,
    ext_expires_in: u64,
    access_token: String,
}

/// Enables authentication to Azure Active Directory using a client certificate
/// that was generated for an App Registration. It will automatically cache the
/// latest token from Azure Active Directory.
///
/// In order to use subject name validation send_cert_chain option must be set
/// to true The certificate is expected to be in base64 encoded PKCS12 format.
///
/// TODO: make `ClientCertificateCredentialExt` directly extended from
/// `ClientCertificateCredential` if `ClientCertificateCredential` is nightly
/// released.
pub struct ClientCertificateCredentialExt {
    http_client: Arc<dyn HttpClient>,

    tenant_id: String,
    client_id: String,
    /// Certificate in PKCS12 format, encoded in base64
    certificate: String,
    /// Certificate Pass, default with ""
    certificate_pass: String,
    options: CertificateCredentialOptions,
}

impl ClientCertificateCredentialExt {
    /// Create a new ClientCertificateCredentialExt
    pub fn new(
        tenant_id: String,
        client_id: String,
        certificate: String,
        certificate_pass: String,
    ) -> Self {
        Self {
            http_client: new_http_client(),
            tenant_id,
            client_id,
            certificate,
            certificate_pass,
            options: CertificateCredentialOptions::default(),
        }
    }

    /// Build a new ClientCertificateCredentialExt according to
    /// a given certificate.
    pub fn build(
        tenant_id: String,
        client_id: String,
        certificate_path: String,
        certificate_pass: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let bytes = std::fs::read(certificate_path)?;
        Ok(ClientCertificateCredentialExt::new(
            tenant_id,
            client_id,
            base64::encode(bytes),
            certificate_pass,
        ))
    }

    fn options(&self) -> &CertificateCredentialOptions {
        &self.options
    }

    fn sign(jwt: &str, pkey: &PKey<Private>) -> Result<Vec<u8>, ErrorStack> {
        let mut signer = Signer::new(MessageDigest::sha256(), pkey)?;
        signer.update(jwt.as_bytes())?;
        signer.sign_to_vec()
    }

    fn get_thumbprint(cert: &X509) -> Result<DigestBytes, ErrorStack> {
        let der = cert.to_der()?;
        let digest = hash(MessageDigest::sha1(), &der)?;
        Ok(digest)
    }

    fn as_jwt_part(part: &[u8]) -> String {
        base64::encode_url_safe(part)
    }
}

fn get_encoded_cert(cert: &X509) -> azure_core::Result<String> {
    Ok(format!(
        "\"{}\"",
        base64::encode(cert.to_pem().map_err(openssl_error)?)
    ))
}

fn openssl_error(err: ErrorStack) -> azure_core::error::Error {
    Error::new(ErrorKind::Credential, err)
}

// Not care about "wasm32" platform, this is the requirement from
// [`TokenCredential`](https://github.com/Azure/azure-sdk-for-rust/blob/main/sdk/core/src/auth.rs#L39-L42).
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl TokenCredential for ClientCertificateCredentialExt {
    // As previous [TODO] shows, following operations in `get_token` is just
    // extended from `ClientCertificateCredential::get_token()` as a special
    // version with caching feature and stable feature.
    // Reference of the REST API: https://learn.microsoft.com/en-us/azure/key-vault/general/common-parameters-and-headers.
    async fn get_token(&self, resource: &str) -> azure_core::Result<TokenResponse> {
        let options = self.options();
        let url = &format!(
            "{}/{}/oauth2/v2.0/token",
            options.authority_host(),
            self.tenant_id
        );

        let certificate = base64::decode(&self.certificate)
            .map_err(|_| Error::message(ErrorKind::Credential, "Base64 decode failed"))?;
        let certificate = Pkcs12::from_der(&certificate)
            .map_err(openssl_error)?
            .parse2(&self.certificate_pass)
            .map_err(openssl_error)?;

        if certificate.cert.as_ref().is_none() {
            return Err(Error::message(
                ErrorKind::Credential,
                "Certificate not found",
            ));
        }
        let cert = certificate.cert.as_ref().unwrap();

        if certificate.pkey.as_ref().is_none() {
            return Err(Error::message(
                ErrorKind::Credential,
                "Private key not found",
            ));
        }
        let pkey = certificate.pkey.as_ref().unwrap();

        let thumbprint =
            ClientCertificateCredentialExt::get_thumbprint(cert).map_err(openssl_error)?;

        let uuid = uuid::Uuid::new_v4();
        let current_time = OffsetDateTime::now_utc().unix_timestamp();
        let expiry_time = current_time + DEFAULT_REFRESH_TIME;
        let x5t = base64::encode(thumbprint);

        let header = match options.send_certificate_chain {
            true => {
                let base_signature = get_encoded_cert(cert)?;
                let x5c = match certificate.ca {
                    Some(chain) => {
                        let chain = chain
                            .into_iter()
                            .map(|x| get_encoded_cert(&x))
                            .collect::<azure_core::Result<Vec<String>>>()?
                            .join(",");
                        format! {"{},{}", base_signature, chain}
                    }
                    None => base_signature,
                };
                format!(
                    r#"{{"alg":"RS256","typ":"JWT", "x5t":"{}", "x5c":[{}]}}"#,
                    x5t, x5c
                )
            }
            false => format!(r#"{{"alg":"RS256","typ":"JWT", "x5t":"{}"}}"#, x5t),
        };
        let header = ClientCertificateCredentialExt::as_jwt_part(header.as_bytes());

        let payload = format!(
            r#"{{"aud":"{}","exp":{},"iss": "{}", "jti": "{}", "nbf": {}, "sub": "{}"}}"#,
            url, expiry_time, self.client_id, uuid, current_time, self.client_id
        );
        let payload = ClientCertificateCredentialExt::as_jwt_part(payload.as_bytes());

        let jwt = format!("{}.{}", header, payload);
        let signature = ClientCertificateCredentialExt::sign(&jwt, pkey).map_err(openssl_error)?;
        let sig = ClientCertificateCredentialExt::as_jwt_part(&signature);
        let client_assertion = format!("{}.{}", jwt, sig);

        let encoded = {
            let mut encoded = &mut form_urlencoded::Serializer::new(String::new());
            encoded = encoded
                .append_pair("client_id", self.client_id.as_str())
                .append_pair("scope", format!("{}/.default", resource).as_str())
                .append_pair(
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                )
                .append_pair("client_assertion", client_assertion.as_str())
                .append_pair("grant_type", "client_credentials");
            encoded.finish()
        };

        let url = Url::parse(url)?;
        let mut req = Request::new(url, Method::Post);
        req.insert_header(
            headers::CONTENT_TYPE,
            content_type::APPLICATION_X_WWW_FORM_URLENCODED,
        );
        req.set_body(encoded);

        let rsp = self.http_client.execute_request(&req).await?;
        let rsp_status = rsp.status();
        let rsp_body = rsp.into_body().collect().await?;

        if !rsp_status.is_success() {
            return Err(ErrorKind::http_response_from_body(rsp_status, &rsp_body).into_error());
        }

        let response: AadTokenResponse = serde_json::from_slice(&rsp_body)?;
        let token = TokenResponse::new(
            AccessToken::new(response.access_token.to_string()),
            OffsetDateTime::now_utc() + Duration::from_secs(response.expires_in),
        );
        Ok(token)
    }
}
