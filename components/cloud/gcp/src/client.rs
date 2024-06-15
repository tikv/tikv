// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryInto,
    fmt::{self, Display},
    io,
    result::Result as StdResult,
    sync::Arc,
};

use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
use serde::Deserialize;
use tame_oauth::gcp::{
    end_user::EndUserCredentialsInner, service_account::ServiceAccountProviderInner,
    EndUserCredentialsInfo, ServiceAccountInfo, TokenOrRequest, TokenProvider,
    TokenProviderWrapper, TokenProviderWrapperInner,
};
use tikv_util::stream::RetryError;

// GCS compatible storage
#[derive(Clone)]
pub(crate) struct GcpClient {
    token_provider: Option<Arc<TokenProviderWrapper>>,
    client: Client<HttpsConnector<HttpConnector>, Body>,
}

impl GcpClient {
    /// Create a new gcp cleint for the given config.
    pub fn with_svc_info(svc_info: Option<ServiceAccountInfo>) -> io::Result<GcpClient> {
        let token_provider = if let Some(info) = svc_info {
            let svc_info_provider = ServiceAccountProviderInner::new(info)
                .or_invalid_input("invalid credentials_blob")?;
            Some(TokenProviderWrapperInner::ServiceAccount(svc_info_provider))
        } else {
            None
        };
        Ok(Self::with_token_provider(token_provider))
    }

    fn with_token_provider(token_provider: Option<TokenProviderWrapperInner>) -> Self {
        let client = Client::builder().build(HttpsConnector::new());
        Self {
            token_provider: token_provider.map(|t| Arc::new(TokenProviderWrapper::wrap(t))),
            client,
        }
    }

    pub fn with_default_provider() -> io::Result<GcpClient> {
        let provider = TokenProviderWrapperInner::get_default_provider()
            .map_err(|e| RequestError::OAuth(e, "default_provider".into()))?;
        Ok(Self::with_token_provider(provider))
    }

    pub fn load_from(credentail_path: Option<&str>) -> io::Result<GcpClient> {
        if let Some(path) = credentail_path {
            let json_data = std::fs::read(path)?;
            let cred_type = CredentialType::parse_from_json(&json_data)?;
            match cred_type {
                CredentialType::ServiceAccount => {
                    let svc_info = serde_json::from_slice(&json_data)?;
                    return Self::with_svc_info(Some(svc_info));
                }
                CredentialType::AuthorizedUser => {
                    let user_credential: EndUserCredentialsInfo =
                        serde_json::from_slice(&json_data)?;
                    let provider = EndUserCredentialsInner::new(user_credential);
                    return Ok(Self::with_token_provider(Some(
                        TokenProviderWrapperInner::EndUser(provider),
                    )));
                }
            }
        };
        Self::with_default_provider()
    }

    pub(crate) async fn set_auth(
        &self,
        req: &mut Request<Body>,
        scope: tame_gcs::Scopes,
        token_provider: Arc<TokenProviderWrapper>,
    ) -> StdResult<(), RequestError> {
        let token_or_request = token_provider
            .get_token(&[scope])
            .map_err(|e| RequestError::OAuth(e, "get_token".to_string()))?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                let res = self
                    .client
                    .request(request.map(From::from))
                    .await
                    .map_err(|e| RequestError::Hyper(e, "set auth request".to_owned()))?;
                if !res.status().is_success() {
                    return Err(status_code_error(
                        res.status(),
                        "set auth request".to_string(),
                    ));
                }
                let (parts, body) = res.into_parts();
                let body = hyper::body::to_bytes(body)
                    .await
                    .map_err(|e| RequestError::Hyper(e, "set auth body".to_owned()))?;
                token_provider
                    .parse_token_response(scope_hash, Response::from_parts(parts, body))
                    .map_err(|e| RequestError::OAuth(e, "set auth parse token".to_string()))?
            }
        };
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            token
                .try_into()
                .map_err(|e| RequestError::OAuth(e, "set auth add auth header".to_string()))?,
        );

        Ok(())
    }

    pub async fn make_request(
        &self,
        mut req: Request<Body>,
        scope: tame_gcs::Scopes,
    ) -> StdResult<Response<Body>, RequestError> {
        if let Some(svc_access) = &self.token_provider {
            self.set_auth(&mut req, scope, svc_access.clone()).await?;
        }
        let uri = req.uri().to_string();
        let res = self
            .client
            .request(req)
            .await
            .map_err(|e| RequestError::Hyper(e, uri.clone()))?;
        if !res.status().is_success() {
            return Err(status_code_error(res.status(), uri));
        }
        Ok(res)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CredentialType {
    ServiceAccount,
    AuthorizedUser,
}

impl CredentialType {
    fn parse_from_json(data: &[u8]) -> StdResult<Self, serde_json::error::Error> {
        let wrapper: TypeWrapper = serde_json::from_slice(data)?;
        Ok(wrapper.cred_type)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct TypeWrapper {
    #[serde(rename = "type")]
    cred_type: CredentialType,
}

trait ResultExt {
    type Ok;

    // Maps the error of this result as an `std::io::Error` with `Other` error
    // kind.
    fn or_io_error<D: Display>(self, msg: D) -> io::Result<Self::Ok>;

    // Maps the error of this result as an `std::io::Error` with `InvalidInput`
    // error kind.
    fn or_invalid_input<D: Display>(self, msg: D) -> io::Result<Self::Ok>;
}

impl<T, E: Display> ResultExt for StdResult<T, E> {
    type Ok = T;
    fn or_io_error<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", msg, e)))
    }
    fn or_invalid_input<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}: {}", msg, e)))
    }
}

#[derive(Debug)]
pub enum RequestError {
    Hyper(hyper::Error, String),
    OAuth(tame_oauth::Error, String),
    Gcs(tame_gcs::Error),
    InvalidEndpoint(http::uri::InvalidUri),
}

impl Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for RequestError {}

impl From<http::uri::InvalidUri> for RequestError {
    fn from(err: http::uri::InvalidUri) -> Self {
        Self::InvalidEndpoint(err)
    }
}

pub fn status_code_error(code: StatusCode, msg: String) -> RequestError {
    RequestError::OAuth(tame_oauth::Error::HttpStatus(code), msg)
}

impl From<RequestError> for io::Error {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::Hyper(e, msg) => {
                Self::new(io::ErrorKind::InvalidInput, format!("HTTP {}: {}", msg, e))
            }
            RequestError::OAuth(tame_oauth::Error::Io(e), _) => e,
            RequestError::OAuth(tame_oauth::Error::HttpStatus(sc), msg) => {
                let fmt = format!("GCS OAuth: {}: {}", msg, sc);
                match sc.as_u16() {
                    401 | 403 => Self::new(io::ErrorKind::PermissionDenied, fmt),
                    404 => Self::new(io::ErrorKind::NotFound, fmt),
                    _ if sc.is_server_error() => Self::new(io::ErrorKind::Interrupted, fmt),
                    _ => Self::new(io::ErrorKind::InvalidInput, fmt),
                }
            }
            RequestError::OAuth(tame_oauth::Error::Auth(e), msg) => Self::new(
                io::ErrorKind::PermissionDenied,
                format!("authorization failed: {}: {}", msg, e),
            ),
            RequestError::OAuth(e, msg) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("oauth failed: {}: {}", msg, e),
            ),
            RequestError::Gcs(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS request: {}", e),
            ),
            RequestError::InvalidEndpoint(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS endpoint URI: {}", e),
            ),
        }
    }
}

impl RetryError for RequestError {
    fn is_retryable(&self) -> bool {
        match self {
            // FIXME: Inspect the error source?
            Self::Hyper(e, _) => {
                e.is_closed()
                    || e.is_connect()
                    || e.is_incomplete_message()
                    || e.is_body_write_aborted()
            }
            // See https://cloud.google.com/storage/docs/exponential-backoff.
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::TOO_MANY_REQUESTS), _) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::REQUEST_TIMEOUT), _) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(status), _) => status.is_server_error(),
            // Consider everything else not retryable.
            _ => false,
        }
    }
}
