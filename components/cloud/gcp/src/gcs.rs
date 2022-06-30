// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::{convert::TryInto, fmt::Display, io, sync::Arc};

use async_trait::async_trait;
use cloud::blob::{
    none_to_empty, BlobConfig, BlobStorage, BucketConf, PutResource, StringNonEmpty,
};
use futures_util::{
    future::TryFutureExt,
    io::{AsyncRead, AsyncReadExt, Cursor},
    stream::{StreamExt, TryStreamExt},
};
use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
pub use kvproto::brpb::{Bucket as InputBucket, CloudDynamic, Gcs as InputConfig};
use tame_gcs::{
    common::{PredefinedAcl, StorageClass},
    objects::{InsertObjectOptional, Metadata, Object},
    types::{BucketName, ObjectId},
};
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};
use tikv_util::stream::{error_stream, retry, AsyncReadAsSyncStreamOfBytes, RetryError};

const GOOGLE_APIS: &str = "https://www.googleapis.com";
const HARDCODED_ENDPOINTS_SUFFIX: &[&str] = &["upload/storage/v1/", "storage/v1/"];

#[derive(Clone, Debug)]
pub struct Config {
    bucket: BucketConf,
    predefined_acl: Option<PredefinedAcl>,
    storage_class: Option<StorageClass>,
    svc_info: Option<ServiceAccountInfo>,
}

impl Config {
    #[cfg(test)]
    pub fn default(bucket: BucketConf) -> Self {
        Self {
            bucket,
            predefined_acl: None,
            storage_class: None,
            svc_info: None,
        }
    }

    pub fn missing_credentials() -> io::Error {
        io::Error::new(io::ErrorKind::InvalidInput, "missing credentials")
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Config> {
        let bucket = BucketConf::from_cloud_dynamic(cloud_dynamic)?;
        let attrs = &cloud_dynamic.attrs;
        let def = &String::new();
        let predefined_acl = parse_predefined_acl(attrs.get("predefined_acl").unwrap_or(def))
            .or_invalid_input("invalid predefined_acl")?;
        let storage_class = parse_storage_class(&none_to_empty(bucket.storage_class.clone()))
            .or_invalid_input("invalid storage_class")?;

        let credentials_blob_opt = StringNonEmpty::opt(
            attrs
                .get("credentials_blob")
                .unwrap_or(&"".to_string())
                .to_string(),
        );
        let svc_info = if let Some(cred) = credentials_blob_opt {
            Some(deserialize_service_account_info(cred)?)
        } else {
            None
        };

        Ok(Config {
            bucket,
            predefined_acl,
            svc_info,
            storage_class,
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let endpoint = StringNonEmpty::opt(input.endpoint);
        let bucket = BucketConf {
            endpoint,
            bucket: StringNonEmpty::required_field(input.bucket, "bucket")?,
            prefix: StringNonEmpty::opt(input.prefix),
            storage_class: StringNonEmpty::opt(input.storage_class),
            region: None,
        };
        let predefined_acl = parse_predefined_acl(&input.predefined_acl)
            .or_invalid_input("invalid predefined_acl")?;
        let storage_class = parse_storage_class(&none_to_empty(bucket.storage_class.clone()))
            .or_invalid_input("invalid storage_class")?;
        let svc_info = if let Some(cred) = StringNonEmpty::opt(input.credentials_blob) {
            Some(deserialize_service_account_info(cred)?)
        } else {
            None
        };
        Ok(Config {
            bucket,
            predefined_acl,
            svc_info,
            storage_class,
        })
    }
}

fn deserialize_service_account_info(
    cred: StringNonEmpty,
) -> std::result::Result<ServiceAccountInfo, RequestError> {
    ServiceAccountInfo::deserialize(cred.to_string())
        .map_err(|e| RequestError::OAuth(e, "deserialize ServiceAccountInfo".to_string()))
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        self.bucket.url("gcs").map_err(|s| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("error creating bucket url: {}", s),
            )
        })
    }
}

// GCS compatible storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Option<Arc<ServiceAccountAccess>>,
    client: Client<HttpsConnector<HttpConnector>, Body>,
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

impl<T, E: Display> ResultExt for Result<T, E> {
    type Ok = T;
    fn or_io_error<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}: {}", msg, e)))
    }
    fn or_invalid_input<D: Display>(self, msg: D) -> io::Result<T> {
        self.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}: {}", msg, e)))
    }
}

enum RequestError {
    Hyper(hyper::Error, String),
    OAuth(tame_oauth::Error, String),
    Gcs(tame_gcs::Error),
    InvalidEndpoint(http::uri::InvalidUri),
}

impl From<http::uri::InvalidUri> for RequestError {
    fn from(err: http::uri::InvalidUri) -> Self {
        Self::InvalidEndpoint(err)
    }
}

fn status_code_error(code: StatusCode, msg: String) -> RequestError {
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
            RequestError::OAuth(tame_oauth::Error::AuthError(e), msg) => Self::new(
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

impl GCSStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        Self::new(Config::from_cloud_dynamic(cloud_dynamic)?)
    }

    /// Create a new GCS storage for the given config.
    pub fn new(config: Config) -> io::Result<GCSStorage> {
        let svc_access = if let Some(si) = &config.svc_info {
            Some(
                ServiceAccountAccess::new(si.clone())
                    .or_invalid_input("invalid credentials_blob")?,
            )
        } else {
            None
        };

        let client = Client::builder().build(HttpsConnector::new());
        Ok(GCSStorage {
            config,
            svc_access: svc_access.map(Arc::new),
            client,
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", prefix, key);
        }
        key.to_owned()
    }

    async fn set_auth(
        &self,
        req: &mut Request<Body>,
        scope: tame_gcs::Scopes,
        svc_access: Arc<ServiceAccountAccess>,
    ) -> Result<(), RequestError> {
        let token_or_request = svc_access
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
                svc_access
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

    async fn make_request(
        &self,
        mut req: Request<Body>,
        scope: tame_gcs::Scopes,
    ) -> Result<Response<Body>, RequestError> {
        // replace the hard-coded GCS endpoint by the custom one.

        if let Some(endpoint) = &self.config.bucket.endpoint {
            let uri = req.uri().to_string();
            let new_url_opt = change_host(endpoint, &uri);
            if let Some(new_url) = new_url_opt {
                *req.uri_mut() = new_url.parse()?;
            }
        }

        if let Some(svc_access) = &self.svc_access {
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

    fn error_to_async_read<E>(kind: io::ErrorKind, e: E) -> Box<dyn AsyncRead + Unpin>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Box::new(error_stream(io::Error::new(kind, e)).into_async_read())
    }
}

fn change_host(host: &StringNonEmpty, url: &str) -> Option<String> {
    let new_host = (|| {
        for hardcoded in HARDCODED_ENDPOINTS_SUFFIX {
            if let Some(res) = host.strip_suffix(hardcoded) {
                return StringNonEmpty::opt(res.to_owned()).unwrap();
            }
        }
        host.to_owned()
    })();
    if let Some(res) = url.strip_prefix(GOOGLE_APIS) {
        return Some([new_host.trim_end_matches('/'), res].concat());
    }
    None
}

// Convert manually since they don't implement FromStr.
fn parse_storage_class(sc: &str) -> Result<Option<StorageClass>, &str> {
    Ok(Some(match sc {
        "" => return Ok(None),
        "STANDARD" => StorageClass::Standard,
        "NEARLINE" => StorageClass::Nearline,
        "COLDLINE" => StorageClass::Coldline,
        "DURABLE_REDUCED_AVAILABILITY" => StorageClass::DurableReducedAvailability,
        "REGIONAL" => StorageClass::Regional,
        "MULTI_REGIONAL" => StorageClass::MultiRegional,
        _ => return Err(sc),
    }))
}

fn parse_predefined_acl(acl: &str) -> Result<Option<PredefinedAcl>, &str> {
    Ok(Some(match acl {
        "" => return Ok(None),
        "authenticatedRead" => PredefinedAcl::AuthenticatedRead,
        "bucketOwnerFullControl" => PredefinedAcl::BucketOwnerFullControl,
        "bucketOwnerRead" => PredefinedAcl::BucketOwnerRead,
        "private" => PredefinedAcl::Private,
        "projectPrivate" => PredefinedAcl::ProjectPrivate,
        "publicRead" => PredefinedAcl::PublicRead,
        _ => return Err(acl),
    }))
}

const STORAGE_NAME: &str = "gcs";

#[async_trait]
impl BlobStorage for GCSStorage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.config.clone()) as Box<dyn BlobConfig>
    }

    async fn put(
        &self,
        name: &str,
        mut reader: PutResource,
        content_length: u64,
    ) -> io::Result<()> {
        if content_length == 0 {
            // It is probably better to just write the empty file
            // However, currently going forward results in a body write aborted error
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no content to write",
            ));
        }
        use std::convert::TryFrom;

        let key = self.maybe_prefix_key(name);
        debug!("save file to GCS storage"; "key" => %key);
        let bucket = BucketName::try_from(self.config.bucket.bucket.to_string())
            .or_invalid_input(format_args!("invalid bucket {}", self.config.bucket.bucket))?;

        let metadata = Metadata {
            name: Some(key),
            storage_class: self.config.storage_class,
            ..Default::default()
        };

        // FIXME: Switch to upload() API so we don't need to read the entire data into memory
        // in order to retry.
        let mut data = Vec::with_capacity(content_length as usize);
        reader.read_to_end(&mut data).await?;
        retry(|| async {
            let data = Cursor::new(data.clone());
            let req = Object::insert_multipart(
                &bucket,
                data,
                content_length,
                &metadata,
                Some(InsertObjectOptional {
                    predefined_acl: self.config.predefined_acl,
                    ..Default::default()
                }),
            )
            .map_err(RequestError::Gcs)?
            .map(|reader| Body::wrap_stream(AsyncReadAsSyncStreamOfBytes::new(reader)));
            self.make_request(req, tame_gcs::Scopes::ReadWrite).await
        })
        .await?;
        Ok::<_, io::Error>(())
    }

    fn get(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let bucket = self.config.bucket.bucket.to_string();
        let name = self.maybe_prefix_key(name);
        debug!("read file from GCS storage"; "key" => %name);
        let oid = match ObjectId::new(bucket, name) {
            Ok(oid) => oid,
            Err(e) => return GCSStorage::error_to_async_read(io::ErrorKind::InvalidInput, e),
        };
        let request = match Object::download(&oid, None /*optional*/) {
            Ok(request) => request.map(|_: io::Empty| Body::empty()),
            Err(e) => return GCSStorage::error_to_async_read(io::ErrorKind::Other, e),
        };
        Box::new(
            self.make_request(request, tame_gcs::Scopes::ReadOnly)
                .and_then(|response| async {
                    if response.status().is_success() {
                        Ok(response.into_body().map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("download from GCS error: {}", e),
                            )
                        }))
                    } else {
                        Err(status_code_error(
                            response.status(),
                            "bucket read".to_string(),
                        ))
                    }
                })
                .err_into::<io::Error>()
                .try_flatten_stream()
                .boxed() // this `.boxed()` pin the stream.
                .into_async_read(),
        )
    }
}

#[cfg(test)]
mod tests {
    use matches::assert_matches;

    use super::*;
    const HARDCODED_ENDPOINTS: &[&str] = &[
        "https://www.googleapis.com/upload/storage/v1",
        "https://www.googleapis.com/storage/v1",
    ];

    #[test]
    fn test_change_host() {
        let host = StringNonEmpty::static_str("http://localhost:4443");
        assert_eq!(
            &change_host(&host, &format!("{}/storage/v1/foo", GOOGLE_APIS)).unwrap(),
            "http://localhost:4443/storage/v1/foo"
        );

        let h1 = url::Url::parse(HARDCODED_ENDPOINTS[0]).unwrap();
        let h2 = url::Url::parse(HARDCODED_ENDPOINTS[1]).unwrap();

        let endpoint = StringNonEmpty::static_str("http://example.com");
        assert_eq!(
            &change_host(&endpoint, h1.as_str()).unwrap(),
            "http://example.com/upload/storage/v1"
        );
        assert_eq!(
            &change_host(&endpoint, h2.as_str()).unwrap(),
            "http://example.com/storage/v1"
        );
        assert_eq!(
            &change_host(&endpoint, &format!("{}/foo", h2)).unwrap(),
            "http://example.com/storage/v1/foo"
        );
        assert_matches!(&change_host(&endpoint, "foo"), None);

        // if we get the endpoint with suffix "/storage/v1/"
        let endpoint = StringNonEmpty::static_str("http://example.com/storage/v1/");
        assert_eq!(
            &change_host(&endpoint, &format!("{}/foo", h2)).unwrap(),
            "http://example.com/storage/v1/foo"
        );

        let endpoint = StringNonEmpty::static_str("http://example.com/upload/storage/v1/");
        assert_eq!(
            &change_host(&endpoint, &format!("{}/foo", h2)).unwrap(),
            "http://example.com/storage/v1/foo"
        );
    }

    #[test]
    fn test_parse_storage_class() {
        assert_matches!(
            parse_storage_class("STANDARD"),
            Ok(Some(StorageClass::Standard))
        );
        assert_matches!(parse_storage_class(""), Ok(None));
        assert_matches!(
            parse_storage_class("NOT_A_STORAGE_CLASS"),
            Err("NOT_A_STORAGE_CLASS")
        );
    }

    #[test]
    fn test_parse_acl() {
        // can't use assert_matches!(), PredefinedAcl doesn't even implement Debug.
        assert!(matches!(
            parse_predefined_acl("private"),
            Ok(Some(PredefinedAcl::Private))
        ));
        assert!(matches!(parse_predefined_acl(""), Ok(None)));
        assert!(matches!(parse_predefined_acl("notAnACL"), Err("notAnACL")));
    }

    #[test]
    fn test_url_of_backend() {
        let bucket_name = StringNonEmpty::static_str("bucket");
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = Some(StringNonEmpty::static_str("/backup 02/prefix/"));
        let gcs = Config::default(bucket.clone());
        // only 'bucket' and 'prefix' should be visible in url_of_backend()
        assert_eq!(
            gcs.url().unwrap().to_string(),
            "gcs://bucket/backup%2002/prefix/"
        );
        bucket.endpoint = Some(StringNonEmpty::static_str("http://endpoint.com"));
        assert_eq!(
            &Config::default(bucket).url().unwrap().to_string(),
            "http://endpoint.com/bucket/backup%2002/prefix/"
        );
    }

    #[test]
    fn test_config_round_trip() {
        let mut input = InputConfig::default();
        input.set_bucket("bucket".to_owned());
        input.set_prefix("backup 02/prefix/".to_owned());
        let c1 = Config::from_input(input.clone()).unwrap();
        let c2 = Config::from_cloud_dynamic(&cloud_dynamic_from_input(input)).unwrap();
        assert_eq!(c1.bucket.bucket, c2.bucket.bucket);
        assert_eq!(c1.bucket.prefix, c2.bucket.prefix);
    }

    fn cloud_dynamic_from_input(mut gcs: InputConfig) -> CloudDynamic {
        let mut bucket = InputBucket::default();
        if !gcs.endpoint.is_empty() {
            bucket.endpoint = gcs.take_endpoint();
        }
        if !gcs.prefix.is_empty() {
            bucket.prefix = gcs.take_prefix();
        }
        if !gcs.storage_class.is_empty() {
            bucket.storage_class = gcs.take_storage_class();
        }
        if !gcs.bucket.is_empty() {
            bucket.bucket = gcs.take_bucket();
        }
        let mut attrs = std::collections::HashMap::new();
        if !gcs.predefined_acl.is_empty() {
            attrs.insert("predefined_acl".to_owned(), gcs.take_predefined_acl());
        }
        if !gcs.credentials_blob.is_empty() {
            attrs.insert("credentials_blob".to_owned(), gcs.take_credentials_blob());
        }
        let mut cd = CloudDynamic::default();
        cd.set_provider_name("gcp".to_owned());
        cd.set_attrs(attrs);
        cd.set_bucket(bucket);
        cd
    }
}
