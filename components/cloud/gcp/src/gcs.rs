// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::{convert::TryInto, fmt::Display, io, sync::Arc};

use cloud::blob::{none_to_empty, BlobStorage, BucketConf, StringNonEmpty};
use futures_util::{
    future::TryFutureExt,
    io::{AsyncRead, AsyncReadExt, Cursor},
    stream::{StreamExt, TryStreamExt},
};
use hyper::{client::HttpConnector, Body, Client, Request, Response, StatusCode};
use hyper_tls::HttpsConnector;
pub use kvproto::backup::{Bucket as InputBucket, CloudDynamic, Gcs as InputConfig};
use tame_gcs::{
    common::{PredefinedAcl, StorageClass},
    objects::{InsertObjectOptional, Metadata, Object},
    types::{BucketName, ObjectId},
};
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};
use tikv_util::stream::{
    block_on_external_io, error_stream, retry, AsyncReadAsSyncStreamOfBytes, RetryError,
};

const HARDCODED_ENDPOINTS: &[&str] = &[
    "https://www.googleapis.com/upload/storage/v1",
    "https://www.googleapis.com/storage/v1",
];

#[derive(Clone, Debug)]
pub struct Config {
    bucket: BucketConf,
    predefined_acl: Option<PredefinedAcl>,
    storage_class: Option<StorageClass>,
    svc_info: ServiceAccountInfo,
}

impl Config {
    #[cfg(test)]
    pub fn default(bucket: BucketConf, svc_info: ServiceAccountInfo) -> Self {
        Self {
            bucket,
            predefined_acl: None,
            storage_class: None,
            svc_info,
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

        let credentials_blob = attrs
            .get("credentials_blob")
            .ok_or_else(Self::missing_credentials)?;
        let svc_info = ServiceAccountInfo::deserialize(credentials_blob)
            .or_invalid_input("invalid credentials_blob")?;

        Ok(Config {
            bucket,
            predefined_acl,
            svc_info,
            storage_class,
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let b = input
            .bucket_info
            .into_option()
            .unwrap_or_else(InputBucket::default);
        let bucket = BucketConf {
            bucket: StringNonEmpty::required_field2(b.bucket, input.bucket, "bucket")?,
            endpoint: StringNonEmpty::opt2(b.endpoint, input.endpoint),
            prefix: StringNonEmpty::opt2(b.prefix, input.prefix),
            storage_class: StringNonEmpty::opt2(b.storage_class, input.storage_class),
            region: None,
        };
        let predefined_acl = parse_predefined_acl(&input.predefined_acl)
            .or_invalid_input("invalid predefined_acl")?;
        let storage_class = parse_storage_class(&none_to_empty(bucket.storage_class.clone()))
            .or_invalid_input("invalid storage_class")?;
        let credentials_blob =
            StringNonEmpty::required_field(input.credentials_blob, "credentials_blob")?;
        let svc_info = ServiceAccountInfo::deserialize(credentials_blob.to_string())
            .or_invalid_input("invalid credentials_blob")?;
        Ok(Config {
            bucket,
            predefined_acl,
            svc_info,
            storage_class,
        })
    }
}

// GCS compatible storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Arc<ServiceAccountAccess>,
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
    Hyper(hyper::Error),
    OAuth(tame_oauth::Error),
    Gcs(tame_gcs::Error),
    InvalidEndpoint(http::uri::InvalidUri),
}

impl From<hyper::Error> for RequestError {
    fn from(err: hyper::Error) -> Self {
        Self::Hyper(err)
    }
}

impl From<tame_oauth::Error> for RequestError {
    fn from(err: tame_oauth::Error) -> Self {
        Self::OAuth(err)
    }
}

impl From<http::uri::InvalidUri> for RequestError {
    fn from(err: http::uri::InvalidUri) -> Self {
        Self::InvalidEndpoint(err)
    }
}

impl From<tame_gcs::Error> for RequestError {
    fn from(err: tame_gcs::Error) -> Self {
        Self::Gcs(err)
    }
}

impl From<StatusCode> for RequestError {
    fn from(code: StatusCode) -> Self {
        Self::OAuth(tame_oauth::Error::HttpStatus(code))
    }
}

impl From<RequestError> for io::Error {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::Hyper(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid HTTP request: {}", e),
            ),
            RequestError::OAuth(tame_oauth::Error::Io(e)) => e,
            RequestError::OAuth(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("authorization failed: {}", e),
            ),
            RequestError::Gcs(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS request: {}", e),
            ),
            RequestError::InvalidEndpoint(e) => Self::new(
                io::ErrorKind::InvalidInput,
                format!("invalid GCS endpoint: {}", e),
            ),
        }
    }
}

impl RetryError for RequestError {
    fn is_retryable(&self) -> bool {
        match self {
            // FIXME: Inspect the error source?
            Self::Hyper(e) => {
                e.is_closed()
                    || e.is_connect()
                    || e.is_incomplete_message()
                    || e.is_body_write_aborted()
            }
            // See https://cloud.google.com/storage/docs/exponential-backoff.
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::TOO_MANY_REQUESTS)) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(StatusCode::REQUEST_TIMEOUT)) => true,
            Self::OAuth(tame_oauth::Error::HttpStatus(status)) => status.is_server_error(),
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
        let svc_access = ServiceAccountAccess::new(config.svc_info.clone())
            .or_invalid_input("invalid credentials_blob")?;
        let client = Client::builder().build(HttpsConnector::new());
        Ok(GCSStorage {
            config,
            svc_access: Arc::new(svc_access),
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
    ) -> Result<(), RequestError> {
        let token_or_request = self.svc_access.get_token(&[scope])?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                let res = self.client.request(request.map(From::from)).await?;
                if !res.status().is_success() {
                    return Err(res.status().into());
                }
                let (parts, body) = res.into_parts();
                let body = hyper::body::to_bytes(body).await?;
                self.svc_access
                    .parse_token_response(scope_hash, Response::from_parts(parts, body))?
            }
        };
        req.headers_mut()
            .insert(http::header::AUTHORIZATION, token.try_into()?);

        Ok(())
    }

    async fn make_request(
        &self,
        mut req: Request<Body>,
        scope: tame_gcs::Scopes,
    ) -> Result<Response<Body>, RequestError> {
        // replace the hard-coded GCS endpoint by the custom one.

        if let Some(endpoint) = &self.config.bucket.endpoint {
            let url = req.uri().to_string();
            for hardcoded in HARDCODED_ENDPOINTS {
                if let Some(res) = url.strip_prefix(hardcoded) {
                    *req.uri_mut() = [endpoint.trim_end_matches('/'), res].concat().parse()?;
                    break;
                }
            }
        }

        self.set_auth(&mut req, scope).await?;
        let res = self.client.request(req).await?;
        if !res.status().is_success() {
            return Err(res.status().into());
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

fn url_for(config: &Config) -> url::Url {
    config.bucket.url("gcs://")
}

const STORAGE_NAME: &str = "gcs";

impl BlobStorage for GCSStorage {
    fn name(&self) -> &'static str {
        &STORAGE_NAME
    }

    fn url(&self) -> url::Url {
        url_for(&self.config)
    }

    fn write(
        &self,
        name: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
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

        block_on_external_io(async move {
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
                )?
                .map(|reader| Body::wrap_stream(AsyncReadAsSyncStreamOfBytes::new(reader)));
                self.make_request(req, tame_gcs::Scopes::ReadWrite).await
            })
            .await?;
            Ok::<_, io::Error>(())
        })?;
        Ok(())
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
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
                        Err(RequestError::from(response.status()))
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
    use super::*;
    use matches::assert_matches;

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
        let bucket_name = StringNonEmpty::required("bucket".to_owned()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = StringNonEmpty::opt("/backup 02/prefix/".to_owned());
        bucket.endpoint = StringNonEmpty::opt("http://endpoint.com".to_owned());
        let svc_info = ServiceAccountInfo::deserialize(
            "{\"private_key\":\"KEY\",\"client_email\":\"service@example.com\",\"token_uri\":\"https://example.com\"}",
        ).unwrap();
        let gcs = Config::default(bucket, svc_info);
        // only 'bucket' and 'prefix' should be visible in url_of_backend()
        assert_eq!(
            url_for(&gcs).to_string(),
            "gcs://bucket/backup%2002/prefix/"
        );
    }
}
