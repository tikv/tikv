// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{
    util::{block_on_external_io, error_stream, AsyncReadAsSyncStreamOfBytes},
    ExternalStorage,
};

use std::{
    convert::TryInto,
    fmt::Display,
    io::{Error, ErrorKind, Read, Result},
    sync::Arc,
};

use bytes::Bytes;
use futures_util::{
    future::{FutureExt, TryFutureExt},
    io::AsyncRead,
    stream::TryStreamExt,
};
use kvproto::backup::Gcs as Config;
use reqwest::{Body, Client};
use tame_gcs::{
    common::{PredefinedAcl, StorageClass},
    objects::{InsertObjectOptional, Metadata, Object},
    types::{BucketName, ObjectId},
};
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};

const HARDCODED_ENDPOINTS: &[&str] = &[
    "https://www.googleapis.com/upload/storage/v1",
    "https://www.googleapis.com/storage/v1",
];

// GCS compatible storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Arc<ServiceAccountAccess>,
    client: Client,
}

trait ResultExt {
    type Ok;

    // Maps the error of this result as an `std::io::Error` with `Other` error
    // kind.
    fn or_io_error<D: Display>(self, msg: D) -> Result<Self::Ok>;

    // Maps the error of this result as an `std::io::Error` with `InvalidInput`
    // error kind.
    fn or_invalid_input<D: Display>(self, msg: D) -> Result<Self::Ok>;
}

impl<T, E: Display> ResultExt for std::result::Result<T, E> {
    type Ok = T;
    fn or_io_error<D: Display>(self, msg: D) -> Result<T> {
        self.map_err(|e| Error::new(ErrorKind::Other, format!("{}: {}", msg, e)))
    }
    fn or_invalid_input<D: Display>(self, msg: D) -> Result<T> {
        self.map_err(|e| Error::new(ErrorKind::InvalidInput, format!("{}: {}", msg, e)))
    }
}

impl GCSStorage {
    /// Create a new GCS storage for the given config.
    pub fn new(config: &Config) -> Result<GCSStorage> {
        if config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing bucket name"));
        }
        if config.credentials_blob.is_empty() {
            return Err(Error::new(ErrorKind::InvalidInput, "missing credentials"));
        }
        let svc_info = ServiceAccountInfo::deserialize(&config.credentials_blob)
            .or_invalid_input("invalid credentials_blob")?;
        let svc_access =
            ServiceAccountAccess::new(svc_info).or_invalid_input("invalid credentials_blob")?;
        let client = Client::builder()
            .build()
            .or_io_error("unable to create reqwest client")?;
        Ok(GCSStorage {
            config: config.clone(),
            svc_access: Arc::new(svc_access),
            client,
        })
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if !self.config.prefix.is_empty() {
            return format!("{}/{}", self.config.prefix, key);
        }
        key.to_owned()
    }

    fn convert_request<R: 'static>(&self, req: http::Request<R>) -> Result<reqwest::Request>
    where
        R: AsyncRead + Send + Unpin,
    {
        let uri = req.uri().to_string();
        self.client
            .request(req.method().clone(), &uri)
            .headers(req.headers().clone())
            .body(Body::wrap_stream(AsyncReadAsSyncStreamOfBytes::new(
                req.into_body(),
            )))
            .build()
            .or_io_error("failed to build request")
    }

    async fn convert_response(
        &self,
        tag: &str,
        res: reqwest::Response,
    ) -> Result<http::Response<Bytes>> {
        let mut builder = http::Response::builder()
            .status(res.status())
            .version(res.version());
        for (key, value) in res.headers().iter() {
            builder = builder.header(key, value);
        }
        // convert_response is only used to read access token.
        let content = res
            .bytes()
            .await
            .or_io_error(format_args!("failed to read {} response", tag))?;
        builder
            .body(content)
            .or_io_error(format_args!("failed to build {} response body", tag))
    }

    async fn set_auth(&self, req: &mut reqwest::Request, scope: tame_gcs::Scopes) -> Result<()> {
        let token_or_request = self
            .svc_access
            .get_token(&[scope])
            .or_io_error("failed to get token")?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                let res = self
                    .client
                    .execute(request.into())
                    .await
                    .or_io_error("request GCS access token failed")?;
                let response = self.convert_response("GCS access token", res).await?;

                self.svc_access
                    .parse_token_response(scope_hash, response)
                    .or_io_error("failed to parse GCS token response")?
            }
        };
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            token
                .try_into()
                .or_io_error("failed to set GCS auth token")?,
        );

        Ok(())
    }

    async fn make_request(
        &self,
        mut req: reqwest::Request,
        scope: tame_gcs::Scopes,
    ) -> Result<reqwest::Response> {
        // replace the hard-coded GCS endpoint by the custom one.
        let endpoint = self.config.get_endpoint();
        if !endpoint.is_empty() {
            let url = req.url().as_str();
            for hardcoded in HARDCODED_ENDPOINTS {
                if url.starts_with(hardcoded) {
                    *req.url_mut() = reqwest::Url::parse(
                        &[endpoint.trim_end_matches('/'), &url[hardcoded.len()..]].concat(),
                    )
                    .or_invalid_input("invalid custom GCS endpoint")?;
                    break;
                }
            }
        }

        self.set_auth(&mut req, scope).await?;
        let response = self
            .client
            .execute(req)
            .await
            .or_io_error("make GCS request failed")?;
        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.or_io_error(format_args!(
                "GCS request failed and failed to read error message, status: {}, error",
                status
            ))?;
            return Err(Error::new(
                ErrorKind::Other,
                format!("request failed. status: {}, text: {}", status, text),
            ));
        }

        Ok(response)
    }

    fn error_to_async_read<E>(kind: ErrorKind, e: E) -> Box<dyn AsyncRead + Unpin>
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Box::new(error_stream(Error::new(kind, e)).into_async_read())
    }
}

impl ExternalStorage for GCSStorage {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> Result<()> {
        use std::convert::TryFrom;

        let key = self.maybe_prefix_key(name);
        debug!("save file to GCS storage"; "key" => %key);
        let bucket = BucketName::try_from(self.config.bucket.clone()).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid bucket {}: {}", self.config.bucket, e),
            )
        })?;
        let storage_class: Option<StorageClass> = if self.config.storage_class.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&self.config.storage_class).or_invalid_input(format_args!(
                    "invalid storage_class {}",
                    self.config.storage_class
                ))?,
            )
        };
        // Convert manually since PredefinedAcl doesn't implement Deserialize.
        let predefined_acl = match self.config.predefined_acl.as_ref() {
            "" => None,
            "authenticatedRead" => Some(PredefinedAcl::AuthenticatedRead),
            "bucketOwnerFullControl" => Some(PredefinedAcl::BucketOwnerFullControl),
            "bucketOwnerRead" => Some(PredefinedAcl::BucketOwnerRead),
            "private" => Some(PredefinedAcl::Private),
            "projectPrivate" => Some(PredefinedAcl::ProjectPrivate),
            "publicRead" => Some(PredefinedAcl::PublicRead),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!("invalid predefined_acl {}", self.config.predefined_acl),
                ));
            }
        };
        let metadata = Metadata {
            name: Some(key),
            storage_class,
            ..Default::default()
        };
        let optional = Some(InsertObjectOptional {
            predefined_acl,
            ..Default::default()
        });
        let req = Object::insert_multipart(&bucket, reader, content_length, &metadata, optional)
            .or_io_error("failed to create GCS insert request")?;
        block_on_external_io(
            self.make_request(self.convert_request(req)?, tame_gcs::Scopes::ReadWrite),
        )?;
        Ok(())
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let bucket = self.config.bucket.clone();
        let name = self.maybe_prefix_key(name);
        debug!("read file from GCS storage"; "key" => %name);
        let oid = match ObjectId::new(bucket, name) {
            Ok(oid) => oid,
            Err(e) => return GCSStorage::error_to_async_read(ErrorKind::InvalidInput, e),
        };
        let request = match Object::download(&oid, None /*optional*/) {
            Ok(request) => request,
            Err(e) => return GCSStorage::error_to_async_read(ErrorKind::Other, e),
        };
        // The body is actually an std::io::Empty. The use of read_to_end is only to convert it
        // into something convenient to convert into reqwest::Body.
        let (parts, mut body) = request.into_parts();
        let mut body_content = vec![];
        if let Err(e) = body.read_to_end(&mut body_content) {
            return GCSStorage::error_to_async_read(ErrorKind::Other, e);
        }

        Box::new(
            self.make_request(
                http::Request::from_parts(parts, body_content).into(),
                tame_gcs::Scopes::ReadOnly,
            )
            .boxed() // this `.boxed()` pin the future.
            .map_ok(|response| {
                response.bytes_stream().map_err(|e| {
                    Error::new(ErrorKind::Other, format!("download from gcs error {}", e))
                })
            })
            .try_flatten_stream()
            .into_async_read(),
        )
    }
}
