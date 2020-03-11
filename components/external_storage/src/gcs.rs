// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::ExternalStorage;

use bytes::Bytes;
use futures::executor::block_on;
use futures::io::{empty, AsyncRead};
use futures01::stream::Stream;
use futures_util::compat::Stream01CompatExt;
use futures_util::io::{AsyncReadExt, Cursor};
use http::Method;
use kvproto::backup::Gcs as Config;
use reqwest::{Body, Client};
use std::convert::TryInto;
use std::io::{Error, ErrorKind, Read, Result};
use std::sync::Arc;
use tame_gcs::common::{PredefinedAcl, StorageClass};
use tame_gcs::objects::{InsertObjectOptional, Metadata, Object};
use tame_gcs::types::BucketName;
use tame_oauth::gcp::{ServiceAccountAccess, ServiceAccountInfo, TokenOrRequest};
use tokio::codec::{BytesCodec, FramedRead};

// GCS compatible storage
#[derive(Clone)]
pub struct GCSStorage {
    config: Config,
    svc_access: Arc<ServiceAccountAccess>,
    client: Client,
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
        let svc_info = ServiceAccountInfo::deserialize(&config.credentials_blob).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "invalid credentials_blob {}: {}",
                    config.credentials_blob, e
                ),
            )
        })?;
        let svc_access = ServiceAccountAccess::new(svc_info).map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "invalid credentials_blob {}: {}",
                    config.credentials_blob, e
                ),
            )
        })?;
        let client = Client::builder().build().map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("unable to create reqwest client: {}", e),
            )
        })?;
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

    fn convert_request<R>(&self, mut req: http::Request<R>) -> Result<reqwest::Request>
    where
        R: AsyncRead + Send + Unpin,
    {
        let uri = req.uri().to_string();
        let builder = match req.method().clone() {
            Method::GET => self.client.get(&uri),
            Method::POST => self.client.post(&uri),
            method => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("method unimplemented: {}", method.as_str()),
                ))
            }
        };
        Ok(builder
            .headers(req.headers().clone())
            .body(Body::wrap_stream(
                FramedRead::new(req.body().compat(), BytesCodec::new())
                    .map(|bytes| bytes.freeze())
                    .compat(),
            ))
            .build()
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to build request: {}", e)))?)
    }

    fn convert_response(&self, mut res: reqwest::Response) -> Result<http::Response<Bytes>> {
        let mut builder = http::Response::builder();
        builder.status(res.status()).version(res.version());
        let headers = builder.headers_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                format!("failed to build response header."),
            )
        })?;
        headers.extend(
            res.headers()
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        // Use blocking IO, since conver_response is only used to read access token.
        let content = block_on(res.bytes())
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to read response: {}", e)))?;
        builder.body(content).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("failed to build response body: {}", e),
            )
        })
    }

    fn set_auth<R>(&self, req: &mut http::Request<R>, scope: tame_gcs::Scopes) -> Result<()>
    where
        R: AsyncRead + Send + Unpin,
    {
        let token_or_request = self
            .svc_access
            .get_token(&[scope])
            .map_err(|e| Error::new(ErrorKind::Other, format!("failed to get token: {}", e)))?;
        let token = match token_or_request {
            TokenOrRequest::Token(token) => token,
            TokenOrRequest::Request {
                request,
                scope_hash,
                ..
            } => {
                // Convert http::Request<Vec<u8>> into http::Request<dyn AsyncRead>
                let (parts, body) = request.into_parts();
                let read_body = Cursor::new(body);
                let new_request = http::Request::from_parts(parts, read_body);
                let req = self.convert_request(new_request)?;
                // Use blocking IO.
                let res = block_on(self.client.execute(req)).map_err(|e| {
                    Error::new(ErrorKind::Other, format!("request token failed: {}", e))
                })?;
                let response = self.convert_response(res)?;

                self.svc_access
                    .parse_token_response(scope_hash, response)
                    .map_err(|e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("failed to parse token response: {}", e),
                        )
                    })?
            }
        };
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            token.try_into().map_err(|e| {
                Error::new(ErrorKind::Other, format!("failed to set auth token: {}", e))
            })?,
        );

        Ok(())
    }

    fn make_request<R>(
        &self,
        mut req: http::Request<R>,
        scope: tame_gcs::Scopes,
    ) -> Result<reqwest::Response>
    where
        R: AsyncRead + Send + Unpin,
    {
        self.set_auth(&mut req, scope)?;
        let request = self.convert_request(req)?;
        let mut response = block_on(self.client.execute(request))
            .map_err(|e| Error::new(ErrorKind::Other, format!("make request fail: {}", e)))?;
        if !response.status().is_success() {
            let text = block_on(response.text()).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!(
                        "request failed and failed to read error message, status: {}, error: {}",
                        response.status(),
                        e
                    ),
                )
            })?;
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "request failed. status: {}, text: {}",
                    response.status(),
                    text
                ),
            ));
        }

        Ok(response)
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
                serde_json::from_str(&self.config.storage_class).map_err(|e| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        format!("invalid storage_class {}: {}", self.config.storage_class, e),
                    )
                })?,
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
            .map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("failed to create insert request: {}", e),
                )
            })?;
        self.make_request(req, tame_gcs::Scopes::ReadWrite)?;

        Ok(())
    }

    fn read(&self, name: &str) -> Result<Box<dyn AsyncRead + Unpin>> {
        // let oid = ObjectId {
        //     bucket: self.config.bucket.clone(),
        //     name: self.maybe_prefix_key(name),
        // };
        // debug!("read file from GCS storage"; "key" => %oid.name);
        // let req = Object::download(oid, None /*optional*/)?;
        // let res = self.make_request(req, tame_gcs::Scopes::Read)?;
        //
        // Box::new(res)

        Ok(Box::new(empty()))
    }
}
